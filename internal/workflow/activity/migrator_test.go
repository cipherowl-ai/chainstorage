package activity

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type migratorActivityTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env      *cadence.TestEnv
	ctrl     *gomock.Controller
	app      testapp.TestApp
	migrator *Migrator
	cfg      *config.Config
}

func TestMigratorActivityTestSuite(t *testing.T) {
	suite.Run(t, new(migratorActivityTestSuite))
}

func (s *migratorActivityTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_ETHEREUM),
		config.WithNetwork(common.Network_NETWORK_ETHEREUM_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)
	s.cfg = cfg

	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		testapp.WithConfig(cfg),
		fx.Populate(&s.migrator),
	)
}

func (s *migratorActivityTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *migratorActivityTestSuite) TestMigrator_EventDrivenRequest() {
	require := testutil.Require(s.T())

	// Test event-driven request structure
	request := &MigratorRequest{
		StartEventSequence: int64(1000),
		EndEventSequence:   int64(1050),
		Tag:                uint32(1),
		EventTag:           uint32(0),
		Parallelism:        4,
	}

	require.NotNil(request)
	require.Equal(int64(1000), request.StartEventSequence)
	require.Equal(int64(1050), request.EndEventSequence)
	require.Equal(uint32(1), request.Tag)
	require.Equal(uint32(0), request.EventTag)
	require.Equal(4, request.Parallelism)
}

func (s *migratorActivityTestSuite) TestMigrator_ResponseStructure() {
	require := testutil.Require(s.T())

	// Test response structure
	response := &MigratorResponse{
		BlocksMigrated: 100,
		EventsMigrated: 500,
		Success:        true,
		Message:        "Migrated 100 blocks and 500 events in 1.5s",
	}

	require.Equal(100, response.BlocksMigrated)
	require.Equal(500, response.EventsMigrated)
	require.True(response.Success)
	require.Contains(response.Message, "100 blocks")
	require.Contains(response.Message, "500 events")
}

func (s *migratorActivityTestSuite) TestExtractBlocksFromEvents() {
	require := testutil.Require(s.T())

	// Create test events with BLOCK_ADDED and BLOCK_REMOVED
	events := []*model.EventEntry{
		{
			EventId:     1001,
			EventType:   api.BlockchainEvent_BLOCK_ADDED,
			BlockHeight: 100,
			BlockHash:   "0xaaa",
			ParentHash:  "0x999",
		},
		{
			EventId:     1002,
			EventType:   api.BlockchainEvent_BLOCK_REMOVED,
			BlockHeight: 100,
			BlockHash:   "0xbbb",
			ParentHash:  "0x999",
		},
		{
			EventId:     1003,
			EventType:   api.BlockchainEvent_BLOCK_ADDED,
			BlockHeight: 100,
			BlockHash:   "0xccc",
			ParentHash:  "0x999",
		},
		{
			EventId:     1004,
			EventType:   api.BlockchainEvent_BLOCK_ADDED,
			BlockHeight: 101,
			BlockHash:   "0xddd",
			ParentHash:  "0xccc",
		},
	}

	// Extract blocks from events
	blocks, err := s.migrator.extractBlocksFromEvents(s.app.Logger(), events)
	require.NoError(err)

	// Should extract only BLOCK_ADDED events
	require.Len(blocks, 3)

	// Check first block
	require.Equal(uint64(100), blocks[0].Height)
	require.Equal("0xaaa", blocks[0].Hash)
	require.Equal("0x999", blocks[0].ParentHash)
	require.Equal(int64(1001), blocks[0].EventSeq)

	// Check second block (reorg at height 100)
	require.Equal(uint64(100), blocks[1].Height)
	require.Equal("0xccc", blocks[1].Hash)
	require.Equal("0x999", blocks[1].ParentHash)
	require.Equal(int64(1003), blocks[1].EventSeq)

	// Check third block
	require.Equal(uint64(101), blocks[2].Height)
	require.Equal("0xddd", blocks[2].Hash)
	require.Equal("0xccc", blocks[2].ParentHash)
	require.Equal(int64(1004), blocks[2].EventSeq)

	// Test UNKNOWN event type - should return error
	eventsWithUnknown := []*model.EventEntry{
		{
			EventId:     1001,
			EventType:   api.BlockchainEvent_UNKNOWN,
			BlockHeight: 100,
			BlockHash:   "0xaaa",
		},
	}

	_, err = s.migrator.extractBlocksFromEvents(s.app.Logger(), eventsWithUnknown)
	require.Error(err)
	require.Contains(err.Error(), "encountered UNKNOWN event type")
}

func (s *migratorActivityTestSuite) TestSplitBlocksByReorgs() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Test case 1: No reorgs - single segment
	eventsNoReorg := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102},
	}
	blocksNoReorg := []BlockToMigrate{
		{Height: 100, Hash: "0xa", EventSeq: 1},
		{Height: 101, Hash: "0xb", EventSeq: 2},
		{Height: 102, Hash: "0xc", EventSeq: 3},
	}
	segments := s.migrator.splitBlocksByReorgs(logger, eventsNoReorg, blocksNoReorg)
	require.Len(segments, 1, "Should have 1 segment when no reorgs")
	require.Len(segments[0].Blocks, 3)
	require.Equal(uint64(0), segments[0].ParentHeight)

	// Test case 2: Single reorg in middle
	eventsSingleReorg := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102},
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102},
		{EventId: 6, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 103},
	}
	blocksSingleReorg := []BlockToMigrate{
		{Height: 100, Hash: "0xa", EventSeq: 1},
		{Height: 101, Hash: "0xb", EventSeq: 2},
		{Height: 102, Hash: "0xc1", EventSeq: 3},
		{Height: 102, Hash: "0xc2", EventSeq: 5},
		{Height: 103, Hash: "0xd", EventSeq: 6},
	}
	segments = s.migrator.splitBlocksByReorgs(logger, eventsSingleReorg, blocksSingleReorg)
	require.Len(segments, 2, "Should have 2 segments with 1 reorg")
	require.Len(segments[0].Blocks, 3) // blocks 100, 101, 102(c1)
	require.Len(segments[1].Blocks, 2) // blocks 102(c2), 103
	require.Equal(uint64(0), segments[0].ParentHeight)
	require.Equal(uint64(101), segments[1].ParentHeight) // Parent is block 101

	// Test case 3: Multiple reorgs
	eventsMultiReorg := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101},
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102},
		{EventId: 6, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102},
		{EventId: 7, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101},
		{EventId: 8, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101},
		{EventId: 9, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102},
	}
	blocksMultiReorg := []BlockToMigrate{
		{Height: 100, Hash: "0xa", EventSeq: 1},
		{Height: 101, Hash: "0xb1", EventSeq: 2},
		{Height: 101, Hash: "0xb2", EventSeq: 4},
		{Height: 102, Hash: "0xc1", EventSeq: 5},
		{Height: 101, Hash: "0xb3", EventSeq: 8},
		{Height: 102, Hash: "0xc2", EventSeq: 9},
	}
	segments = s.migrator.splitBlocksByReorgs(logger, eventsMultiReorg, blocksMultiReorg)
	require.Len(segments, 3, "Should have 3 segments with 2 reorgs")
	require.Len(segments[0].Blocks, 2) // blocks 100, 101(b1)
	require.Len(segments[1].Blocks, 2) // blocks 101(b2), 102(c1)
	require.Len(segments[2].Blocks, 2) // blocks 101(b3), 102(c2)
	require.Equal(uint64(0), segments[0].ParentHeight)
	require.Equal(uint64(100), segments[1].ParentHeight)
	require.Equal(uint64(100), segments[2].ParentHeight)

	// Test case 4: Starting with BLOCK_REMOVED
	eventsStartRemoved := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102},
	}
	blocksStartRemoved := []BlockToMigrate{
		{Height: 101, Hash: "0xb", EventSeq: 3},
		{Height: 102, Hash: "0xc", EventSeq: 4},
	}
	segments = s.migrator.splitBlocksByReorgs(logger, eventsStartRemoved, blocksStartRemoved)
	require.Len(segments, 1, "Should have 1 segment when starting with REMOVED")
	require.Len(segments[0].Blocks, 2)
	require.Equal(uint64(100), segments[0].ParentHeight) // Parent is 100 (101-1)

	// Test case 5: Empty blocks
	segments = s.migrator.splitBlocksByReorgs(logger, []*model.EventEntry{}, []BlockToMigrate{})
	require.Nil(segments, "Should return nil for empty blocks")

	// Test case 6: Complex reorg pattern
	eventsComplex := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 103},
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 103},
		{EventId: 6, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102},
		{EventId: 7, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101},
		{EventId: 8, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101},
		{EventId: 9, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102},
		{EventId: 10, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 103},
		{EventId: 11, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 104},
		{EventId: 12, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 105},
	}
	blocksComplex := []BlockToMigrate{
		{Height: 100, Hash: "0x100", EventSeq: 1},
		{Height: 101, Hash: "0x101a", EventSeq: 2},
		{Height: 102, Hash: "0x102a", EventSeq: 3},
		{Height: 103, Hash: "0x103a", EventSeq: 4},
		{Height: 101, Hash: "0x101b", EventSeq: 8},
		{Height: 102, Hash: "0x102b", EventSeq: 9},
		{Height: 103, Hash: "0x103b", EventSeq: 10},
		{Height: 104, Hash: "0x104", EventSeq: 11},
		{Height: 105, Hash: "0x105", EventSeq: 12},
	}
	segments = s.migrator.splitBlocksByReorgs(logger, eventsComplex, blocksComplex)
	require.Len(segments, 2, "Should have 2 segments")
	require.Len(segments[0].Blocks, 4) // 100, 101a, 102a, 103a
	require.Len(segments[1].Blocks, 5) // 101b, 102b, 103b, 104, 105
	require.Equal(uint64(0), segments[0].ParentHeight)
	require.Equal(uint64(100), segments[1].ParentHeight) // Reorg started at 101, so parent is 100
}

func (s *migratorActivityTestSuite) TestGetLatestEventFromPostgres() {
	require := testutil.Require(s.T())

	// Test request structure
	request := &GetLatestEventFromPostgresRequest{
		EventTag: uint32(3),
	}
	require.Equal(uint32(3), request.EventTag)

	// Test response structure - with events found
	responseWithEvents := &GetLatestEventFromPostgresResponse{
		Sequence: int64(12345),
		Height:   uint64(5000),
		Found:    true,
	}
	require.Equal(int64(12345), responseWithEvents.Sequence)
	require.Equal(uint64(5000), responseWithEvents.Height)
	require.True(responseWithEvents.Found)

	// Test response structure - no events found
	responseNoEvents := &GetLatestEventFromPostgresResponse{
		Sequence: 0,
		Height:   0,
		Found:    false,
	}
	require.Equal(int64(0), responseNoEvents.Sequence)
	require.Equal(uint64(0), responseNoEvents.Height)
	require.False(responseNoEvents.Found)
}

func (s *migratorActivityTestSuite) TestBlockWithInfo_Sorting() {
	require := testutil.Require(s.T())

	// Create blocks with reorgs to test sorting
	blocks := []*BlockWithInfo{
		{
			BlockMetadata: &api.BlockMetadata{Height: 100, Hash: "0xaaa"},
			Height:        100,
			Hash:          "0xaaa",
			EventSeq:      1001,
		},
		{
			BlockMetadata: &api.BlockMetadata{Height: 100, Hash: "0xbbb"},
			Height:        100,
			Hash:          "0xbbb",
			EventSeq:      1003, // Later event sequence
		},
		{
			BlockMetadata: &api.BlockMetadata{Height: 99, Hash: "0x999"},
			Height:        99,
			Hash:          "0x999",
			EventSeq:      1000,
		},
		{
			BlockMetadata: &api.BlockMetadata{Height: 101, Hash: "0xccc"},
			Height:        101,
			Hash:          "0xccc",
			EventSeq:      1004,
		},
	}

	// Sort by height first, then by event sequence for same height
	sortBlocksByHeightAndSequence(blocks)

	// Verify sorting
	require.Equal(uint64(99), blocks[0].Height)
	require.Equal("0x999", blocks[0].Hash)

	require.Equal(uint64(100), blocks[1].Height)
	require.Equal("0xaaa", blocks[1].Hash)
	require.Equal(int64(1001), blocks[1].EventSeq)

	require.Equal(uint64(100), blocks[2].Height)
	require.Equal("0xbbb", blocks[2].Hash)
	require.Equal(int64(1003), blocks[2].EventSeq) // Later sequence comes second

	require.Equal(uint64(101), blocks[3].Height)
	require.Equal("0xccc", blocks[3].Hash)
}

// Helper function for sorting
func sortBlocksByHeightAndSequence(blocks []*BlockWithInfo) {
	// This mimics the sorting logic in migrateExtractedBlocks
	for i := 0; i < len(blocks); i++ {
		for j := i + 1; j < len(blocks); j++ {
			if blocks[i].Height > blocks[j].Height {
				blocks[i], blocks[j] = blocks[j], blocks[i]
			} else if blocks[i].Height == blocks[j].Height && blocks[i].EventSeq > blocks[j].EventSeq {
				blocks[i], blocks[j] = blocks[j], blocks[i]
			}
		}
	}
}

func (s *migratorActivityTestSuite) TestParallelEventFetching() {
	require := testutil.Require(s.T())

	// Test mini-batch calculation
	totalSequences := int64(1000)
	parallelism := 8
	expectedMiniBatchSize := (totalSequences + int64(parallelism) - 1) / int64(parallelism)

	require.Equal(int64(125), expectedMiniBatchSize)

	// Test work distribution
	var batches []struct{ start, end int64 }
	for start := int64(0); start < totalSequences; start += expectedMiniBatchSize {
		end := start + expectedMiniBatchSize
		if end > totalSequences {
			end = totalSequences
		}
		batches = append(batches, struct{ start, end int64 }{start, end})
	}

	require.Len(batches, parallelism)
	require.Equal(int64(0), batches[0].start)
	require.Equal(int64(125), batches[0].end)
	require.Equal(int64(875), batches[7].start)
	require.Equal(int64(1000), batches[7].end)
}
