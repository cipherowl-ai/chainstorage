package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type migratorIntegrationTestSuite struct {
	suite.Suite
	ctrl          *gomock.Controller
	sourceStorage *metastoragemocks.MockMetaStorage
	destStorage   *metastoragemocks.MockMetaStorage
	logger        *zap.Logger
}

func TestMigratorIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(migratorIntegrationTestSuite))
}

func (s *migratorIntegrationTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.sourceStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.destStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.logger, _ = zap.NewDevelopment()
}

func (s *migratorIntegrationTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *migratorIntegrationTestSuite) TestMigrateExtractedBlocks_NoReorg_ValidationPasses() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	// Setup test data - no reorgs
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0x102"},
	}

	// Events contain all the block information needed

	// Mock block data fetching from source
	block100 := &api.BlockMetadata{
		Tag: 1, Height: 100, Hash: "0x100", ParentHash: "0x99", ParentHeight: 99,
	}
	block101 := &api.BlockMetadata{
		Tag: 1, Height: 101, Hash: "0x101", ParentHash: "0x100", ParentHeight: 100,
	}
	block102 := &api.BlockMetadata{
		Tag: 1, Height: 102, Hash: "0x102", ParentHash: "0x101", ParentHeight: 101,
	}

	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(100), "0x100").
		Return(block100, nil)
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(101), "0x101").
		Return(block101, nil)
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(102), "0x102").
		Return(block102, nil)

	// For validation, fetch parent block (99) from destination
	parentBlock := &api.BlockMetadata{
		Tag: 1, Height: 99, Hash: "0x99", ParentHash: "0x98",
	}
	s.destStorage.EXPECT().
		GetBlockByHeight(ctx, uint32(1), uint64(99)).
		Return(parentBlock, nil)

	// Expect bulk persist with validation - THIS IS THE KEY TEST
	// The blocks should be persisted with the parent block for validation
	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false,
			[]*api.BlockMetadata{block100, block101, block102},
			parentBlock).
		Return(nil)

	// Create migrator with mocked storage
	migrator := &Migrator{}
	data := &MigrationData{
		SourceStorage: s.sourceStorage,
		DestStorage:   s.destStorage,
	}
	request := &MigratorRequest{
		Tag:         1,
		Parallelism: 1,
	}

	// Execute
	count, err := migrator.migrateExtractedBlocks(ctx, s.logger, data, request, events)

	// Verify
	require.NoError(err)
	require.Equal(3, count)
}

func (s *migratorIntegrationTestSuite) TestMigrateExtractedBlocks_SingleReorg_CorrectParentValidation() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	// Setup test data - reorg at height 102
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0x102a"},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102, BlockHash: "0x102a"},
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0x102b"},
		{EventId: 6, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 103, BlockHash: "0x103"},
	}

	// Events contain all the block information needed

	// Mock block data
	block100 := &api.BlockMetadata{
		Tag: 1, Height: 100, Hash: "0x100", ParentHash: "0x99", ParentHeight: 99,
	}
	block101 := &api.BlockMetadata{
		Tag: 1, Height: 101, Hash: "0x101", ParentHash: "0x100", ParentHeight: 100,
	}
	block102a := &api.BlockMetadata{
		Tag: 1, Height: 102, Hash: "0x102a", ParentHash: "0x101", ParentHeight: 101,
	}
	block102b := &api.BlockMetadata{
		Tag: 1, Height: 102, Hash: "0x102b", ParentHash: "0x101", ParentHeight: 101,
	}
	block103 := &api.BlockMetadata{
		Tag: 1, Height: 103, Hash: "0x103", ParentHash: "0x102b", ParentHeight: 102,
	}

	// Mock fetching blocks from source
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(100), "0x100").
		Return(block100, nil)
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(101), "0x101").
		Return(block101, nil)
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(102), "0x102a").
		Return(block102a, nil)
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(102), "0x102b").
		Return(block102b, nil)
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(103), "0x103").
		Return(block103, nil)

	// CRITICAL TEST: Two segments with different parent validations

	// Segment 1: blocks 100, 101, 102a - validate against parent height 99
	parentBlock99 := &api.BlockMetadata{
		Tag: 1, Height: 99, Hash: "0x99", ParentHash: "0x98",
	}
	s.destStorage.EXPECT().
		GetBlockByHeight(ctx, uint32(1), uint64(99)).
		Return(parentBlock99, nil)

	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false,
			[]*api.BlockMetadata{block100, block101, block102a},
			parentBlock99).
		Return(nil)

	// Segment 2: blocks 102b, 103 - validate against parent height 101 (reorg point)
	s.destStorage.EXPECT().
		GetBlockByHeight(ctx, uint32(1), uint64(101)).
		Return(block101, nil)

	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false,
			[]*api.BlockMetadata{block102b, block103},
			block101).
		Return(nil)

	// Create migrator
	migrator := &Migrator{}
	data := &MigrationData{
		SourceStorage: s.sourceStorage,
		DestStorage:   s.destStorage,
	}
	request := &MigratorRequest{
		Tag:         1,
		Parallelism: 1,
	}

	// Execute
	count, err := migrator.migrateExtractedBlocks(ctx, s.logger, data, request, events)

	// Verify
	require.NoError(err)
	require.Equal(5, count) // Total blocks persisted
}

func (s *migratorIntegrationTestSuite) TestMigrateExtractedBlocks_ValidationFailure() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	// Setup test data - blocks with broken chain
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101"},
	}

	// Events contain all the block information needed

	block100 := &api.BlockMetadata{
		Tag: 1, Height: 100, Hash: "0x100", ParentHash: "0x99", ParentHeight: 99,
	}
	block101 := &api.BlockMetadata{
		Tag: 1, Height: 101, Hash: "0x101", ParentHash: "0xWRONG", ParentHeight: 100, // Wrong parent
	}

	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(100), "0x100").
		Return(block100, nil)
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(101), "0x101").
		Return(block101, nil)

	// Get parent for validation
	parentBlock := &api.BlockMetadata{
		Tag: 1, Height: 99, Hash: "0x99", ParentHash: "0x98",
	}
	s.destStorage.EXPECT().
		GetBlockByHeight(ctx, uint32(1), uint64(99)).
		Return(parentBlock, nil)

	// Expect PersistBlockMetas to fail due to validation error
	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false,
			[]*api.BlockMetadata{block100, block101},
			parentBlock).
		Return(xerrors.Errorf("chain is not continuous"))

	// Create migrator
	migrator := &Migrator{}
	data := &MigrationData{
		SourceStorage: s.sourceStorage,
		DestStorage:   s.destStorage,
	}
	request := &MigratorRequest{
		Tag:         1,
		Parallelism: 1,
	}

	// Execute - should fail
	count, err := migrator.migrateExtractedBlocks(ctx, s.logger, data, request, events)

	// Verify failure
	require.Error(err)
	require.Contains(err.Error(), "chain is not continuous")
	require.Equal(0, count)
}

func (s *migratorIntegrationTestSuite) TestMigrateExtractedBlocks_ComplexReorg_MultipleSegments() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	// Complex scenario: multiple reorgs
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100", ParentHash: "0x99"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101a", ParentHash: "0x100"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101, BlockHash: "0x101a"},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101b", ParentHash: "0x100"},
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0x102a", ParentHash: "0x101b"},
		{EventId: 6, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102, BlockHash: "0x102a"},
		{EventId: 7, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101, BlockHash: "0x101b"},
		{EventId: 8, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101c", ParentHash: "0x100"},
		{EventId: 9, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0x102b", ParentHash: "0x101c"},
	}

	// Events contain all the block information needed

	// Mock blocks
	block100 := &api.BlockMetadata{Tag: 1, Height: 100, Hash: "0x100", ParentHash: "0x99", ParentHeight: 99}
	block101a := &api.BlockMetadata{Tag: 1, Height: 101, Hash: "0x101a", ParentHash: "0x100", ParentHeight: 100}
	block101b := &api.BlockMetadata{Tag: 1, Height: 101, Hash: "0x101b", ParentHash: "0x100", ParentHeight: 100}
	block102a := &api.BlockMetadata{Tag: 1, Height: 102, Hash: "0x102a", ParentHash: "0x101b", ParentHeight: 101}
	block101c := &api.BlockMetadata{Tag: 1, Height: 101, Hash: "0x101c", ParentHash: "0x100", ParentHeight: 100}
	block102b := &api.BlockMetadata{Tag: 1, Height: 102, Hash: "0x102b", ParentHash: "0x101c", ParentHeight: 101}

	// Mock fetching
	s.sourceStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(100), "0x100").Return(block100, nil)
	s.sourceStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(101), "0x101a").Return(block101a, nil)
	s.sourceStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(101), "0x101b").Return(block101b, nil)
	s.sourceStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(102), "0x102a").Return(block102a, nil)
	s.sourceStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(101), "0x101c").Return(block101c, nil)
	s.sourceStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(102), "0x102b").Return(block102b, nil)

	// Three segments expected:
	// Segment 1: [100, 101a] - parent 99
	parentBlock99 := &api.BlockMetadata{Tag: 1, Height: 99, Hash: "0x99"}
	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(99)).Return(parentBlock99, nil)
	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false, []*api.BlockMetadata{block100, block101a}, parentBlock99).
		Return(nil)

	// Segment 2: [101b, 102a] - parent 100 (reorg at 101)
	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(100)).Return(block100, nil)
	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false, []*api.BlockMetadata{block101b, block102a}, block100).
		Return(nil)

	// Segment 3: [101c, 102b] - parent 100 (second reorg at 101)
	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(100)).Return(block100, nil).Times(1)
	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false, []*api.BlockMetadata{block101c, block102b}, block100).
		Return(nil)

	// Create migrator
	migrator := &Migrator{}
	data := &MigrationData{
		SourceStorage: s.sourceStorage,
		DestStorage:   s.destStorage,
	}
	request := &MigratorRequest{
		Tag:         1,
		Parallelism: 1,
	}

	// Execute
	count, err := migrator.migrateExtractedBlocks(ctx, s.logger, data, request, events)

	// Verify
	require.NoError(err)
	require.Equal(6, count)
}

func (s *migratorIntegrationTestSuite) TestMigrateExtractedBlocks_SkippedBlocks() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	// Test with skipped blocks
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100", ParentHash: "0x99", BlockSkipped: false},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "", ParentHash: "", BlockSkipped: true}, // Skipped
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0x102", ParentHash: "0x100", BlockSkipped: false},
	}

	// Events contain all the block information needed

	// Regular blocks
	block100 := &api.BlockMetadata{
		Tag: 1, Height: 100, Hash: "0x100", ParentHash: "0x99", ParentHeight: 99,
	}
	block102 := &api.BlockMetadata{
		Tag: 1, Height: 102, Hash: "0x102", ParentHash: "0x100", ParentHeight: 100,
	}

	// Skipped blocks shouldn't be fetched from source
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(100), "0x100").
		Return(block100, nil)
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(102), "0x102").
		Return(block102, nil)
	// NO expectation for block 101 since it's skipped

	// Get parent for validation
	parentBlock := &api.BlockMetadata{
		Tag: 1, Height: 99, Hash: "0x99",
	}
	s.destStorage.EXPECT().
		GetBlockByHeight(ctx, uint32(1), uint64(99)).
		Return(parentBlock, nil)

	// Expect persistence including the skipped block (created inline)
	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false,
			gomock.Any(), // We'll validate this in a custom matcher
			parentBlock).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			// Verify the skipped block was created correctly
			require.Len(blocks, 3)
			require.Equal(uint64(101), blocks[1].Height)
			require.True(blocks[1].Skipped)
			require.Empty(blocks[1].Hash)
			require.Empty(blocks[1].ParentHash)
			return nil
		})

	// Create migrator
	migrator := &Migrator{}
	data := &MigrationData{
		SourceStorage: s.sourceStorage,
		DestStorage:   s.destStorage,
	}
	request := &MigratorRequest{
		Tag:         1,
		Parallelism: 1,
	}

	// Execute
	count, err := migrator.migrateExtractedBlocks(ctx, s.logger, data, request, events)

	// Verify
	require.NoError(err)
	require.Equal(3, count)
}

func (s *migratorIntegrationTestSuite) TestMigrateExtractedBlocks_ParentNotFound_ContinuesWithoutValidation() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	// Genesis or missing parent scenario
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 0, BlockHash: "0x0"}, // Genesis
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 1, BlockHash: "0x1"},
	}

	// Events contain all the block information needed

	block0 := &api.BlockMetadata{
		Tag: 1, Height: 0, Hash: "0x0", ParentHash: "", ParentHeight: 0,
	}
	block1 := &api.BlockMetadata{
		Tag: 1, Height: 1, Hash: "0x1", ParentHash: "0x0", ParentHeight: 0,
	}

	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(0), "0x0").
		Return(block0, nil)
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(1), "0x1").
		Return(block1, nil)

	// ParentHeight is 0, so no parent fetch attempted
	// Persist without validation (lastBlock = nil)
	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false,
			[]*api.BlockMetadata{block0, block1},
			nil). // No parent validation for genesis
		Return(nil)

	// Create migrator
	migrator := &Migrator{}
	data := &MigrationData{
		SourceStorage: s.sourceStorage,
		DestStorage:   s.destStorage,
	}
	request := &MigratorRequest{
		Tag:         1,
		Parallelism: 1,
	}

	// Execute
	count, err := migrator.migrateExtractedBlocks(ctx, s.logger, data, request, events)

	// Verify
	require.NoError(err)
	require.Equal(2, count)
}

func (s *migratorIntegrationTestSuite) migrationData() *MigrationData {
	return &MigrationData{SourceStorage: s.sourceStorage, DestStorage: s.destStorage}
}

func (s *migratorIntegrationTestSuite) TestMigrateExtractedBlocks_BlockReadFailure_FailsBatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101"},
	}

	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(100), "0x100").
		Return(&api.BlockMetadata{Tag: 1, Height: 100, Hash: "0x100"}, nil).
		AnyTimes()
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(101), "0x101").
		Return(nil, storage.ErrItemNotFound).
		AnyTimes()
	// No PersistBlockMetas expectation: nothing of the batch may land.

	migrator := &Migrator{}
	for _, parallelism := range []int{1, 2} {
		count, err := migrator.migrateExtractedBlocks(ctx, s.logger, s.migrationData(), &MigratorRequest{Tag: 1, Parallelism: parallelism}, events)

		require.ErrorIs(err, storage.ErrItemNotFound)
		require.Contains(err.Error(), "height=101")
		require.Contains(err.Error(), "0x101")
		require.Equal(0, count)
	}
}

func (s *migratorIntegrationTestSuite) TestMigrateExtractedBlocks_ParentReadFailure_FailsBatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
	}

	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(100), "0x100").
		Return(&api.BlockMetadata{Tag: 1, Height: 100, Hash: "0x100"}, nil)
	s.destStorage.EXPECT().
		GetBlockByHeight(ctx, uint32(1), uint64(99)).
		Return(nil, xerrors.New("connection refused"))

	migrator := &Migrator{}
	count, err := migrator.migrateExtractedBlocks(ctx, s.logger, s.migrationData(), &MigratorRequest{Tag: 1, Parallelism: 1}, events)

	require.Error(err)
	require.Contains(err.Error(), "connection refused")
	require.Equal(0, count)
}

func (s *migratorIntegrationTestSuite) TestMigrateExtractedBlocks_ParentBelowStartHeight_ContinuesWithoutValidation() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
	}

	block := &api.BlockMetadata{Tag: 1, Height: 100, Hash: "0x100"}
	s.sourceStorage.EXPECT().
		GetBlockByHash(ctx, uint32(1), uint64(100), "0x100").
		Return(block, nil)
	s.destStorage.EXPECT().
		GetBlockByHeight(ctx, uint32(1), uint64(99)).
		Return(nil, xerrors.Errorf("height is below the start: %w", storage.ErrInvalidHeight))
	s.destStorage.EXPECT().
		PersistBlockMetas(ctx, false, []*api.BlockMetadata{block}, nil).
		Return(nil)

	migrator := &Migrator{}
	count, err := migrator.migrateExtractedBlocks(ctx, s.logger, s.migrationData(), &MigratorRequest{Tag: 1, Parallelism: 1}, events)

	require.NoError(err)
	require.Equal(1, count)
}

func (s *migratorIntegrationTestSuite) TestGetSegmentParent_CanonicalParent() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	parent := &api.BlockMetadata{Tag: 1, Height: 99, Hash: "0x99"}
	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(99)).Return(parent, nil)

	got, err := getSegmentParent(ctx, s.destStorage, 1, &api.BlockMetadata{Tag: 1, Height: 100, ParentHash: "0x99"})

	require.NoError(err)
	require.Equal(parent, got)
}

func (s *migratorIntegrationTestSuite) TestGetSegmentParent_RetriedReorg_UsesTheForkTheSegmentExtends() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	// An earlier attempt left the winning fork canonical at this height.
	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(99)).
		Return(&api.BlockMetadata{Tag: 1, Height: 99, Hash: "0x99b"}, nil)
	forkParent := &api.BlockMetadata{Tag: 1, Height: 99, Hash: "0x99a"}
	s.destStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(99), "0x99a").Return(forkParent, nil)

	got, err := getSegmentParent(ctx, s.destStorage, 1, &api.BlockMetadata{Tag: 1, Height: 100, ParentHash: "0x99a"})

	require.NoError(err)
	require.Equal(forkParent, got)
}

func (s *migratorIntegrationTestSuite) TestGetSegmentParent_RetriedReorg_ParentBelowASkippedHeight() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(99)).
		Return(&api.BlockMetadata{Tag: 1, Height: 99, Hash: "0x99b"}, nil)
	forkParent := &api.BlockMetadata{Tag: 1, Height: 98, Hash: "0x98a"}
	s.destStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(98), "0x98a").Return(forkParent, nil)

	got, err := getSegmentParent(ctx, s.destStorage, 1, &api.BlockMetadata{Tag: 1, Height: 100, ParentHash: "0x98a", ParentHeight: 98})

	require.NoError(err)
	require.Equal(forkParent, got)
}

func (s *migratorIntegrationTestSuite) TestGetSegmentParent_UnknownParent_KeepsCanonicalSoValidationFails() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	canonical := &api.BlockMetadata{Tag: 1, Height: 99, Hash: "0x99"}
	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(99)).Return(canonical, nil)
	s.destStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(99), "0xunknown").
		Return(nil, xerrors.Errorf("block not found: %w", storage.ErrItemNotFound))

	got, err := getSegmentParent(ctx, s.destStorage, 1, &api.BlockMetadata{Tag: 1, Height: 100, ParentHash: "0xunknown"})

	require.NoError(err)
	require.Equal(canonical, got)
}

func (s *migratorIntegrationTestSuite) TestGetSegmentParent_ForkParentReadFailure_Fails() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(99)).
		Return(&api.BlockMetadata{Tag: 1, Height: 99, Hash: "0x99"}, nil)
	s.destStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(99), "0xother").
		Return(nil, xerrors.New("connection refused"))

	_, err := getSegmentParent(ctx, s.destStorage, 1, &api.BlockMetadata{Tag: 1, Height: 100, ParentHash: "0xother"})

	require.Error(err)
	require.Contains(err.Error(), "connection refused")
}

func (s *migratorIntegrationTestSuite) TestGetSegmentParent_NoParentHash_UsesCanonical() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	parent := &api.BlockMetadata{Tag: 1, Height: 99, Hash: "0x99"}
	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(99)).Return(parent, nil)

	got, err := getSegmentParent(ctx, s.destStorage, 1, &api.BlockMetadata{Tag: 1, Height: 100})

	require.NoError(err)
	require.Equal(parent, got)
}

func (s *migratorIntegrationTestSuite) TestMigrateBatch_CopiesBlocksThenEvents() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	events := []*model.EventEntry{
		{EventId: 7, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
	}
	block := &api.BlockMetadata{Tag: 1, Height: 100, Hash: "0x100"}

	s.destStorage.EXPECT().GetMaxEventId(ctx, uint32(3)).Return(int64(6), nil)
	s.sourceStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(100), "0x100").Return(block, nil)
	s.destStorage.EXPECT().GetBlockByHeight(ctx, uint32(1), uint64(99)).Return(nil, storage.ErrItemNotFound)
	gomock.InOrder(
		s.destStorage.EXPECT().PersistBlockMetas(ctx, false, []*api.BlockMetadata{block}, nil).Return(nil),
		s.destStorage.EXPECT().AddEventEntries(ctx, uint32(3), events).Return(nil),
	)

	migrator := &Migrator{}
	request := &MigratorRequest{StartEventSequence: 7, EndEventSequence: 8, Tag: 1, EventTag: 3, Parallelism: 1}
	blocksMigrated, eventsMigrated, err := migrator.migrateBatch(ctx, s.logger, s.migrationData(), request, events)

	require.NoError(err)
	require.Equal(1, blocksMigrated)
	require.Equal(1, eventsMigrated)
}

func (s *migratorIntegrationTestSuite) TestMigrateBatch_NoEventsInTheDestinationYet() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 0, BlockHash: "0x0"},
	}
	block := &api.BlockMetadata{Tag: 1, Height: 0, Hash: "0x0"}

	s.destStorage.EXPECT().GetMaxEventId(ctx, uint32(3)).Return(int64(0), storage.ErrNoEventHistory)
	s.sourceStorage.EXPECT().GetBlockByHash(ctx, uint32(1), uint64(0), "0x0").Return(block, nil)
	s.destStorage.EXPECT().PersistBlockMetas(ctx, false, []*api.BlockMetadata{block}, nil).Return(nil)
	s.destStorage.EXPECT().AddEventEntries(ctx, uint32(3), events).Return(nil)

	migrator := &Migrator{}
	request := &MigratorRequest{StartEventSequence: 1, EndEventSequence: 2, Tag: 1, EventTag: 3, Parallelism: 1}
	blocksMigrated, _, err := migrator.migrateBatch(ctx, s.logger, s.migrationData(), request, events)

	require.NoError(err)
	require.Equal(1, blocksMigrated)
}

func (s *migratorIntegrationTestSuite) TestMigrateBatch_RetryAfterTheEventsLanded_SkipsTheBlocks() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	events := []*model.EventEntry{
		{EventId: 7, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
		{EventId: 8, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101"},
	}

	// No block read or write: only the events are compared again.
	s.destStorage.EXPECT().GetMaxEventId(ctx, uint32(3)).Return(int64(8), nil)
	s.destStorage.EXPECT().AddEventEntries(ctx, uint32(3), events).Return(nil)

	migrator := &Migrator{}
	request := &MigratorRequest{StartEventSequence: 7, EndEventSequence: 9, Tag: 1, EventTag: 3, Parallelism: 1}
	blocksMigrated, eventsMigrated, err := migrator.migrateBatch(ctx, s.logger, s.migrationData(), request, events)

	require.NoError(err)
	require.Equal(0, blocksMigrated)
	require.Equal(2, eventsMigrated)
}

func (s *migratorIntegrationTestSuite) TestMigrateBatch_MaxEventIdReadFailure_FailsBatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	s.destStorage.EXPECT().GetMaxEventId(ctx, uint32(3)).Return(int64(0), xerrors.New("connection refused"))

	migrator := &Migrator{}
	request := &MigratorRequest{StartEventSequence: 7, EndEventSequence: 8, Tag: 1, EventTag: 3}
	_, _, err := migrator.migrateBatch(ctx, s.logger, s.migrationData(), request, nil)

	require.Error(err)
	require.Contains(err.Error(), "connection refused")
}

func (s *migratorIntegrationTestSuite) TestSetWatermarkAtEventTip_LastAddedBlock() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	parent := &api.BlockMetadata{Tag: 1, Height: 100, Hash: "0x100"}
	tip := &api.BlockMetadata{Tag: 1, Height: 101, Hash: "0x101", ParentHash: "0x100"}

	s.destStorage.EXPECT().GetMaxEventId(ctx, uint32(3)).Return(int64(2), nil)
	s.destStorage.EXPECT().GetEventsByEventIdRange(ctx, uint32(3), int64(1), int64(3)).Return([]*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101", ParentHash: "0x100"},
	}, nil)
	s.destStorage.EXPECT().GetBlocksByHeightRange(ctx, uint32(1), uint64(100), uint64(102)).Return([]*api.BlockMetadata{parent, tip}, nil)
	s.destStorage.EXPECT().PersistBlockMetas(ctx, true, []*api.BlockMetadata{tip}, nil).Return(nil)
	s.destStorage.EXPECT().GetLatestBlock(ctx, uint32(1)).Return(tip, nil)

	response, err := setWatermarkAtEventTip(ctx, s.destStorage, 0, &SetPostgresWatermarkRequest{Tag: 1, EventTag: 3})

	require.NoError(err)
	require.Equal(&SetPostgresWatermarkResponse{Height: 101, Hash: "0x101"}, response)
}

func (s *migratorIntegrationTestSuite) TestSetWatermarkAtEventTip_StreamEndsInRollback() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	parent := &api.BlockMetadata{Tag: 1, Height: 100, Hash: "0x100"}
	tip := &api.BlockMetadata{Tag: 1, Height: 101, Hash: "0x101", ParentHash: "0x100"}

	// The last BLOCK_ADDED is 102, but the stream then removes it.
	s.destStorage.EXPECT().GetMaxEventId(ctx, uint32(3)).Return(int64(4), nil)
	s.destStorage.EXPECT().GetEventsByEventIdRange(ctx, uint32(3), int64(1), int64(5)).Return([]*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101", ParentHash: "0x100"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0x102", ParentHash: "0x101"},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102, BlockHash: "0x102", ParentHash: "0x101"},
	}, nil)
	s.destStorage.EXPECT().GetBlocksByHeightRange(ctx, uint32(1), uint64(100), uint64(102)).Return([]*api.BlockMetadata{parent, tip}, nil)
	s.destStorage.EXPECT().PersistBlockMetas(ctx, true, []*api.BlockMetadata{tip}, nil).Return(nil)
	s.destStorage.EXPECT().GetLatestBlock(ctx, uint32(1)).Return(tip, nil)

	response, err := setWatermarkAtEventTip(ctx, s.destStorage, 0, &SetPostgresWatermarkRequest{Tag: 1, EventTag: 3})

	require.NoError(err)
	require.Equal(&SetPostgresWatermarkResponse{Height: 101, Hash: "0x101"}, response)
}

func (s *migratorIntegrationTestSuite) TestSetWatermarkAtEventTip_TipAtBlockStartHeight() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	tip := &api.BlockMetadata{Tag: 1, Height: 100, Hash: "0x100"}

	s.destStorage.EXPECT().GetMaxEventId(ctx, uint32(3)).Return(int64(1), nil)
	s.destStorage.EXPECT().GetEventsByEventIdRange(ctx, uint32(3), int64(1), int64(2)).Return([]*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100"},
	}, nil)
	s.destStorage.EXPECT().GetBlocksByHeightRange(ctx, uint32(1), uint64(100), uint64(101)).Return([]*api.BlockMetadata{tip}, nil)
	s.destStorage.EXPECT().PersistBlockMetas(ctx, true, []*api.BlockMetadata{tip}, nil).Return(nil)
	s.destStorage.EXPECT().GetLatestBlock(ctx, uint32(1)).Return(tip, nil)

	response, err := setWatermarkAtEventTip(ctx, s.destStorage, 100, &SetPostgresWatermarkRequest{Tag: 1, EventTag: 3})

	require.NoError(err)
	require.Equal(uint64(100), response.Height)
}

func (s *migratorIntegrationTestSuite) TestSetWatermarkAtEventTip_CanonicalBlockDiffers_Fails() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	s.destStorage.EXPECT().GetMaxEventId(ctx, uint32(3)).Return(int64(1), nil)
	s.destStorage.EXPECT().GetEventsByEventIdRange(ctx, uint32(3), int64(1), int64(2)).Return([]*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101"},
	}, nil)
	s.destStorage.EXPECT().GetBlocksByHeightRange(ctx, uint32(1), uint64(100), uint64(102)).Return([]*api.BlockMetadata{
		{Tag: 1, Height: 100, Hash: "0x100"},
		{Tag: 1, Height: 101, Hash: "0xother"},
	}, nil)
	// No PersistBlockMetas expectation: the watermark must not move.

	_, err := setWatermarkAtEventTip(ctx, s.destStorage, 0, &SetPostgresWatermarkRequest{Tag: 1, EventTag: 3})

	require.Error(err)
	require.Contains(err.Error(), "0xother")
}

func (s *migratorIntegrationTestSuite) TestSetWatermarkAtEventTip_EarlierWatermarkAboveTip_Fails() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	tip := &api.BlockMetadata{Tag: 1, Height: 101, Hash: "0x101"}

	s.destStorage.EXPECT().GetMaxEventId(ctx, uint32(3)).Return(int64(1), nil)
	s.destStorage.EXPECT().GetEventsByEventIdRange(ctx, uint32(3), int64(1), int64(2)).Return([]*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101"},
	}, nil)
	s.destStorage.EXPECT().GetBlocksByHeightRange(ctx, uint32(1), uint64(100), uint64(102)).Return([]*api.BlockMetadata{
		{Tag: 1, Height: 100, Hash: "0x100"},
		tip,
	}, nil)
	s.destStorage.EXPECT().PersistBlockMetas(ctx, true, []*api.BlockMetadata{tip}, nil).Return(nil)
	s.destStorage.EXPECT().GetLatestBlock(ctx, uint32(1)).Return(&api.BlockMetadata{Tag: 1, Height: 105, Hash: "0x105"}, nil)

	_, err := setWatermarkAtEventTip(ctx, s.destStorage, 0, &SetPostgresWatermarkRequest{Tag: 1, EventTag: 3})

	require.Error(err)
	require.Contains(err.Error(), "height=105")
}

func TestStartHeartbeat_RecordsUntilStopped(t *testing.T) {
	require := testutil.Require(t)

	recorded := make(chan struct{}, 1)
	stop := startHeartbeat(context.Background(), time.Millisecond, func() {
		select {
		case recorded <- struct{}{}:
		default:
		}
	})
	<-recorded

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		require.Fail("heartbeat goroutine did not exit")
	}
}
