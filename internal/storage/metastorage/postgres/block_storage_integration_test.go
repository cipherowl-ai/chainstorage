package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	tag = 1
)

type blockStorageTestSuite struct {
	suite.Suite
	accessor internal.MetaStorage
	config   *config.Config
	db       *sql.DB
}

func (s *blockStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var accessor internal.MetaStorage
	cfg, err := config.New()
	require.NoError(err)

	// Skip tests if Postgres is not configured
	if cfg.AWS.Postgres == nil {
		s.T().Skip("Postgres not configured, skipping test suite")
		return
	}

	// Set the starting block height
	cfg.Chain.BlockStartHeight = 10
	s.config = cfg
	// Create a new test application with Postgres configuration
	app := testapp.New(
		s.T(),
		fx.Provide(NewMetaStorage),
		testapp.WithIntegration(),
		testapp.WithConfig(s.config),
		fx.Populate(&accessor),
	)
	defer app.Close()
	s.accessor = accessor

	// Get database connection for cleanup
	db, err := newDBConnection(context.Background(), cfg.AWS.Postgres)
	require.NoError(err)
	s.db = db
}

func (s *blockStorageTestSuite) TearDownTest() {
	if s.db != nil {
		ctx := context.Background()
		s.T().Log("Clearing database tables after test")
		// Clear all tables in reverse order due to foreign key constraints
		tables := []string{"block_events", "canonical_blocks", "block_metadata"}
		for _, table := range tables {
			_, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", table))
			if err != nil {
				s.T().Logf("Failed to clear table %s: %v", table, err)
			}
		}
	}
}

func (s *blockStorageTestSuite) TearDownSuite() {
	if s.db != nil {
		s.db.Close()
	}
}

func (s *blockStorageTestSuite) TestPersistBlockMetasByMaxWriteSize() {
	tests := []struct {
		totalBlocks int
	}{
		{totalBlocks: 2},
		{totalBlocks: 4},
		{totalBlocks: 8},
		{totalBlocks: 64},
	}
	for _, test := range tests {
		s.T().Run(fmt.Sprintf("test %d blocks", test.totalBlocks), func(t *testing.T) {
			s.runTestPersistBlockMetas(test.totalBlocks)
		})
	}
}

func (s *blockStorageTestSuite) runTestPersistBlockMetas(totalBlocks int) {
	require := testutil.Require(s.T())
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, totalBlocks, tag)
	zaptest.NewLogger(s.T())
	ctx := context.TODO()

	// shuffle it to make sure it still works
	shuffleSeed := time.Now().UnixNano()
	rand.Shuffle(len(blocks), func(i, j int) { blocks[i], blocks[j] = blocks[j], blocks[i] })
	logger := zaptest.NewLogger(s.T())
	logger.Info("shuffled blocks", zap.Int64("seed", shuffleSeed))

	fmt.Println("Persisting blocks")
	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	if err != nil {
		panic(err)
	}

	expectedLatestBlock := proto.Clone(blocks[totalBlocks-1])

	// fetch range with missing item
	fmt.Println("Fetching range with missing item")
	_, err = s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+uint64(totalBlocks+100))
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))

	// fetch valid range
	fmt.Println("Fetching valid range")
	fetchedBlocks, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+uint64(totalBlocks))
	if err != nil {
		panic(err)
	}
	sort.Slice(fetchedBlocks, func(i, j int) bool {
		return fetchedBlocks[i].Height < fetchedBlocks[j].Height
	})
	assert.Len(s.T(), fetchedBlocks, int(totalBlocks))

	for i := 0; i < len(blocks); i++ {
		//get block by height
		// fetch block through three ways, should always return identical result
		fetchedBlockMeta, err := s.accessor.GetBlockByHeight(ctx, tag, blocks[i].Height)
		if err != nil {
			panic(err)
		}
		s.equalProto(blocks[i], fetchedBlockMeta)

		fetchedBlockMeta, err = s.accessor.GetBlockByHash(ctx, tag, blocks[i].Height, blocks[i].Hash)
		if err != nil {
			panic(err)
		}
		s.equalProto(blocks[i], fetchedBlockMeta)

		fetchedBlockMeta, err = s.accessor.GetBlockByHash(ctx, tag, blocks[i].Height, "")
		if err != nil {
			panic(err)
		}
		s.equalProto(blocks[i], fetchedBlockMeta)

		s.equalProto(blocks[i], fetchedBlocks[i])
	}

	fetchedBlocksMeta, err := s.accessor.GetBlocksByHeights(ctx, tag, []uint64{startHeight + 1, startHeight + uint64(totalBlocks/2), startHeight, startHeight + uint64(totalBlocks) - 1})
	if err != nil {
		fmt.Println("Error fetching blocks by heights", err)
		panic(err)
	}
	assert.Len(s.T(), fetchedBlocksMeta, 4)
	s.equalProto(blocks[1], fetchedBlocksMeta[0])
	s.equalProto(blocks[totalBlocks/2], fetchedBlocksMeta[1])
	s.equalProto(blocks[0], fetchedBlocksMeta[2])
	s.equalProto(blocks[totalBlocks-1], fetchedBlocksMeta[3])

	fetchedBlockMeta, err := s.accessor.GetLatestBlock(ctx, tag)
	if err != nil {
		fmt.Println("Error fetching latest block", err)
		panic(err)
	}
	s.equalProto(expectedLatestBlock, fetchedBlockMeta)

}

func (s *blockStorageTestSuite) TestPersistBlockMetasByInvalidChain() {
	require := testutil.Require(s.T())
	blocks := testutil.MakeBlockMetadatas(100, tag)
	blocks[73].Hash = "0xdeadbeef"
	err := s.accessor.PersistBlockMetas(context.Background(), true, blocks, nil)
	require.Error(err)
	require.True(xerrors.Is(err, parser.ErrInvalidChain))
}

func (s *blockStorageTestSuite) TestPersistBlockMetasByInvalidLastBlock() {
	require := testutil.Require(s.T())
	blocks := testutil.MakeBlockMetadatasFromStartHeight(1_000_000, 100, tag)
	lastBlock := testutil.MakeBlockMetadata(999_999, tag)
	lastBlock.Hash = "0xdeadbeef"
	err := s.accessor.PersistBlockMetas(context.Background(), true, blocks, lastBlock)
	require.Error(err)
	require.True(xerrors.Is(err, parser.ErrInvalidChain))
}

func (s *blockStorageTestSuite) TestPersistBlockMetasWithSkippedBlocks() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 100, tag)
	// Mark 37th block as skipped and point the next block to the previous block.
	blocks[37] = &api.BlockMetadata{
		Tag:     tag,
		Height:  startHeight + 37,
		Skipped: true,
	}
	blocks[38].ParentHeight = blocks[36].Height
	blocks[38].ParentHash = blocks[36].Hash
	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	fetchedBlocks, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+100)
	require.NoError(err)
	require.Equal(blocks, fetchedBlocks)
}

func (s *blockStorageTestSuite) TestPersistBlockMetas() {
	s.runTestPersistBlockMetas(10)
}

func (s *blockStorageTestSuite) TestPersistBlockMetasNotContinuous() {
	blocks := testutil.MakeBlockMetadatas(10, tag)
	blocks[2] = blocks[9]
	err := s.accessor.PersistBlockMetas(context.TODO(), true, blocks[:9], nil)
	assert.NotNil(s.T(), err)
}

func (s *blockStorageTestSuite) TestPersistBlockMetasDuplicatedHeights() {
	blocks := testutil.MakeBlockMetadatas(10, tag)
	blocks[9].Height = 2
	err := s.accessor.PersistBlockMetas(context.TODO(), true, blocks, nil)
	assert.NotNil(s.T(), err)
}

func (s *blockStorageTestSuite) TestGetBlocksNotExist() {
	_, err := s.accessor.GetLatestBlock(context.TODO(), tag)
	assert.True(s.T(), xerrors.Is(err, errors.ErrItemNotFound))
}

func (s *blockStorageTestSuite) TestGetBlockByHeightInvalidHeight() {
	_, err := s.accessor.GetBlockByHeight(context.TODO(), tag, 0)
	assert.True(s.T(), xerrors.Is(err, errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightsInvalidHeight() {
	_, err := s.accessor.GetBlocksByHeights(context.TODO(), tag, []uint64{0})
	assert.True(s.T(), xerrors.Is(err, errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightsBlockNotFound() {
	_, err := s.accessor.GetBlocksByHeights(context.TODO(), tag, []uint64{15})
	assert.True(s.T(), xerrors.Is(err, errors.ErrItemNotFound))
}

func (s *blockStorageTestSuite) TestGetBlockByHashInvalidHeight() {
	_, err := s.accessor.GetBlockByHash(context.TODO(), tag, 0, "0x0")
	assert.True(s.T(), xerrors.Is(err, errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightRangeInvalidRange() {
	_, err := s.accessor.GetBlocksByHeightRange(context.TODO(), tag, 100, 100)
	assert.True(s.T(), xerrors.Is(err, errors.ErrOutOfRange))

	_, err = s.accessor.GetBlocksByHeightRange(context.TODO(), tag, 0, s.config.Chain.BlockStartHeight)
	assert.True(s.T(), xerrors.Is(err, errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) equalProto(x, y any) {
	if diff := cmp.Diff(x, y, protocmp.Transform()); diff != "" {
		assert.FailNow(s.T(), diff)
	}
}

func (s *blockStorageTestSuite) TestWatermarkVisibilityControl() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight

	// Create blocks
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 10, tag)

	// Persist blocks WITHOUT watermark (updateWatermark=false)
	err := s.accessor.PersistBlockMetas(ctx, false, blocks, nil)
	require.NoError(err)

	// GetLatestBlock should return ErrItemNotFound because no blocks are watermarked
	_, err = s.accessor.GetLatestBlock(ctx, tag)
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound), "Expected ErrItemNotFound when no watermark exists")

	// Now persist the same blocks WITH watermark (updateWatermark=true)
	err = s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	// GetLatestBlock should now return the highest block
	latestBlock, err := s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	require.NotNil(latestBlock)
	require.Equal(blocks[9].Height, latestBlock.Height)
	require.Equal(blocks[9].Hash, latestBlock.Hash)

	// Add more blocks with watermark
	moreBlocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight+10, 5, tag)
	err = s.accessor.PersistBlockMetas(ctx, true, moreBlocks, nil)
	require.NoError(err)

	// GetLatestBlock should return the new highest watermarked block
	latestBlock, err = s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	require.Equal(moreBlocks[4].Height, latestBlock.Height)
	require.Equal(moreBlocks[4].Hash, latestBlock.Hash)
}

func (s *blockStorageTestSuite) TestWatermarkWithReorg() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight

	// Create initial chain
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 10, tag)
	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	// Verify latest block
	latestBlock, err := s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	require.Equal(blocks[9].Height, latestBlock.Height)

	// Simulate reorg: create alternative chain from height startHeight+7
	// This represents the reorg scenario where we need to replace blocks
	reorgBlocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight+7, 3, tag)
	// Change hashes to simulate different blocks
	for i := range reorgBlocks {
		reorgBlocks[i].Hash = fmt.Sprintf("0xreorg%d", i)
		if i > 0 {
			reorgBlocks[i].ParentHash = reorgBlocks[i-1].Hash
		} else {
			reorgBlocks[i].ParentHash = blocks[6].Hash // Link to pre-reorg chain
		}
	}

	// Persist reorg blocks with watermark
	err = s.accessor.PersistBlockMetas(ctx, true, reorgBlocks, blocks[6])
	require.NoError(err)

	// GetLatestBlock should return the new reorg tip
	latestBlock, err = s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	require.Equal(reorgBlocks[2].Height, latestBlock.Height)
	require.Equal(reorgBlocks[2].Hash, latestBlock.Hash)
}

func (s *blockStorageTestSuite) TestWatermarkMultipleTags() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight

	tag1 := uint32(1)
	tag2 := uint32(2)

	// Create blocks for tag1
	blocks1 := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 5, tag1)
	err := s.accessor.PersistBlockMetas(ctx, true, blocks1, nil)
	require.NoError(err)

	// Create blocks for tag2
	blocks2 := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 10, tag2)
	err = s.accessor.PersistBlockMetas(ctx, true, blocks2, nil)
	require.NoError(err)

	// Verify each tag has its own latest block
	latestBlock1, err := s.accessor.GetLatestBlock(ctx, tag1)
	require.NoError(err)
	require.Equal(blocks1[4].Height, latestBlock1.Height)

	latestBlock2, err := s.accessor.GetLatestBlock(ctx, tag2)
	require.NoError(err)
	require.Equal(blocks2[9].Height, latestBlock2.Height)
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightRangeStillWorks() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight

	// Create blocks without watermark
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 10, tag)
	err := s.accessor.PersistBlockMetas(ctx, false, blocks, nil)
	require.NoError(err)

	// GetBlocksByHeightRange should still work even without watermark
	// This is important for defense-in-depth validation
	fetchedBlocks, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+10)
	require.NoError(err)
	require.Len(fetchedBlocks, 10)

	// Verify the blocks are correct
	sort.Slice(fetchedBlocks, func(i, j int) bool {
		return fetchedBlocks[i].Height < fetchedBlocks[j].Height
	})
	for i := 0; i < 10; i++ {
		s.equalProto(blocks[i], fetchedBlocks[i])
	}
}

func TestIntegrationBlockStorageTestSuite(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	suite.Run(t, &blockStorageTestSuite{config: cfg})
}
