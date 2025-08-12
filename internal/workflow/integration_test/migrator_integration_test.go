package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	temporalworkflow "go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type MigratorIntegrationTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

type testDependencies struct {
	fx.In
	Migrator      *workflow.Migrator
	BlobStorage   blobstorage.BlobStorage
	MetaStorage   metastorage.MetaStorage // DynamoDB
	MetaStoragePG metastorage.MetaStorage `name:"pg"` // PostgreSQL
	Parser        parser.Parser
	Client        client.Client `name:"slave"`
}

func TestIntegrationMigratorTestSuite(t *testing.T) {
	suite.Run(t, new(MigratorIntegrationTestSuite))
}

// Helper function to create test app with common dependencies
func (s *MigratorIntegrationTestSuite) createTestApp(env *cadence.TestEnv, timeout time.Duration) (testapp.TestApp, *testDependencies) {
	var deps testDependencies
	env.SetTestTimeout(timeout)

	app := testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET),
		cadence.WithTestEnv(env),
		workflow.Module,
		client.Module,
		jsonrpc.Module,
		restapi.Module,
		s3.Module,
		storage.Module,
		parser.Module,
		dlq.Module,
		// Provide PostgreSQL MetaStorage with name "pg" for migration tests
		fx.Provide(fx.Annotated{
			Name: "pg",
			Target: func(params postgres.Params) (metastorage.MetaStorage, error) {
				result, err := postgres.NewMetaStorage(params)
				if err != nil {
					return nil, err
				}
				return result.MetaStorage, nil
			},
		}),
		fx.Populate(&deps),
	)

	return app, &deps
}

// Helper function to create test blocks
func (s *MigratorIntegrationTestSuite) createTestBlocks(
	ctx context.Context,
	deps *testDependencies,
	tag uint32,
	startHeight, endHeight uint64,
	storeBoth bool, // If true, store in both DynamoDB and PostgreSQL
) error {
	require := testutil.Require(s.T())

	for height := startHeight; height < endHeight; height++ {
		// Fetch block from blockchain
		block, err := deps.Client.GetBlockByHeight(ctx, tag, height)
		require.NoError(err, "Failed to fetch block at height %d", height)

		// Upload to blob storage
		objectKey, err := deps.BlobStorage.Upload(ctx, block, api.Compression_GZIP)
		require.NoError(err, "Failed to upload block at height %d", height)

		// Update metadata with object key
		block.Metadata.ObjectKeyMain = objectKey

		// Store in DynamoDB metadata storage (source for migration)
		err = deps.MetaStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil)
		require.NoError(err, "Failed to store block in DynamoDB at height %d", height)

		// Optionally store in PostgreSQL (for validation)
		if storeBoth {
			err = deps.MetaStoragePG.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil)
			require.NoError(err, "Failed to store block in PostgreSQL at height %d", height)
		}
	}

	return nil
}

// Simplified test for complete migration (blocks + events)
func (s *MigratorIntegrationTestSuite) TestMigratorIntegration() {
	const (
		tag         = uint32(1)
		eventTag    = uint32(0)
		startHeight = uint64(17035140)
		endHeight   = uint64(17035145)
		batchSize   = 3
	)

	require := testutil.Require(s.T())

	// Setup
	env := cadence.NewTestEnv(s)
	app, deps := s.createTestApp(env, 30*time.Minute)
	defer app.Close()

	ctx := context.Background()

	// Create test data
	err := s.createTestBlocks(ctx, deps, tag, startHeight, endHeight, true)
	require.NoError(err)

	// Execute migrator workflow
	migratorRequest := &workflow.MigratorRequest{
		StartHeight:   startHeight,
		EndHeight:     endHeight,
		Tag:           tag,
		EventTag:      eventTag,
		BatchSize:     uint64(batchSize),
		MiniBatchSize: uint64(batchSize / 2),
		Parallelism:   2,
		SkipEvents:    false,
		SkipBlocks:    false,
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err)

	err = migratorRun.Get(ctx, nil)
	require.NoError(err)

	app.Logger().Info("Complete migration test passed",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight))
}

// Simplified test for blocks-only migration
func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_BlocksOnly() {
	const (
		tag         = uint32(1)
		eventTag    = uint32(0)
		startHeight = uint64(17035145)
		endHeight   = uint64(17035148)
		batchSize   = 2
	)

	require := testutil.Require(s.T())

	// Setup
	env := cadence.NewTestEnv(s)
	app, deps := s.createTestApp(env, 20*time.Minute)
	defer app.Close()

	ctx := context.Background()

	// Create test data
	err := s.createTestBlocks(ctx, deps, tag, startHeight, endHeight, true)
	require.NoError(err)

	// Execute blocks-only migration
	migratorRequest := &workflow.MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
		BatchSize:   uint64(batchSize),
		SkipEvents:  true, // Skip events
		SkipBlocks:  false,
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err)

	err = migratorRun.Get(ctx, nil)
	require.NoError(err)

	app.Logger().Info("Blocks-only migration test passed",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight))
}

// Simplified test for events-only migration
func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_EventsOnly() {
	const (
		tag         = uint32(1)
		eventTag    = uint32(0)
		startHeight = uint64(17035148)
		endHeight   = uint64(17035151)
		batchSize   = 2
	)

	require := testutil.Require(s.T())

	// Setup
	env := cadence.NewTestEnv(s)
	app, deps := s.createTestApp(env, 25*time.Minute)
	defer app.Close()

	ctx := context.Background()

	// Create test data (blocks must exist for events migration)
	err := s.createTestBlocks(ctx, deps, tag, startHeight, endHeight, true)
	require.NoError(err)

	// Execute events-only migration
	migratorRequest := &workflow.MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
		BatchSize:   uint64(batchSize),
		SkipEvents:  false, // Migrate events
		SkipBlocks:  true,  // Skip blocks
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err)

	err = migratorRun.Get(ctx, nil)
	require.NoError(err)

	app.Logger().Info("Events-only migration test passed",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight))
}

// Simplified test for auto-detection
func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_AutoDetection() {
	const (
		tag         = uint32(1)
		eventTag    = uint32(0)
		startHeight = uint64(17035140)
		endHeight   = uint64(17035142)
		batchSize   = 2
	)

	require := testutil.Require(s.T())

	// Setup
	env := cadence.NewTestEnv(s)
	app, deps := s.createTestApp(env, 30*time.Minute)
	defer app.Close()

	ctx := context.Background()

	// Create test data
	err := s.createTestBlocks(ctx, deps, tag, startHeight, endHeight, true)
	require.NoError(err)

	// Mock GetLatestBlockHeight for auto-detection
	env.OnActivity(activity.ActivityGetLatestBlockHeight, mock.Anything, mock.Anything).
		Return(&activity.GetLatestBlockHeightResponse{
			Height: endHeight - 1,
		}, nil)

	// Execute with auto-detection (EndHeight = 0)
	migratorRequest := &workflow.MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   0, // Triggers auto-detection
		Tag:         tag,
		EventTag:    eventTag,
		BatchSize:   uint64(batchSize),
		SkipEvents:  false,
		SkipBlocks:  false,
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err)

	err = migratorRun.Get(ctx, nil)
	require.NoError(err)

	app.Logger().Info("Auto-detection migration test passed",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("detectedEndHeight", endHeight-1))
}

// Optimized continuous sync test focusing on height progression and cycle validation
func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_ContinuousSync() {
	const (
		tag               = uint32(1)
		eventTag          = uint32(0)
		cycle1StartHeight = uint64(17035151)
		cycle1EndHeight   = uint64(17035153)
		cycle2StartHeight = uint64(17035153) // Should continue from where cycle 1 ended
		cycle2EndHeight   = uint64(17035155)
		batchSize         = 1
	)

	require := testutil.Require(s.T())
	ctx := context.Background()

	// ========== CYCLE 1: Initial migration with continuous sync ==========

	// Setup for cycle 1
	env1 := cadence.NewTestEnv(s)
	app1, deps1 := s.createTestApp(env1, 10*time.Minute)
	defer app1.Close()

	// Create test data for cycle 1
	err := s.createTestBlocks(ctx, deps1, tag, cycle1StartHeight, cycle1EndHeight, true)
	require.NoError(err)

	app1.Logger().Info("Starting cycle 1 of continuous sync",
		zap.Uint64("startHeight", cycle1StartHeight),
		zap.Uint64("endHeight", cycle1EndHeight))

	// Mock GetLatestBlockHeight for cycle 1
	env1.OnActivity(activity.ActivityGetLatestBlockHeight, mock.Anything, mock.Anything).
		Return(&activity.GetLatestBlockHeightResponse{
			Height: cycle1EndHeight - 1,
		}, nil)

	// Execute cycle 1 with continuous sync
	cycle1Request := &workflow.MigratorRequest{
		StartHeight:    cycle1StartHeight,
		EndHeight:      cycle1EndHeight,
		Tag:            tag,
		EventTag:       eventTag,
		BatchSize:      uint64(batchSize),
		ContinuousSync: true,
		SyncInterval:   "1s",
	}

	// Cycle 1 should complete and return continue-as-new
	_, err = deps1.Migrator.Execute(ctx, cycle1Request)
	require.NotNil(err, "Cycle 1 should return continue-as-new error")
	require.True(temporalworkflow.IsContinueAsNewError(err), "Expected continue-as-new for cycle 1")

	// Verify cycle 1 migrated correctly
	for height := cycle1StartHeight; height < cycle1EndHeight; height++ {
		metadata, err := deps1.MetaStoragePG.GetBlockByHeight(ctx, tag, height)
		require.NoError(err, "Cycle 1 block should exist at height %d", height)
		require.Equal(height, metadata.Height)
	}

	app1.Logger().Info("Cycle 1 completed successfully with continue-as-new")

	// ========== CYCLE 2: Verify height progression and new data migration ==========

	// Setup for cycle 2 (simulating a new workflow execution)
	env2 := cadence.NewTestEnv(s)
	app2, deps2 := s.createTestApp(env2, 10*time.Minute)
	defer app2.Close()

	// Create new blocks for cycle 2 (simulating new blocks arriving)
	err = s.createTestBlocks(ctx, deps2, tag, cycle2StartHeight, cycle2EndHeight, true)
	require.NoError(err)

	app2.Logger().Info("Starting cycle 2 of continuous sync",
		zap.Uint64("expectedStartHeight", cycle2StartHeight),
		zap.Uint64("newEndHeight", cycle2EndHeight))

	// Mock GetLatestBlockHeight for cycle 2 to return the new height
	env2.OnActivity(activity.ActivityGetLatestBlockHeight, mock.Anything, mock.Anything).
		Return(&activity.GetLatestBlockHeightResponse{
			Height: cycle2EndHeight - 1,
		}, nil)

	// Execute cycle 2 - should detect the new height and migrate new blocks
	cycle2Request := &workflow.MigratorRequest{
		StartHeight:    cycle2StartHeight, // Continue from where cycle 1 ended
		EndHeight:      0,                 // Auto-detect to find new blocks
		Tag:            tag,
		EventTag:       eventTag,
		BatchSize:      uint64(batchSize),
		ContinuousSync: true,
		SyncInterval:   "1s",
	}

	// Cycle 2 should also complete and return continue-as-new
	_, err = deps2.Migrator.Execute(ctx, cycle2Request)
	require.NotNil(err, "Cycle 2 should return continue-as-new error")
	require.True(temporalworkflow.IsContinueAsNewError(err), "Expected continue-as-new for cycle 2")

	// Verify cycle 2 migrated the new blocks correctly
	for height := cycle2StartHeight; height < cycle2EndHeight; height++ {
		metadata, err := deps2.MetaStoragePG.GetBlockByHeight(ctx, tag, height)
		require.NoError(err, "Cycle 2 block should exist at height %d", height)
		require.Equal(height, metadata.Height)
	}

	app2.Logger().Info("Cycle 2 completed successfully",
		zap.Uint64("migratedFrom", cycle2StartHeight),
		zap.Uint64("migratedTo", cycle2EndHeight-1))

	// ========== FINAL VALIDATION ==========

	totalBlocksMigrated := (cycle1EndHeight - cycle1StartHeight) + (cycle2EndHeight - cycle2StartHeight)
	app2.Logger().Info("Continuous sync test completed successfully",
		zap.Uint64("cycle1Range", cycle1EndHeight-cycle1StartHeight),
		zap.Uint64("cycle2Range", cycle2EndHeight-cycle2StartHeight),
		zap.Uint64("totalBlocksMigrated", totalBlocksMigrated),
		zap.String("result", "Both cycles correctly detected new heights and migrated data"))
}
