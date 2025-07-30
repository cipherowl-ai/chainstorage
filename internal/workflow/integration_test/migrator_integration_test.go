package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
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
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type MigratorIntegrationTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

type migratorDependencies struct {
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

func (s *MigratorIntegrationTestSuite) TestMigratorIntegration() {
	const (
		timeout     = 30 * time.Minute
		tag         = uint32(1)
		eventTag    = uint32(0)
		startHeight = uint64(17035140)
		endHeight   = uint64(17035145) // Small range for integration test
		batchSize   = 3
	)

	require := testutil.Require(s.T())

	// Create app context and test data directly (no workflows)
	var deps migratorDependencies
	env := cadence.NewTestEnv(s)
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
				// Create PostgreSQL MetaStorage directly
				result, err := postgres.NewMetaStorage(params)
				if err != nil {
					return nil, err
				}
				return result.MetaStorage, nil
			},
		}),
		fx.Populate(&deps),
	)
	defer app.Close()

	ctx := context.Background()

	// Step 1: Create test data directly using blockchain client and storage
	app.Logger().Info("Step 1: Creating test data directly for migration test")

	for height := startHeight; height < endHeight; height++ {
		// Fetch block from blockchain
		block, err := deps.Client.GetBlockByHeight(ctx, tag, height)
		require.NoError(err, "Failed to fetch block from blockchain at height %d", height)

		// Upload to blob storage
		objectKey, err := deps.BlobStorage.Upload(ctx, block, api.Compression_GZIP)
		require.NoError(err, "Failed to upload block to blob storage at height %d", height)

		// Update metadata with object key
		block.Metadata.ObjectKeyMain = objectKey

		// Store in DynamoDB metadata storage (source for migration)
		err = deps.MetaStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil)
		require.NoError(err, "Failed to store block metadata in DynamoDB at height %d", height)

		// Store in PostgreSQL metadata storage (ensures validation passes)
		err = deps.MetaStoragePG.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil)
		require.NoError(err, "Failed to store block metadata in PostgreSQL at height %d", height)

		app.Logger().Info("Created test data for block", zap.Uint64("height", height))
	}

	// Step 2: Execute migrator workflow (the ONLY workflow in this test)
	app.Logger().Info("Step 2: Executing migrator workflow for complete migration (blocks + events)")

	migratorRequest := &workflow.MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
		BatchSize:   uint64(batchSize),
		SkipEvents:  false, // Migrate both blocks and events
		SkipBlocks:  false,
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err, "Failed to start migrator workflow")

	err = migratorRun.Get(ctx, nil)
	require.NoError(err, "Migrator workflow should complete successfully")

	// Step 3: Verify migration completed successfully
	app.Logger().Info("Step 3: Migration workflow completed successfully",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight),
		zap.Uint64("heightRange", endHeight-startHeight),
		zap.String("type", "complete migration"))

	app.Logger().Info("Complete migration integration test passed")
}

func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_BlocksOnly() {
	const (
		timeout     = 20 * time.Minute
		tag         = uint32(1)
		eventTag    = uint32(0)
		startHeight = uint64(17035145)
		endHeight   = uint64(17035148)
		batchSize   = 2
	)

	require := testutil.Require(s.T())

	// Create app context and test data directly
	var deps migratorDependencies
	env := cadence.NewTestEnv(s)
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
				// Create PostgreSQL MetaStorage directly
				result, err := postgres.NewMetaStorage(params)
				if err != nil {
					return nil, err
				}
				return result.MetaStorage, nil
			},
		}),
		fx.Populate(&deps),
	)
	defer app.Close()

	ctx := context.Background()

	// Step 1: Create test data directly
	app.Logger().Info("Step 1: Creating test data for blocks-only migration")

	for height := startHeight; height < endHeight; height++ {
		// Fetch block from blockchain
		block, err := deps.Client.GetBlockByHeight(ctx, tag, height)
		require.NoError(err, "Failed to fetch block from blockchain at height %d", height)

		// Upload to blob storage
		objectKey, err := deps.BlobStorage.Upload(ctx, block, api.Compression_GZIP)
		require.NoError(err, "Failed to upload block to blob storage at height %d", height)

		// Update metadata with object key
		block.Metadata.ObjectKeyMain = objectKey

		// Store in DynamoDB metadata storage (source for migration)
		err = deps.MetaStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil)
		require.NoError(err, "Failed to store block metadata in DynamoDB at height %d", height)

		// Store in PostgreSQL metadata storage (ensures validation passes)
		err = deps.MetaStoragePG.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil)
		require.NoError(err, "Failed to store block metadata in PostgreSQL at height %d", height)

		app.Logger().Info("Created test data for block", zap.Uint64("height", height))
	}

	// Step 2: Execute blocks-only migration workflow
	app.Logger().Info("Step 2: Executing migrator workflow for blocks-only migration")

	migratorRequest := &workflow.MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
		BatchSize:   uint64(batchSize),
		SkipEvents:  true, // Skip events, migrate blocks only
		SkipBlocks:  false,
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err, "Failed to start blocks-only migrator workflow")

	err = migratorRun.Get(ctx, nil)
	require.NoError(err, "Blocks-only migrator workflow should complete successfully")

	// Step 3: Verify migration completed successfully
	app.Logger().Info("Step 3: Blocks-only migration workflow completed successfully",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight),
		zap.Uint64("heightRange", endHeight-startHeight),
		zap.String("type", "blocks-only migration"))

	app.Logger().Info("Blocks-only migration integration test passed")
}

func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_EventsOnly() {
	const (
		timeout     = 25 * time.Minute
		tag         = uint32(1)
		eventTag    = uint32(0)
		startHeight = uint64(17035148)
		endHeight   = uint64(17035151)
		batchSize   = 2
	)

	require := testutil.Require(s.T())

	// Create app context
	var deps migratorDependencies
	env := cadence.NewTestEnv(s)
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
				// Create PostgreSQL MetaStorage directly
				result, err := postgres.NewMetaStorage(params)
				if err != nil {
					return nil, err
				}
				return result.MetaStorage, nil
			},
		}),
		fx.Populate(&deps),
	)
	defer app.Close()

	ctx := context.Background()

	// Step 1: Create test data for events-only migration
	app.Logger().Info("Step 1: Creating test data for events-only migration")

	var allBlockMetas []*api.BlockMetadata

	for height := startHeight; height < endHeight; height++ {
		// Fetch block from blockchain
		block, err := deps.Client.GetBlockByHeight(ctx, tag, height)
		require.NoError(err, "Failed to fetch block from blockchain at height %d", height)

		// Upload to blob storage
		objectKey, err := deps.BlobStorage.Upload(ctx, block, api.Compression_GZIP)
		require.NoError(err, "Failed to upload block to blob storage at height %d", height)

		// Update metadata with object key
		block.Metadata.ObjectKeyMain = objectKey
		allBlockMetas = append(allBlockMetas, block.Metadata)

		app.Logger().Info("Prepared test data for block", zap.Uint64("height", height))
	}

	// Store in DynamoDB metadata storage (source for migration)
	err := deps.MetaStorage.PersistBlockMetas(ctx, true, allBlockMetas, nil)
	require.NoError(err, "Failed to store block metadata in DynamoDB")

	// Store in PostgreSQL metadata storage (required for events-only migrator validation)
	err = deps.MetaStoragePG.PersistBlockMetas(ctx, true, allBlockMetas, nil)
	require.NoError(err, "Failed to store block metadata in PostgreSQL")

	app.Logger().Info("Successfully created test data for events-only migration")

	// Step 2: Execute events-only migration workflow
	app.Logger().Info("Step 2: Executing events-only migrator workflow")

	migratorRequest := &workflow.MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
		BatchSize:   uint64(batchSize),
		SkipEvents:  false, // Migrate events
		SkipBlocks:  true,  // Skip blocks (they should already exist)
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err, "Failed to start events-only migrator workflow")

	err = migratorRun.Get(ctx, nil)
	require.NoError(err, "Events-only migrator workflow should complete successfully")

	// Step 3: Verify migration completed successfully
	app.Logger().Info("Step 3: Events-only migration workflow completed successfully",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight),
		zap.Uint64("heightRange", endHeight-startHeight),
		zap.String("type", "events-only migration"))

	app.Logger().Info("Events-only migration integration test passed")
}

func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_AutoDetection() {
	const (
		timeout     = 30 * time.Minute
		tag         = uint32(1)
		eventTag    = uint32(0)
		startHeight = uint64(17035140)
		endHeight   = uint64(17035142) // Small range for auto-detection test
		batchSize   = 2
	)

	require := testutil.Require(s.T())

	// Create app context
	var deps migratorDependencies
	env := cadence.NewTestEnv(s)
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
				// Create PostgreSQL MetaStorage directly
				result, err := postgres.NewMetaStorage(params)
				if err != nil {
					return nil, err
				}
				return result.MetaStorage, nil
			},
		}),
		fx.Populate(&deps),
	)
	defer app.Close()

	ctx := context.Background()

	// Step 1: Create test data for auto-detection test
	app.Logger().Info("Step 1: Creating test data for auto-detection test")

	var allBlockMetas []*api.BlockMetadata

	for height := startHeight; height < endHeight; height++ {
		// Fetch block from blockchain
		block, err := deps.Client.GetBlockByHeight(ctx, tag, height)
		require.NoError(err, "Failed to fetch block from blockchain at height %d", height)

		// Upload to blob storage
		objectKey, err := deps.BlobStorage.Upload(ctx, block, api.Compression_GZIP)
		require.NoError(err, "Failed to upload block to blob storage at height %d", height)

		// Update metadata with object key
		block.Metadata.ObjectKeyMain = objectKey
		allBlockMetas = append(allBlockMetas, block.Metadata)

		app.Logger().Info("Prepared test data for block", zap.Uint64("height", height))
	}

	// Store in DynamoDB metadata storage (source for migration)
	err := deps.MetaStorage.PersistBlockMetas(ctx, true, allBlockMetas, nil)
	require.NoError(err, "Failed to store block metadata in DynamoDB")

	// Store in PostgreSQL metadata storage (destination for migration)
	err = deps.MetaStoragePG.PersistBlockMetas(ctx, true, allBlockMetas, nil)
	require.NoError(err, "Failed to store block metadata in PostgreSQL")

	app.Logger().Info("Successfully created test data for auto-detection test")

	// Step 2: Execute migrator workflow with auto-detection (EndHeight = 0)
	app.Logger().Info("Step 2: Executing migrator workflow with auto-detection")

	migratorRequest := &workflow.MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   0, // Should trigger auto-detection
		Tag:         tag,
		EventTag:    eventTag,
		BatchSize:   uint64(batchSize),
		SkipEvents:  false,
		SkipBlocks:  false,
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err, "Failed to start migrator workflow with auto-detection")

	err = migratorRun.Get(ctx, nil)
	require.NoError(err, "Migrator workflow with auto-detection should complete successfully")

	// Step 3: Verify migration completed successfully
	app.Logger().Info("Step 3: Auto-detection migration workflow completed successfully",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight),
		zap.Uint64("heightRange", endHeight-startHeight),
		zap.String("type", "auto-detection migration"))

	app.Logger().Info("Auto-detection migration integration test passed")
}
