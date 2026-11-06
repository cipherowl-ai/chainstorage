package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// migrateParityStoresTestSuite compares what the migrator wrote to a real PostgreSQL with a real
// DynamoDB, which keep the same metadata in different shapes.
type migrateParityStoresTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestIntegrationMigrateParityStoresTestSuite(t *testing.T) {
	suite.Run(t, new(migrateParityStoresTestSuite))
}

func parityStoresChain() []*api.BlockMetadata {
	blocks := make([]*api.BlockMetadata, 120)
	for i := range blocks {
		block := parityTestBlock(1000+uint64(i), "a")
		block.Timestamp = &timestamppb.Timestamp{Seconds: 1700000000 + int64(i)}
		blocks[i] = block
	}
	// A skipped block has no hash, and the next block links past it.
	blocks[10] = &api.BlockMetadata{Tag: 1, Height: 1010, Skipped: true}
	blocks[11].ParentHash = blocks[9].Hash
	blocks[11].ParentHeight = blocks[9].Height
	return blocks
}

func (s *migrateParityStoresTestSuite) TestParityOnMigratedRows() {
	const (
		tag      = uint32(1)
		eventTag = uint32(1)
	)
	require := testutil.Require(s.T())
	ctx := context.Background()

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_BASE),
		config.WithNetwork(common.Network_NETWORK_BASE_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)
	cfg.Workflows.Migrator.BackoffInterval = 0

	// A later suite asserts a query plan on the shared database, and these rows would reach its
	// statistics even after they are gone.
	if cfg.IsIntegrationTest() {
		postgresCfg := cfg.AWS.Postgres
		masterDB := openIntegrationPostgres(s.T(), postgresCfg.Host, postgresCfg.Port, postgresCfg.Database, postgresCfg.User, postgresCfg.Password)
		defer func() { _ = masterDB.Close() }()
		dbName := fmt.Sprintf("migrate_parity_test_%d", time.Now().UnixNano())
		_, err = masterDB.ExecContext(ctx, "CREATE DATABASE "+pq.QuoteIdentifier(dbName))
		require.NoError(err)
		defer func() {
			_, _ = masterDB.ExecContext(ctx, "DROP DATABASE IF EXISTS "+pq.QuoteIdentifier(dbName)+" WITH (FORCE)")
		}()
		postgresCfg.Database = dbName
	}

	var deps struct {
		fx.In
		Source dynamodb.Params
		Dest   postgres.Params
	}
	app := testapp.New(s.T(), testapp.WithIntegration(), testapp.WithConfig(cfg), fx.Populate(&deps))
	defer app.Close()

	// The first DynamoDB storage resets the tables. The migrator creates its own in every activity,
	// and those must keep what the test seeded.
	source, err := dynamodb.NewMetaStorage(deps.Source)
	require.NoError(err)
	cfg.AWS.IsResetLocal = false
	dest, err := postgres.NewMetaStorage(deps.Dest)
	require.NoError(err)
	pool, err := postgres.GetConnectionPool(ctx, cfg.AWS.Postgres)
	require.NoError(err)
	db := pool.DB()
	goose.SetBaseFS(postgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	blocks := parityStoresChain()
	addEvents := func(eventType api.BlockchainEvent_Type, blocks ...*api.BlockMetadata) {
		events := make([]*model.BlockEvent, len(blocks))
		for i, block := range blocks {
			events[i] = model.NewBlockEventWithBlockMeta(eventType, block)
		}
		require.NoError(source.MetaStorage.AddEvents(ctx, eventTag, events))
	}
	require.NoError(source.MetaStorage.PersistBlockMetas(ctx, true, blocks, nil))
	addEvents(api.BlockchainEvent_BLOCK_ADDED, blocks...)
	addEvents(api.BlockchainEvent_BLOCK_REMOVED, blocks[119], blocks[118])
	fork := []*api.BlockMetadata{parityTestBlock(1118, "b"), parityTestBlock(1119, "b"), parityTestBlock(1120, "b")}
	fork[0].ParentHash = blocks[117].Hash
	require.NoError(source.MetaStorage.PersistBlockMetas(ctx, true, fork, nil))
	addEvents(api.BlockchainEvent_BLOCK_ADDED, fork...)

	env := cadence.NewTestEnv(s)
	env.SetTestTimeout(5 * time.Minute)
	var migrator *workflow.Migrator
	migratorApp := testapp.New(
		s.T(),
		testapp.WithIntegration(),
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(env),
		workflow.Module,
		fx.Populate(&migrator),
	)
	defer migratorApp.Close()
	_, err = migrator.Execute(ctx, &workflow.MigratorRequest{Tag: tag, EventTag: eventTag, AutoResume: true, BatchSize: 50})
	require.NoError(err)

	const (
		heights = uint64(121)
		events  = uint64(125)
	)
	options := migrateParityOptions{
		tag:              tag,
		eventTag:         eventTag,
		blockStartHeight: 1000,
		blockSamples:     heights,
		eventSamples:     events,
		seed:             defaultParitySeed,
		walk:             true,
		walkChunkSize:    50,
		walkParallelism:  2,
		requireCaughtUp:  true,
	}
	check := func() *migrateParityReport {
		report, err := newMigrateParityChecker(source.MetaStorage, dest.MetaStorage, options).run(ctx)
		require.NoError(err)
		return report
	}

	report := check()
	require.True(report.Passed, "%v", report.Problems)
	require.Equal(heights, report.HeightsWalked)
	require.Equal(events, report.EventsWalked)
	require.Equal(heights, report.BlocksCompared)
	require.Equal(events, report.EventsCompared)
	require.Equal(fork[2].Hash, report.Postgres.TipHash)

	_, err = db.ExecContext(ctx, `UPDATE block_metadata SET object_key_main = 'other' WHERE height = 1020`)
	require.NoError(err)
	report = check()
	require.False(report.Passed)
	require.Equal(1, report.ProblemCount)
	require.Contains(report.Problems[0], "object_key_main")
	_, err = db.ExecContext(ctx, `UPDATE block_metadata SET object_key_main = $1 WHERE height = 1020`, blocks[20].ObjectKeyMain)
	require.NoError(err)

	_, err = db.ExecContext(ctx, `DELETE FROM block_events WHERE event_tag = $1 AND event_sequence = 10`, eventTag)
	require.NoError(err)
	report = check()
	require.False(report.Passed)
	require.Contains(report.Problems[0], "postgres events")
}
