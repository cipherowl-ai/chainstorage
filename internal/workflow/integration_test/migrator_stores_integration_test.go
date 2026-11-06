package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	migratorStoresTag      = uint32(1)
	migratorStoresEventTag = uint32(1)
)

// migratorStoresTestSuite runs the migrator workflow from a real DynamoDB to a real PostgreSQL.
type migratorStoresTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	cfg    *config.Config
	source metastorage.MetaStorage
	dest   metastorage.MetaStorage
	db     *sql.DB
}

func TestIntegrationMigratorStoresTestSuite(t *testing.T) {
	suite.Run(t, new(migratorStoresTestSuite))
}

func (s *migratorStoresTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_BASE),
		config.WithNetwork(common.Network_NETWORK_BASE_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)
	cfg.Chain.BlockStartHeight = 100
	cfg.Workflows.Migrator.ActivityRetry.MaximumAttempts = 1
	cfg.Workflows.Migrator.BackoffInterval = 0
	s.cfg = cfg

	// The first DynamoDB storage resets the tables. The migrator creates its own in every activity,
	// and those must keep what the test seeded.
	var deps struct {
		fx.In
		Source dynamodb.Params
		Dest   postgres.Params
	}
	app := testapp.New(
		s.T(),
		testapp.WithIntegration(),
		testapp.WithConfig(cfg),
		fx.Populate(&deps),
	)
	defer app.Close()

	source, err := dynamodb.NewMetaStorage(deps.Source)
	require.NoError(err)
	s.source = source.MetaStorage
	cfg.AWS.IsResetLocal = false

	dest, err := postgres.NewMetaStorage(deps.Dest)
	require.NoError(err)
	s.dest = dest.MetaStorage

	ctx := context.Background()
	pool, err := postgres.GetConnectionPool(ctx, cfg.AWS.Postgres)
	require.NoError(err)
	s.db = pool.DB()
	goose.SetBaseFS(postgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, s.db, "db/migrations"))
	s.clearPostgres()
}

func (s *migratorStoresTestSuite) TearDownTest() {
	if s.db != nil {
		s.clearPostgres()
	}
}

// clearPostgres cascades because other suites share this database and leave rows that reference
// block_metadata behind.
func (s *migratorStoresTestSuite) clearPostgres() {
	_, err := s.db.ExecContext(context.Background(), "TRUNCATE block_metadata CASCADE")
	testutil.Require(s.T()).NoError(err)
}

func (s *migratorStoresTestSuite) runMigrator(request *workflow.MigratorRequest) error {
	env := cadence.NewTestEnv(s)
	env.SetTestTimeout(5 * time.Minute)

	var migrator *workflow.Migrator
	app := testapp.New(
		s.T(),
		testapp.WithIntegration(),
		testapp.WithConfig(s.cfg),
		cadence.WithTestEnv(env),
		workflow.Module,
		fx.Populate(&migrator),
	)
	defer app.Close()

	request.Tag = migratorStoresTag
	request.EventTag = migratorStoresEventTag
	_, err := migrator.Execute(context.Background(), request)
	return err
}

func migratorStoresBlock(height uint64, fork string, parentFork string) *api.BlockMetadata {
	return &api.BlockMetadata{
		Tag:           migratorStoresTag,
		Height:        height,
		Hash:          fmt.Sprintf("0x%d%s", height, fork),
		ParentHash:    fmt.Sprintf("0x%d%s", height-1, parentFork),
		ParentHeight:  height - 1,
		ObjectKeyMain: fmt.Sprintf("blocks/%d/%s", height, fork),
	}
}

func migratorStoresChain(start, end uint64, fork string) []*api.BlockMetadata {
	blocks := make([]*api.BlockMetadata, 0, end-start)
	for height := start; height < end; height++ {
		blocks = append(blocks, migratorStoresBlock(height, fork, fork))
	}
	return blocks
}

// streamBlocks does what the replicator and the streamer do for new blocks.
func (s *migratorStoresTestSuite) streamBlocks(store metastorage.MetaStorage, blocks []*api.BlockMetadata) {
	require := testutil.Require(s.T())
	ctx := context.Background()
	require.NoError(store.PersistBlockMetas(ctx, true, blocks, nil))
	events := make([]*model.BlockEvent, len(blocks))
	for i, block := range blocks {
		events[i] = model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
	}
	require.NoError(store.AddEvents(ctx, migratorStoresEventTag, events))
}

// removeBlocks does what the streamer does for a reorg, last block first.
func (s *migratorStoresTestSuite) removeBlocks(blocks []*api.BlockMetadata) {
	events := make([]*model.BlockEvent, 0, len(blocks))
	for i := len(blocks) - 1; i >= 0; i-- {
		events = append(events, model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_REMOVED, blocks[i]))
	}
	testutil.Require(s.T()).NoError(s.source.AddEvents(context.Background(), migratorStoresEventTag, events))
}

func (s *migratorStoresTestSuite) requireLatestBlock(want *api.BlockMetadata) {
	require := testutil.Require(s.T())
	latest, err := s.dest.GetLatestBlock(context.Background(), migratorStoresTag)
	require.NoError(err)
	require.Equal(want.Height, latest.Height)
	require.Equal(want.Hash, latest.Hash)
}

func (s *migratorStoresTestSuite) requireEventsCopied() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	maxEventId, err := s.source.GetMaxEventId(ctx, migratorStoresEventTag)
	require.NoError(err)
	destMaxEventId, err := s.dest.GetMaxEventId(ctx, migratorStoresEventTag)
	require.NoError(err)
	require.Equal(maxEventId, destMaxEventId)

	want, err := s.source.GetEventsByEventIdRange(ctx, migratorStoresEventTag, model.EventIdStartValue, maxEventId+1)
	require.NoError(err)
	got, err := s.dest.GetEventsByEventIdRange(ctx, migratorStoresEventTag, model.EventIdStartValue, maxEventId+1)
	require.NoError(err)
	for i := range want {
		require.Equal(want[i].EventId, got[i].EventId)
		require.Equal(want[i].EventType, got[i].EventType)
		require.Equal(want[i].BlockHeight, got[i].BlockHeight)
		require.Equal(want[i].BlockHash, got[i].BlockHash)
		require.Equal(want[i].BlockSkipped, got[i].BlockSkipped)
	}
}

func (s *migratorStoresTestSuite) TestBoundedRunCopiesEveryEventAndSetsTheWatermark() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	chain := migratorStoresChain(100, 141, "a")
	s.streamBlocks(s.source, chain)
	s.removeBlocks(chain[len(chain)-2:])
	fork := []*api.BlockMetadata{
		migratorStoresBlock(139, "b", "a"),
		migratorStoresBlock(140, "b", "b"),
		migratorStoresBlock(141, "b", "b"),
	}
	s.streamBlocks(s.source, fork)

	require.NoError(s.runMigrator(&workflow.MigratorRequest{AutoResume: true, BatchSize: 7, CheckpointSize: 1000}))

	s.requireEventsCopied()
	s.requireLatestBlock(fork[2])

	// Both sides of the reorg landed, and the canonical chain is the one that won.
	removed, err := s.dest.GetBlockByHash(ctx, migratorStoresTag, 140, chain[40].Hash)
	require.NoError(err)
	require.Equal(chain[40].ObjectKeyMain, removed.ObjectKeyMain)
	canonical, err := s.dest.GetBlocksByHeightRange(ctx, migratorStoresTag, 100, 142)
	require.NoError(err)
	for i, block := range append(chain[:39:39], fork...) {
		require.Equal(block.Hash, canonical[i].Hash)
		require.Equal(block.ObjectKeyMain, canonical[i].ObjectKeyMain)
	}

	// Nothing is left to copy, and the run still succeeds.
	require.NoError(s.runMigrator(&workflow.MigratorRequest{AutoResume: true}))
	s.requireLatestBlock(fork[2])

	// The chain moved on, and the next run takes the watermark with it.
	next := migratorStoresBlock(142, "b", "b")
	s.streamBlocks(s.source, []*api.BlockMetadata{next})
	require.NoError(s.runMigrator(&workflow.MigratorRequest{AutoResume: true}))
	s.requireEventsCopied()
	s.requireLatestBlock(next)
}

func (s *migratorStoresTestSuite) TestWatermarkStopsAtTheTipWhenTheStreamEndsInARollback() {
	require := testutil.Require(s.T())

	chain := migratorStoresChain(100, 106, "a")
	s.streamBlocks(s.source, chain)
	s.removeBlocks(chain[4:])

	require.NoError(s.runMigrator(&workflow.MigratorRequest{AutoResume: true}))

	s.requireEventsCopied()
	s.requireLatestBlock(chain[3])
}

func (s *migratorStoresTestSuite) TestMissingBlockFailsTheRunBeforeAnythingLands() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	chain := migratorStoresChain(100, 106, "a")
	s.streamBlocks(s.source, chain)
	missing := migratorStoresBlock(106, "a", "a")
	require.NoError(s.source.AddEvents(ctx, migratorStoresEventTag, []*model.BlockEvent{
		model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, missing),
	}))

	err := s.runMigrator(&workflow.MigratorRequest{AutoResume: true})

	require.Error(err)
	require.Contains(err.Error(), "height=106")
	_, err = s.dest.GetMaxEventId(ctx, migratorStoresEventTag)
	require.ErrorIs(err, storage.ErrNoEventHistory)
	_, err = s.dest.GetLatestBlock(ctx, migratorStoresTag)
	require.ErrorIs(err, storage.ErrItemNotFound)
}

func (s *migratorStoresTestSuite) TestEventTheInsertWouldDropStopsTheRunWithoutAHole() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	chain := migratorStoresChain(100, 110, "a")
	require.NoError(s.source.PersistBlockMetas(ctx, true, chain, nil))
	events := make([]*model.BlockEvent, len(chain))
	for i, block := range chain {
		events[i] = model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
	}
	// The insert joins on the event's block tag, and no block carries this one.
	events[4].Tag = migratorStoresTag + 1
	require.NoError(s.source.AddEvents(ctx, migratorStoresEventTag, events))

	// The second batch holds the event, with more events after it. A restart must stop in the same
	// place instead of resuming past a hole.
	for attempt := 0; attempt < 2; attempt++ {
		err := s.runMigrator(&workflow.MigratorRequest{AutoResume: true, BatchSize: 3})
		require.Error(err)
		require.Contains(err.Error(), "1 of 3 events were not stored as given, starting at event 5")

		maxEventId, err := s.dest.GetMaxEventId(ctx, migratorStoresEventTag)
		require.NoError(err)
		require.Equal(int64(3), maxEventId)
	}
	_, err := s.dest.GetLatestBlock(ctx, migratorStoresTag)
	require.ErrorIs(err, storage.ErrItemNotFound)
}

func (s *migratorStoresTestSuite) TestDifferentEventInTheDestinationFailsTheRun() {
	require := testutil.Require(s.T())

	s.streamBlocks(s.source, migratorStoresChain(100, 103, "a"))
	s.streamBlocks(s.dest, migratorStoresChain(100, 103, "x"))

	err := s.runMigrator(&workflow.MigratorRequest{StartEventSequence: 1})

	require.Error(err)
	require.Contains(err.Error(), "3 of 3 events were not stored as given, starting at event 1")
}

func (s *migratorStoresTestSuite) TestRetryOfABatchHoldingAReorgSucceeds() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	// Events 1-11 add heights 100-110, 12-16 remove 110-106, and 17-23 add 106-112 on a new fork.
	chain := migratorStoresChain(100, 111, "a")
	s.streamBlocks(s.source, chain)
	s.removeBlocks(chain[6:])
	fork := append([]*api.BlockMetadata{migratorStoresBlock(106, "b", "a")}, migratorStoresChain(107, 113, "b")...)
	s.streamBlocks(s.source, fork)

	// Copy up to height 107, so that the next batch starts inside the range the reorg replaces.
	require.NoError(s.runMigrator(&workflow.MigratorRequest{StartEventSequence: 1, EndEventSequence: 9}))

	// Fail that batch after it has persisted both forks. Its first segment now extends a fork that
	// is no longer canonical.
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO block_events (event_tag, event_sequence, event_type, block_metadata_id, height, hash)
		SELECT $1, 20, 'BLOCK_ADDED', id, height, hash FROM block_metadata WHERE tag = $2 AND hash = $3`,
		migratorStoresEventTag, migratorStoresTag, chain[0].Hash)
	require.NoError(err)
	err = s.runMigrator(&workflow.MigratorRequest{StartEventSequence: 9})
	require.Error(err)
	require.Contains(err.Error(), "starting at event 20")
	_, err = s.db.ExecContext(ctx, `DELETE FROM block_events WHERE event_tag = $1 AND event_sequence = 20`, migratorStoresEventTag)
	require.NoError(err)

	require.NoError(s.runMigrator(&workflow.MigratorRequest{AutoResume: true}))

	s.requireEventsCopied()
	s.requireLatestBlock(fork[len(fork)-1])
}

func (s *migratorStoresTestSuite) TestWrongCanonicalBlockBelowTheTipFailsTheRun() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	s.streamBlocks(s.source, migratorStoresChain(100, 105, "a"))
	require.NoError(s.runMigrator(&workflow.MigratorRequest{AutoResume: true}))

	// The events still agree on the tip, so only the read below it can tell.
	other := migratorStoresBlock(103, "x", "a")
	require.NoError(s.dest.PersistBlockMetas(ctx, false, []*api.BlockMetadata{other}, nil))

	err := s.runMigrator(&workflow.MigratorRequest{AutoResume: true})

	require.Error(err)
	require.Contains(err.Error(), "failed to get blocks [103, 104]")
	require.Contains(err.Error(), "chain is not continuous")
}

// The cutover runbook restarts ingestion on PostgreSQL at the block after the migrated tip.
func (s *migratorStoresTestSuite) TestIngestionContinuesFromTheMigratedTip() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	chain := migratorStoresChain(100, 106, "a")
	s.streamBlocks(s.source, chain)
	s.removeBlocks(chain[4:])
	require.NoError(s.runMigrator(&workflow.MigratorRequest{AutoResume: true}))
	maxEventId, err := s.dest.GetMaxEventId(ctx, migratorStoresEventTag)
	require.NoError(err)

	// The replicator writes past the tip, over the blocks the rollback removed.
	next := append([]*api.BlockMetadata{migratorStoresBlock(104, "b", "a")}, migratorStoresChain(105, 107, "b")...)
	require.NoError(s.dest.PersistBlockMetas(ctx, true, next, chain[3]))

	env := cadence.NewTestActivityEnv(s)
	var streamer *activity.Streamer
	app := testapp.New(
		s.T(),
		testapp.WithIntegration(),
		testapp.WithConfig(s.cfg),
		cadence.WithTestEnv(env),
		fx.Provide(activity.NewStreamer),
		fx.Provide(func() metastorage.MetaStorage { return s.dest }),
		fx.Populate(&streamer),
	)
	defer app.Close()

	response, err := streamer.Execute(env.BackgroundContext(), &activity.StreamerRequest{
		BatchSize: 10,
		Tag:       migratorStoresTag,
		EventTag:  migratorStoresEventTag,
	})
	require.NoError(err)
	require.Equal(uint64(106), response.LatestStreamedHeight)

	events, err := s.dest.GetEventsByEventIdRange(ctx, migratorStoresEventTag, maxEventId+1, maxEventId+4)
	require.NoError(err)
	for i, event := range events {
		require.Equal(api.BlockchainEvent_BLOCK_ADDED, event.EventType)
		require.Equal(next[i].Hash, event.BlockHash)
	}
	s.requireLatestBlock(next[2])
}
