package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

const (
	migratorCheckpointSize = 1000
	migratorBatchSize      = 100
)

type migratorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env      *cadence.TestEnv
	migrator *Migrator
	app      testapp.TestApp
	cfg      *config.Config
}

func TestMigratorTestSuite(t *testing.T) {
	suite.Run(t, new(migratorTestSuite))
}

func (s *migratorTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	// Override config to speed up the test
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.Migrator.BatchSize = migratorBatchSize
	cfg.Workflows.Migrator.CheckpointSize = migratorCheckpointSize
	cfg.Workflows.Migrator.BackoffInterval = time.Second
	s.cfg = cfg

	s.env = cadence.NewTestEnv(s)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(s.env),
		fx.Populate(&s.migrator),
	)
}

func (s *migratorTestSuite) TearDownTest() {
	s.app.Close()
	s.env.AssertExpectations(s.T())
}

func (s *migratorTestSuite) TestMigrator_Success() {
	require := testutil.Require(s.T())

	startHeight := uint64(1000)
	endHeight := uint64(1200)
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			require.Equal(tag, request.Tag)
			require.Equal(eventTag, request.EventTag)
			require.Equal(int(migratorBatchSize/10), request.BatchSize)
			require.False(request.SkipEvents)
			require.False(request.SkipBlocks)

			// Simulate migrating the requested range
			batchSize := request.EndHeight - request.StartHeight
			return &activity.MigratorResponse{
				BlocksMigrated: int(batchSize),
				EventsMigrated: int(batchSize * 5), // Simulate 5 events per block
				Success:        true,
				Message:        "Migration completed successfully",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_WithCheckpoint() {
	require := testutil.Require(s.T())

	startHeight := uint64(1000)
	endHeight := uint64(startHeight + migratorCheckpointSize + 100) // Exceed checkpoint
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(&activity.MigratorResponse{
			BlocksMigrated: 100,
			EventsMigrated: 500,
			Success:        true,
			Message:        "Migration completed successfully",
		}, nil)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *migratorTestSuite) TestMigrator_SkipBlocks() {
	require := testutil.Require(s.T())

	startHeight := uint64(1000)
	endHeight := uint64(1200)
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			require.True(request.SkipBlocks)
			require.False(request.SkipEvents)
			return &activity.MigratorResponse{
				BlocksMigrated: 0,
				EventsMigrated: 100,
				Success:        true,
				Message:        "Events migrated successfully",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
		SkipBlocks:  true,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_SkipEvents() {
	require := testutil.Require(s.T())

	startHeight := uint64(1000)
	endHeight := uint64(1200)
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			require.False(request.SkipBlocks)
			require.True(request.SkipEvents)
			return &activity.MigratorResponse{
				BlocksMigrated: 100,
				EventsMigrated: 0,
				Success:        true,
				Message:        "Blocks migrated successfully",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
		SkipEvents:  true,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_CustomBatchSize() {
	require := testutil.Require(s.T())

	startHeight := uint64(1000)
	endHeight := uint64(1200)
	tag := uint32(1)
	eventTag := uint32(0)
	customBatchSize := uint64(50)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			// Internal activity batch size should be 1/10 of workflow batch size
			require.Equal(int(customBatchSize/10), request.BatchSize)
			return &activity.MigratorResponse{
				BlocksMigrated: 50,
				EventsMigrated: 250,
				Success:        true,
				Message:        "Migration completed successfully",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
		BatchSize:   customBatchSize,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_CustomBackoffInterval() {
	require := testutil.Require(s.T())

	startHeight := uint64(1000)
	endHeight := uint64(1200)
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(&activity.MigratorResponse{
			BlocksMigrated: 100,
			EventsMigrated: 500,
			Success:        true,
			Message:        "Migration completed successfully",
		}, nil)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight:     startHeight,
		EndHeight:       endHeight,
		Tag:             tag,
		EventTag:        eventTag,
		BackoffInterval: "2s",
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_ActivityFailure() {
	require := testutil.Require(s.T())

	startHeight := uint64(1000)
	endHeight := uint64(1200)
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(&activity.MigratorResponse{
			BlocksMigrated: 0,
			EventsMigrated: 0,
			Success:        false,
			Message:        "Migration failed due to connection error",
		}, nil)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
	})
	require.Error(err)
	require.Contains(err.Error(), "migration batch failed")
}

func (s *migratorTestSuite) TestMigrator_ValidateRequest() {
	require := testutil.Require(s.T())

	// StartHeight >= EndHeight should fail
	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight: 1200,
		EndHeight:   1000,
		Tag:         1,
		EventTag:    0,
	})
	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
	require.Contains(err.Error(), "Field validation for 'EndHeight' failed on the 'gtfield' tag")

	// StartHeight == EndHeight should fail
	_, err = s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight: 1000,
		EndHeight:   1000,
		Tag:         1,
		EventTag:    0,
	})
	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
}

func (s *migratorTestSuite) TestMigrator_InvalidBackoffInterval() {
	require := testutil.Require(s.T())

	startHeight := uint64(1000)
	endHeight := uint64(1200)
	tag := uint32(1)
	eventTag := uint32(0)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight:     startHeight,
		EndHeight:       endHeight,
		Tag:             tag,
		EventTag:        eventTag,
		BackoffInterval: "invalid-duration",
	})
	require.Error(err)
	require.Contains(err.Error(), "failed to parse backoff interval")
}

func (s *migratorTestSuite) TestMigrator_MultipleBatches() {
	require := testutil.Require(s.T())

	startHeight := uint64(1000)
	endHeight := uint64(1300) // 3 batches of 100
	tag := uint32(1)
	eventTag := uint32(0)

	callCount := 0
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			callCount++

			// Verify batch boundaries
			switch callCount {
			case 1:
				require.Equal(uint64(1000), request.StartHeight)
				require.Equal(uint64(1100), request.EndHeight)
			case 2:
				require.Equal(uint64(1100), request.StartHeight)
				require.Equal(uint64(1200), request.EndHeight)
			case 3:
				require.Equal(uint64(1200), request.StartHeight)
				require.Equal(uint64(1300), request.EndHeight)
			}

			return &activity.MigratorResponse{
				BlocksMigrated: 100,
				EventsMigrated: 500,
				Success:        true,
				Message:        "Batch migrated successfully",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
		Tag:         tag,
		EventTag:    eventTag,
	})
	require.NoError(err)
	require.Equal(3, callCount)
}
