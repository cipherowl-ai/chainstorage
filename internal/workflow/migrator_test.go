package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

const (
	migratorCheckpointSize = 50000
	migratorBatchSize      = 5000
)

type migratorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env      *cadence.TestEnv
	migrator *Migrator
	app      testapp.TestApp
	cfg      *config.Config

	watermarkRequests []*activity.SetPostgresWatermarkRequest
	watermarkErr      error
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
	cfg.Workflows.Migrator.Parallelism = 8
	s.cfg = cfg

	s.env = cadence.NewTestEnv(s)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(s.cfg),
		cadence.WithTestEnv(s.env),
		fx.Populate(&s.migrator),
	)

	s.watermarkRequests = nil
	s.watermarkErr = nil
	s.env.OnActivity(activity.ActivitySetPostgresWatermark, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.SetPostgresWatermarkRequest) (*activity.SetPostgresWatermarkResponse, error) {
			s.watermarkRequests = append(s.watermarkRequests, request)
			if s.watermarkErr != nil {
				return nil, s.watermarkErr
			}
			return &activity.SetPostgresWatermarkResponse{Height: 600, Hash: "0x600"}, nil
		}).
		Maybe()
}

func (s *migratorTestSuite) TearDownTest() {
	s.app.Close()
	s.env.AssertExpectations(s.T())
}

func (s *migratorTestSuite) TestMigrator_EventDriven_Success() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	endSequence := int64(6000) // 5000 events, fits in one batch
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			require.Equal(tag, request.Tag)
			expectedEventTag := s.cfg.Workflows.Migrator.GetEffectiveEventTag(eventTag)
			require.Equal(expectedEventTag, request.EventTag)

			// Event-driven migration processes both blocks and events
			eventCount := request.EndEventSequence - request.StartEventSequence
			blockCount := eventCount / 10 // Assume roughly 10% are BLOCK_ADDED events

			return &activity.MigratorResponse{
				BlocksMigrated: int(blockCount),
				EventsMigrated: int(eventCount),
				Success:        true,
				Message:        "Event-driven migration completed successfully",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_WithCheckpoint() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	endSequence := startSequence + int64(migratorCheckpointSize) + 10000 // Exceed checkpoint
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(&activity.MigratorResponse{
			BlocksMigrated: 500,
			EventsMigrated: 5000,
			Success:        true,
			Message:        "Migration batch completed",
		}, nil)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))
	require.Empty(s.watermarkRequests, "the watermark belongs to the end of the run, not to a checkpoint")
}

func (s *migratorTestSuite) TestMigrator_CustomBatchSize() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	endSequence := int64(3000)
	tag := uint32(1)
	eventTag := uint32(0)
	customBatchSize := uint64(500)

	callCount := 0
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			callCount++
			// Each batch should be customBatchSize
			batchSize := request.EndEventSequence - request.StartEventSequence
			require.LessOrEqual(batchSize, int64(customBatchSize))

			return &activity.MigratorResponse{
				BlocksMigrated: 50,
				EventsMigrated: int(batchSize),
				Success:        true,
				Message:        "Batch completed",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		BatchSize:          customBatchSize,
	})
	require.NoError(err)
	// Should have been called 4 times (2000 events / 500 per batch)
	require.Equal(4, callCount)
}

func (s *migratorTestSuite) TestMigrator_AutoResume() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	eventTag := uint32(3)

	// Mock GetLatestEventFromPostgres to return a sequence
	s.env.OnActivity(activity.ActivityGetLatestEventFromPostgres, mock.Anything, mock.Anything).
		Return(&activity.GetLatestEventFromPostgresResponse{
			Sequence: int64(5000),
			Height:   uint64(1000),
			Found:    true,
		}, nil).Once()

	// The workflow should resume from sequence 5001
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			// Should start from next sequence after latest
			require.Equal(int64(5001), request.StartEventSequence)

			return &activity.MigratorResponse{
				BlocksMigrated: 100,
				EventsMigrated: 1000,
				Success:        true,
				Message:        "Resumed migration",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: 0, // Will be auto-detected
		EndEventSequence:   10000,
		Tag:                tag,
		EventTag:           eventTag,
		AutoResume:         true,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_ContinuousSync() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	tag := uint32(1)
	eventTag := uint32(0)

	// Mock GetMaxEventId activity since EndEventSequence is 0
	s.env.OnActivity(activity.ActivityGetMaxEventId, mock.Anything, mock.Anything).
		Return(&activity.GetMaxEventIdResponse{
			MaxEventId: int64(6000),
			Found:      true,
		}, nil).Once()

	callCount := 0
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			callCount++
			if callCount > 2 {
				// Stop after 2 iterations to prevent infinite loop in test
				s.env.CancelWorkflow()
			}

			return &activity.MigratorResponse{
				BlocksMigrated: 100,
				EventsMigrated: 1000,
				Success:        true,
				Message:        "Batch migrated",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   0, // Will be auto-detected for continuous sync
		Tag:                tag,
		EventTag:           eventTag,
		ContinuousSync:     true,
		SyncInterval:       "1s",
	})

	// Should get a continue-as-new error for continuous sync
	if err != nil {
		require.True(IsContinueAsNewError(err) || s.env.IsWorkflowCompleted())
	}
}

func (s *migratorTestSuite) TestMigrator_ContinuousSync_CopiesLastEventWithoutWatermark() {
	require := testutil.Require(s.T())

	s.mockMaxEventId(6000)
	var requests []*activity.MigratorRequest
	s.mockMigrator(&requests)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: 6000,
		Tag:                1,
		ContinuousSync:     true,
		SyncInterval:       "1s",
	})
	require.True(IsContinueAsNewError(err))
	require.Len(requests, 1)
	require.Equal(int64(6000), requests[0].StartEventSequence)
	require.Equal(int64(6001), requests[0].EndEventSequence)
	require.Empty(s.watermarkRequests)
}

func (s *migratorTestSuite) TestMigrator_ContinuousSync_CaughtUp() {
	require := testutil.Require(s.T())

	s.mockMaxEventId(6000)
	var requests []*activity.MigratorRequest
	s.mockMigrator(&requests)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: 6001,
		Tag:                1,
		ContinuousSync:     true,
		SyncInterval:       "1s",
	})
	require.True(IsContinueAsNewError(err))
	require.Empty(requests)
	require.Empty(s.watermarkRequests)
}

func (s *migratorTestSuite) TestMigrator_BoundedRunSetsWatermark() {
	require := testutil.Require(s.T())

	var requests []*activity.MigratorRequest
	s.mockMigrator(&requests)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: 1000,
		EndEventSequence:   2000,
		Tag:                2,
		EventTag:           3,
	})
	require.NoError(err)
	require.Len(requests, 1)
	require.Equal([]*activity.SetPostgresWatermarkRequest{{Tag: 2, EventTag: 3}}, s.watermarkRequests)
}

func (s *migratorTestSuite) TestMigrator_BoundedRunFailsWhenWatermarkFails() {
	require := testutil.Require(s.T())

	s.watermarkErr = xerrors.New("no canonical block at the tip")
	var requests []*activity.MigratorRequest
	s.mockMigrator(&requests)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: 1000,
		EndEventSequence:   2000,
		Tag:                1,
	})
	require.Error(err)
	require.Contains(err.Error(), "no canonical block at the tip")
}

func (s *migratorTestSuite) TestMigrator_AutoDetectedEndIncludesLastEvent() {
	require := testutil.Require(s.T())

	s.mockMaxEventId(6000)
	var requests []*activity.MigratorRequest
	s.mockMigrator(&requests)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: 5000,
		Tag:                1,
	})
	require.NoError(err)
	require.Len(requests, 1)
	require.Equal(int64(5000), requests[0].StartEventSequence)
	require.Equal(int64(6001), requests[0].EndEventSequence)
	require.Len(s.watermarkRequests, 1)
}

func (s *migratorTestSuite) TestMigrator_AutoResumeCopiesLastEvent() {
	require := testutil.Require(s.T())

	s.mockLatestEventInPostgres(5999)
	s.mockMaxEventId(6000)
	var requests []*activity.MigratorRequest
	s.mockMigrator(&requests)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		Tag:        1,
		AutoResume: true,
	})
	require.NoError(err)
	require.Len(requests, 1)
	require.Equal(int64(6000), requests[0].StartEventSequence)
	require.Equal(int64(6001), requests[0].EndEventSequence)
	require.Len(s.watermarkRequests, 1)
}

func (s *migratorTestSuite) TestMigrator_CaughtUpBoundedRunSetsWatermark() {
	require := testutil.Require(s.T())

	s.mockLatestEventInPostgres(6000)
	s.mockMaxEventId(6000)
	var requests []*activity.MigratorRequest
	s.mockMigrator(&requests)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		Tag:        1,
		AutoResume: true,
	})
	require.NoError(err)
	require.Empty(requests)
	require.Len(s.watermarkRequests, 1)
}

func (s *migratorTestSuite) TestMigrator_DestinationAheadOfSourceFails() {
	require := testutil.Require(s.T())

	s.mockLatestEventInPostgres(6001)
	s.mockMaxEventId(6000)
	var requests []*activity.MigratorRequest
	s.mockMigrator(&requests)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		Tag:        1,
		AutoResume: true,
	})
	require.Error(err)
	require.Contains(err.Error(), "must be less than")
	require.Empty(requests)
	require.Empty(s.watermarkRequests)
}

func (s *migratorTestSuite) TestMigrator_ExplicitEmptyRangeFails() {
	require := testutil.Require(s.T())

	var requests []*activity.MigratorRequest
	s.mockMigrator(&requests)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: 1000,
		EndEventSequence:   1000,
		Tag:                1,
	})
	require.Error(err)
	require.Contains(err.Error(), "must be less than")
	require.Empty(requests)
	require.Empty(s.watermarkRequests)
}

func (s *migratorTestSuite) mockMaxEventId(maxEventId int64) {
	s.env.OnActivity(activity.ActivityGetMaxEventId, mock.Anything, mock.Anything).
		Return(&activity.GetMaxEventIdResponse{MaxEventId: maxEventId, Found: true}, nil).
		Maybe()
}

func (s *migratorTestSuite) mockLatestEventInPostgres(sequence int64) {
	s.env.OnActivity(activity.ActivityGetLatestEventFromPostgres, mock.Anything, mock.Anything).
		Return(&activity.GetLatestEventFromPostgresResponse{Sequence: sequence, Found: true}, nil).
		Once()
}

func (s *migratorTestSuite) mockMigrator(requests *[]*activity.MigratorRequest) {
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			*requests = append(*requests, request)
			return &activity.MigratorResponse{Success: true}, nil
		}).
		Maybe()
}

func (s *migratorTestSuite) TestMigrator_Parallelism() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	endSequence := int64(6000)
	tag := uint32(1)
	eventTag := uint32(0)
	parallelism := 4

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			// Verify parallelism is passed through
			require.Equal(parallelism, request.Parallelism)

			eventCount := request.EndEventSequence - request.StartEventSequence
			return &activity.MigratorResponse{
				BlocksMigrated: int(eventCount / 10),
				EventsMigrated: int(eventCount),
				Success:        true,
				Message:        "Parallel migration completed",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		Parallelism:        parallelism,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_LargeMigration() {
	require := testutil.Require(s.T())

	startSequence := int64(1000000)
	endSequence := int64(2000000) // 1 million events
	tag := uint32(1)
	eventTag := uint32(0)

	batchCount := 0
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			batchCount++

			// Each batch should be the configured batch size
			batchSize := request.EndEventSequence - request.StartEventSequence
			require.LessOrEqual(batchSize, int64(migratorBatchSize))

			// Simulate processing
			blockCount := batchSize / 10

			return &activity.MigratorResponse{
				BlocksMigrated: int(blockCount),
				EventsMigrated: int(batchSize),
				Success:        true,
				Message:        "Large batch processed",
			}, nil
		})

	// This should trigger checkpoints
	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		BatchSize:          migratorBatchSize,
		CheckpointSize:     migratorCheckpointSize,
	})

	// Should hit checkpoint and continue-as-new
	require.Error(err)
	require.True(IsContinueAsNewError(err))

	// Should have processed checkpoint size worth of events
	expectedBatches := int(migratorCheckpointSize / migratorBatchSize)
	require.Equal(expectedBatches, batchCount)
}
