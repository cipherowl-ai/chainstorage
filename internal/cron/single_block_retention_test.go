package cron

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	workflowpkg "github.com/coinbase/chainstorage/internal/workflow"
	workflowactivity "github.com/coinbase/chainstorage/internal/workflow/activity"
)

type retentionCronCohortRepository struct {
	pending []retirement.RetentionCohort
	due     []retirement.RetentionCohort
	err     error

	requestedStartHeight uint64
	requestedEndHeight   uint64
	requestedLimit       int

	// watermark drives the probe floor. found=false makes the selector fall
	// back to the operator's approved_start_height, which is the behaviour
	// every pre-existing case in this file expects.
	watermark          uint64
	watermarkFound     bool
	watermarkErr       error
	watermarkMinHeight uint64
}

func (r *retentionCronCohortRepository) RetentionFloorWatermark(
	_ context.Context,
	_ string,
	_ uint32,
	minHeight uint64,
) (uint64, bool, error) {
	r.watermarkMinHeight = minHeight
	if r.watermarkErr != nil {
		return 0, false, r.watermarkErr
	}
	return r.watermark, r.watermarkFound, nil
}

func (r *retentionCronCohortRepository) ListRetentionCohorts(
	_ context.Context,
	_ string,
	_ string,
	_ uint32,
	startHeight uint64,
	endHeight uint64,
	_ time.Time,
	limit int,
) ([]retirement.RetentionCohort, []retirement.RetentionCohort, error) {
	r.requestedStartHeight = startHeight
	r.requestedEndHeight = endHeight
	r.requestedLimit = limit
	if r.err != nil {
		return nil, nil, r.err
	}
	return r.pending, r.due, nil
}

func newSingleBlockRetentionCronTask(t *testing.T, configOpts ...config.ConfigOption) (
	*singleBlockRetentionTask,
	*batchConsolidatorCronRuntime,
	*retentionCronCohortRepository,
	*metastoragemocks.MockMetaStorage,
	*config.Config,
	*gomock.Controller,
) {
	t.Helper()
	cfg, err := config.New(configOpts...)
	require.NoError(t, err)
	cfg.Chain.BlockTag.Stable = 2
	cfg.Chain.BlockTag.Latest = 2
	cfg.StorageType.MetaStorageType = config.MetaStorageType_POSTGRES
	cfg.AWS.Postgres = &config.PostgresConfig{}
	cfg.Cron.SingleBlockRetention = config.SingleBlockRetentionCronConfig{
		Enabled:                     true,
		MaxObjectRanges:             250,
		WorkflowParallelism:         10,
		WindowBlocks:                1_000_000,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         423_300_000,
		ApprovedEndHeight:           437_068_000,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ProductionDeleteEnabled:     true,
	}

	logger := zaptest.NewLogger(t)
	runtime := &batchConsolidatorCronRuntime{logger: logger}
	activity := workflowactivity.NewSingleBlockRetention(workflowactivity.SingleBlockRetentionParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Runtime: runtime,
	})
	retentionWorkflow := workflowpkg.NewSingleBlockRetention(workflowpkg.SingleBlockRetentionParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Runtime:  runtime,
		Activity: activity,
	})
	ctrl := gomock.NewController(t)
	metaStorage := metastoragemocks.NewMockMetaStorage(ctrl)
	task, err := NewSingleBlockRetention(SingleBlockRetentionTaskParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Config:               cfg,
		Runtime:              runtime,
		MetaStorage:          metaStorage,
		SingleBlockRetention: retentionWorkflow,
	})
	require.NoError(t, err)
	retentionTask := task.(*singleBlockRetentionTask)
	cohortRepository := &retentionCronCohortRepository{}
	retentionTask.selectorFactory = func(ctx context.Context) (*retirement.Selector, error) {
		return retirement.NewSelector(cohortRepository), nil
	}
	return retentionTask, runtime, cohortRepository, metaStorage, cfg, ctrl
}

var _ cadence.Runtime = (*batchConsolidatorCronRuntime)(nil)

func TestSingleBlockRetentionCronLaunchesWindowAnchoredAtOldestDueCohort(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, "workflow.single_block_retention/auto_retain", runtime.executions[0].options.ID)
	request, ok := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.True(t, ok)
	require.True(t, request.Execute)
	require.Equal(t, uint64(430_000_000), request.StartHeight)
	require.Equal(t, uint64(431_000_000), request.EndHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedChain, request.ApprovedChain)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, request.ApprovedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.ApprovedEndHeight)
	require.Equal(t, 250, request.MaxObjectRanges)
	require.Equal(t, 10, request.Parallelism)
	require.True(t, request.ProductionDeleteEnabled)
	require.True(t, request.DirectStorageClientsGuarded)
	require.True(t, request.SingleBlockWritersGuarded)
	require.True(t, request.FallbackReadsValidated)
	require.Zero(t, request.FallbackErrorCount)
	require.False(t, request.EligibilityCutoff.IsZero())
	require.WithinDuration(t, time.Now().UTC(), request.EligibilityCutoff, time.Minute)
	require.Nil(t, request.Checkpoint)
	require.Equal(t, cfg.GetEffectiveBlockTag(0), request.Tag)
	// The probe walks the approved envelope, not an unbounded range, and asks
	// for a full workflow batch so pending-cohort ordering cannot mask the
	// oldest due work.
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.requestedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, cohortRepository.requestedEndHeight)
	require.Equal(t, retirement.MaxRetentionCohortsPerWorkflow+1, cohortRepository.requestedLimit)
}

func TestSingleBlockRetentionCronAnchorsBelowPendingCohorts(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	// A stuck in-flight cohort near the envelope tail sorts ahead of due
	// cohorts in the selector merge; the window must still anchor at the
	// lowest height in the probe set.
	cohortRepository.pending = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/stuck.cscb.zstd",
		StartHeight:           436_900_000,
		EndHeight:             436_901_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-30 * time.Minute),
	}}
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/oldest.cscb.zstd",
		StartHeight:           424_000_000,
		EndHeight:             424_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-48 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, uint64(424_000_000), request.StartHeight)
	require.Equal(t, uint64(425_000_000), request.EndHeight)
}

func TestSingleBlockRetentionCronClipsWindowToApprovedEnd(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 1_000_000
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/tail.cscb.zstd",
		StartHeight:           437_000_000,
		EndHeight:             437_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, uint64(437_000_000), request.StartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.EndHeight)
}

func TestSingleBlockRetentionCronIdlesWhenNothingIsDue(t *testing.T) {
	task, runtime, _, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestSingleBlockRetentionCronSkipsWhenRetentionWorkflowIsOpen(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	runtime.openWorkflowID = []string{"workflow.single_block_retention/manual_run"}
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
	require.Equal(t, "workflow.single_block_retention", runtime.requestedWorkflowType)
}

func TestSingleBlockRetentionCronResolvesOpenEndedApprovalFromConsolidationCursor(t *testing.T) {
	task, runtime, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.ApprovedEndHeight = 0
	cfg.Cron.SingleBlockRetention.AllowOpenEndedApproval = true
	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(438_000_000), true, nil)
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, uint64(438_000_000), request.ApprovedEndHeight)
	require.Equal(t, uint64(430_000_000), request.StartHeight)
	require.Equal(t, uint64(431_000_000), request.EndHeight)
}

func TestSingleBlockRetentionCronIdlesWithoutConsolidationCursor(t *testing.T) {
	task, runtime, _, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.ApprovedEndHeight = 0
	cfg.Cron.SingleBlockRetention.AllowOpenEndedApproval = true
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(uint64(0), false, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestSingleBlockRetentionCronFailsClosedOnIncompleteStandingApproval(t *testing.T) {
	testCases := []struct {
		name     string
		mutate   func(cronConfig *config.SingleBlockRetentionCronConfig)
		expected string
	}{
		{
			name: "missing approved chain",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.ApprovedChain = ""
			},
			expected: "approved_chain",
		},
		{
			name: "open-ended without opt-in",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.ApprovedEndHeight = 0
				cronConfig.AllowOpenEndedApproval = false
			},
			expected: "allow_open_ended_approval",
		},
		{
			name: "inverted approved range",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.ApprovedEndHeight = cronConfig.ApprovedStartHeight
			},
			expected: "is invalid",
		},
		{
			name: "direct storage clients unguarded",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.DirectStorageClientsGuarded = false
			},
			expected: "direct_storage_clients_guarded",
		},
		{
			name: "single-block writers unguarded",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.SingleBlockWritersGuarded = false
			},
			expected: "single_block_writers_guarded",
		},
		{
			name: "fallback reads unvalidated",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.FallbackReadsValidated = false
			},
			expected: "fallback_reads_validated",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			task, runtime, _, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
			defer ctrl.Finish()
			testCase.mutate(&cfg.Cron.SingleBlockRetention)

			err := task.Run(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.expected)
			require.Empty(t, runtime.executions)
		})
	}
}

func TestSingleBlockRetentionCronRequiresProductionDeleteEnablementInProduction(t *testing.T) {
	task, runtime, _, _, cfg, ctrl := newSingleBlockRetentionCronTask(t, config.WithEnvironment(config.EnvProduction))
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.ProductionDeleteEnabled = false

	err := task.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "production_delete_enabled")
	require.Empty(t, runtime.executions)
}

func TestSingleBlockRetentionCronPropagatesProbeErrors(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.err = errors.New("connection reset")

	err := task.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to probe due retention cohorts")
	require.Empty(t, runtime.executions)
}

func TestSingleBlockRetentionCronDefaults(t *testing.T) {
	task, _, _, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()

	require.Equal(t, "single_block_retention", task.Name())
	require.Equal(t, defaultSingleBlockRetentionCronSpec, task.Spec())
	require.Equal(t, int64(1), task.Parallelism())
	require.True(t, task.Enabled())
	cfg.Cron.SingleBlockRetention.Enabled = false
	require.False(t, task.Enabled())
}

// TestSingleBlockRetentionCronProbesFromTheFloorWatermark is the regression
// guard for INF-1330. The probe must start at the watermark, not the operator's
// approved floor: the due-cohort query's cohort-expansion join is bounded only
// by the height range, and past roughly 1.5M heights of width the planner drops
// idx_block_consolidation_shadow_tag_height for a sequential scan and the probe
// stops finishing inside its statement timeout.
func TestSingleBlockRetentionCronProbesFromTheFloorWatermark(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.watermark = 436_000_000
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           436_000_000,
		EndHeight:             436_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	// The watermark bounds the probe...
	require.Equal(t, uint64(436_000_000), cohortRepository.requestedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.watermarkMinHeight,
		"the approved floor must bound the watermark lookup so it stays cheap")

	// ...but authorization is unchanged. The workflow still carries the
	// operator's approved envelope, so narrowing the probe can never widen what
	// the sweep is permitted to delete.
	require.Len(t, runtime.executions, 1)
	request, ok := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.True(t, ok)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, request.ApprovedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.ApprovedEndHeight)
}

// TestSingleBlockRetentionCronKeepsApprovedFloorWhenWatermarkIsLower pins that
// a watermark below the approval floor never drags the probe underneath it.
// Retention may not delete there, so work found below is not the probe's
// business.
func TestSingleBlockRetentionCronKeepsApprovedFloorWhenWatermarkIsLower(t *testing.T) {
	task, _, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.watermark = 1_000
	cohortRepository.watermarkFound = true

	require.NoError(t, task.Run(context.Background()))
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.requestedStartHeight)
}

// TestSingleBlockRetentionCronIdlesWhenWatermarkPassesApprovedEnd covers the
// finished-sweep case: with no outstanding work at or below the approved end
// there is nothing to probe for, so the tick must idle instead of launching a
// workflow over an empty range.
func TestSingleBlockRetentionCronIdlesWhenWatermarkPassesApprovedEnd(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.watermark = cfg.Cron.SingleBlockRetention.ApprovedEndHeight
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
	require.Zero(t, cohortRepository.requestedStartHeight, "the probe must not run at all")
}

// TestSingleBlockRetentionCronPropagatesWatermarkErrors keeps a failing
// watermark lookup loud. Falling back to the approved floor would work today
// and quietly reintroduce the unbounded range this mechanism exists to prevent.
func TestSingleBlockRetentionCronPropagatesWatermarkErrors(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.watermarkErr = errors.New("watermark unavailable")

	err := task.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "watermark")
	require.Empty(t, runtime.executions)
}
