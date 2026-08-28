package cron

import (
	"context"
	"errors"
	"sync"
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

	// The due/pending cohort fixtures are the SELECTABLE universe: what
	// production's ListRetentionCohorts could return. rawDueHeights models due
	// shadow rows that the due-floor candidate sees but the cohort query
	// excludes (active repair, canonical or metadata mismatch) — the gap
	// between the two is exactly what the cron's advance loop exists for.
	//
	// This double honors range semantics UNCONDITIONALLY, in both the floor
	// and the cohort listing. An earlier version returned every fixture
	// regardless of the requested window, so a probe searching entirely the
	// wrong range still looked correct and the INF-1416 starvation bug was
	// invisible to every test in this file.
	rawDueHeights  []uint64
	dueFloorErr    error
	dueFloorMinArg uint64
	dueFloorEndArg uint64
	dueFloorCalls  int
}

// dueFloorFixtures returns every height the production due-floor candidate
// would see: raw due rows plus the start height of every due-eligible or
// pending cohort (pending rows carry a passed delete_after, so the raw floor
// sees them too).
func (r *retentionCronCohortRepository) dueFloorFixtures(eligibilityCutoff time.Time) []uint64 {
	heights := append([]uint64{}, r.rawDueHeights...)
	for _, cohort := range r.due {
		if !cohort.EligibleAt.After(eligibilityCutoff) {
			heights = append(heights, cohort.StartHeight)
		}
	}
	for _, cohort := range r.pending {
		heights = append(heights, cohort.StartHeight)
	}
	return heights
}

func (r *retentionCronCohortRepository) RetentionDueFloor(
	_ context.Context,
	_ string,
	_ uint32,
	minHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
) (uint64, bool, error) {
	r.dueFloorCalls++
	r.dueFloorMinArg = minHeight
	r.dueFloorEndArg = endHeight
	if r.dueFloorErr != nil {
		return 0, false, r.dueFloorErr
	}
	found := false
	lowest := uint64(0)
	for _, height := range r.dueFloorFixtures(eligibilityCutoff) {
		if height < minHeight || height >= endHeight {
			continue
		}
		if !found || height < lowest {
			found = true
			lowest = height
		}
	}
	return lowest, found, nil
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
	// Honour the requested height range unconditionally. Without this the
	// double returns due cohorts no matter what window was asked for, so a
	// probe that searches the wrong window still looks correct and the bug is
	// invisible to tests.
	return filterCohortsToRange(r.pending, startHeight, endHeight),
		filterCohortsToRange(r.due, startHeight, endHeight),
		nil
}

func filterCohortsToRange(cohorts []retirement.RetentionCohort, startHeight, endHeight uint64) []retirement.RetentionCohort {
	filtered := make([]retirement.RetentionCohort, 0, len(cohorts))
	for _, cohort := range cohorts {
		if cohort.StartHeight >= startHeight && cohort.StartHeight < endHeight {
			filtered = append(filtered, cohort)
		}
	}
	return filtered
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
	// The probe walks ONE WINDOW anchored at the due floor — not the whole
	// envelope (that is what times out, INF-1416) and not the watermark (a
	// not-yet-due row there would hide due work above the window) — and asks
	// for a full workflow batch so pending-cohort ordering cannot mask the
	// oldest due work.
	require.Equal(t, uint64(430_000_000), cohortRepository.requestedStartHeight)
	require.Equal(t, uint64(431_000_000), cohortRepository.requestedEndHeight,
		"the probe must be clipped to one window above the due floor")
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.dueFloorMinArg,
		"the floor candidate search is bounded below by the approved floor")
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, cohortRepository.dueFloorEndArg,
		"the floor candidate search is bounded above by the approved end")
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
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	// A due row must exist for the cohort probe to run at all; without one the
	// floor candidate reports nothing due and the tick idles before Select.
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/cold.cscb.zstd",
		StartHeight:           cfg.Cron.SingleBlockRetention.ApprovedStartHeight,
		EndHeight:             cfg.Cron.SingleBlockRetention.ApprovedStartHeight + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}
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
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/floor.cscb.zstd",
		StartHeight:           cfg.Cron.SingleBlockRetention.ApprovedStartHeight,
		EndHeight:             cfg.Cron.SingleBlockRetention.ApprovedStartHeight + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.dueFloorMinArg,
		"a watermark below the approved floor must not lower the floor search")
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

// recordingScope captures gauge writes so the probe_failed contract can be
// asserted directly rather than inferred.
type recordingScope struct {
	tally.Scope
	mu     sync.Mutex
	gauges map[string]float64
}

func newRecordingScope() *recordingScope {
	return &recordingScope{Scope: tally.NoopScope, gauges: make(map[string]float64)}
}

func (s *recordingScope) SubScope(string) tally.Scope { return s }

func (s *recordingScope) Gauge(name string) tally.Gauge {
	return &recordingGauge{scope: s, name: name}
}

func (s *recordingScope) get(name string) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.gauges[name]
	return v, ok
}

type recordingGauge struct {
	scope *recordingScope
	name  string
}

func (g *recordingGauge) Update(v float64) {
	g.scope.mu.Lock()
	defer g.scope.mu.Unlock()
	g.scope.gauges[g.name] = v
}

// TestSingleBlockRetentionCronProbeFailedGaugeTracksOutcome pins the signal the
// failure alert depends on.
//
// It must be a gauge written on every exit path, not the instrument's
// result_type="error" counter: tally suppresses zero counter deltas, so that
// counter emits nothing until the first failure and then goes silent, leaving a
// single isolated sample that PromQL increase() cannot turn into a delta. The
// first outage would be missed — the one case the alert exists for.
func TestSingleBlockRetentionCronProbeFailedGaugeTracksOutcome(t *testing.T) {
	t.Run("failure reports 1", func(t *testing.T) {
		task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
		defer ctrl.Finish()
		scope := newRecordingScope()
		task.metrics = scope
		cohortRepository.watermarkErr = errors.New("probe exploded")

		require.Error(t, task.Run(context.Background()))
		require.Empty(t, runtime.executions)

		got, ok := scope.get("probe_failed")
		require.True(t, ok, "a failed tick must still write the gauge")
		require.Equal(t, float64(1), got)
	})

	t.Run("idle tick reports 0", func(t *testing.T) {
		// Nothing due is a success, not a failure — otherwise the alert would
		// fire every time retention has simply caught up.
		task, runtime, _, _, _, ctrl := newSingleBlockRetentionCronTask(t)
		defer ctrl.Finish()
		scope := newRecordingScope()
		task.metrics = scope

		require.NoError(t, task.Run(context.Background()))
		require.Empty(t, runtime.executions)

		got, ok := scope.get("probe_failed")
		require.True(t, ok, "an idle tick must write a 0 baseline, not stay silent")
		require.Equal(t, float64(0), got)
	})

	t.Run("successful sweep reports 0", func(t *testing.T) {
		task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
		defer ctrl.Finish()
		scope := newRecordingScope()
		task.metrics = scope
		cohortRepository.due = []retirement.RetentionCohort{{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           430_000_000,
			EndHeight:             430_001_000,
			RowCount:              1_000,
			EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
		}}

		require.NoError(t, task.Run(context.Background()))
		require.Len(t, runtime.executions, 1)

		got, ok := scope.get("probe_failed")
		require.True(t, ok)
		require.Equal(t, float64(0), got)
	})
}

// TestSingleBlockRetentionCronBoundsProbeByWindowBlocks is the regression guard
// for INF-1416. WindowBlocks used to bound only the launched sweep, so the probe
// spanned [watermark, approvedEnd] however far behind retention had fallen and
// its cost grew with the backlog instead of with one tick's work.
//
// That is not a tuning nit. Past roughly 2-3M blocks of width the planner
// abandons the index path for the due-cohort expansion join and sequentially
// scans the whole block_metadata table; measured on robinhood-mainnet prod, 250k
// of width ran in 20.7s, 1M in 49.2s, and 6.74M did not finish inside the 60s
// statement timeout. Nor can it recover on its own: the watermark only advances
// once deletes land, deletes need the probe to return cohorts, and the frontier
// keeps widening the range meanwhile.
func TestSingleBlockRetentionCronBoundsProbeByWindowBlocks(t *testing.T) {
	task, _, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200_000
	// A cold backlog: the watermark sits at the approved floor while the
	// approved end is ~13.7M blocks above it, and due work starts right at
	// the floor.
	cohortRepository.watermark = cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	cohortRepository.watermarkFound = true
	cohortRepository.rawDueHeights = []uint64{cfg.Cron.SingleBlockRetention.ApprovedStartHeight}

	require.NoError(t, task.Run(context.Background()))

	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.requestedStartHeight)
	require.Equal(t,
		cfg.Cron.SingleBlockRetention.ApprovedStartHeight+200_000,
		cohortRepository.requestedEndHeight,
		"probe must span exactly one window regardless of how large the backlog is")
	require.Less(t,
		cohortRepository.requestedEndHeight-cohortRepository.requestedStartHeight,
		cfg.Cron.SingleBlockRetention.ApprovedEndHeight-cfg.Cron.SingleBlockRetention.ApprovedStartHeight,
		"the whole point is that the probe is narrower than the approved envelope")
}

// TestSingleBlockRetentionCronProbesWholeRangeWhenNarrowerThanWindow pins the
// other side: when the remaining range is already smaller than one window the
// probe must not be padded out past the approved end.
func TestSingleBlockRetentionCronProbesWholeRangeWhenNarrowerThanWindow(t *testing.T) {
	task, _, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 1_000_000
	cohortRepository.watermark = cfg.Cron.SingleBlockRetention.ApprovedEndHeight - 50_000
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/tail.cscb.zstd",
		StartHeight:           cfg.Cron.SingleBlockRetention.ApprovedEndHeight - 50_000,
		EndHeight:             cfg.Cron.SingleBlockRetention.ApprovedEndHeight - 50_000 + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight-50_000, cohortRepository.requestedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, cohortRepository.requestedEndHeight,
		"a range narrower than the window must not be widened past the approved end")
}

// TestSingleBlockRetentionCronKeepsSweepAuthorizationAtApprovedEnd pins that
// narrowing the probe never narrows -- or widens -- what the sweep is authorized
// to delete. An earlier revision of the INF-1416 fix clipped the sweep window to
// the probe end too; that underflowed when the anchor sat above the probe end
// and silently pushed the window PAST the approved envelope.
func TestSingleBlockRetentionCronKeepsSweepAuthorizationAtApprovedEnd(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200_000
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/tail.cscb.zstd",
		StartHeight:           cfg.Cron.SingleBlockRetention.ApprovedEndHeight - 1_000,
		EndHeight:             cfg.Cron.SingleBlockRetention.ApprovedEndHeight,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	require.Len(t, runtime.executions, 1)
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.LessOrEqual(t, request.EndHeight, cfg.Cron.SingleBlockRetention.ApprovedEndHeight,
		"the sweep window must never extend past the approved end")
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, request.ApprovedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.ApprovedEndHeight)
}

// TestSingleBlockRetentionCronFindsDueWorkBeyondAnEmptyFirstWindow is the
// regression guard for the review finding on INF-1416: a windowed probe must not
// be able to hide due work behind a not-yet-due row at its floor.
//
// RetentionFloorWatermark is the lowest UNDELETED height and deliberately does
// not filter on due time, so it pins to a row that is not due yet. Anchoring a
// bounded probe there searches [watermark, watermark+window), finds nothing, and
// idles -- and because the watermark is recomputed identically every tick, due
// work above the window is never discovered until the low row matures. The
// pre-windowed probe spanned the whole approved range and could not hide it.
//
// The interleaving is reachable in production: out-of-order backfill and repair
// promotion set each row's retention clock independently.
//
// This case is range-aware on purpose. A double that returns due cohorts
// regardless of the requested window cannot observe this bug at all -- it would
// pass while the probe searched entirely the wrong range.
func TestSingleBlockRetentionCronFindsDueWorkBeyondAnEmptyFirstWindow(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200

	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// The watermark pins here: undeleted, but NOT due for another hour.
	cohortRepository.watermark = floor
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{{
		// Due, but 300 blocks up — outside a 200-block window anchored at the
		// watermark.
		ConsolidatedObjectKey: "consolidated/later.cscb.zstd",
		StartHeight:           floor + 300,
		EndHeight:             floor + 400,
		RowCount:              100,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	require.Len(t, runtime.executions, 1,
		"due work 300 blocks above a 200-block window must still be found; anchoring the window on the not-yet-due watermark hides it forever")
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, floor+300, request.StartHeight)
	// The probe anchored on the due floor, not the watermark.
	require.Equal(t, floor+300, cohortRepository.requestedStartHeight)
	require.Equal(t, floor+500, cohortRepository.requestedEndHeight)
	// Authorization is still the operator's envelope, unchanged.
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, request.ApprovedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.ApprovedEndHeight)
}

// TestSingleBlockRetentionCronIdlesWhenNothingIsDueAnywhere pins the other
// reading of an empty probe: with the window anchored on the due floor, "no due
// floor" genuinely means nothing is due in the whole approved range, and idling
// is then correct rather than a hidden backlog.
func TestSingleBlockRetentionCronIdlesWhenNothingIsDueAnywhere(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()

	require.NoError(t, task.Run(context.Background()))

	require.Empty(t, runtime.executions)
	require.Zero(t, cohortRepository.requestedEndHeight,
		"with nothing due the cron must not run the expensive due-cohort probe at all")
}

// TestSingleBlockRetentionCronAdvancesPastUnselectableWindow is the regression
// guard for the second-round review finding on INF-1416. The due-floor
// candidate deliberately does not evaluate join-level selectability, so a due
// row covered by an active repair (or missing canonical membership) can sit at
// the floor. Anchoring one window there and idling when it selects nothing
// would pin the search on that row forever — the deeper version of the same
// starvation the first fix addressed. The cron must step past such a window
// and find the selectable cohort above it, in the same tick.
func TestSingleBlockRetentionCronAdvancesPastUnselectableWindow(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// A due shadow row the cohort query cannot return (e.g. repair-covered):
	// visible to the floor candidate, absent from the selectable fixtures.
	cohortRepository.rawDueHeights = []uint64{floor}
	// The selectable cohort sits beyond the first 200-block window.
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/selectable.cscb.zstd",
		StartHeight:           floor + 300,
		EndHeight:             floor + 400,
		RowCount:              100,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	require.Len(t, runtime.executions, 1,
		"a due-but-unselectable row at the floor must not hide the selectable cohort above the window")
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, floor+300, request.StartHeight)
	require.Equal(t, floor+300, cohortRepository.requestedStartHeight,
		"the second probe attempt must anchor on the next due floor, past the dead window")
	require.GreaterOrEqual(t, cohortRepository.dueFloorCalls, 2,
		"stepping past a dead window requires a fresh floor lookup")
}

// TestSingleBlockRetentionCronAlarmsWhenAdvancesAreExhausted pins that a tick
// which steps past its full advance budget without finding selectable work
// raises probe_advance_exhausted instead of idling silently. Due work exists
// and nothing was launched; the next tick will walk the same dead windows, so
// this state needs a human, not quiet.
func TestSingleBlockRetentionCronAlarmsWhenAdvancesAreExhausted(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	scope := newRecordingScope()
	task.metrics = scope
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// Unselectable due rows in every window the advance budget can reach.
	for i := uint64(0); i <= uint64(maxRetentionProbeAdvances); i++ {
		cohortRepository.rawDueHeights = append(cohortRepository.rawDueHeights, floor+i*200)
	}

	require.NoError(t, task.Run(context.Background()))

	require.Empty(t, runtime.executions)
	got, ok := scope.get("probe_advance_exhausted")
	require.True(t, ok, "an exhausted tick must write the gauge")
	require.Equal(t, float64(1), got)
	require.Equal(t, maxRetentionProbeAdvances, cohortRepository.dueFloorCalls)
}

// TestSingleBlockRetentionCronResetsProbeGaugesOnNoProbeTicks is the two-tick
// regression for the gauge-staleness review warning: after a tick that probed a
// real window, a later tick that never probes (approved range fully retired)
// must not keep exposing the previous tick's scanned width or floor — a stale
// probe_window_blocks claims a scan that never ran.
func TestSingleBlockRetentionCronResetsProbeGaugesOnNoProbeTicks(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	scope := newRecordingScope()
	task.metrics = scope
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           floor,
		EndHeight:             floor + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	// Tick 1: a real probe over a real window.
	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	width, ok := scope.get("probe_window_blocks")
	require.True(t, ok)
	require.Greater(t, width, float64(0))

	// Tick 2: everything approved is retired — the completed-range early
	// return, which never probes.
	cohortRepository.watermark = cfg.Cron.SingleBlockRetention.ApprovedEndHeight
	cohortRepository.watermarkFound = true
	runtime.openWorkflowID = nil
	require.NoError(t, task.Run(context.Background()))

	width, ok = scope.get("probe_window_blocks")
	require.True(t, ok)
	require.Zero(t, width, "a no-probe tick must not expose the previous tick's scanned width")
	dueFloorHeight, ok := scope.get("due_floor_height")
	require.True(t, ok)
	require.Zero(t, dueFloorHeight)
}
