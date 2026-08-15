package cron

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/workflow"
)

type (
	SingleBlockRetentionTaskParams struct {
		fx.In
		fxparams.Params
		Config               *config.Config
		Runtime              cadence.Runtime
		MetaStorage          metastorage.MetaStorage
		SingleBlockRetention *workflow.SingleBlockRetention
	}

	singleBlockRetentionTask struct {
		config               *config.Config
		logger               *zap.Logger
		metrics              tally.Scope
		runtime              cadence.Runtime
		metaStorage          metastorage.MetaStorage
		singleBlockRetention *workflow.SingleBlockRetention

		selectorMu      sync.Mutex
		selector        *retirement.Selector
		selectorFactory func(ctx context.Context) (*retirement.Selector, error)
	}
)

const (
	autoRetainSuffix                        = "auto_retain"
	defaultSingleBlockRetentionCronSpec     = "@every 1h"
	defaultSingleBlockRetentionWindowBlocks = uint64(250_000)
	singleBlockRetentionOpenPageSize        = 1000
)

func NewSingleBlockRetention(params SingleBlockRetentionTaskParams) (Task, error) {
	task := &singleBlockRetentionTask{
		config:               params.Config,
		logger:               log.WithPackage(params.Logger),
		metrics:              params.Metrics.SubScope("cron").SubScope("single_block_retention"),
		runtime:              params.Runtime,
		metaStorage:          params.MetaStorage,
		singleBlockRetention: params.SingleBlockRetention,
	}
	task.selectorFactory = task.newPostgresSelector
	return task, nil
}

func (t *singleBlockRetentionTask) Name() string {
	return "single_block_retention"
}

func (t *singleBlockRetentionTask) Spec() string {
	spec := t.config.Cron.SingleBlockRetention.Spec
	if spec == "" {
		return defaultSingleBlockRetentionCronSpec
	}
	return spec
}

func (t *singleBlockRetentionTask) Parallelism() int64 {
	parallelism := t.config.Cron.SingleBlockRetention.Parallelism
	if parallelism <= 0 {
		return 1
	}
	return parallelism
}

func (t *singleBlockRetentionTask) Enabled() bool {
	return t.config.Cron.SingleBlockRetention.Enabled
}

func (t *singleBlockRetentionTask) DelayStartDuration() time.Duration {
	return t.config.Cron.SingleBlockRetention.DelayStartDuration
}

// Run launches at most one bounded execute sweep anchored at the oldest due
// retention work. Eligibility is database state (a row leaves the due set only
// when its retirement is finalized as deleted-and-verified), so anchoring every
// window at the due minimum guarantees deferred, failed, or repair-re-created
// rows are re-selected by a later tick instead of skipped past.
func (t *singleBlockRetentionTask) Run(ctx context.Context) error {
	cronConfig := t.config.Cron.SingleBlockRetention
	if err := t.validateStandingApproval(cronConfig); err != nil {
		return err
	}

	workflowID := t.autoRetainWorkflowID()
	openWorkflowID, open, err := t.openSingleBlockRetentionWorkflow(ctx)
	if err != nil {
		return err
	}
	if open {
		t.logger.Info(
			"single_block_retention cron skipped because a retention workflow is already open",
			zap.String("open_workflow_id", openWorkflowID),
			zap.String("auto_workflow_id", workflowID),
		)
		return nil
	}

	tag := t.config.GetEffectiveBlockTag(0)
	approvedEnd, err := t.resolveApprovedEndHeight(ctx, cronConfig, tag)
	if err != nil {
		return err
	}
	if approvedEnd <= cronConfig.ApprovedStartHeight {
		t.logger.Info(
			"single_block_retention cron has no approved range yet",
			zap.Uint32("tag", tag),
			zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
			zap.Uint64("approved_end_height", approvedEnd),
		)
		return nil
	}

	bucket, err := t.config.WriteBlockStorageBucket()
	if err != nil {
		return xerrors.Errorf("failed to resolve write block storage bucket: %w", err)
	}
	storageGeneration, err := t.config.WriteBlockStorageGeneration()
	if err != nil {
		return xerrors.Errorf("failed to resolve write block storage generation: %w", err)
	}
	selector, err := t.getSelector(ctx)
	if err != nil {
		return err
	}
	eligibilityCutoff := time.Now().UTC()
	// One height-ordered cohort probes the due set; its cost is bounded by the
	// undeleted backlog through the partial retention-due index, never by the
	// width of the approved range.
	cohorts, hasMore, err := selector.Select(
		ctx,
		bucket,
		storageGeneration,
		tag,
		cronConfig.ApprovedStartHeight,
		approvedEnd,
		eligibilityCutoff,
		1,
	)
	if err != nil {
		return xerrors.Errorf("failed to probe due retention cohorts: %w", err)
	}
	if len(cohorts) == 0 {
		t.metrics.Gauge("oldest_due_age_seconds").Update(0)
		t.logger.Info(
			"single_block_retention cron found no due cohorts",
			zap.Uint32("tag", tag),
			zap.String("bucket", bucket),
			zap.String("storage_generation", storageGeneration),
			zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
			zap.Uint64("approved_end_height", approvedEnd),
			zap.Bool("has_more", hasMore),
		)
		return nil
	}
	anchor := cohorts[0]
	oldestDueAge := eligibilityCutoff.Sub(anchor.EligibleAt)
	t.metrics.Gauge("oldest_due_age_seconds").Update(oldestDueAge.Seconds())

	windowBlocks := cronConfig.WindowBlocks
	if windowBlocks == 0 {
		windowBlocks = defaultSingleBlockRetentionWindowBlocks
	}
	// The selected cohort lies inside the approved range, so the window is
	// always non-empty; it is clipped to keep each sweep bounded.
	windowStart := anchor.StartHeight
	windowEnd := approvedEnd
	if windowEnd-windowStart > windowBlocks {
		windowEnd = windowStart + windowBlocks
	}
	if windowEnd <= windowStart {
		return xerrors.Errorf(
			"single_block_retention cron derived an invalid window [%d, %d)",
			windowStart,
			windowEnd,
		)
	}

	request := &workflow.SingleBlockRetentionRequest{
		Tag:                         0,
		StartHeight:                 windowStart,
		EndHeight:                   windowEnd,
		EligibilityCutoff:           eligibilityCutoff,
		MaxObjectRanges:             cronConfig.MaxObjectRanges,
		Parallelism:                 cronConfig.WorkflowParallelism,
		Execute:                     true,
		ProductionDeleteEnabled:     cronConfig.ProductionDeleteEnabled,
		DirectStorageClientsGuarded: cronConfig.DirectStorageClientsGuarded,
		SingleBlockWritersGuarded:   cronConfig.SingleBlockWritersGuarded,
		FallbackReadsValidated:      cronConfig.FallbackReadsValidated,
		FallbackErrorCount:          0,
		ApprovedChain:               cronConfig.ApprovedChain,
		ApprovedStartHeight:         cronConfig.ApprovedStartHeight,
		ApprovedEndHeight:           approvedEnd,
	}
	workflowCtx := workflow.WithWorkflowID(ctx, workflowID)
	run, err := t.singleBlockRetention.Execute(workflowCtx, request)
	if err != nil {
		if isWorkflowAlreadyStarted(err) {
			t.logger.Info(
				"single_block_retention cron skipped because a retention workflow was already started",
				zap.String("workflow_id", workflowID),
			)
			return nil
		}
		return xerrors.Errorf("failed to start single_block_retention cron workflow: %w", err)
	}
	t.metrics.Counter("launched").Inc(1)
	t.logger.Info(
		"started single_block_retention cron workflow",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", run.GetRunID()),
		zap.Uint32("tag", tag),
		zap.String("bucket", bucket),
		zap.String("storage_generation", storageGeneration),
		zap.Uint64("window_start_height", windowStart),
		zap.Uint64("window_end_height", windowEnd),
		zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
		zap.Uint64("approved_end_height", approvedEnd),
		zap.Time("eligibility_cutoff", eligibilityCutoff),
		zap.Duration("oldest_due_age", oldestDueAge),
		zap.Int("max_object_ranges", cronConfig.MaxObjectRanges),
		zap.Int("workflow_parallelism", cronConfig.WorkflowParallelism),
	)
	return nil
}

// validateStandingApproval fails closed unless the reviewed configuration
// carries the complete operator approval a manual launch would have supplied.
func (t *singleBlockRetentionTask) validateStandingApproval(cronConfig config.SingleBlockRetentionCronConfig) error {
	if t.config.StorageType.MetaStorageType != config.MetaStorageType_POSTGRES || t.config.AWS.Postgres == nil {
		return xerrors.New("single_block_retention cron requires Postgres meta storage")
	}
	if cronConfig.ApprovedChain == "" {
		return xerrors.New("single_block_retention cron requires cron.single_block_retention.approved_chain")
	}
	if cronConfig.ApprovedEndHeight == 0 && !cronConfig.AllowOpenEndedApproval {
		return xerrors.New("single_block_retention cron requires approved_end_height or the explicit allow_open_ended_approval opt-in")
	}
	if cronConfig.ApprovedEndHeight != 0 && cronConfig.ApprovedEndHeight <= cronConfig.ApprovedStartHeight {
		return xerrors.Errorf(
			"single_block_retention cron approved range [%d, %d) is invalid",
			cronConfig.ApprovedStartHeight,
			cronConfig.ApprovedEndHeight,
		)
	}
	if !cronConfig.DirectStorageClientsGuarded {
		return xerrors.New("single_block_retention cron requires direct_storage_clients_guarded")
	}
	if !cronConfig.SingleBlockWritersGuarded {
		return xerrors.New("single_block_retention cron requires single_block_writers_guarded")
	}
	if !cronConfig.FallbackReadsValidated {
		return xerrors.New("single_block_retention cron requires fallback_reads_validated")
	}
	if isProductionRetentionCronEnvironment(t.config.Env()) && !cronConfig.ProductionDeleteEnabled {
		return xerrors.New("single_block_retention cron requires production_delete_enabled in production")
	}
	return nil
}

// resolveApprovedEndHeight resolves an open-ended standing approval to the
// consolidation frontier: everything below the auto-consolidate cursor is
// consolidated, and rows only become due after promotion stamps their
// retention deadline, so the cursor is a safe, monotonic envelope end.
func (t *singleBlockRetentionTask) resolveApprovedEndHeight(
	ctx context.Context,
	cronConfig config.SingleBlockRetentionCronConfig,
	tag uint32,
) (uint64, error) {
	if cronConfig.ApprovedEndHeight != 0 {
		return cronConfig.ApprovedEndHeight, nil
	}
	cursorHeight, cursorFound, err := t.metaStorage.GetBlockConsolidationCursor(
		ctx,
		metastorage.BatchConsolidatorAutoConsolidateCursor,
		tag,
	)
	if err != nil {
		return 0, xerrors.Errorf("failed to resolve consolidation cursor for open-ended retention approval: %w", err)
	}
	if !cursorFound {
		t.logger.Info(
			"single_block_retention cron found no consolidation cursor for open-ended approval",
			zap.Uint32("tag", tag),
		)
		return 0, nil
	}
	return cursorHeight, nil
}

func (t *singleBlockRetentionTask) autoRetainWorkflowID() string {
	return fmt.Sprintf("%s/%s", t.config.Workflows.SingleBlockRetention.WorkflowIdentity, autoRetainSuffix)
}

func (t *singleBlockRetentionTask) openSingleBlockRetentionWorkflow(ctx context.Context) (string, bool, error) {
	workflowIdentity := t.config.Workflows.SingleBlockRetention.WorkflowIdentity
	openWorkflows, err := t.runtime.ListOpenWorkflows(
		ctx,
		t.config.Cadence.Domain,
		singleBlockRetentionOpenPageSize,
		workflowIdentity,
	)
	if err != nil {
		return "", false, xerrors.Errorf("failed to list open workflows for single_block_retention cron: %w", err)
	}
	if openWorkflows == nil {
		return "", false, nil
	}
	for _, wf := range openWorkflows.Executions {
		if wf.GetType().GetName() == workflowIdentity {
			return wf.GetExecution().GetWorkflowId(), true, nil
		}
	}
	return "", false, nil
}

func (t *singleBlockRetentionTask) getSelector(ctx context.Context) (*retirement.Selector, error) {
	t.selectorMu.Lock()
	defer t.selectorMu.Unlock()
	if t.selector != nil {
		return t.selector, nil
	}
	selector, err := t.selectorFactory(ctx)
	if err != nil {
		return nil, err
	}
	t.selector = selector
	return t.selector, nil
}

func (t *singleBlockRetentionTask) newPostgresSelector(ctx context.Context) (*retirement.Selector, error) {
	pool, err := metapostgres.GetConnectionPool(ctx, t.config.AWS.Postgres)
	if err != nil {
		return nil, xerrors.Errorf("failed to get single_block_retention cron Postgres pool: %w", err)
	}
	db := pool.DB()
	if db == nil {
		return nil, xerrors.New("single_block_retention cron Postgres pool returned a nil database")
	}
	return retirement.NewSelector(retirement.NewPostgresRepository(db)), nil
}

func isProductionRetentionCronEnvironment(env config.Env) bool {
	value := strings.ToLower(string(env))
	return value == "production" || value == "prod"
}
