package cron

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
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

		selectorFactory func(ctx context.Context) (*retirement.Selector, error)

		// probeResumeHeight is where the next tick's advance walk continues
		// after a tick exhausts maxRetentionProbeAdvances without finding
		// selectable work. Without it every tick restarts from the watermark
		// and re-walks the same dead windows, so selectable work beyond the
		// advance budget is starved permanently; with it consecutive exhausted
		// ticks ratchet through the dead zone, budget-sized step by step, and
		// reach whatever lies beyond.
		//
		// Deliberately in-memory, not persisted: a stored cursor would need
		// its own reconciliation against repairs and completed sweeps (the
		// same reason the watermark is recomputed every tick), while losing
		// this one on restart merely restarts the walk from the watermark —
		// the safe direction, re-inspecting windows rather than skipping any.
		// It is cleared whenever a tick launches, finds nothing due, or finds
		// the approved range complete, so dead-prefix rows that later become
		// selectable are re-inspected on the next full pass of the ring.
		// Atomic only as insurance against a misconfigured parallelism > 1;
		// the cron runs this task single-flight.
		probeResumeHeight atomic.Uint64
	}
)

const (
	autoRetainSuffix                        = "auto_retain"
	defaultSingleBlockRetentionCronSpec     = "@every 1h"
	defaultSingleBlockRetentionWindowBlocks = uint64(250_000)

	// maxRetentionProbeAdvances bounds how many consecutive probe windows one
	// tick may step past when a window's due rows all turn out unselectable
	// (covered by an active repair, missing canonical membership, and so on —
	// the due-floor candidate deliberately does not evaluate those; see
	// RetentionDueFloor). Each advance costs one cheap floor lookup plus one
	// bounded probe over a window that selects nothing, so the cap bounds
	// per-tick cost, not correctness: at 4 advances a tick steps over up to
	// 4 x window_blocks of solidly unselectable due rows, and a dead zone
	// larger than that (hundreds of consecutive broken consolidated objects)
	// is an incident to alarm on — probe_advance_exhausted goes to 1 — but the
	// walk also RESUMES where it stopped (probeResumeHeight), so consecutive
	// ticks ratchet through a dead zone of any width instead of retrying the
	// same prefix forever.
	maxRetentionProbeAdvances        = 4
	singleBlockRetentionOpenPageSize = 1000
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
func (t *singleBlockRetentionTask) Run(ctx context.Context) (err error) {
	// probe_failed is the only signal that reliably detects a failed tick, and
	// it has to be a gauge written on EVERY exit path rather than a counter.
	//
	// Tally suppresses zero deltas for counters (stats.go: `if delta == 0 {
	// return }`), so the instrument's result_type="error" counter emits nothing
	// until the first failure and then falls silent again. A single isolated
	// sample gives PromQL increase() no delta to compute, so an
	// increase(...) > 0 alert on it never fires — the first outage, which is
	// exactly the one worth catching, is missed entirely.
	//
	// Gauges report whenever they were updated since the last flush, so writing
	// 0 on every successful tick keeps a continuous baseline and a plain
	// `> 0` threshold fires on the first failure with no delta involved.
	//
	// Deferred on the named return so every path is covered: the early
	// non-failure exits (workflow already open, no approved range, nothing due)
	// report 0, and anything returning an error reports 1.
	defer func() {
		failed := float64(0)
		if err != nil {
			failed = 1
		}
		t.metrics.Gauge("probe_failed").Update(failed)
	}()

	cronConfig := t.config.Cron.SingleBlockRetention
	if err := t.validateStandingApproval(cronConfig); err != nil {
		return err
	}

	workflowID := t.autoRetainWorkflowID()
	// The open-workflow guard is best-effort dedup: workflow visibility is
	// eventually consistent, so a manual run started moments before this tick
	// can slip past it. Correctness never depends on exclusivity — per-row
	// retirement claims and manifest conflict checks serialize destructive
	// work — the guard only avoids wasted contention, and the fixed auto
	// workflow ID makes duplicate auto launches impossible.
	openWorkflowID, open, err := t.openSingleBlockRetentionWorkflow(ctx)
	if err != nil {
		return err
	}
	if open {
		// A no-probe tick writes the probe-only gauges as zero rather than
		// leaving them holding the previous tick's values: a stale
		// probe_window_blocks would claim this tick scanned a window it never
		// ran. oldest_due_age_seconds is deliberately NOT reset here — the
		// backlog it measures still exists while the open sweep works it, and
		// zeroing it would silence the age alarm exactly when a sweep is stuck.
		t.metrics.Gauge("probe_window_blocks").Update(0)
		t.metrics.Gauge("due_floor_height").Update(0)
		t.metrics.Gauge("probe_advance_exhausted").Update(0)
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
	// Probe from the watermark rather than the operator's approved floor.
	//
	// The probe's cost is NOT bounded by the undeleted backlog alone, which is
	// what an earlier version of this comment claimed and what justified
	// dropping the floor to 0 in production. The due-cohort query joins its
	// due_keys CTE back to block_consolidation_shadow to expand each cohort,
	// and that join is bounded only by the height range: measured on
	// solana-mainnet prod, past roughly 1.5M heights of width the planner
	// abandons idx_block_consolidation_shadow_tag_height for a sequential scan
	// of the whole table and the probe stops finishing inside its 60s statement
	// timeout. A fixed floor widens at the chain's block rate and reaches that
	// point unaided (INF-1330).
	//
	// The watermark is the lowest height still holding an undeleted single-block
	// object, so it tracks live work and holds the range at the retention delay
	// expressed in blocks. It is recomputed every tick rather than persisted:
	// a stored floor would need its own reconciliation for repairs and failed
	// sweeps, and a stale one strands data silently.
	probeStart, err := selector.FloorWatermark(ctx, storageGeneration, tag, cronConfig.ApprovedStartHeight)
	if err != nil {
		return xerrors.Errorf("failed to resolve retention floor watermark: %w", err)
	}
	if probeStart >= approvedEnd {
		// Everything approved has been retired. Report a zero-width range so the
		// width alarm cannot mistake a finished sweep for unbounded growth.
		// Every probe-only gauge is reset here as well: a gauge left holding the
		// previous tick's value would report a scanned window on a tick that
		// never probed, contradicting the gauge's meaning.
		t.metrics.Gauge("floor_watermark_height").Update(float64(probeStart))
		t.metrics.Gauge("probe_range_blocks").Update(0)
		t.metrics.Gauge("probe_window_blocks").Update(0)
		t.metrics.Gauge("due_floor_height").Update(0)
		t.metrics.Gauge("probe_advance_exhausted").Update(0)
		t.metrics.Gauge("oldest_due_age_seconds").Update(0)
		t.metrics.Gauge("probe_backlog_truncated").Update(0)
		t.probeResumeHeight.Store(0)
		t.logger.Info(
			"single_block_retention cron found no outstanding single-block work",
			zap.Uint32("tag", tag),
			zap.String("bucket", bucket),
			zap.String("storage_generation", storageGeneration),
			zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
			zap.Uint64("approved_end_height", approvedEnd),
			zap.Uint64("floor_watermark_height", probeStart),
		)
		return nil
	}
	// probe_range_blocks is the leading indicator for the plan flip described
	// above: it is the one number that predicts the timeout before it happens,
	// where oldest_due_age_seconds cannot — that gauge is derived from this very
	// probe, so it reads zero exactly when the probe fails.
	t.metrics.Gauge("floor_watermark_height").Update(float64(probeStart))
	t.metrics.Gauge("probe_range_blocks").Update(float64(approvedEnd - probeStart))

	// Bound the PROBE by the same window that bounds the sweep.
	//
	// Without this the probe spans [watermark, approvedEnd] no matter how far
	// behind retention is, and its cost grows with the backlog rather than with
	// the work one tick can do. Past roughly 2-3M blocks of width the planner
	// abandons the index path for the due-cohort expansion join and sequentially
	// scans the whole block_metadata table, so the probe stops finishing inside
	// its statement timeout. Measured on robinhood-mainnet prod: 250k blocks of
	// width runs in 20.7s, 1M in 49.2s, and 6.74M does not complete in 60s.
	//
	// That failure cannot clear itself. The watermark is the lowest height still
	// holding an undeleted single-block object, so it only advances once deletes
	// land, deletes need the probe to return cohorts, and the frontier keeps
	// widening the range in the meantime. Enabling retention against a cold
	// backlog therefore lands past the flip on the very first tick and stays
	// there (INF-1416; same plan flip as INF-1330 on solana-mainnet).
	//
	// The window is anchored on the earliest DUE height, not on the watermark:
	// the watermark pins to any undeleted row, due or not, and a window anchored
	// on a not-yet-due row can be empty while due work sits above it — recomputed
	// identically every tick, forever. The due floor is a candidate rather than a
	// selectability proof (see RetentionDueFloor), so a window whose due rows all
	// turn out unselectable advances instead of idling; the advance count is
	// bounded and its exhaustion is alarmed, never silent.
	//
	// probe_range_blocks above deliberately still reports the FULL approved
	// range, so the backlog remains visible to alerting; probe_window_blocks
	// reports what was actually scanned.
	windowBlocks := cronConfig.WindowBlocks
	if windowBlocks == 0 {
		windowBlocks = defaultSingleBlockRetentionWindowBlocks
	}

	// probe_duration_seconds covers the whole search — floor lookups, probes,
	// and advances — so a slow tick is visible no matter which stage is slow.
	probeStartedAt := time.Now()
	var (
		cohorts     []retirement.RetentionCohort
		hasMore     bool
		resumeAfter uint64
		probeEnd    uint64
	)
	found := false
	searchStart := probeStart
	// Resume an exhausted walk. A cursor at or below the watermark is stale
	// (the watermark caught up past it) and one at or beyond the approved end
	// has finished the ring; both restart the walk from the watermark so
	// dead-prefix rows that have since become selectable are re-inspected.
	if resume := t.probeResumeHeight.Load(); resume > searchStart && resume < approvedEnd {
		searchStart = resume
	} else {
		t.probeResumeHeight.Store(0)
	}
	for attempt := 0; attempt < maxRetentionProbeAdvances; attempt++ {
		dueFloor, dueFound, err := selector.DueFloor(ctx, storageGeneration, tag, searchStart, approvedEnd, eligibilityCutoff)
		if err != nil {
			t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
			return xerrors.Errorf("failed to resolve retention due floor: %w", err)
		}
		if !dueFound {
			// Nothing due in [searchStart, approvedEnd). Because the floor
			// candidate matches a superset of selectable rows, this is proof
			// there is nothing to select, and idling is correct. Reset every
			// probe gauge so this tick cannot exhibit the previous tick's
			// values.
			t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
			t.metrics.Gauge("due_floor_height").Update(0)
			t.metrics.Gauge("probe_window_blocks").Update(0)
			t.metrics.Gauge("probe_advance_exhausted").Update(0)
			t.metrics.Gauge("oldest_due_age_seconds").Update(0)
			t.metrics.Gauge("probe_backlog_truncated").Update(0)
			// Wrap the ring: nothing due above searchStart, so the next tick
			// walks from the watermark again and re-inspects any dead prefix
			// whose rows may have become selectable since.
			t.probeResumeHeight.Store(0)
			t.logger.Info(
				"single_block_retention cron found nothing due",
				zap.Uint32("tag", tag),
				zap.String("bucket", bucket),
				zap.String("storage_generation", storageGeneration),
				zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
				zap.Uint64("approved_end_height", approvedEnd),
				zap.Uint64("floor_watermark_height", probeStart),
				zap.Uint64("search_start_height", searchStart),
				zap.Int("probe_advances", attempt),
			)
			return nil
		}
		probeStart = dueFloor
		probeEnd = approvedEnd
		if probeEnd-probeStart > windowBlocks {
			probeEnd = probeStart + windowBlocks
		}
		t.metrics.Gauge("due_floor_height").Update(float64(probeStart))
		t.metrics.Gauge("probe_window_blocks").Update(float64(probeEnd - probeStart))

		// Asks for a full workflow batch because the selector sorts pending
		// (in-flight) cohorts by prepared_at ahead of height-ordered due cohorts:
		// anchoring on the first cohort alone could hide older due work behind a
		// stuck pending cohort indefinitely.
		cohorts, hasMore, resumeAfter, err = selector.Select(
			ctx,
			bucket,
			storageGeneration,
			tag,
			probeStart,
			probeEnd,
			eligibilityCutoff,
			retirement.MaxRetentionCohortsPerWorkflow,
		)
		if err != nil {
			t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
			return xerrors.Errorf("failed to probe due retention cohorts: %w", err)
		}
		if len(cohorts) > 0 {
			found = true
			break
		}
		// The probe selected nothing here. There are two very different reasons
		// for that and they must not be conflated:
		//
		//   - candidates were EXHAUSTED: every due row in this window is
		//     excluded at the join level (active repair, pending manifest,
		//     canonical or metadata mismatch), so the window really is dead and
		//     the search steps past it. Idling instead would pin the search
		//     here forever, hiding selectable work above.
		//   - selection stopped on its expansion BUDGET with candidates still
		//     unexamined (resumeAfter > 0). Stepping past the window would skip
		//     cohorts nobody looked at, stranding them; the search resumes at
		//     the first unexamined height instead, so a dead prefix of any size
		//     is walked with bounded work per tick.
		nextStart := probeEnd
		truncated := resumeAfter > probeStart && resumeAfter < probeEnd
		if truncated {
			nextStart = resumeAfter
		}
		t.logger.Info(
			"single_block_retention cron advancing past a window with no selectable cohorts",
			zap.Uint32("tag", tag),
			zap.Uint64("window_start_height", probeStart),
			zap.Uint64("window_end_height", probeEnd),
			zap.Uint64("next_search_height", nextStart),
			zap.Bool("selection_budget_truncated", truncated),
			zap.Int("probe_advances", attempt+1),
		)
		searchStart = nextStart
	}
	t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
	if !found {
		// Every window we were willing to inspect this tick holds only
		// unselectable due rows. This is an alarm state, not an idle: due work
		// exists, nothing was launched, and the next tick will walk the same
		// windows. A dead zone this wide means a mass of repair-covered or
		// inconsistent objects that needs a human.
		t.metrics.Gauge("probe_advance_exhausted").Update(1)
		t.metrics.Gauge("oldest_due_age_seconds").Update(0)
		t.metrics.Gauge("probe_backlog_truncated").Update(0)
		// Resume here next tick instead of re-walking the same dead prefix —
		// searchStart is the end of the last window inspected. Consecutive
		// exhausted ticks therefore ratchet forward budget-by-budget until
		// they reach selectable work, nothing due, or the approved end.
		t.probeResumeHeight.Store(searchStart)
		t.logger.Warn(
			"single_block_retention cron exhausted probe advances without selectable work",
			zap.Uint32("tag", tag),
			zap.String("bucket", bucket),
			zap.String("storage_generation", storageGeneration),
			zap.Uint64("approved_end_height", approvedEnd),
			zap.Uint64("last_window_end_height", probeEnd),
			zap.Uint64("resume_height", searchStart),
			zap.Int("probe_advances", maxRetentionProbeAdvances),
		)
		return nil
	}
	t.metrics.Gauge("probe_advance_exhausted").Update(0)
	t.probeResumeHeight.Store(0)
	// Anchor at the minimum start height and age the gauge from the oldest
	// eligibility across the whole probe set, so neither is masked by
	// pending-cohort ordering. Stuck pending cohorts are themselves overdue,
	// so they keep the age alarm honest rather than silencing it.
	anchor := cohorts[0]
	oldestEligibleAt := cohorts[0].EligibleAt
	for _, cohort := range cohorts[1:] {
		if cohort.StartHeight < anchor.StartHeight {
			anchor = cohort
		}
		if cohort.EligibleAt.Before(oldestEligibleAt) {
			oldestEligibleAt = cohort.EligibleAt
		}
	}
	oldestDueAge := eligibilityCutoff.Sub(oldestEligibleAt)
	t.metrics.Gauge("oldest_due_age_seconds").Update(oldestDueAge.Seconds())
	backlogTruncated := float64(0)
	if hasMore {
		backlogTruncated = 1
	}
	t.metrics.Gauge("probe_backlog_truncated").Update(backlogTruncated)

	// The selected cohort lies inside the approved range, so the window is
	// always non-empty; it is clipped to keep each sweep bounded.
	//
	// This deliberately still clips to approvedEnd, NOT to probeEnd. The sweep's
	// authorization is the operator's approved envelope and narrowing the probe
	// must never change it. Clipping to probeEnd also underflows when the anchor
	// sits above it, which silently widened the window past approvedEnd.
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
		// Pass the resolved tag so the sweep executes under the same tag the
		// probe and window derivation used, even if a rolling deploy bumps the
		// stable tag between launch and activity execution.
		Tag:                         tag,
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
		zap.Uint64("floor_watermark_height", probeStart),
		zap.Uint64("probe_range_blocks", approvedEnd-probeStart),
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

// getSelector resolves per tick instead of caching: the postgres factory rides
// the process-global connection-pool cache, and re-resolving keeps the cron
// healthy if the pool is ever closed or recycled underneath it.
func (t *singleBlockRetentionTask) getSelector(ctx context.Context) (*retirement.Selector, error) {
	return t.selectorFactory(ctx)
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
