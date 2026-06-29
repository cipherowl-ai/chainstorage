package workflow

import (
	"context"
	"strconv"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type (
	BatchConsolidator struct {
		baseWorkflow
		batchConsolidator *activity.BatchConsolidator
	}

	BatchConsolidatorParams struct {
		fx.In
		fxparams.Params
		Runtime           cadence.Runtime
		BatchConsolidator *activity.BatchConsolidator
	}

	BatchConsolidatorRequest struct {
		Mode           config.ConsolidationMode `validate:"omitempty,oneof=shadow_dual_write historical_backfill"`
		Tag            uint32
		StartHeight    uint64
		EndHeight      uint64 `validate:"gtfield=StartHeight"`
		BatchSize      uint64
		CheckpointSize uint64
		MaxBlocks      uint64
		Parallelism    int `validate:"omitempty,gt=0"`
	}

	batchConsolidatorPendingActivity struct {
		request *activity.BatchConsolidatorRequest
		future  workflow.Future
	}
)

var (
	_ InstrumentedRequest = (*BatchConsolidatorRequest)(nil)
)

const (
	batchConsolidatorHeightGauge              = "workflow.batch_consolidator.height"
	batchConsolidatorObjectCounter            = "workflow.batch_consolidator.object"
	batchConsolidatorConsolidatedBlockCounter = "workflow.batch_consolidator.consolidated_block"
	batchConsolidatorEmptyBatchCounter        = "workflow.batch_consolidator.empty_batch"
	batchConsolidatorShadowStatsChangeID      = "batch-consolidator-shadow-stats"
	batchConsolidatorShadowStatsVersion       = 1
	batchConsolidatorMaxParallelism           = 10
	maxUint64                                 = ^uint64(0)
)

func NewBatchConsolidator(params BatchConsolidatorParams) *BatchConsolidator {
	w := &BatchConsolidator{
		baseWorkflow:      newBaseWorkflow(&params.Config.Workflows.BatchConsolidator, params.Runtime),
		batchConsolidator: params.BatchConsolidator,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *BatchConsolidator) Execute(ctx context.Context, request *BatchConsolidatorRequest) (client.WorkflowRun, error) {
	workflowID := w.name
	if override, ok := workflowIDFromContext(ctx); ok {
		workflowID = override
	}
	return w.startWorkflow(ctx, workflowID, request)
}

func (r *BatchConsolidatorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}

func (w *BatchConsolidator) execute(ctx workflow.Context, request *BatchConsolidatorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		var cfg config.BatchConsolidatorWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}
		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}
		maxBlocks := cfg.MaxBlocks
		if request.MaxBlocks > 0 {
			maxBlocks = request.MaxBlocks
		}
		parallelism := 1
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}
		if parallelism > batchConsolidatorMaxParallelism {
			return xerrors.Errorf("batch_consolidator parallelism(%d) exceeds max(%d)", parallelism, batchConsolidatorMaxParallelism)
		}
		mode, err := batchConsolidatorMode(request.Mode, cfg.Storage.Consolidation.Mode)
		if err != nil {
			return err
		}
		storageMaxBlocks := cfg.Storage.Consolidation.MaxBlocks
		if storageMaxBlocks == 0 {
			return xerrors.New("batch_consolidator storage consolidation max_blocks must be positive")
		}
		if maxBlocks == 0 || maxBlocks > storageMaxBlocks {
			maxBlocks = storageMaxBlocks
		}
		shardSize := cfg.Storage.Consolidation.ShardSize
		if batchSize == 0 {
			return xerrors.New("batch_consolidator batch_size must be positive")
		}
		if checkpointSize <= batchSize {
			return xerrors.Errorf("batch_consolidator checkpoint_size(%d) must be greater than batch_size(%d)", checkpointSize, batchSize)
		}
		if maxBlocks == 0 {
			return xerrors.New("batch_consolidator max_blocks must be positive")
		}
		if shardSize == 0 {
			return xerrors.New("batch_consolidator storage consolidation shard_size must be positive")
		}
		tag := cfg.GetEffectiveBlockTag(request.Tag)
		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
		})
		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
			zap.Uint32("effective_tag", tag),
			zap.Uint64("batch_size", batchSize),
			zap.Uint64("checkpoint_size", checkpointSize),
			zap.Uint64("max_blocks", maxBlocks),
			zap.Uint64("storage_max_blocks", storageMaxBlocks),
			zap.Uint64("shard_size", shardSize),
			zap.Int("parallelism", parallelism),
			zap.String("mode", string(mode)),
		)
		logger.Info("workflow started")
		ctx = w.withActivityOptions(ctx)
		statsCtx := w.withShadowStatsActivityOptions(ctx, cfg)
		usePersistedShadowStats := workflow.GetVersion(
			ctx,
			batchConsolidatorShadowStatsChangeID,
			workflow.DefaultVersion,
			batchConsolidatorShadowStatsVersion,
		) != workflow.DefaultVersion && mode != config.ConsolidationModePromoteFinalized

		if mode == config.ConsolidationModeHistoricalBackfill {
			if err := w.validateHistoricalBackfillRange(ctx, logger, tag, request.StartHeight, request.EndHeight, cfg.IrreversibleDistance); err != nil {
				return err
			}
		}

		workflowEndHeight := request.EndHeight
		if mode == config.ConsolidationModePromoteFinalized {
			plan, err := w.batchConsolidator.GetPromotionPlan(ctx, &activity.BatchConsolidatorPlanRequest{
				Tag:         tag,
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			})
			if err != nil {
				return xerrors.Errorf("failed to plan promote_finalized range: %w", err)
			}
			workflowEndHeight = plan.EndHeight
			logger.Info(
				"planned promote_finalized range",
				zap.Uint64("requested_end_height", request.EndHeight),
				zap.Uint64("effective_end_height", workflowEndHeight),
				zap.Uint64("latest_height", plan.LatestHeight),
				zap.Uint64("safe_promotion_height", plan.SafePromotionHeight),
				zap.Uint64("promotion_gate_height", plan.PromotionGateHeight),
			)
			if workflowEndHeight <= request.StartHeight {
				logger.Info("promote_finalized range has no currently safe heights")
				return nil
			}
		}

		// Keep default shadow activity payloads unchanged for existing workflow history replay.
		activityMode := config.ConsolidationMode("")
		if request.Mode != "" {
			activityMode = mode
		}

		for batchStart := request.StartHeight; batchStart < workflowEndHeight; {
			if batchStart-request.StartHeight >= checkpointSize {
				newRequest := *request
				newRequest.StartHeight = batchStart
				logger.Info("checkpoint reached", zap.Reflect("newRequest", newRequest))
				return w.continueAsNew(ctx, &newRequest)
			}

			batchEnd := batchConsolidatorWindowEnd(batchStart, workflowEndHeight, batchSize, shardSize)
			if batchEnd <= batchStart {
				return xerrors.Errorf("batch_consolidator made no height progress from %d to %d", batchStart, batchEnd)
			}

			if mode == config.ConsolidationModeHistoricalBackfill && parallelism > 1 {
				objectsInBatch, blocksInBatch, err := w.processHistoricalBackfillBatchParallel(
					ctx,
					statsCtx,
					logger,
					metrics,
					tag,
					activityMode,
					batchStart,
					batchEnd,
					maxBlocks,
					parallelism,
					usePersistedShadowStats,
				)
				if err != nil {
					return err
				}
				metrics.Gauge(batchConsolidatorHeightGauge).Update(float64(batchEnd - 1))
				logger.Info(
					"processed shadow batch",
					zap.Uint64("batch_start", batchStart),
					zap.Uint64("batch_end", batchEnd),
					zap.Uint64("objects", objectsInBatch),
					zap.Uint64("consolidated_blocks", blocksInBatch),
				)
				batchStart = batchEnd
				continue
			}

			objectsInBatch := uint64(0)
			blocksInBatch := uint64(0)
			lastShadowObjects := uint64(0)
			lastShadowBlocks := uint64(0)
			if usePersistedShadowStats {
				baseline, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
					Mode:        activityMode,
					Tag:         tag,
					StartHeight: batchStart,
					EndHeight:   batchEnd,
				})
				if err != nil {
					return xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
				}
				lastShadowObjects = baseline.ShadowObjects
				lastShadowBlocks = baseline.ShadowBlocks
			}
			for {
				response, err := w.batchConsolidator.Execute(ctx, &activity.BatchConsolidatorRequest{
					Mode:        activityMode,
					Tag:         tag,
					StartHeight: batchStart,
					EndHeight:   batchEnd,
					MaxBlocks:   maxBlocks,
				})
				if err != nil {
					return xerrors.Errorf("failed to consolidate shadow batch [%d, %d): %w", batchStart, batchEnd, err)
				}
				newObjects := uint64(0)
				newBlocks := uint64(0)
				if usePersistedShadowStats {
					stats, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
						Mode:        activityMode,
						Tag:         tag,
						StartHeight: batchStart,
						EndHeight:   batchEnd,
					})
					if err != nil {
						return xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
					}
					if stats.ShadowObjects < lastShadowObjects || stats.ShadowBlocks < lastShadowBlocks {
						return xerrors.Errorf(
							"batch_consolidator shadow stats regressed for batch [%d, %d): objects %d -> %d, blocks %d -> %d",
							batchStart,
							batchEnd,
							lastShadowObjects,
							stats.ShadowObjects,
							lastShadowBlocks,
							stats.ShadowBlocks,
						)
					}
					newObjects = stats.ShadowObjects - lastShadowObjects
					newBlocks = stats.ShadowBlocks - lastShadowBlocks
					lastShadowObjects = stats.ShadowObjects
					lastShadowBlocks = stats.ShadowBlocks
				}
				if response.ScannedBlocks == 0 {
					if newObjects > 0 {
						objectsInBatch += newObjects
						metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(newObjects))
					}
					if newBlocks > 0 {
						blocksInBatch += newBlocks
						metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(newBlocks))
					}
					metrics.Counter(batchConsolidatorEmptyBatchCounter).Inc(1)
					break
				}
				if response.ConsolidatedBlocks == 0 {
					return xerrors.Errorf("batch_consolidator made no progress for non-empty shadow scan [%d, %d)", batchStart, batchEnd)
				}
				if !usePersistedShadowStats {
					newObjects = 1
					newBlocks = response.ConsolidatedBlocks
				}
				if newObjects > 0 {
					objectsInBatch += newObjects
					metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(newObjects))
				}
				if newBlocks > 0 {
					blocksInBatch += newBlocks
					metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(newBlocks))
				}
				logger.Info(
					"processed shadow object",
					zap.Uint64("batch_start", batchStart),
					zap.Uint64("batch_end", batchEnd),
					zap.Uint64("scanned_blocks", response.ScannedBlocks),
					zap.Uint64("consolidated_blocks", response.ConsolidatedBlocks),
					zap.Uint64("new_objects", newObjects),
					zap.Uint64("new_consolidated_blocks", newBlocks),
					zap.Uint64("shadow_objects", lastShadowObjects),
					zap.Uint64("shadow_blocks", lastShadowBlocks),
					zap.String("object_key", response.ObjectKey),
				)
				if response.ScannedBlocks < maxBlocks {
					break
				}
			}
			metrics.Gauge(batchConsolidatorHeightGauge).Update(float64(batchEnd - 1))
			logger.Info(
				"processed shadow batch",
				zap.Uint64("batch_start", batchStart),
				zap.Uint64("batch_end", batchEnd),
				zap.Uint64("objects", objectsInBatch),
				zap.Uint64("consolidated_blocks", blocksInBatch),
			)
			batchStart = batchEnd
		}

		logger.Info("workflow finished")
		return nil
	})
}

func (w *BatchConsolidator) processHistoricalBackfillBatchParallel(
	ctx workflow.Context,
	statsCtx workflow.Context,
	logger *zap.Logger,
	metrics client.MetricsHandler,
	tag uint32,
	activityMode config.ConsolidationMode,
	batchStart uint64,
	batchEnd uint64,
	maxBlocks uint64,
	parallelism int,
	usePersistedShadowStats bool,
) (uint64, uint64, error) {
	lastShadowObjects := uint64(0)
	lastShadowBlocks := uint64(0)
	if usePersistedShadowStats {
		baseline, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
			Mode:        activityMode,
			Tag:         tag,
			StartHeight: batchStart,
			EndHeight:   batchEnd,
		})
		if err != nil {
			return 0, 0, xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
		}
		lastShadowObjects = baseline.ShadowObjects
		lastShadowBlocks = baseline.ShadowBlocks
	}

	objectsInBatch := uint64(0)
	blocksInBatch := uint64(0)
	for windowStart := batchStart; windowStart < batchEnd; {
		pending := make([]batchConsolidatorPendingActivity, 0, parallelism)
		for len(pending) < parallelism && windowStart < batchEnd {
			windowEnd := batchConsolidatorObjectWindowEnd(windowStart, batchEnd, maxBlocks)
			if windowEnd <= windowStart {
				return 0, 0, xerrors.Errorf("batch_consolidator made no parallel window progress from %d to %d", windowStart, windowEnd)
			}
			request := &activity.BatchConsolidatorRequest{
				Mode:        activityMode,
				Tag:         tag,
				StartHeight: windowStart,
				EndHeight:   windowEnd,
				MaxBlocks:   maxBlocks,
			}
			pending = append(pending, batchConsolidatorPendingActivity{
				request: request,
				future:  workflow.ExecuteActivity(ctx, activity.ActivityBatchConsolidator, request),
			})
			windowStart = windowEnd
		}

		var firstErr error
		type successResult struct {
			request  *activity.BatchConsolidatorRequest
			response activity.BatchConsolidatorResponse
		}
		successes := make([]successResult, 0, len(pending))
		for _, item := range pending {
			var response activity.BatchConsolidatorResponse
			request := item.request
			if err := item.future.Get(ctx, &response); err != nil {
				if firstErr == nil {
					firstErr = xerrors.Errorf("failed to consolidate shadow batch [%d, %d): %w", request.StartHeight, request.EndHeight, err)
				}
				continue
			}
			successes = append(successes, successResult{request: request, response: response})
		}
		if firstErr != nil {
			return 0, 0, firstErr
		}

		for _, success := range successes {
			request := success.request
			response := success.response
			if response.ScannedBlocks == 0 {
				metrics.Counter(batchConsolidatorEmptyBatchCounter).Inc(1)
				continue
			}
			if response.ConsolidatedBlocks == 0 {
				return 0, 0, xerrors.Errorf(
					"batch_consolidator made no progress for non-empty shadow scan [%d, %d)",
					request.StartHeight,
					request.EndHeight,
				)
			}

			newObjects := uint64(0)
			newBlocks := uint64(0)
			if !usePersistedShadowStats {
				newObjects = 1
				newBlocks = response.ConsolidatedBlocks
				objectsInBatch += newObjects
				blocksInBatch += newBlocks
				metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(newObjects))
				metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(newBlocks))
			}
			logger.Info(
				"processed shadow object",
				zap.Uint64("batch_start", batchStart),
				zap.Uint64("batch_end", batchEnd),
				zap.Uint64("window_start", request.StartHeight),
				zap.Uint64("window_end", request.EndHeight),
				zap.Uint64("scanned_blocks", response.ScannedBlocks),
				zap.Uint64("consolidated_blocks", response.ConsolidatedBlocks),
				zap.Uint64("new_objects", newObjects),
				zap.Uint64("new_consolidated_blocks", newBlocks),
				zap.Uint64("shadow_objects", lastShadowObjects),
				zap.Uint64("shadow_blocks", lastShadowBlocks),
				zap.String("object_key", response.ObjectKey),
			)
		}
	}

	if usePersistedShadowStats {
		stats, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
			Mode:        activityMode,
			Tag:         tag,
			StartHeight: batchStart,
			EndHeight:   batchEnd,
		})
		if err != nil {
			return 0, 0, xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
		}
		if stats.ShadowObjects < lastShadowObjects || stats.ShadowBlocks < lastShadowBlocks {
			return 0, 0, xerrors.Errorf(
				"batch_consolidator shadow stats regressed for batch [%d, %d): objects %d -> %d, blocks %d -> %d",
				batchStart,
				batchEnd,
				lastShadowObjects,
				stats.ShadowObjects,
				lastShadowBlocks,
				stats.ShadowBlocks,
			)
		}
		objectsInBatch = stats.ShadowObjects - lastShadowObjects
		blocksInBatch = stats.ShadowBlocks - lastShadowBlocks
		if objectsInBatch > 0 {
			metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(objectsInBatch))
		}
		if blocksInBatch > 0 {
			metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(blocksInBatch))
		}
	}

	return objectsInBatch, blocksInBatch, nil
}

func batchConsolidatorMode(
	requestMode config.ConsolidationMode,
	configMode config.ConsolidationMode,
) (config.ConsolidationMode, error) {
	mode := requestMode
	if mode == "" {
		mode = configMode
	}
	switch mode {
	case config.ConsolidationModeShadowDualWrite, config.ConsolidationModeHistoricalBackfill, config.ConsolidationModePromoteFinalized:
		return mode, nil
	default:
		return "", xerrors.Errorf(
			"batch_consolidator requires mode %q, %q, or %q, got %q",
			config.ConsolidationModeShadowDualWrite,
			config.ConsolidationModeHistoricalBackfill,
			config.ConsolidationModePromoteFinalized,
			mode,
		)
	}
}

func (w *BatchConsolidator) validateHistoricalBackfillRange(
	ctx workflow.Context,
	logger *zap.Logger,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	irreversibleDistance uint64,
) error {
	latest, err := w.batchConsolidator.GetLatestBlock(ctx, &activity.BatchConsolidatorLatestBlockRequest{
		Tag: tag,
	})
	if err != nil {
		return xerrors.Errorf("failed to get latest block for historical backfill range validation: %w", err)
	}
	if latest == nil {
		return xerrors.New("latest block response is nil for historical backfill range validation")
	}
	safeEndHeight, ok := batchConsolidatorHistoricalSafeEndHeight(latest.Height, irreversibleDistance)
	if !ok {
		return xerrors.Errorf(
			"historical_backfill range [%d, %d) is unsafe: latest height %d is below irreversible_distance %d",
			startHeight,
			endHeight,
			latest.Height,
			irreversibleDistance,
		)
	}
	if endHeight > safeEndHeight {
		return xerrors.Errorf(
			"historical_backfill range [%d, %d) is unsafe: end_height must be <= %d for latest_height=%d irreversible_distance=%d",
			startHeight,
			endHeight,
			safeEndHeight,
			latest.Height,
			irreversibleDistance,
		)
	}
	logger.Info(
		"validated historical backfill range",
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", startHeight),
		zap.Uint64("end_height", endHeight),
		zap.Uint64("latest_height", latest.Height),
		zap.Uint64("irreversible_distance", irreversibleDistance),
		zap.Uint64("safe_end_height", safeEndHeight),
	)
	return nil
}

func batchConsolidatorHistoricalSafeEndHeight(latestHeight uint64, irreversibleDistance uint64) (uint64, bool) {
	if irreversibleDistance == 0 {
		if latestHeight == maxUint64 {
			return maxUint64, true
		}
		return latestHeight + 1, true
	}
	if latestHeight < irreversibleDistance-1 {
		return 0, false
	}
	latestFinalizedHeight := latestHeight - irreversibleDistance + 1
	if latestFinalizedHeight == maxUint64 {
		return maxUint64, true
	}
	return latestFinalizedHeight + 1, true
}

func (w *BatchConsolidator) withShadowStatsActivityOptions(
	ctx workflow.Context,
	cfg config.BatchConsolidatorWorkflowConfig,
) workflow.Context {
	base := cfg.Base()
	retryPolicy := w.getShadowStatsActivityRetryPolicy(base.ActivityRetry)
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              base.TaskList,
		StartToCloseTimeout:    base.ActivityStartToCloseTimeout,
		ScheduleToCloseTimeout: base.ActivityScheduleToCloseTimeout,
		HeartbeatTimeout:       base.ActivityHeartbeatTimeout,
		RetryPolicy:            retryPolicy,
	})
}

func (w *BatchConsolidator) getShadowStatsActivityRetryPolicy(cfg *config.RetryPolicy) *temporal.RetryPolicy {
	retryPolicy := w.getRetryPolicy(cfg)
	if retryPolicy != nil {
		retryPolicyCopy := *retryPolicy
		// Stats reads are side-effect-free and cheap. Unlimited attempts prevent
		// old workers on the shared task queue from exhausting retries during a
		// rolling deploy before a new worker polls the new stats activity name.
		retryPolicyCopy.MaximumAttempts = 0
		retryPolicy = &retryPolicyCopy
	}
	return retryPolicy
}

func batchConsolidatorWindowEnd(startHeight uint64, requestedEndHeight uint64, batchSize uint64, shardSize uint64) uint64 {
	batchEnd := startHeight + batchSize
	if batchEnd < startHeight || batchEnd > requestedEndHeight {
		batchEnd = requestedEndHeight
	}
	shardEnd := batchConsolidatorShardEnd(startHeight, shardSize)
	if shardEnd < batchEnd {
		return shardEnd
	}
	return batchEnd
}

func batchConsolidatorObjectWindowEnd(startHeight uint64, batchEnd uint64, maxBlocks uint64) uint64 {
	windowEnd := startHeight + maxBlocks
	if windowEnd < startHeight || windowEnd > batchEnd {
		return batchEnd
	}
	return windowEnd
}

func batchConsolidatorShardEnd(height uint64, shardSize uint64) uint64 {
	return (height/shardSize + 1) * shardSize
}
