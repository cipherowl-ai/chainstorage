package workflow

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
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
	Migrator struct {
		baseWorkflow
		migrator                   *activity.Migrator
		getLatestBlockHeight       *activity.GetLatestBlockHeightActivity
		getLatestBlockFromPostgres *activity.GetLatestBlockFromPostgresActivity
		getLatestEventFromPostgres *activity.GetLatestEventFromPostgresActivity
	}

	MigratorParams struct {
		fx.In
		fxparams.Params
		Runtime                    cadence.Runtime
		Migrator                   *activity.Migrator
		GetLatestBlockHeight       *activity.GetLatestBlockHeightActivity
		GetLatestBlockFromPostgres *activity.GetLatestBlockFromPostgresActivity
		GetLatestEventFromPostgres *activity.GetLatestEventFromPostgresActivity
	}

	MigratorRequest struct {
		StartHeight     uint64
		EndHeight       uint64 // Optional. If not specified, will query latest block from DynamoDB.
		EventTag        uint32
		Tag             uint32
		BatchSize       uint64 // Optional. If not specified, it is read from the workflow config.
		MiniBatchSize   uint64 // Optional. If not specified, it is read from the workflow config.
		CheckpointSize  uint64 // Optional. If not specified, it is read from the workflow config.
		Parallelism     int    // Optional. If not specified, it is read from the workflow config.
		SkipEvents      bool   // Optional. Skip event migration (blocks only)
		SkipBlocks      bool   // Optional. Skip block migration (events only)
		BackoffInterval string // Optional. If not specified, it is read from the workflow config.
		ContinuousSync  bool   // Optional. Whether to continuously sync data in infinite loop mode
		SyncInterval    string // Optional. Interval for continuous sync (e.g., "1m", "30s"). Defaults to 1 minute if not specified or invalid.
		AutoResume      bool   // Optional. Automatically determine StartHeight from latest block in PostgreSQL destination
	}
)

var (
	_ InstrumentedRequest = (*MigratorRequest)(nil)
)

const (
	// migrator metrics. need to have `workflow.migrator` as prefix
	migratorHeightGauge   = "workflow.migrator.height"
	migratorBlocksCounter = "workflow.migrator.blocks_migrated"
	migratorEventsCounter = "workflow.migrator.events_migrated"
	migratorProgressGauge = "workflow.migrator.progress"
)

func NewMigrator(params MigratorParams) *Migrator {
	w := &Migrator{
		baseWorkflow:               newBaseWorkflow(&params.Config.Workflows.Migrator, params.Runtime),
		migrator:                   params.Migrator,
		getLatestBlockHeight:       params.GetLatestBlockHeight,
		getLatestBlockFromPostgres: params.GetLatestBlockFromPostgres,
		getLatestEventFromPostgres: params.GetLatestEventFromPostgres,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Migrator) Execute(ctx context.Context, request *MigratorRequest) (client.WorkflowRun, error) {
	// Add timestamp to workflow ID to avoid ALLOW_DUPLICATE_FAILED_ONLY policy conflicts
	timestamp := time.Now().Unix()
	workflowID := fmt.Sprintf("%s-%d", w.name, timestamp)
	if request.Tag != 0 {
		workflowID = fmt.Sprintf("%s-%d/block_tag=%d", w.name, timestamp, request.Tag)
	}
	return w.startMigratorWorkflow(ctx, workflowID, request)
}

// startMigratorWorkflow starts a migrator workflow with a custom reuse policy
// that allows restarting failed workflows but prevents concurrent execution
func (w *Migrator) startMigratorWorkflow(ctx context.Context, workflowID string, request *MigratorRequest) (client.WorkflowRun, error) {
	if err := w.validateRequestCtx(ctx, request); err != nil {
		return nil, err
	}
	cfg := w.config.Base()
	workflowOptions := client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                cfg.TaskList,
		WorkflowRunTimeout:                       cfg.WorkflowRunTimeout,
		WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		RetryPolicy:                              w.getRetryPolicy(cfg.WorkflowRetry),
	}
	execution, err := w.runtime.ExecuteWorkflow(ctx, workflowOptions, w.name, request)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute workflow: %w", err)
	}
	return execution, nil
}

func (w *Migrator) execute(ctx workflow.Context, request *MigratorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		if err := w.validateRequest(request); err != nil {
			return err
		}

		var cfg config.MigratorWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		// Both skip flags cannot be true
		if request.SkipEvents && request.SkipBlocks {
			return xerrors.New("cannot skip both events and blocks - nothing to migrate")
		}

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}

		miniBatchSize := cfg.MiniBatchSize
		if miniBatchSize <= 0 {
			miniBatchSize = batchSize / 10 // Calculate from batch size
			if miniBatchSize == 0 {
				miniBatchSize = 10 // Minimum fallback
			}
		}
		if request.MiniBatchSize > 0 {
			miniBatchSize = request.MiniBatchSize
		}

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}

		parallelism := cfg.Parallelism
		if parallelism <= 0 {
			parallelism = 1 // Fallback default if config not set
		}
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}

		backoffInterval := cfg.BackoffInterval
		if request.BackoffInterval != "" {
			parsedInterval, err := time.ParseDuration(request.BackoffInterval)
			if err != nil {
				return xerrors.Errorf("failed to parse backoff interval: %w", err)
			}
			backoffInterval = parsedInterval
		}

		// Use config's continuous sync setting as default if not explicitly set in request
		continuousSync := cfg.ContinuousSync || request.ContinuousSync

		syncInterval := defaultSyncInterval
		if cfg.SyncInterval > 0 {
			syncInterval = cfg.SyncInterval
		}
		if request.SyncInterval != "" {
			interval, err := time.ParseDuration(request.SyncInterval)
			if err == nil {
				syncInterval = interval
			}
		}

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
		})
		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		// Set up activity options early so we can use activities
		ctx = w.withActivityOptions(ctx)

		// Handle auto-resume functionality - use latest event height instead of block height
		if request.AutoResume && request.StartHeight == 0 {
			logger.Info("AutoResume enabled, querying PostgreSQL destination for latest migrated event")
			postgresEventResp, err := w.getLatestEventFromPostgres.Execute(ctx, &activity.GetLatestEventFromPostgresRequest{EventTag: request.EventTag})
			if err != nil {
				return xerrors.Errorf("failed to get latest event height from PostgreSQL: %w", err)
			}

			if postgresEventResp.Found {
				// Resume from the latest event height (blocks will be remigrated if needed, PostgreSQL handles duplicates)
				request.StartHeight = postgresEventResp.Height
				logger.Info("Auto-resume: found latest event in PostgreSQL destination",
					zap.Uint64("latestEventHeight", postgresEventResp.Height),
					zap.Uint64("resumeFromHeight", request.StartHeight))
			} else {
				// No events found in destination, start from the beginning
				request.StartHeight = 0
				logger.Info("Auto-resume: no events found in PostgreSQL destination, starting from beginning")
			}
		}

		// Handle end height auto-detection if not provided
		if request.EndHeight == 0 {
			logger.Info("No end height provided, fetching latest block height from DynamoDB via activity...")
			resp, err := w.getLatestBlockHeight.Execute(ctx, &activity.GetLatestBlockHeightRequest{Tag: tag})
			if err != nil {
				return xerrors.Errorf("failed to get latest block height from DynamoDB: %w", err)
			}

			if continuousSync {
				// For continuous sync, set end height to current latest block
				request.EndHeight = resp.Height + 1
				logger.Info("Auto-detected end height for continuous sync", zap.Uint64("endHeight", request.EndHeight))
			} else {
				request.EndHeight = resp.Height + 1
				logger.Info("Auto-detected end height from DynamoDB", zap.Uint64("endHeight", request.EndHeight))
			}
		}

		// Validate end height after auto-detection and auto-resume
		if !continuousSync && request.StartHeight >= request.EndHeight {
			return xerrors.Errorf("startHeight (%d) must be less than endHeight (%d)",
				request.StartHeight, request.EndHeight)
		}

		// Additional handling for continuous sync:
		// If EndHeight <= StartHeight, we are caught up. Instead of erroring, schedule next cycle.
		if continuousSync && request.EndHeight != 0 && request.EndHeight <= request.StartHeight {
			logger.Info("Continuous sync: caught up (no new blocks). Running event catch-up before next cycle",
				zap.Uint64("startHeight", request.StartHeight),
				zap.Uint64("endHeight", request.EndHeight))

			// No special event catch-up needed - normal migration handles this

			// Prepare for next cycle
			newRequest := *request
			newRequest.StartHeight = request.EndHeight
			newRequest.EndHeight = 0 // re-detect on next cycle

			// Wait for syncInterval before starting a new continuous sync workflow
			logger.Info("waiting for sync interval before next catch-up cycle",
				zap.Duration("syncInterval", syncInterval))
			err := workflow.Sleep(ctx, syncInterval)
			if err != nil {
				return xerrors.Errorf("workflow sleep failed during caught-up continuous sync: %w", err)
			}

			logger.Info("starting next continuous sync cycle after catch-up",
				zap.Uint64("nextStartHeight", newRequest.StartHeight))
			return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
		}

		// Special case: if auto-resume found we're already caught up
		if request.AutoResume && request.StartHeight >= request.EndHeight {
			logger.Info("Auto-resume detected: already caught up, no migration needed",
				zap.Uint64("startHeight", request.StartHeight),
				zap.Uint64("endHeight", request.EndHeight))
			return nil // Successfully completed with no work to do
		}

		// Validate skip-blocks requirements (moved here after logger is available)
		if request.SkipBlocks && !request.SkipEvents {
			logger.Warn("Events-only migration requested (skip-blocks=true)")
			logger.Warn("Block metadata must already exist in PostgreSQL for this height range")
			logger.Warn("If validation fails, migrate blocks first with skip-events=true")
		}

		logger.Info("migrator workflow started")

		totalHeightRange := request.EndHeight - request.StartHeight
		processedHeights := uint64(0)

		for batchStart := request.StartHeight; batchStart < request.EndHeight; batchStart += batchSize {
			// Check for checkpoint - only check after processing at least one batch
			processedSoFar := batchStart - request.StartHeight
			if processedSoFar > 0 && processedSoFar >= checkpointSize {
				newRequest := *request
				newRequest.StartHeight = batchStart
				logger.Info("checkpoint reached", zap.Reflect("newRequest", newRequest))
				return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
			}

			batchEnd := batchStart + batchSize
			if batchEnd > request.EndHeight {
				batchEnd = request.EndHeight
			}

			logger.Info("migrating batch",
				zap.Uint64("batchStart", batchStart),
				zap.Uint64("batchEnd", batchEnd))

			// Fan out this batch into multiple parallel activities to utilize multiple workers.
			// We shard the height range evenly among `parallelism` shards.
			numShards := parallelism
			if numShards < 1 {
				numShards = 1
			}

			// Avoid launching more shards than heights.
			totalHeightsInBatch := batchEnd - batchStart
			if totalHeightsInBatch < uint64(numShards) {
				numShards = int(totalHeightsInBatch)
				if numShards == 0 {
					numShards = 1
				}
			}

			shardSize := (totalHeightsInBatch + uint64(numShards) - 1) / uint64(numShards) // ceil-divide

			type shardResult struct {
				resp   *activity.MigratorResponse
				err    error
				sStart uint64
				sEnd   uint64
			}

			resultsCh := workflow.NewChannel(ctx)
			launched := 0

			for i := 0; i < numShards; i++ {
				sStart := batchStart + uint64(i)*shardSize
				sEnd := sStart + shardSize
				if sEnd > batchEnd {
					sEnd = batchEnd
				}
				if sStart >= sEnd {
					continue
				}

				// Build request for this shard. Keep event sorting in activity; set internal activity parallelism to 1
				// to avoid over-saturation since we are already fanning out at the workflow level.
				shardReq := &activity.MigratorRequest{
					StartHeight: sStart,
					EndHeight:   sEnd,
					EventTag:    request.EventTag,
					Tag:         tag,
					BatchSize:   int(miniBatchSize),
					Parallelism: 1,
					SkipEvents:  request.SkipEvents,
					SkipBlocks:  request.SkipBlocks,
				}

				launched++
				workflow.Go(ctx, func(ctx workflow.Context) {
					resp, err := w.migrator.Execute(ctx, shardReq)
					resultsCh.Send(ctx, shardResult{resp: resp, err: err, sStart: sStart, sEnd: sEnd})
				})
			}

			// Collect results from all shards
			var totalBlocksMigrated, totalEventsMigrated int
			for received := 0; received < launched; received++ {
				var r shardResult
				resultsCh.Receive(ctx, &r)
				if r.err != nil {
					logger.Error(
						"failed to migrate shard",
						zap.Uint64("shardStart", r.sStart),
						zap.Uint64("shardEnd", r.sEnd),
						zap.Error(r.err),
					)
					return xerrors.Errorf("failed to migrate shard [%v, %v): %w", r.sStart, r.sEnd, r.err)
				}
				if r.resp == nil || !r.resp.Success {
					msg := ""
					if r.resp != nil {
						msg = r.resp.Message
					}
					logger.Error(
						"migration batch failed",
						zap.Uint64("shardStart", r.sStart),
						zap.Uint64("shardEnd", r.sEnd),
						zap.String("message", msg),
					)
					return xerrors.Errorf("migration batch failed [%v, %v): %s", r.sStart, r.sEnd, msg)
				}
				totalBlocksMigrated += r.resp.BlocksMigrated
				totalEventsMigrated += r.resp.EventsMigrated
			}

			// Update metrics for the whole batch after all shards complete
			processedHeights += batchEnd - batchStart
			progress := float64(processedHeights) / float64(totalHeightRange) * 100

			metrics.Gauge(migratorHeightGauge).Update(float64(batchEnd - 1))
			metrics.Counter(migratorBlocksCounter).Inc(int64(totalBlocksMigrated))
			metrics.Counter(migratorEventsCounter).Inc(int64(totalEventsMigrated))
			metrics.Gauge(migratorProgressGauge).Update(progress)

			logger.Info(
				"migrated batch successfully",
				zap.Uint64("batchStart", batchStart),
				zap.Uint64("batchEnd", batchEnd),
				zap.Int("blocksMigrated", totalBlocksMigrated),
				zap.Int("eventsMigrated", totalEventsMigrated),
				zap.Float64("progress", progress),
			)

			// Add backoff if configured
			if backoffInterval > 0 {
				_ = workflow.Sleep(ctx, backoffInterval)
			}
		}

		if continuousSync {
			logger.Info("continuous sync enabled, preparing for next sync cycle")
			newRequest := *request
			newRequest.StartHeight = request.EndHeight
			newRequest.EndHeight = 0      // Will be auto-detected on next cycle
			newRequest.AutoResume = false // AutoResume should only happen on first workflow run

			// Wait for syncInterval before starting a new continuous sync workflow
			logger.Info("waiting for sync interval before next cycle",
				zap.Duration("syncInterval", syncInterval))
			err := workflow.Sleep(ctx, syncInterval)
			if err != nil {
				return xerrors.Errorf("workflow sleep failed during continuous sync: %w", err)
			}

			logger.Info("starting new continuous sync workflow",
				zap.Uint64("nextStartHeight", newRequest.StartHeight),
				zap.Reflect("newRequest", newRequest))
			return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
		}

		logger.Info("migrator workflow finished",
			zap.Uint64("totalHeights", totalHeightRange),
			zap.Uint64("processedHeights", processedHeights))

		return nil
	})
}

func (r *MigratorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}
