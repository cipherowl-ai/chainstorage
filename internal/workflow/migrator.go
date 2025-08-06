package workflow

import (
	"context"
	"strconv"
	"time"

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
	}

	MigratorParams struct {
		fx.In
		fxparams.Params
		Runtime                    cadence.Runtime
		Migrator                   *activity.Migrator
		GetLatestBlockHeight       *activity.GetLatestBlockHeightActivity
		GetLatestBlockFromPostgres *activity.GetLatestBlockFromPostgresActivity
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
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Migrator) Execute(ctx context.Context, request *MigratorRequest) (client.WorkflowRun, error) {
	workflowId := w.name
	if v, ok := ctx.Value("workflowId").(string); ok && v != "" {
		workflowId = v
	}
	return w.startWorkflow(ctx, workflowId, request)
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

		miniBatchSize := batchSize / 10 // Default mini-batch size
		if miniBatchSize == 0 {
			miniBatchSize = 10 // Minimum mini-batch size
		}
		if request.MiniBatchSize > 0 {
			miniBatchSize = request.MiniBatchSize
		}

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}

		parallelism := 1 // Default parallelism
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

		// Handle auto-resume functionality
		if request.AutoResume && request.StartHeight == 0 {
			logger.Info("AutoResume enabled, querying PostgreSQL destination for latest migrated block")
			postgresResp, err := w.getLatestBlockFromPostgres.Execute(ctx, &activity.GetLatestBlockFromPostgresRequest{Tag: tag})
			if err != nil {
				return xerrors.Errorf("failed to get latest block height from PostgreSQL: %w", err)
			}

			if postgresResp.Found {
				// Resume from the next block after the latest migrated block
				request.StartHeight = postgresResp.Height + 1
				logger.Info("Auto-resume: found latest block in PostgreSQL destination",
					zap.Uint64("latestHeight", postgresResp.Height),
					zap.Uint64("resumeFromHeight", request.StartHeight))
			} else {
				// No blocks found in destination, start from the beginning
				request.StartHeight = 0
				logger.Info("Auto-resume: no blocks found in PostgreSQL destination, starting from beginning")
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

		// Additional validation for continuous sync
		if continuousSync && request.EndHeight != 0 && request.EndHeight <= request.StartHeight {
			return xerrors.Errorf("with ContinuousSync enabled, EndHeight (%d) must be 0 OR greater than StartHeight (%d)",
				request.EndHeight, request.StartHeight)
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
			// Check for checkpoint
			if batchStart-request.StartHeight >= checkpointSize {
				newRequest := *request
				newRequest.StartHeight = batchStart
				logger.Info(
					"checkpoint reached",
					zap.Reflect("newRequest", newRequest),
				)
				return w.continueAsNew(ctx, &newRequest)
			}

			batchEnd := batchStart + batchSize
			if batchEnd > request.EndHeight {
				batchEnd = request.EndHeight
			}

			logger.Info("migrating batch",
				zap.Uint64("batchStart", batchStart),
				zap.Uint64("batchEnd", batchEnd))

			migratorRequest := &activity.MigratorRequest{
				StartHeight: batchStart,
				EndHeight:   batchEnd,
				EventTag:    request.EventTag,
				Tag:         tag,
				BatchSize:   int(miniBatchSize), // Use miniBatchSize for activity batch size
				Parallelism: parallelism,
				SkipEvents:  request.SkipEvents,
				SkipBlocks:  request.SkipBlocks,
			}

			response, err := w.migrator.Execute(ctx, migratorRequest)
			if err != nil {
				logger.Error(
					"failed to migrate batch",
					zap.Uint64("batchStart", batchStart),
					zap.Uint64("batchEnd", batchEnd),
					zap.Error(err),
				)
				return xerrors.Errorf("failed to migrate batch [%v, %v): %w", batchStart, batchEnd, err)
			}

			if !response.Success {
				logger.Error(
					"migration batch failed",
					zap.Uint64("batchStart", batchStart),
					zap.Uint64("batchEnd", batchEnd),
					zap.String("message", response.Message),
				)
				return xerrors.Errorf("migration batch failed [%v, %v): %s", batchStart, batchEnd, response.Message)
			}

			// Update metrics
			processedHeights += batchEnd - batchStart
			progress := float64(processedHeights) / float64(totalHeightRange) * 100

			metrics.Gauge(migratorHeightGauge).Update(float64(batchEnd - 1))
			metrics.Counter(migratorBlocksCounter).Inc(int64(response.BlocksMigrated))
			metrics.Counter(migratorEventsCounter).Inc(int64(response.EventsMigrated))
			metrics.Gauge(migratorProgressGauge).Update(progress)

			logger.Info(
				"migrated batch successfully",
				zap.Uint64("batchStart", batchStart),
				zap.Uint64("batchEnd", batchEnd),
				zap.Int("blocksMigrated", response.BlocksMigrated),
				zap.Int("eventsMigrated", response.EventsMigrated),
				zap.Float64("progress", progress),
			)

			// Add backoff if configured
			if backoffInterval > 0 {
				_ = workflow.Sleep(ctx, backoffInterval)
			}
		}

		if continuousSync {
			logger.Info("continuous sync enabled, starting new sync cycle")
			newRequest := *request
			newRequest.StartHeight = request.EndHeight
			newRequest.EndHeight = 0
			// Wait for syncInterval before starting a new continuous sync workflow
			err := workflow.Sleep(ctx, syncInterval)
			if err != nil {
				return xerrors.Errorf("workflow sleep failed during continuous sync: %w", err)
			}
			logger.Info("starting new continuous sync workflow", zap.Reflect("newRequest", newRequest))
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
