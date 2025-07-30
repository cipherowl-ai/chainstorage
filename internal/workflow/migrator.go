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
		migrator             *activity.Migrator
		getLatestBlockHeight *activity.GetLatestBlockHeightActivity
	}

	MigratorParams struct {
		fx.In
		fxparams.Params
		Runtime              cadence.Runtime
		Migrator             *activity.Migrator
		GetLatestBlockHeight *activity.GetLatestBlockHeightActivity
	}

	MigratorRequest struct {
		StartHeight     uint64
		EndHeight       uint64 // Optional. If not specified, will query latest block from DynamoDB
		EventTag        uint32
		Tag             uint32
		BatchSize       uint64 // Optional. If not specified, it is read from the workflow config.
		CheckpointSize  uint64 // Optional. If not specified, it is read from the workflow config.
		SkipEvents      bool   // Optional. Skip event migration (blocks only)
		SkipBlocks      bool   // Optional. Skip block migration (events only)
		BackoffInterval string // Optional. If not specified, it is read from the workflow config.
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
		baseWorkflow:         newBaseWorkflow(&params.Config.Workflows.Migrator, params.Runtime),
		migrator:             params.Migrator,
		getLatestBlockHeight: params.GetLatestBlockHeight,
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

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}

		backoffInterval := cfg.BackoffInterval
		if request.BackoffInterval != "" {
			parsedInterval, err := time.ParseDuration(request.BackoffInterval)
			if err != nil {
				return xerrors.Errorf("failed to parse backoff interval: %w", err)
			}
			backoffInterval = parsedInterval
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

		// Handle end height auto-detection if not provided
		if request.EndHeight == 0 {
			logger.Info("No end height provided, fetching latest block height from DynamoDB via activity...")
			resp, err := w.getLatestBlockHeight.Execute(ctx, &activity.GetLatestBlockHeightRequest{Tag: tag})
			if err != nil {
				return xerrors.Errorf("failed to get latest block height from DynamoDB: %w", err)
			}
			request.EndHeight = resp.Height + 1
			logger.Info("Auto-detected end height from DynamoDB", zap.Uint64("endHeight", request.EndHeight))
		}

		// Validate end height after auto-detection
		if request.StartHeight >= request.EndHeight {
			return xerrors.Errorf("startHeight (%d) must be less than endHeight (%d)",
				request.StartHeight, request.EndHeight)
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
				BatchSize:   int(batchSize / 10), // Internal activity batch size should be smaller
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
