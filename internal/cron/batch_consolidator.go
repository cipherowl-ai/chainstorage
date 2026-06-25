package cron

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/workflow"
)

type (
	BatchConsolidatorTaskParams struct {
		fx.In
		fxparams.Params
		Config            *config.Config
		Runtime           cadence.Runtime
		MetaStorage       metastorage.MetaStorage
		BatchConsolidator *workflow.BatchConsolidator
	}

	batchConsolidatorTask struct {
		config            *config.Config
		logger            *zap.Logger
		runtime           cadence.Runtime
		metaStorage       metastorage.MetaStorage
		batchConsolidator *workflow.BatchConsolidator
	}
)

const (
	autoPromoteFinalizedSuffix       = "auto_promote_finalized"
	batchConsolidatorOpenPageSize    = 1000
	defaultBatchConsolidatorCronSpec = "@every 30m"
)

func NewBatchConsolidator(params BatchConsolidatorTaskParams) (Task, error) {
	return &batchConsolidatorTask{
		config:            params.Config,
		logger:            log.WithPackage(params.Logger),
		runtime:           params.Runtime,
		metaStorage:       params.MetaStorage,
		batchConsolidator: params.BatchConsolidator,
	}, nil
}

func (t *batchConsolidatorTask) Name() string {
	return "batch_consolidator"
}

func (t *batchConsolidatorTask) Spec() string {
	spec := t.config.Cron.BatchConsolidator.Spec
	if spec == "" {
		return defaultBatchConsolidatorCronSpec
	}
	return spec
}

func (t *batchConsolidatorTask) Parallelism() int64 {
	parallelism := t.config.Cron.BatchConsolidator.Parallelism
	if parallelism <= 0 {
		return 1
	}
	return parallelism
}

func (t *batchConsolidatorTask) Enabled() bool {
	return t.config.Cron.BatchConsolidator.Enabled
}

func (t *batchConsolidatorTask) DelayStartDuration() time.Duration {
	return t.config.Cron.BatchConsolidator.DelayStartDuration
}

func (t *batchConsolidatorTask) Run(ctx context.Context) error {
	consolidation := t.config.AWS.Storage.Consolidation
	if !consolidation.Enabled {
		return xerrors.New("batch_consolidator cron requires aws.storage.consolidation.enabled=true")
	}
	if consolidation.Mode != config.ConsolidationModePromoteFinalized {
		return xerrors.Errorf("batch_consolidator cron requires consolidation mode %q, got %q", config.ConsolidationModePromoteFinalized, consolidation.Mode)
	}
	if consolidation.SafePromotionLag == nil {
		return xerrors.New("batch_consolidator cron requires safe_promotion_lag")
	}

	cronConfig := t.config.Cron.BatchConsolidator
	if cronConfig.MaxRangeBlocks == 0 {
		return xerrors.New("batch_consolidator cron max_range_blocks must be positive")
	}

	workflowID := t.autoPromoteWorkflowID()
	openWorkflowID, open, err := t.openBatchConsolidatorWorkflow(ctx)
	if err != nil {
		return err
	}
	if open {
		t.logger.Info(
			"batch_consolidator cron skipped because a batch_consolidator workflow is already open",
			zap.String("open_workflow_id", openWorkflowID),
			zap.String("auto_workflow_id", workflowID),
		)
		return nil
	}

	tag := t.config.GetEffectiveBlockTag(0)
	searchStart := cronConfig.StartHeight
	latest, err := t.metaStorage.GetLatestBlock(ctx, tag)
	if err != nil {
		return xerrors.Errorf("failed to get latest block for batch_consolidator cron: %w", err)
	}
	if latest == nil {
		return xerrors.New("latest block not found for batch_consolidator cron")
	}
	searchEnd, safeHeight, ok := batchConsolidatorCronSafeEndHeight(latest.GetHeight(), *consolidation.SafePromotionLag)
	if !ok {
		t.logger.Info(
			"batch_consolidator cron has no safe promotion range",
			zap.Uint32("tag", tag),
			zap.Uint64("latest_height", latest.GetHeight()),
			zap.Uint64("safe_promotion_lag", *consolidation.SafePromotionLag),
		)
		return nil
	}
	if consolidation.PromotionGateHeight != nil && *consolidation.PromotionGateHeight < searchEnd {
		searchEnd = *consolidation.PromotionGateHeight
	}
	if searchEnd <= searchStart {
		t.logger.Info(
			"batch_consolidator cron safe range is below configured start height",
			zap.Uint32("tag", tag),
			zap.Uint64("start_height", searchStart),
			zap.Uint64("safe_end_height", searchEnd),
			zap.Uint64("latest_height", latest.GetHeight()),
			zap.Uint64("safe_promotion_height", safeHeight),
		)
		return nil
	}

	startHeight, found, err := t.metaStorage.GetFirstPromotableBlockConsolidationShadow(ctx, tag, searchStart, searchEnd)
	if err != nil {
		return xerrors.Errorf("failed to get first promotable consolidation shadow: %w", err)
	}
	if !found {
		t.logger.Info(
			"batch_consolidator cron found no promotable consolidation shadows",
			zap.Uint32("tag", tag),
			zap.Uint64("start_height", searchStart),
			zap.Uint64("end_height", searchEnd),
			zap.Uint64("latest_height", latest.GetHeight()),
			zap.Uint64("safe_promotion_height", safeHeight),
		)
		return nil
	}

	endHeight := batchConsolidatorCronRangeEnd(startHeight, cronConfig.MaxRangeBlocks)
	if endHeight > searchEnd {
		endHeight = searchEnd
	}
	if endHeight <= startHeight {
		t.logger.Info(
			"batch_consolidator cron planned an empty promotion range",
			zap.Uint32("tag", tag),
			zap.Uint64("start_height", startHeight),
			zap.Uint64("end_height", endHeight),
		)
		return nil
	}

	request := &workflow.BatchConsolidatorRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	}
	workflowCtx := workflow.WithWorkflowID(ctx, workflowID)
	run, err := t.batchConsolidator.Execute(workflowCtx, request)
	if err != nil {
		if isWorkflowAlreadyStarted(err) {
			t.logger.Info("batch_consolidator cron skipped because auto promotion workflow was already started", zap.String("workflow_id", workflowID))
			return nil
		}
		return xerrors.Errorf("failed to start batch_consolidator cron workflow: %w", err)
	}
	t.logger.Info(
		"started batch_consolidator cron workflow",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", run.GetRunID()),
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", startHeight),
		zap.Uint64("end_height", endHeight),
		zap.Uint64("latest_height", latest.GetHeight()),
		zap.Uint64("safe_promotion_height", safeHeight),
	)
	return nil
}

func (t *batchConsolidatorTask) autoPromoteWorkflowID() string {
	return fmt.Sprintf("%s/%s", t.config.Workflows.BatchConsolidator.WorkflowIdentity, autoPromoteFinalizedSuffix)
}

func (t *batchConsolidatorTask) openBatchConsolidatorWorkflow(ctx context.Context) (string, bool, error) {
	workflowIdentity := t.config.Workflows.BatchConsolidator.WorkflowIdentity
	openWorkflows, err := t.runtime.ListOpenWorkflows(ctx, t.config.Cadence.Domain, batchConsolidatorOpenPageSize, workflowIdentity)
	if err != nil {
		return "", false, xerrors.Errorf("failed to list open workflows for batch_consolidator cron: %w", err)
	}
	if openWorkflows == nil {
		return "", false, nil
	}
	for _, wf := range openWorkflows.Executions {
		if wf.GetType().GetName() == workflowIdentity {
			workflowID := wf.GetExecution().GetWorkflowId()
			return workflowID, true, nil
		}
	}
	return "", false, nil
}

func batchConsolidatorCronSafeEndHeight(latestHeight uint64, safePromotionLag uint64) (uint64, uint64, bool) {
	if latestHeight < safePromotionLag {
		return 0, 0, false
	}
	safeHeight := latestHeight - safePromotionLag
	if safeHeight == ^uint64(0) {
		return safeHeight, safeHeight, true
	}
	return safeHeight + 1, safeHeight, true
}

func batchConsolidatorCronRangeEnd(startHeight uint64, maxRangeBlocks uint64) uint64 {
	endHeight := startHeight + maxRangeBlocks
	if endHeight < startHeight {
		return ^uint64(0)
	}
	return endHeight
}

func isWorkflowAlreadyStarted(err error) bool {
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if xerrors.As(err, &alreadyStarted) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "already started")
}
