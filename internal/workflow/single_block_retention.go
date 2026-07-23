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
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type (
	SingleBlockRetention struct {
		baseWorkflow
		activity *activity.SingleBlockRetention
	}

	SingleBlockRetentionParams struct {
		fx.In
		fxparams.Params
		Runtime  cadence.Runtime
		Activity *activity.SingleBlockRetention
	}

	SingleBlockRetentionRequest struct {
		Tag                         uint32
		StartHeight                 uint64
		EndHeight                   uint64
		MaxObjectRanges             int `validate:"omitempty,gt=0,lte=250"`
		Execute                     bool
		ProductionDeleteEnabled     bool
		DirectStorageClientsGuarded bool
		SingleBlockWritersGuarded   bool
		FallbackErrorCount          uint64
	}

	SingleBlockRetentionCompletedRange struct {
		ConsolidatedObjectKey string `json:"consolidated_object_key"`
		StartHeight           uint64 `json:"start_height"`
		EndHeight             uint64 `json:"end_height"`
		EligibleRows          uint64 `json:"eligible_rows"`
	}

	SingleBlockRetentionResult struct {
		StartedAt             time.Time                                   `json:"started_at"`
		CompletedAt           time.Time                                   `json:"completed_at"`
		Tag                   uint32                                      `json:"tag"`
		SelectionStartHeight  uint64                                      `json:"selection_start_height"`
		SelectionEndHeight    uint64                                      `json:"selection_end_height"`
		Execute               bool                                        `json:"execute"`
		SelectedObjectRanges  uint64                                      `json:"selected_object_ranges"`
		MoreEligibleRanges    bool                                        `json:"more_eligible_ranges"`
		ProcessedObjectRanges uint64                                      `json:"processed_object_ranges"`
		CompletedObjectRanges []SingleBlockRetentionCompletedRange        `json:"completed_object_ranges,omitempty"`
		RangeResults          []*activity.SingleBlockRetentionRangeResult `json:"range_results,omitempty"`
		ScannedRows           uint64                                      `json:"scanned_rows"`
		PlannedRows           uint64                                      `json:"planned_rows"`
		DeletedVerifiedRows   uint64                                      `json:"deleted_verified_rows"`
		AlreadyRetiredRows    uint64                                      `json:"already_retired_rows"`
		SkippedSlots          uint64                                      `json:"skipped_slots"`
		DeferredRows          uint64                                      `json:"deferred_rows"`
		FailedRows            uint64                                      `json:"failed_rows"`
		DeletedVersions       uint64                                      `json:"deleted_versions"`
		DeletedMarkers        uint64                                      `json:"deleted_markers"`
		RetiredBytes          uint64                                      `json:"retired_bytes"`
		FailureMessage        string                                      `json:"failure_message,omitempty"`
	}
)

var _ InstrumentedRequest = (*SingleBlockRetentionRequest)(nil)

const maxSingleBlockRetentionRetryDelay = 30 * time.Minute

func NewSingleBlockRetention(params SingleBlockRetentionParams) *SingleBlockRetention {
	w := &SingleBlockRetention{
		baseWorkflow: newBaseWorkflow(&params.Config.Workflows.SingleBlockRetention, params.Runtime),
		activity:     params.Activity,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *SingleBlockRetention) Execute(
	ctx context.Context,
	request *SingleBlockRetentionRequest,
) (client.WorkflowRun, error) {
	workflowID := w.name
	if override, ok := workflowIDFromContext(ctx); ok {
		workflowID = override
	}
	return w.startWorkflow(ctx, workflowID, request)
}

func (r *SingleBlockRetentionRequest) GetTags() map[string]string {
	tag := uint32(0)
	if r != nil {
		tag = r.Tag
	}
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(tag)),
	}
}

func (w *SingleBlockRetention) execute(
	ctx workflow.Context,
	request *SingleBlockRetentionRequest,
) (*SingleBlockRetentionResult, error) {
	result := &SingleBlockRetentionResult{StartedAt: workflow.Now(ctx).UTC()}
	if request != nil {
		result.Execute = request.Execute
	}
	err := w.executeWorkflow(ctx, request, func() error {
		var cfg config.SingleBlockRetentionWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}
		if err := validateSingleBlockRetentionExecutionRequest(request); err != nil {
			return err
		}

		maxObjectRanges := cfg.MaxObjectRanges
		if request.MaxObjectRanges > 0 {
			maxObjectRanges = request.MaxObjectRanges
		}
		if maxObjectRanges <= 0 || maxObjectRanges > retirement.MaxRetentionCohortsPerWorkflow {
			return xerrors.Errorf(
				"single_block_retention max_object_ranges must be between 1 and %d: %d",
				retirement.MaxRetentionCohortsPerWorkflow,
				maxObjectRanges,
			)
		}
		tag := cfg.GetEffectiveBlockTag(request.Tag)
		result.Tag = tag
		result.SelectionStartHeight = request.StartHeight
		result.SelectionEndHeight = request.EndHeight

		logger := w.getLogger(ctx).With(
			zap.Uint32("effective_tag", tag),
			zap.Int("max_object_ranges", maxObjectRanges),
			zap.Uint64("selection_start_height", request.StartHeight),
			zap.Uint64("selection_end_height", request.EndHeight),
			zap.Bool("execute", request.Execute),
		)
		logger.Info("single-block retention workflow started")
		activityCtx := w.withActivityOptions(ctx)
		selected, err := w.activity.Select(activityCtx, &activity.SingleBlockRetentionSelectRequest{
			Tag:         tag,
			StartHeight: request.StartHeight,
			EndHeight:   request.EndHeight,
			Limit:       maxObjectRanges,
		})
		if err != nil {
			return xerrors.Errorf("failed to select retention cohorts: %w", err)
		}
		if len(selected.Cohorts) > maxObjectRanges {
			return xerrors.Errorf(
				"single-block retention selector returned %d cohorts above limit %d",
				len(selected.Cohorts),
				maxObjectRanges,
			)
		}
		if selected.HasMore && len(selected.Cohorts) == 0 {
			return xerrors.New("single-block retention selector reported a backlog without returning a cohort")
		}
		result.SelectedObjectRanges = uint64(len(selected.Cohorts))
		result.MoreEligibleRanges = selected.HasMore

		for _, cohort := range selected.Cohorts {
			if err := validateSelectedSingleBlockRetentionCohort(
				cohort,
				request.StartHeight,
				request.EndHeight,
			); err != nil {
				return err
			}
			processRequest := &activity.SingleBlockRetentionProcessRequest{
				Tag:                         tag,
				Cohort:                      cohort,
				Execute:                     request.Execute,
				ProductionDeleteEnabled:     request.ProductionDeleteEnabled,
				DirectStorageClientsGuarded: request.DirectStorageClientsGuarded,
				SingleBlockWritersGuarded:   request.SingleBlockWritersGuarded,
				FallbackErrorCount:          request.FallbackErrorCount,
			}
			rangeResult, err := w.activity.Process(activityCtx, processRequest)
			if err != nil {
				return xerrors.Errorf(
					"failed to process retention cohort %q [%d, %d): %w",
					cohort.ConsolidatedObjectKey,
					cohort.StartHeight,
					cohort.EndHeight,
					err,
				)
			}
			if request.Execute && rangeResult.RetryAfter > 0 {
				if rangeResult.RetryAfter > maxSingleBlockRetentionRetryDelay {
					return xerrors.Errorf(
						"retention cohort %q requested retry delay %s above maximum %s",
						cohort.ConsolidatedObjectKey,
						rangeResult.RetryAfter,
						maxSingleBlockRetentionRetryDelay,
					)
				}
				logger.Info(
					"single-block retention cohort deferred for safety quiescence",
					zap.String("consolidated_object_key", cohort.ConsolidatedObjectKey),
					zap.Uint64("start_height", cohort.StartHeight),
					zap.Uint64("end_height", cohort.EndHeight),
					zap.Duration("retry_after", rangeResult.RetryAfter),
					zap.String("retry_reason", rangeResult.RetryReason),
				)
				if err := workflow.Sleep(ctx, rangeResult.RetryAfter); err != nil {
					return xerrors.Errorf("failed to wait for retention safety quiescence: %w", err)
				}
				rangeResult, err = w.activity.Process(activityCtx, processRequest)
				if err != nil {
					return xerrors.Errorf(
						"failed to retry retention cohort %q [%d, %d): %w",
						cohort.ConsolidatedObjectKey,
						cohort.StartHeight,
						cohort.EndHeight,
						err,
					)
				}
			}
			result.addRangeResult(rangeResult)
			if request.Execute {
				if rangeResult.RetryAfter > 0 {
					return xerrors.Errorf(
						"retention cohort %q remained deferred after safety-quiescence retry: %s",
						cohort.ConsolidatedObjectKey,
						rangeResult.RetryReason,
					)
				}
				if rangeResult.FailureMessage != "" {
					return xerrors.Errorf(
						"retention cohort %q failed: %s",
						cohort.ConsolidatedObjectKey,
						rangeResult.FailureMessage,
					)
				}
				if rangeResult.FailedRows > 0 || rangeResult.DeferredRows > 0 || !rangeResult.Terminal {
					return xerrors.Errorf(
						"retention cohort %q did not finish: failed_rows=%d deferred_rows=%d verified_through_exclusive=%d",
						cohort.ConsolidatedObjectKey,
						rangeResult.FailedRows,
						rangeResult.DeferredRows,
						rangeResult.VerifiedThroughExclusive,
					)
				}
			}
		}
		logger.Info(
			"single-block retention workflow completed",
			zap.Uint64("selected_object_ranges", result.SelectedObjectRanges),
			zap.Bool("more_eligible_ranges", result.MoreEligibleRanges),
			zap.Uint64("processed_object_ranges", result.ProcessedObjectRanges),
			zap.Int("completed_object_ranges", len(result.CompletedObjectRanges)),
			zap.Uint64("deleted_verified_rows", result.DeletedVerifiedRows),
			zap.Uint64("retired_bytes", result.RetiredBytes),
		)
		return nil
	})
	result.CompletedAt = workflow.Now(ctx).UTC()
	if err != nil {
		result.FailureMessage = err.Error()
	}
	return result, err
}

func (r *SingleBlockRetentionResult) addRangeResult(result *activity.SingleBlockRetentionRangeResult) {
	if result == nil {
		return
	}
	r.ProcessedObjectRanges++
	r.RangeResults = append(r.RangeResults, result)
	r.ScannedRows += result.ScannedRows
	r.PlannedRows += result.PlannedRows
	r.DeletedVerifiedRows += result.DeletedVerifiedRows
	r.AlreadyRetiredRows += result.AlreadyRetiredRows
	r.SkippedSlots += result.SkippedSlots
	r.DeferredRows += result.DeferredRows
	r.FailedRows += result.FailedRows
	r.DeletedVersions += result.DeletedVersions
	r.DeletedMarkers += result.DeletedMarkers
	r.RetiredBytes += result.RetiredBytes
	if result.Terminal {
		r.CompletedObjectRanges = append(r.CompletedObjectRanges, SingleBlockRetentionCompletedRange{
			ConsolidatedObjectKey: result.Cohort.ConsolidatedObjectKey,
			StartHeight:           result.Cohort.StartHeight,
			EndHeight:             result.Cohort.EndHeight,
			EligibleRows:          result.Cohort.RowCount,
		})
	}
}

func validateSingleBlockRetentionExecutionRequest(
	request *SingleBlockRetentionRequest,
) error {
	if request == nil {
		return xerrors.New("single-block retention request is required")
	}
	if !request.Execute {
		return validateSingleBlockRetentionSelectionRange(request.StartHeight, request.EndHeight)
	}
	if err := validateSingleBlockRetentionSelectionRange(request.StartHeight, request.EndHeight); err != nil {
		return err
	}
	if !request.DirectStorageClientsGuarded {
		return xerrors.New("retention execution requires direct storage clients to be guarded or out of scope")
	}
	if !request.SingleBlockWritersGuarded {
		return xerrors.New("retention execution requires every single-block writer to honor the retirement fence")
	}
	if request.FallbackErrorCount != 0 {
		return xerrors.Errorf("retention execution requires zero fallback read errors, got %d", request.FallbackErrorCount)
	}
	return nil
}

func validateSingleBlockRetentionSelectionRange(startHeight uint64, endHeight uint64) error {
	if endHeight == 0 && startHeight != 0 {
		return xerrors.New("single-block retention end height is required when start height is set")
	}
	if endHeight != 0 && endHeight <= startHeight {
		return xerrors.Errorf("invalid single-block retention range [%d, %d)", startHeight, endHeight)
	}
	return nil
}

func validateSelectedSingleBlockRetentionCohort(
	cohort retirement.RetentionCohort,
	selectionStartHeight uint64,
	selectionEndHeight uint64,
) error {
	if cohort.ConsolidatedObjectKey == "" || cohort.EndHeight <= cohort.StartHeight ||
		cohort.RowCount == 0 || cohort.EligibleAt.IsZero() {
		return xerrors.Errorf("invalid selected single-block retention cohort: %+v", cohort)
	}
	if selectionEndHeight != 0 &&
		(cohort.StartHeight < selectionStartHeight || cohort.EndHeight > selectionEndHeight) {
		return xerrors.Errorf(
			"selected retention cohort %q [%d, %d) is outside requested range [%d, %d)",
			cohort.ConsolidatedObjectKey,
			cohort.StartHeight,
			cohort.EndHeight,
			selectionStartHeight,
			selectionEndHeight,
		)
	}
	return nil
}
