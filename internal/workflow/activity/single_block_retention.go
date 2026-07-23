package activity

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	chains3 "github.com/coinbase/chainstorage/internal/s3"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	SingleBlockRetention struct {
		selectActivity  baseActivity
		processActivity baseActivity
		config          *config.Config
		s3Client        chains3.Client
		componentsMu    sync.Mutex
		selector        *retirement.Selector
		planner         *retirement.Planner
	}

	SingleBlockRetentionParams struct {
		fx.In
		fxparams.Params
		Runtime  cadence.Runtime
		S3Client chains3.Client `optional:"true"`
	}

	SingleBlockRetentionSelectRequest struct {
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64
		Limit       int `validate:"required,gt=0,lte=250"`
	}

	SingleBlockRetentionSelectResponse struct {
		Cohorts []retirement.RetentionCohort `json:"cohorts"`
		HasMore bool                         `json:"has_more"`
	}

	SingleBlockRetentionProcessRequest struct {
		Tag                         uint32
		Cohort                      retirement.RetentionCohort
		Execute                     bool
		ProductionDeleteEnabled     bool
		DirectStorageClientsGuarded bool
		SingleBlockWritersGuarded   bool
		FallbackReadsValidated      bool
		FallbackErrorCount          uint64
	}

	SingleBlockRetentionReasonCount struct {
		Reason string `json:"reason"`
		Rows   uint64 `json:"rows"`
	}

	SingleBlockRetentionRangeResult struct {
		Cohort                   retirement.RetentionCohort        `json:"cohort"`
		ScannedRows              uint64                            `json:"scanned_rows"`
		PlannedRows              uint64                            `json:"planned_rows"`
		DeletedVerifiedRows      uint64                            `json:"deleted_verified_rows"`
		AlreadyRetiredRows       uint64                            `json:"already_retired_rows"`
		SkippedSlots             uint64                            `json:"skipped_slots"`
		DeferredRows             uint64                            `json:"deferred_rows"`
		FailedRows               uint64                            `json:"failed_rows"`
		DeletedVersions          uint64                            `json:"deleted_versions"`
		DeletedMarkers           uint64                            `json:"deleted_markers"`
		RetiredBytes             uint64                            `json:"retired_bytes"`
		VerifiedThroughExclusive uint64                            `json:"verified_through_exclusive"`
		FirstIncompleteHeight    *uint64                           `json:"first_incomplete_height,omitempty"`
		Terminal                 bool                              `json:"terminal"`
		RetryAfter               time.Duration                     `json:"retry_after,omitempty"`
		RetryReason              string                            `json:"retry_reason,omitempty"`
		FailureMessage           string                            `json:"failure_message,omitempty"`
		Blockers                 []SingleBlockRetentionReasonCount `json:"blockers,omitempty"`
	}
)

func NewSingleBlockRetention(params SingleBlockRetentionParams) *SingleBlockRetention {
	a := &SingleBlockRetention{
		selectActivity:  newBaseActivity(ActivitySingleBlockRetentionSelect, params.Runtime),
		processActivity: newBaseActivity(ActivitySingleBlockRetentionProcess, params.Runtime),
		config:          params.Config,
		s3Client:        params.S3Client,
	}
	a.selectActivity.register(a.executeSelect)
	a.processActivity.register(a.executeProcess)
	return a
}

func (a *SingleBlockRetention) Select(
	ctx workflow.Context,
	request *SingleBlockRetentionSelectRequest,
) (*SingleBlockRetentionSelectResponse, error) {
	var response SingleBlockRetentionSelectResponse
	err := a.selectActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *SingleBlockRetention) Process(
	ctx workflow.Context,
	request *SingleBlockRetentionProcessRequest,
) (*SingleBlockRetentionRangeResult, error) {
	var response SingleBlockRetentionRangeResult
	err := a.processActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *SingleBlockRetention) executeSelect(
	ctx context.Context,
	request *SingleBlockRetentionSelectRequest,
) (*SingleBlockRetentionSelectResponse, error) {
	if err := a.selectActivity.validateRequest(request); err != nil {
		return nil, err
	}
	selector, _, err := a.getComponents(ctx)
	if err != nil {
		return nil, err
	}
	tag := a.config.GetEffectiveBlockTag(request.Tag)
	sdkactivity.RecordHeartbeat(ctx, "single_block_retention.select.started", tag, request.Limit)
	cohorts, hasMore, err := selector.Select(
		ctx,
		tag,
		request.StartHeight,
		request.EndHeight,
		request.Limit,
	)
	if err != nil {
		return nil, err
	}
	a.selectActivity.getLogger(ctx).Info(
		"selected single-block retention cohorts",
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", request.StartHeight),
		zap.Uint64("end_height", request.EndHeight),
		zap.Int("cohorts", len(cohorts)),
		zap.Bool("has_more", hasMore),
		zap.Int("limit", request.Limit),
	)
	sdkactivity.RecordHeartbeat(ctx, "single_block_retention.select.completed", tag, len(cohorts))
	return &SingleBlockRetentionSelectResponse{
		Cohorts: cohorts,
		HasMore: hasMore,
	}, nil
}

func (a *SingleBlockRetention) executeProcess(
	ctx context.Context,
	request *SingleBlockRetentionProcessRequest,
) (*SingleBlockRetentionRangeResult, error) {
	if err := a.processActivity.validateRequest(request); err != nil {
		return nil, err
	}
	if err := validateSingleBlockRetentionProcessRequest(a.config, request); err != nil {
		return nil, err
	}
	_, planner, err := a.getComponents(ctx)
	if err != nil {
		return nil, err
	}

	req := a.planRequest(request)
	logger := a.processActivity.getLogger(ctx).With(
		zap.Uint32("tag", req.Tag),
		zap.String("consolidated_object_key", request.Cohort.ConsolidatedObjectKey),
		zap.Uint64("start_height", req.StartHeight),
		zap.Uint64("end_height", req.EndHeight),
		zap.Bool("execute", req.Execute),
	)
	sdkactivity.RecordHeartbeat(
		ctx,
		"single_block_retention.process.started",
		req.Tag,
		req.StartHeight,
		req.EndHeight,
		req.Execute,
	)

	if request.Execute {
		reconcileReport, reconcileErr := planner.Reconcile(ctx, req)
		var reconcileResult *SingleBlockRetentionRangeResult
		if reconcileReport != nil {
			reconcileResult, err = summarizeSingleBlockRetentionReport(request.Cohort, reconcileReport)
			if err != nil {
				return nil, err
			}
			if setSingleBlockRetentionRetry(reconcileResult) {
				return reconcileResult, nil
			}
		}
		if reconcileErr != nil {
			if reconcileResult == nil {
				return nil, xerrors.Errorf("failed to reconcile retention cohort: %w", reconcileErr)
			}
			reconcileResult.FailureMessage = reconcileErr.Error()
			logger.Error("failed to reconcile single-block retention cohort", zap.Error(reconcileErr))
			return reconcileResult, nil
		}
	}

	report, err := planner.Plan(ctx, req)
	if err != nil {
		return nil, xerrors.Errorf("failed to plan retention cohort: %w", err)
	}
	if request.Execute {
		if err := validateSingleBlockRetentionExecutionPlan(request.Cohort, report); err != nil {
			return nil, err
		}
		if applyErr := planner.Apply(ctx, req, report); applyErr != nil {
			result, summaryErr := summarizeSingleBlockRetentionReport(request.Cohort, report)
			if summaryErr != nil {
				return nil, summaryErr
			}
			if setSingleBlockRetentionRetry(result) {
				return result, nil
			}
			result.FailureMessage = applyErr.Error()
			logger.Error("failed to apply single-block retention cohort", zap.Error(applyErr))
			return result, nil
		}
	}

	result, err := summarizeSingleBlockRetentionReport(request.Cohort, report)
	if err != nil {
		return nil, err
	}
	if request.Execute && setSingleBlockRetentionRetry(result) {
		return result, nil
	}
	logger.Info(
		"processed single-block retention cohort",
		zap.Uint64("scanned_rows", result.ScannedRows),
		zap.Uint64("deleted_verified_rows", result.DeletedVerifiedRows),
		zap.Uint64("already_retired_rows", result.AlreadyRetiredRows),
		zap.Uint64("deferred_rows", result.DeferredRows),
		zap.Uint64("failed_rows", result.FailedRows),
		zap.Bool("terminal", result.Terminal),
	)
	sdkactivity.RecordHeartbeat(
		ctx,
		"single_block_retention.process.completed",
		req.Tag,
		req.StartHeight,
		req.EndHeight,
		result.DeletedVerifiedRows,
		result.Terminal,
	)
	return result, nil
}

func (a *SingleBlockRetention) getComponents(
	ctx context.Context,
) (*retirement.Selector, *retirement.Planner, error) {
	a.componentsMu.Lock()
	defer a.componentsMu.Unlock()
	if a.selector != nil && a.planner != nil {
		return a.selector, a.planner, nil
	}
	if a.config.StorageType.MetaStorageType != config.MetaStorageType_POSTGRES || a.config.AWS.Postgres == nil {
		return nil, nil, xerrors.New("single-block retention workflow requires Postgres meta storage")
	}
	if a.config.StorageType.BlobStorageType != config.BlobStorageType_UNSPECIFIED &&
		a.config.StorageType.BlobStorageType != config.BlobStorageType_S3 {
		return nil, nil, xerrors.New("single-block retention workflow requires S3 blob storage")
	}
	if a.s3Client == nil {
		return nil, nil, xerrors.New("single-block retention workflow requires an S3 client")
	}
	pool, err := metapostgres.GetConnectionPool(ctx, a.config.AWS.Postgres)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to get single-block retention Postgres pool: %w", err)
	}
	db := pool.DB()
	if db == nil {
		return nil, nil, xerrors.New("single-block retention Postgres pool returned a nil database")
	}
	repo := retirement.NewPostgresRepository(db)
	a.selector = retirement.NewSelector(repo)
	a.planner = retirement.NewPlanner(repo, retirement.NewS3ObjectStore(a.s3Client))
	return a.selector, a.planner, nil
}

func (a *SingleBlockRetention) planRequest(request *SingleBlockRetentionProcessRequest) retirement.PlanRequest {
	blockchain, network, sidechain := singleBlockRetentionChainNames(a.config)
	chainParts := []string{blockchain, network}
	if sidechain != "" {
		chainParts = append(chainParts, sidechain)
	}
	tag := a.config.GetEffectiveBlockTag(request.Tag)
	return retirement.PlanRequest{
		Environment:                 string(a.config.Env()),
		Blockchain:                  blockchain,
		Network:                     network,
		Sidechain:                   sidechain,
		Bucket:                      a.config.AWS.Bucket,
		Tag:                         tag,
		StartHeight:                 request.Cohort.StartHeight,
		EndHeight:                   request.Cohort.EndHeight,
		Now:                         time.Now().UTC(),
		Execute:                     request.Execute,
		ProductionDeleteEnabled:     request.ProductionDeleteEnabled,
		DirectStorageClientsGuarded: request.DirectStorageClientsGuarded,
		SingleBlockWritersGuarded:   request.SingleBlockWritersGuarded,
		FallbackErrorCount:          request.FallbackErrorCount,
		Approval: retirement.Approval{
			Chain:       strings.Join(chainParts, "-"),
			StartHeight: request.Cohort.StartHeight,
			EndHeight:   request.Cohort.EndHeight,
		},
	}
}

func singleBlockRetentionChainNames(cfg *config.Config) (string, string, string) {
	blockchain := cfg.Blockchain().GetName()
	network := strings.TrimPrefix(cfg.Network().GetName(), blockchain+"-")
	sidechain := ""
	if cfg.Sidechain() != api.SideChain_SIDECHAIN_NONE {
		sidechain = strings.TrimPrefix(
			cfg.Sidechain().GetName(),
			blockchain+"-"+network+"-",
		)
	}
	return blockchain, network, sidechain
}

func validateSingleBlockRetentionProcessRequest(
	cfg *config.Config,
	request *SingleBlockRetentionProcessRequest,
) error {
	if request == nil {
		return xerrors.New("single-block retention process request is required")
	}
	cohort := request.Cohort
	if cohort.ConsolidatedObjectKey == "" || cohort.EndHeight <= cohort.StartHeight ||
		cohort.RowCount == 0 || cohort.EligibleAt.IsZero() {
		return xerrors.Errorf("invalid single-block retention cohort: %+v", cohort)
	}
	if !request.Execute {
		return nil
	}
	if !request.DirectStorageClientsGuarded {
		return xerrors.New("retention execution requires direct storage clients to be guarded or out of scope")
	}
	if !request.SingleBlockWritersGuarded {
		return xerrors.New("retention execution requires every single-block writer to honor the retirement fence")
	}
	if !request.FallbackReadsValidated {
		return xerrors.New("retention execution requires explicit fallback-disabled read validation")
	}
	if request.FallbackErrorCount != 0 {
		return xerrors.Errorf("retention execution requires zero fallback read errors, got %d", request.FallbackErrorCount)
	}
	if cfg != nil && isProductionRetentionEnvironment(cfg.Env()) && !request.ProductionDeleteEnabled {
		return xerrors.New("production retention execution requires explicit production-delete enablement")
	}
	return nil
}

func isProductionRetentionEnvironment(env config.Env) bool {
	value := strings.ToLower(string(env))
	return value == "production" || value == "prod"
}

func summarizeSingleBlockRetentionReport(
	cohort retirement.RetentionCohort,
	report *retirement.Report,
) (*SingleBlockRetentionRangeResult, error) {
	if report == nil {
		return nil, xerrors.New("single-block retention report is required")
	}
	if report.StartHeight != cohort.StartHeight || report.EndHeight != cohort.EndHeight {
		return nil, xerrors.Errorf(
			"retention report range [%d, %d) does not match cohort [%d, %d)",
			report.StartHeight,
			report.EndHeight,
			cohort.StartHeight,
			cohort.EndHeight,
		)
	}
	result := &SingleBlockRetentionRangeResult{
		Cohort:                   cohort,
		ScannedRows:              uint64(len(report.Items)),
		VerifiedThroughExclusive: cohort.StartHeight,
	}
	reasons := make(map[string]uint64)
	items := append([]retirement.Candidate(nil), report.Items...)
	sort.Slice(items, func(i, j int) bool {
		return items[i].Height < items[j].Height
	})
	for _, item := range items {
		if item.SkipReason != retirement.SkipSkippedBlock &&
			item.ConsolidatedKey != cohort.ConsolidatedObjectKey {
			return nil, xerrors.Errorf(
				"retention report row at height %d belongs to consolidated object %q, expected %q",
				item.Height,
				item.ConsolidatedKey,
				cohort.ConsolidatedObjectKey,
			)
		}
		switch item.Action {
		case retirement.ActionReportOnly, retirement.ActionDeleteObjectVersion:
			result.PlannedRows++
		case retirement.ActionDeletedVerified:
			result.DeletedVerifiedRows++
			result.DeletedVersions += uint64(len(item.VersionIDs))
			result.DeletedMarkers += uint64(len(item.DeleteMarkerVersionIDs))
			result.RetiredBytes += item.SingleBlockBytes
		case retirement.ActionAlreadyDeleted:
			result.AlreadyRetiredRows++
		case retirement.ActionDeletedObjectVersion:
			result.DeferredRows++
		default:
			if item.SkipReason == retirement.SkipSkippedBlock {
				result.SkippedSlots++
			} else if isDeferredRetentionReason(item.SkipReason) {
				result.DeferredRows++
			} else {
				result.FailedRows++
			}
		}
		if item.SkipReason != "" && item.SkipReason != retirement.SkipRetirementAlreadyFinalized &&
			item.SkipReason != retirement.SkipSkippedBlock {
			reasons[item.SkipReason]++
		}
	}
	reasonNames := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonNames = append(reasonNames, reason)
	}
	sort.Strings(reasonNames)
	for _, reason := range reasonNames {
		result.Blockers = append(result.Blockers, SingleBlockRetentionReasonCount{
			Reason: reason,
			Rows:   reasons[reason],
		})
	}

	nextHeight := cohort.StartHeight
	for _, item := range items {
		if item.Height < nextHeight {
			return nil, xerrors.Errorf("retention report contains duplicate or out-of-order height %d", item.Height)
		}
		if item.Height > nextHeight {
			result.FirstIncompleteHeight = uint64Pointer(nextHeight)
			break
		}
		if !terminalRetentionCandidate(item) {
			result.FirstIncompleteHeight = uint64Pointer(item.Height)
			break
		}
		nextHeight++
		result.VerifiedThroughExclusive = nextHeight
	}
	if result.FirstIncompleteHeight == nil && nextHeight < cohort.EndHeight {
		result.FirstIncompleteHeight = uint64Pointer(nextHeight)
	}
	result.Terminal = result.FirstIncompleteHeight == nil &&
		result.VerifiedThroughExclusive == cohort.EndHeight &&
		result.ScannedRows > 0
	return result, nil
}

func validateSingleBlockRetentionExecutionPlan(
	cohort retirement.RetentionCohort,
	report *retirement.Report,
) error {
	if report == nil {
		return xerrors.New("single-block retention execution report is required")
	}
	if report.StartHeight != cohort.StartHeight || report.EndHeight != cohort.EndHeight {
		return xerrors.Errorf(
			"retention execution report range [%d, %d) does not match cohort [%d, %d)",
			report.StartHeight,
			report.EndHeight,
			cohort.StartHeight,
			cohort.EndHeight,
		)
	}
	items := append([]retirement.Candidate(nil), report.Items...)
	sort.Slice(items, func(i, j int) bool {
		return items[i].Height < items[j].Height
	})
	nextHeight := cohort.StartHeight
	for _, item := range items {
		if item.Height != nextHeight {
			return xerrors.Errorf(
				"retention execution plan has non-contiguous height: expected=%d actual=%d",
				nextHeight,
				item.Height,
			)
		}
		if item.SkipReason == retirement.SkipSkippedBlock {
			if item.Action != retirement.ActionSkip {
				return xerrors.Errorf(
					"skipped retention slot at height %d has invalid action %q",
					item.Height,
					item.Action,
				)
			}
		} else {
			if item.ConsolidatedKey != cohort.ConsolidatedObjectKey {
				return xerrors.Errorf(
					"retention execution plan row at height %d belongs to consolidated object %q, expected %q",
					item.Height,
					item.ConsolidatedKey,
					cohort.ConsolidatedObjectKey,
				)
			}
			if item.Action != retirement.ActionDeleteObjectVersion &&
				item.Action != retirement.ActionAlreadyDeleted {
				return xerrors.Errorf(
					"retention execution plan row at height %d is not executable: action=%q reason=%q",
					item.Height,
					item.Action,
					item.SkipReason,
				)
			}
		}
		nextHeight++
	}
	if nextHeight != cohort.EndHeight {
		return xerrors.Errorf(
			"retention execution plan does not cover cohort [%d, %d): verified through %d",
			cohort.StartHeight,
			cohort.EndHeight,
			nextHeight,
		)
	}
	return nil
}

func terminalRetentionCandidate(item retirement.Candidate) bool {
	return item.Action == retirement.ActionDeletedVerified ||
		item.Action == retirement.ActionAlreadyDeleted ||
		item.Action == retirement.ActionSkip && item.SkipReason == retirement.SkipSkippedBlock
}

func isDeferredRetentionReason(reason string) bool {
	switch reason {
	case retirement.SkipRetentionPeriodActive,
		retirement.SkipCSCBSafetyQuiescenceActive,
		retirement.SkipRetirementVerificationPending,
		retirement.SkipRetirementClaimActive:
		return true
	default:
		return false
	}
}

func setSingleBlockRetentionRetry(result *SingleBlockRetentionRangeResult) bool {
	if result == nil || result.FailedRows != 0 || result.DeferredRows == 0 || len(result.Blockers) != 1 {
		return false
	}
	switch result.Blockers[0].Reason {
	case retirement.SkipCSCBSafetyQuiescenceActive:
		result.RetryAfter = retirement.RetentionSafetyQuiescencePeriod
	case retirement.SkipRetirementClaimActive:
		result.RetryAfter = retirement.RetirementClaimLease
	default:
		return false
	}
	result.RetryReason = result.Blockers[0].Reason
	return true
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}
