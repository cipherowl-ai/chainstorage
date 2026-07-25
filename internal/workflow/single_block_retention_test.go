package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type singleBlockRetentionTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env      *cadence.TestEnv
	workflow *SingleBlockRetention
	app      testapp.TestApp
	cfg      *config.Config
}

func TestSingleBlockRetentionWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(singleBlockRetentionTestSuite))
}

func TestValidateSingleBlockRetentionSelectionRange(t *testing.T) {
	require.ErrorContains(t, validateSingleBlockRetentionSelectionRange(100, 0), "end height is required")
	require.ErrorContains(t, validateSingleBlockRetentionSelectionRange(100, 100), "invalid")
	require.NoError(t, validateSingleBlockRetentionSelectionRange(0, 0))
	require.NoError(t, validateSingleBlockRetentionSelectionRange(100, 200))
}

func TestValidateSelectedSingleBlockRetentionCohort(t *testing.T) {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)

	require.NoError(t, validateSelectedSingleBlockRetentionCohort(cohort, 100, 110))
	require.NoError(t, validateSelectedSingleBlockRetentionCohort(cohort, 0, 0))
	require.ErrorContains(
		t,
		validateSelectedSingleBlockRetentionCohort(cohort, 101, 110),
		"outside requested range",
	)
	require.ErrorContains(
		t,
		validateSelectedSingleBlockRetentionCohort(cohort, 100, 109),
		"outside requested range",
	)
	cohort.EligibleAt = time.Time{}
	require.ErrorContains(t, validateSelectedSingleBlockRetentionCohort(cohort, 0, 0), "invalid selected")
}

func (s *singleBlockRetentionTestSuite) SetupTest() {
	cfg, err := config.New()
	require.NoError(s.T(), err)
	cfg.Workflows.SingleBlockRetention.MaxObjectRanges = 2
	s.cfg = cfg
	s.env = cadence.NewTestEnv(s)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(s.env),
		fx.Provide(func() metastorage.MetaStorage {
			return nil
		}),
		fx.Provide(func() blobstorage.BlobStorage {
			return nil
		}),
		fx.Populate(&s.workflow),
	)
}

func (s *singleBlockRetentionTestSuite) TearDownTest() {
	s.app.Close()
	s.env.AssertExpectations(s.T())
}

func (s *singleBlockRetentionTestSuite) TestDryRunReturnsPlannedRangesWithoutDeleting() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	var selectRequest *activity.SingleBlockRetentionSelectRequest
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionSelectRequest) (*activity.SingleBlockRetentionSelectResponse, error) {
			selectRequest = request
			return &activity.SingleBlockRetentionSelectResponse{
				Cohorts: []retirement.RetentionCohort{cohort},
				HasMore: true,
			}, nil
		})
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionRangeResult{
			Cohort:                   cohort,
			ScannedRows:              10,
			PlannedRows:              10,
			VerifiedThroughExclusive: 100,
		}, nil)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:             2,
		StartHeight:     100,
		EndHeight:       110,
		MaxObjectRanges: 1,
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), &activity.SingleBlockRetentionSelectRequest{
		Tag:         2,
		StartHeight: 100,
		EndHeight:   110,
		Limit:       1,
	}, selectRequest)

	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.False(s.T(), result.Execute)
	require.Equal(s.T(), uint32(2), result.Tag)
	require.Equal(s.T(), uint64(100), result.SelectionStartHeight)
	require.Equal(s.T(), uint64(110), result.SelectionEndHeight)
	require.Equal(s.T(), uint64(1), result.SelectedObjectRanges)
	require.True(s.T(), result.MoreEligibleRanges)
	require.Equal(s.T(), uint64(1), result.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(10), result.PlannedRows)
	require.Empty(s.T(), result.CompletedObjectRanges)
	require.Empty(s.T(), result.FailureMessage)
}

func (s *singleBlockRetentionTestSuite) TestExecuteReturnsExactCompletedRanges() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{cohort},
		}, nil)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			require.True(s.T(), request.FallbackReadsValidated)
			// The operator approval must reach the activity verbatim, never
			// rewritten to whatever cohort was selected.
			require.Equal(s.T(), "solana-mainnet", request.ApprovedChain)
			require.Equal(s.T(), uint64(100), request.ApprovedStartHeight)
			require.Equal(s.T(), uint64(110), request.ApprovedEndHeight)
			return &activity.SingleBlockRetentionRangeResult{
				Cohort:                   request.Cohort,
				ScannedRows:              request.Cohort.RowCount,
				DeletedVerifiedRows:      request.Cohort.RowCount,
				DeletedVersions:          request.Cohort.RowCount,
				RetiredBytes:             request.Cohort.RowCount * 100,
				VerifiedThroughExclusive: request.Cohort.EndHeight,
				Terminal:                 true,
			}, nil
		})

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   110,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           110,
	})
	require.NoError(s.T(), err)

	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.True(s.T(), result.Execute)
	require.Equal(s.T(), "solana-mainnet", result.ApprovedChain)
	require.Equal(s.T(), uint64(100), result.ApprovedStartHeight)
	require.Equal(s.T(), uint64(110), result.ApprovedEndHeight)
	require.Equal(s.T(), uint64(1), result.SelectedObjectRanges)
	require.Equal(s.T(), uint64(1), result.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(10), result.DeletedVerifiedRows)
	require.Equal(s.T(), uint64(10), result.DeletedVersions)
	require.Equal(s.T(), uint64(1000), result.RetiredBytes)
	require.Equal(s.T(), []SingleBlockRetentionCompletedRange{
		{
			ConsolidatedObjectKey: cohort.ConsolidatedObjectKey,
			StartHeight:           100,
			EndHeight:             110,
			EligibleRows:          10,
		},
	}, result.CompletedObjectRanges)
}

func (s *singleBlockRetentionTestSuite) TestExecuteFailsClosedWhenCohortDoesNotMatchApprovedRange() {
	first := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	second := testRetentionCohort("consolidated/200-210.cscb.zstd", 200, 210)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{first, second},
		}, nil)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   210,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           210,
	})
	require.ErrorContains(s.T(), err, "does not exactly match the approved range")
}

func (s *singleBlockRetentionTestSuite) TestExecuteRetriesSafetyQuiescenceOnce() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{cohort},
		}, nil)
	attempt := 0
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			attempt++
			if attempt == 1 {
				return &activity.SingleBlockRetentionRangeResult{
					Cohort:       request.Cohort,
					DeferredRows: request.Cohort.RowCount,
					RetryAfter:   time.Minute,
					RetryReason:  retirement.SkipCSCBSafetyQuiescenceActive,
				}, nil
			}
			return &activity.SingleBlockRetentionRangeResult{
				Cohort:                   request.Cohort,
				ScannedRows:              request.Cohort.RowCount,
				DeletedVerifiedRows:      request.Cohort.RowCount,
				VerifiedThroughExclusive: request.Cohort.EndHeight,
				Terminal:                 true,
			}, nil
		}).Twice()

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   110,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           110,
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, attempt)
}

func (s *singleBlockRetentionTestSuite) TestExecuteFailsClosedWhenRangeIsIncomplete() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{cohort},
		}, nil)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionRangeResult{
			Cohort:                   cohort,
			ScannedRows:              10,
			FailedRows:               1,
			VerifiedThroughExclusive: 105,
		}, nil)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   110,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           110,
	})
	require.ErrorContains(s.T(), err, "did not finish")
}

func TestValidateSingleBlockRetentionExecutionRequestGates(t *testing.T) {
	validRequest := func() *SingleBlockRetentionRequest {
		return &SingleBlockRetentionRequest{
			StartHeight:                 100,
			EndHeight:                   110,
			Execute:                     true,
			DirectStorageClientsGuarded: true,
			SingleBlockWritersGuarded:   true,
			FallbackReadsValidated:      true,
			ApprovedChain:               "solana-mainnet",
			ApprovedStartHeight:         100,
			ApprovedEndHeight:           110,
		}
	}
	require.NoError(t, validateSingleBlockRetentionExecutionRequest(validRequest()))

	request := validRequest()
	request.StartHeight = 0
	request.EndHeight = 0
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request),
		"explicit exact selection range",
	)

	request = validRequest()
	request.ApprovedChain = ""
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request),
		"operator approval chain",
	)

	request = validRequest()
	request.ApprovedStartHeight = 110
	request.ApprovedEndHeight = 110
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request),
		"valid exact approved range",
	)

	request = validRequest()
	request.ApprovedEndHeight = 109
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request),
		"must exactly match the selection range",
	)

	request = validRequest()
	request.FallbackReadsValidated = false
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request),
		"explicit fallback-disabled read validation",
	)

	request = validRequest()
	request.FallbackErrorCount = 3
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request),
		"zero fallback read errors",
	)
}

func TestValidateApprovedSingleBlockRetentionCohort(t *testing.T) {
	request := &SingleBlockRetentionRequest{
		ApprovedStartHeight: 100,
		ApprovedEndHeight:   110,
	}
	require.NoError(t, validateApprovedSingleBlockRetentionCohort(
		testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110),
		request,
	))
	require.ErrorContains(
		t,
		validateApprovedSingleBlockRetentionCohort(
			testRetentionCohort("consolidated/100-109.cscb.zstd", 100, 109),
			request,
		),
		"does not exactly match the approved range",
	)
}

func testRetentionCohort(key string, start uint64, end uint64) retirement.RetentionCohort {
	return retirement.RetentionCohort{
		ConsolidatedObjectKey: key,
		StartHeight:           start,
		EndHeight:             end,
		RowCount:              end - start,
		EligibleAt:            time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC),
	}
}
