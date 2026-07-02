package cadence

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/coinbase/chainstorage/internal/config"
)

func TestNewWorkerOptionsPreservesDefaultActivityConcurrency(t *testing.T) {
	require := require.New(t)

	options := newWorkerOptions(config.WorkerConfig{TaskList: "default"})

	require.True(options.EnableSessionWorker)
	require.Equal(2*time.Second, options.DeadlockDetectionTimeout)
	require.Zero(options.MaxConcurrentActivityExecutionSize)
}

func TestNewWorkerOptionsAppliesActivityConcurrencyLimit(t *testing.T) {
	require := require.New(t)

	options := newWorkerOptions(config.WorkerConfig{
		TaskList:                           "batch_consolidator",
		MaxConcurrentActivityExecutionSize: 1,
	})

	require.True(options.EnableSessionWorker)
	require.Equal(2*time.Second, options.DeadlockDetectionTimeout)
	require.Equal(1, options.MaxConcurrentActivityExecutionSize)
}

func TestListOpenWorkflowExecutionsPaginatesAndAppliesTypeFilter(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	calls := make([]*workflowservice.ListOpenWorkflowExecutionsRequest, 0, 2)
	responses := []*workflowservice.ListOpenWorkflowExecutionsResponse{
		{
			Executions: []*workflowpb.WorkflowExecutionInfo{
				{
					Execution: &commonpb.WorkflowExecution{WorkflowId: "custom-manual-workflow-id-1"},
					Type:      &commonpb.WorkflowType{Name: "workflow.batch_consolidator"},
				},
			},
			NextPageToken: []byte("page-2"),
		},
		{
			Executions: []*workflowpb.WorkflowExecutionInfo{
				{
					Execution: &commonpb.WorkflowExecution{WorkflowId: "custom-manual-workflow-id-2"},
					Type:      &commonpb.WorkflowType{Name: "workflow.batch_consolidator"},
				},
			},
		},
	}

	response, err := listOpenWorkflowExecutions(
		ctx,
		"chainstorage-prod",
		100,
		"workflow.batch_consolidator",
		func(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
			calls = append(calls, request)
			return responses[len(calls)-1], nil
		},
	)

	require.NoError(err)
	require.Len(calls, 2)
	require.Equal("chainstorage-prod", calls[0].GetNamespace())
	require.Equal(int32(100), calls[0].GetMaximumPageSize())
	require.Empty(calls[0].GetNextPageToken())
	require.Equal("workflow.batch_consolidator", calls[0].GetTypeFilter().GetName())
	require.Equal([]byte("page-2"), calls[1].GetNextPageToken())
	require.Equal("workflow.batch_consolidator", calls[1].GetTypeFilter().GetName())
	require.Len(response.GetExecutions(), 2)
	require.Equal("custom-manual-workflow-id-1", response.GetExecutions()[0].GetExecution().GetWorkflowId())
	require.Equal("custom-manual-workflow-id-2", response.GetExecutions()[1].GetExecution().GetWorkflowId())
	require.Empty(response.GetNextPageToken())
}
