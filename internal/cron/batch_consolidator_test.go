package cron

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/timesource"
	workflowpkg "github.com/coinbase/chainstorage/internal/workflow"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	batchConsolidatorCronRuntime struct {
		logger         *zap.Logger
		openWorkflowID []string
		executions     []batchConsolidatorCronExecution
	}

	batchConsolidatorCronExecution struct {
		options client.StartWorkflowOptions
		request any
	}

	batchConsolidatorCronRun struct {
		id    string
		runID string
	}
)

var _ cadence.Runtime = (*batchConsolidatorCronRuntime)(nil)

func TestBatchConsolidatorCronStartsBoundedPromotionWorkflow(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000
	cfg.Cron.BatchConsolidator.MaxRangeBlocks = 200

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 2_000}, nil)
	metaStorage.EXPECT().
		GetFirstPromotableBlockConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(1_901)).
		Return(uint64(1_500), true, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, "workflow.batch_consolidator/auto_promote_finalized", runtime.executions[0].options.ID)
	require.Equal(t, "default", runtime.executions[0].options.TaskQueue)
	require.Equal(t, &workflowpkg.BatchConsolidatorRequest{
		Tag:         tag,
		StartHeight: 1_500,
		EndHeight:   1_700,
	}, runtime.executions[0].request)
}

func TestBatchConsolidatorCronCapsSearchAndWorkflowRangeAtPromotionGate(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	gateHeight := uint64(1_600)
	cfg.AWS.Storage.Consolidation.PromotionGateHeight = &gateHeight
	cfg.Cron.BatchConsolidator.StartHeight = 1_000
	cfg.Cron.BatchConsolidator.MaxRangeBlocks = 10_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 2_000}, nil)
	metaStorage.EXPECT().
		GetFirstPromotableBlockConsolidationShadow(gomock.Any(), tag, uint64(1_000), gateHeight).
		Return(uint64(1_500), true, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, &workflowpkg.BatchConsolidatorRequest{
		Tag:         tag,
		StartHeight: 1_500,
		EndHeight:   gateHeight,
	}, runtime.executions[0].request)
}

func TestBatchConsolidatorCronNoOpsWhenNoPromotableShadowExists(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 2_000}, nil)
	metaStorage.EXPECT().
		GetFirstPromotableBlockConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(1_901)).
		Return(uint64(0), false, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronSkipsWhenBatchConsolidatorWorkflowIsAlreadyOpen(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	runtime.openWorkflowID = []string{"workflow.batch_consolidator.solana-mainnet.1-2.promote"}
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetFirstPromotableBlockConsolidationShadow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func newBatchConsolidatorCronTask(t *testing.T) (*batchConsolidatorTask, *batchConsolidatorCronRuntime, *metastoragemocks.MockMetaStorage, *config.Config, *gomock.Controller) {
	t.Helper()
	cfg, err := config.New()
	require.NoError(t, err)
	cfg.Chain.BlockTag.Stable = 2
	cfg.Chain.BlockTag.Latest = 2
	cfg.Cron.BatchConsolidator.Enabled = true
	cfg.Cron.BatchConsolidator.MaxRangeBlocks = 10_000
	cfg.AWS.Storage.Consolidation.Enabled = true
	cfg.AWS.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized
	safeLag := uint64(100)
	cfg.AWS.Storage.Consolidation.SafePromotionLag = &safeLag

	logger := zaptest.NewLogger(t)
	runtime := &batchConsolidatorCronRuntime{logger: logger}
	batchConsolidator := workflowpkg.NewBatchConsolidator(workflowpkg.BatchConsolidatorParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Runtime: runtime,
	})
	ctrl := gomock.NewController(t)
	metaStorage := metastoragemocks.NewMockMetaStorage(ctrl)
	task, err := NewBatchConsolidator(BatchConsolidatorTaskParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Config:            cfg,
		Runtime:           runtime,
		MetaStorage:       metaStorage,
		BatchConsolidator: batchConsolidator,
	})
	require.NoError(t, err)
	return task.(*batchConsolidatorTask), runtime, metaStorage, cfg, ctrl
}

func (r *batchConsolidatorCronRuntime) RegisterWorkflow(w any, options workflow.RegisterOptions) {}

func (r *batchConsolidatorCronRuntime) RegisterActivity(a any, options activity.RegisterOptions) {}

func (r *batchConsolidatorCronRuntime) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, request any) (client.WorkflowRun, error) {
	r.executions = append(r.executions, batchConsolidatorCronExecution{
		options: options,
		request: request,
	})
	return batchConsolidatorCronRun{id: options.ID, runID: "test-run-id"}, nil
}

func (r *batchConsolidatorCronRuntime) ExecuteActivity(ctx workflow.Context, activity any, request any, response any) error {
	panic("ExecuteActivity is not used by batch_consolidator cron tests")
}

func (r *batchConsolidatorCronRuntime) GetLogger(ctx workflow.Context) *zap.Logger {
	return r.logger
}

func (r *batchConsolidatorCronRuntime) GetMetricsHandler(ctx workflow.Context) client.MetricsHandler {
	return client.MetricsNopHandler
}

func (r *batchConsolidatorCronRuntime) GetActivityLogger(ctx context.Context) *zap.Logger {
	return r.logger
}

func (r *batchConsolidatorCronRuntime) GetTimeSource(ctx workflow.Context) timesource.TimeSource {
	return timesource.NewRealTimeSource()
}

func (r *batchConsolidatorCronRuntime) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string) error {
	return nil
}

func (r *batchConsolidatorCronRuntime) OnStart(ctx context.Context) error {
	return nil
}

func (r *batchConsolidatorCronRuntime) OnStop(ctx context.Context) error {
	return nil
}

func (r *batchConsolidatorCronRuntime) ListOpenWorkflows(ctx context.Context, namespace string, maxPageSize int32) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	executions := make([]*workflowpb.WorkflowExecutionInfo, 0, len(r.openWorkflowID))
	for _, workflowID := range r.openWorkflowID {
		executions = append(executions, &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
	}
	return &workflowservice.ListOpenWorkflowExecutionsResponse{Executions: executions}, nil
}

func (r batchConsolidatorCronRun) GetID() string {
	return r.id
}

func (r batchConsolidatorCronRun) GetRunID() string {
	return r.runID
}

func (r batchConsolidatorCronRun) Get(ctx context.Context, valuePtr any) error {
	return nil
}

func (r batchConsolidatorCronRun) GetWithOptions(ctx context.Context, valuePtr any, options client.WorkflowRunGetOptions) error {
	return nil
}
