package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type batchConsolidatorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env               *cadence.TestEnv
	batchConsolidator *BatchConsolidator
	app               testapp.TestApp
	cfg               *config.Config
}

func TestBatchConsolidatorWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(batchConsolidatorTestSuite))
}

func (s *batchConsolidatorTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.BatchConsolidator.BatchSize = 100
	cfg.Workflows.BatchConsolidator.CheckpointSize = 500
	cfg.Workflows.BatchConsolidator.MaxBlocks = 25
	s.cfg = cfg

	s.env = cadence.NewTestEnv(s)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(s.cfg),
		cadence.WithTestEnv(s.env),
		fx.Provide(func() metastorage.MetaStorage {
			return nil
		}),
		fx.Provide(func() blobstorage.BlobStorage {
			return nil
		}),
		fx.Populate(&s.batchConsolidator),
	)
}

func (s *batchConsolidatorTestSuite) TearDownTest() {
	s.app.Close()
	s.env.AssertExpectations(s.T())
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorProcessesBatches() {
	require := testutil.Require(s.T())

	var requests []*activity.BatchConsolidatorRequest
	seenByStart := make(map[uint64]int)
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			seenByStart[request.StartHeight]++
			if seenByStart[request.StartHeight] > 1 {
				return &activity.BatchConsolidatorResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
				}, nil
			}
			return &activity.BatchConsolidatorResponse{
				StartHeight:        request.StartHeight,
				EndHeight:          request.EndHeight,
				ScannedBlocks:      request.EndHeight - request.StartHeight,
				ConsolidatedBlocks: request.EndHeight - request.StartHeight,
				ObjectKey:          "consolidated/object.zstd",
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1250,
		MaxBlocks:   100,
	})
	require.NoError(err)
	require.Len(requests, 5)
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 100}, requests[0])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 100}, requests[1])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1100, EndHeight: 1200, MaxBlocks: 100}, requests[2])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1100, EndHeight: 1200, MaxBlocks: 100}, requests[3])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1200, EndHeight: 1250, MaxBlocks: 100}, requests[4])
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorCheckpointContinuesAsNew() {
	require := testutil.Require(s.T())

	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(&activity.BatchConsolidatorResponse{
			ConsolidatedBlocks: 100,
			ObjectKey:          "consolidated/object.zstd",
		}, nil)

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1700,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorRequestOverridesConfig() {
	require := testutil.Require(s.T())

	var requests []*activity.BatchConsolidatorRequest
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1100,
		BatchSize:   40,
		MaxBlocks:   10,
	})
	require.NoError(err)
	require.Len(requests, 3)
	require.Equal(uint64(10), requests[0].MaxBlocks)
	require.Equal(uint64(1040), requests[0].EndHeight)
	require.Equal(uint64(1080), requests[1].EndHeight)
	require.Equal(uint64(1100), requests[2].EndHeight)
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorCapsMaxBlocksAtStorageLimit() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.Storage.Consolidation.MaxBlocks = 10

	var requests []*activity.BatchConsolidatorRequest
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1020,
		MaxBlocks:   100,
	})
	require.NoError(err)
	require.Len(requests, 1)
	require.Equal(uint64(10), requests[0].MaxBlocks)
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorCapsWindowAtShardBoundary() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.BatchSize = 10000
	s.cfg.Workflows.BatchConsolidator.CheckpointSize = 20000
	s.cfg.Workflows.BatchConsolidator.Storage.Consolidation.ShardSize = 10000

	var requests []*activity.BatchConsolidatorRequest
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 9500,
		EndHeight:   10500,
	})
	require.NoError(err)
	require.Len(requests, 2)
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 9500, EndHeight: 10000, MaxBlocks: 25}, requests[0])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 10000, EndHeight: 10500, MaxBlocks: 25}, requests[1])
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorRepeatsHeightBatchUntilEmpty() {
	require := testutil.Require(s.T())

	var requests []*activity.BatchConsolidatorRequest
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			if len(requests) <= 2 {
				return &activity.BatchConsolidatorResponse{
					StartHeight:        request.StartHeight,
					EndHeight:          request.EndHeight,
					ScannedBlocks:      request.MaxBlocks,
					ConsolidatedBlocks: request.MaxBlocks,
					ObjectKey:          "consolidated/object.zstd",
				}, nil
			}
			return &activity.BatchConsolidatorResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1100,
		MaxBlocks:   25,
	})
	require.NoError(err)
	require.Len(requests, 3)
	for _, request := range requests {
		require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 25}, request)
	}
}
