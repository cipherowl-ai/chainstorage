package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

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
			if request.StatsOnly {
				return &activity.BatchConsolidatorResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
				}, nil
			}
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
	normalRequests := nonStatsRequests(requests)
	require.Len(normalRequests, 5)
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 100}, normalRequests[0])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 100}, normalRequests[1])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1100, EndHeight: 1200, MaxBlocks: 100}, normalRequests[2])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1100, EndHeight: 1200, MaxBlocks: 100}, normalRequests[3])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1200, EndHeight: 1250, MaxBlocks: 100}, normalRequests[4])
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
	normalRequests := nonStatsRequests(requests)
	require.Len(normalRequests, 3)
	require.Equal(uint64(10), normalRequests[0].MaxBlocks)
	require.Equal(uint64(1040), normalRequests[0].EndHeight)
	require.Equal(uint64(1080), normalRequests[1].EndHeight)
	require.Equal(uint64(1100), normalRequests[2].EndHeight)
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
	normalRequests := nonStatsRequests(requests)
	require.Len(normalRequests, 1)
	require.Equal(uint64(10), normalRequests[0].MaxBlocks)
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
	normalRequests := nonStatsRequests(requests)
	require.Len(normalRequests, 2)
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 9500, EndHeight: 10000, MaxBlocks: 25}, normalRequests[0])
	require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 10000, EndHeight: 10500, MaxBlocks: 25}, normalRequests[1])
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorRepeatsHeightBatchUntilEmpty() {
	require := testutil.Require(s.T())

	var requests []*activity.BatchConsolidatorRequest
	normalCalls := 0
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			if request.StatsOnly {
				return &activity.BatchConsolidatorResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
				}, nil
			}
			normalCalls++
			if normalCalls <= 2 {
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
	normalRequests := nonStatsRequests(requests)
	require.Len(normalRequests, 3)
	for _, request := range normalRequests {
		require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 25}, request)
	}
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorAccountsLostActivityCompletionFromShadowStats() {
	require := testutil.Require(s.T())

	var requests []*activity.BatchConsolidatorRequest
	call := 0
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			if request.StatsOnly {
				return &activity.BatchConsolidatorResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
				}, nil
			}
			call++
			switch call {
			case 1:
				return nil, xerrors.New("completion lost after side effects")
			case 2:
				return &activity.BatchConsolidatorResponse{
					StartHeight:        request.StartHeight,
					EndHeight:          request.EndHeight,
					ScannedBlocks:      request.MaxBlocks,
					ConsolidatedBlocks: request.MaxBlocks,
					ObjectKey:          "consolidated/second.cscb.zstd",
					ShadowObjects:      2,
					ShadowBlocks:       2 * request.MaxBlocks,
				}, nil
			default:
				return &activity.BatchConsolidatorResponse{
					StartHeight:   request.StartHeight,
					EndHeight:     request.EndHeight,
					ShadowObjects: 2,
					ShadowBlocks:  2 * request.MaxBlocks,
				}, nil
			}
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1100,
		MaxBlocks:   25,
	})
	require.NoError(err)

	normalRequests := nonStatsRequests(requests)
	require.Len(normalRequests, 3)
	for _, request := range normalRequests {
		require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 25}, request)
	}
}

func nonStatsRequests(requests []*activity.BatchConsolidatorRequest) []*activity.BatchConsolidatorRequest {
	normalRequests := make([]*activity.BatchConsolidatorRequest, 0, len(requests))
	for _, request := range requests {
		if !request.StatsOnly {
			normalRequests = append(normalRequests, request)
		}
	}
	return normalRequests
}
