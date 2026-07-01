package workflow

import (
	"context"
	"sort"
	"sync/atomic"
	"testing"
	"time"

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
	cfg.Workflows.BatchConsolidator.Storage.Consolidation.Enabled = true
	cfg.Workflows.BatchConsolidator.Storage.Consolidation.Mode = config.ConsolidationModeShadowDualWrite
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
	s.mockEmptyShadowStats()
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

	s.mockEmptyShadowStats()
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
	s.mockEmptyShadowStats()
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
	s.mockEmptyShadowStats()
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
	s.mockEmptyShadowStats()
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
	normalCalls := 0
	s.mockEmptyShadowStats()
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
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
	require.Len(requests, 3)
	for _, request := range requests {
		require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 25}, request)
	}
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorIgnoresParallelismOutsideAutoConsolidate() {
	require := testutil.Require(s.T())

	var requests []*activity.BatchConsolidatorRequest
	normalCalls := 0
	s.mockEmptyShadowStats()
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			normalCalls++
			if normalCalls == 1 {
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
		Parallelism: 4,
	})
	require.NoError(err)
	require.Len(requests, 2)
	for _, request := range requests {
		require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 25}, request)
	}
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorRejectsExcessiveParallelism() {
	require := testutil.Require(s.T())

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   200,
		MaxBlocks:   25,
		Parallelism: batchConsolidatorMaxParallelism + 1,
	})
	require.Error(err)
	require.Contains(err.Error(), "parallelism(11) exceeds max(10)")
}

func (s *batchConsolidatorTestSuite) TestAutoConsolidateValidatesFinalizedRange() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	var requests []*activity.BatchConsolidatorRequest
	s.mockAutoConsolidateLatestHeight(120)
	s.mockEmptyShadowStats()
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   111,
		MaxBlocks:   11,
	})
	require.NoError(err)
	require.Len(requests, 1)
	require.Equal(&activity.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   111,
		MaxBlocks:   11,
	}, requests[0])
}

func (s *batchConsolidatorTestSuite) TestDeprecatedHistoricalBackfillAliasRemainsAccepted() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	var requests []*activity.BatchConsolidatorRequest
	s.mockAutoConsolidateLatestHeight(120)
	s.mockEmptyShadowStats()
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeHistoricalBackfill,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   111,
		MaxBlocks:   11,
	})
	require.NoError(err)
	require.Len(requests, 1)
	require.Equal(config.ConsolidationModeHistoricalBackfill, requests[0].Mode)
}

func (s *batchConsolidatorTestSuite) TestAutoConsolidateRejectsReorgUnsafeRange() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	s.mockAutoConsolidateLatestHeight(120)

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   113,
	})
	require.Error(err)
	require.Contains(err.Error(), "auto_consolidate range [100, 113) is unsafe")
}

func (s *batchConsolidatorTestSuite) TestAutoConsolidateDuplicateRunNoOpsWhenScanEmpty() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	var requests []*activity.BatchConsolidatorRequest
	s.mockAutoConsolidateLatestHeight(120)
	s.mockEmptyShadowStats()
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   111,
		MaxBlocks:   11,
	})
	require.NoError(err)
	require.Len(requests, 1)
	require.Equal(config.ConsolidationModeAutoConsolidate, requests[0].Mode)
}

func (s *batchConsolidatorTestSuite) TestAutoConsolidateWaitsForFullObjectWindow() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	s.mockAutoConsolidateLatestHeight(120)
	s.mockEmptyShadowStats()

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   111,
		MaxBlocks:   25,
	})
	require.NoError(err)
}

func (s *batchConsolidatorTestSuite) TestAutoConsolidateProcessesObjectWindowsInParallel() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	s.cfg.Workflows.BatchConsolidator.BatchSize = 100
	s.cfg.Workflows.BatchConsolidator.CheckpointSize = 500
	var requests []*activity.BatchConsolidatorRequest
	s.mockAutoConsolidateLatestHeight(220)
	s.mockEmptyShadowStats()
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{
				StartHeight:        request.StartHeight,
				EndHeight:          request.EndHeight,
				ScannedBlocks:      request.EndHeight - request.StartHeight,
				ConsolidatedBlocks: request.EndHeight - request.StartHeight,
				ObjectKey:          "consolidated/object.zstd",
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   200,
		MaxBlocks:   25,
		Parallelism: 4,
	})
	require.NoError(err)
	require.Len(requests, 4)
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].StartHeight < requests[j].StartHeight
	})
	require.Equal(&activity.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   125,
		MaxBlocks:   25,
	}, requests[0])
	require.Equal(&activity.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 125,
		EndHeight:   150,
		MaxBlocks:   25,
	}, requests[1])
	require.Equal(&activity.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 150,
		EndHeight:   175,
		MaxBlocks:   25,
	}, requests[2])
	require.Equal(&activity.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 175,
		EndHeight:   200,
		MaxBlocks:   25,
	}, requests[3])
}

func (s *batchConsolidatorTestSuite) TestAutoConsolidateRunsObjectWindowsConcurrently() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	s.cfg.Workflows.BatchConsolidator.BatchSize = 100
	s.cfg.Workflows.BatchConsolidator.CheckpointSize = 500
	var inFlight int64
	var maxInFlight int64
	s.mockAutoConsolidateLatestHeight(220)
	s.mockEmptyShadowStats()
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			current := atomic.AddInt64(&inFlight, 1)
			for {
				maxSeen := atomic.LoadInt64(&maxInFlight)
				if current <= maxSeen || atomic.CompareAndSwapInt64(&maxInFlight, maxSeen, current) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&inFlight, -1)
			return &activity.BatchConsolidatorResponse{
				StartHeight:        request.StartHeight,
				EndHeight:          request.EndHeight,
				ScannedBlocks:      request.EndHeight - request.StartHeight,
				ConsolidatedBlocks: request.EndHeight - request.StartHeight,
				ObjectKey:          "consolidated/object.zstd",
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   200,
		MaxBlocks:   25,
		Parallelism: 4,
	})
	require.NoError(err)
	observedMaxInFlight := atomic.LoadInt64(&maxInFlight)
	s.T().Logf("max in-flight batch_consolidator activities: %d", observedMaxInFlight)
	require.Greater(observedMaxInFlight, int64(1))
}

func (s *batchConsolidatorTestSuite) TestAutoConsolidateResumeAfterLostActivityCompletion() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	var requests []*activity.BatchConsolidatorRequest
	s.mockAutoConsolidateLatestHeight(120)
	statsCalls := 0
	s.env.OnActivity(activity.ActivityBatchConsolidatorStats, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorStatsRequest) (*activity.BatchConsolidatorStatsResponse, error) {
			require.Equal(config.ConsolidationModeAutoConsolidate, request.Mode)
			statsCalls++
			if statsCalls == 1 {
				return &activity.BatchConsolidatorStatsResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
				}, nil
			}
			return &activity.BatchConsolidatorStatsResponse{
				StartHeight:   request.StartHeight,
				EndHeight:     request.EndHeight,
				ShadowObjects: 1,
				ShadowBlocks:  25,
			}, nil
		})
	call := 0
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			require.Equal(config.ConsolidationModeAutoConsolidate, request.Mode)
			call++
			if call == 1 {
				return nil, xerrors.New("completion lost after side effects")
			}
			return &activity.BatchConsolidatorResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		})

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   111,
		MaxBlocks:   11,
	})
	require.NoError(err)
	require.Equal(2, statsCalls)
	require.Len(requests, 2)
}

func TestBatchConsolidatorAutoConsolidateSafeEndHeight(t *testing.T) {
	require := testutil.Require(t)

	safeEnd, ok := batchConsolidatorAutoConsolidateSafeEndHeight(120, 10)
	require.True(ok)
	require.Equal(uint64(112), safeEnd)

	safeEnd, ok = batchConsolidatorAutoConsolidateSafeEndHeight(120, 0)
	require.True(ok)
	require.Equal(uint64(121), safeEnd)

	_, ok = batchConsolidatorAutoConsolidateSafeEndHeight(8, 10)
	require.False(ok)
}

func (s *batchConsolidatorTestSuite) TestPromoteFinalizedProcessesOnlyPlannedRange() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized
	var requests []*activity.BatchConsolidatorRequest
	s.env.OnActivity(activity.ActivityBatchConsolidatorPlan, mock.Anything, mock.Anything).
		Return(&activity.BatchConsolidatorPlanResponse{
			StartHeight:         1000,
			EndHeight:           1100,
			LatestHeight:        1200,
			SafePromotionHeight: 1190,
			PromotionGateHeight: 1100,
		}, nil)
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{
				StartHeight:        request.StartHeight,
				EndHeight:          request.EndHeight,
				ScannedBlocks:      request.EndHeight - request.StartHeight,
				ConsolidatedBlocks: request.EndHeight - request.StartHeight,
			}, nil
		}).Once()
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		}).Once()

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1300,
		MaxBlocks:   100,
	})
	require.NoError(err)
	require.Equal([]*activity.BatchConsolidatorRequest{
		{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 100},
		{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 100},
	}, requests)
}

func (s *batchConsolidatorTestSuite) TestPromoteFinalizedNoOpsWhenPlanHasNoSafeRange() {
	require := testutil.Require(s.T())

	s.cfg.Workflows.BatchConsolidator.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized
	s.env.OnActivity(activity.ActivityBatchConsolidatorPlan, mock.Anything, mock.Anything).
		Return(&activity.BatchConsolidatorPlanResponse{
			StartHeight:         1000,
			EndHeight:           1000,
			LatestHeight:        900,
			SafePromotionHeight: 0,
			PromotionGateHeight: 1200,
		}, nil)

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1300,
		MaxBlocks:   100,
	})
	require.NoError(err)
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorAccountsLostActivityCompletionFromShadowStats() {
	require := testutil.Require(s.T())

	var requests []*activity.BatchConsolidatorRequest
	statsCalls := 0
	s.env.OnActivity(activity.ActivityBatchConsolidatorStats, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorStatsRequest) (*activity.BatchConsolidatorStatsResponse, error) {
			statsCalls++
			if statsCalls == 1 {
				return &activity.BatchConsolidatorStatsResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
				}, nil
			}
			return &activity.BatchConsolidatorStatsResponse{
				StartHeight:   request.StartHeight,
				EndHeight:     request.EndHeight,
				ShadowObjects: 2,
				ShadowBlocks:  50,
			}, nil
		})
	call := 0
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
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
				}, nil
			default:
				return &activity.BatchConsolidatorResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
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

	require.GreaterOrEqual(statsCalls, 3)
	require.Len(requests, 3)
	for _, request := range requests {
		require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 25}, request)
	}
}

func (s *batchConsolidatorTestSuite) TestBatchConsolidatorAccountsLostActivityCompletionWhenRetryScanIsEmpty() {
	require := testutil.Require(s.T())

	var requests []*activity.BatchConsolidatorRequest
	statsCalls := 0
	s.env.OnActivity(activity.ActivityBatchConsolidatorStats, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorStatsRequest) (*activity.BatchConsolidatorStatsResponse, error) {
			statsCalls++
			if statsCalls == 1 {
				return &activity.BatchConsolidatorStatsResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
				}, nil
			}
			return &activity.BatchConsolidatorStatsResponse{
				StartHeight:   request.StartHeight,
				EndHeight:     request.EndHeight,
				ShadowObjects: 1,
				ShadowBlocks:  25,
			}, nil
		})
	call := 0
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			call++
			if call == 1 {
				return nil, xerrors.New("completion lost after side effects")
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

	require.Equal(2, statsCalls)
	require.Len(requests, 2)
	for _, request := range requests {
		require.Equal(&activity.BatchConsolidatorRequest{Tag: 2, StartHeight: 1000, EndHeight: 1100, MaxBlocks: 25}, request)
	}
}

func (s *batchConsolidatorTestSuite) TestShadowStatsActivityRetryPolicyUsesUnlimitedAttempts() {
	require := testutil.Require(s.T())

	activityRetry := s.cfg.Workflows.BatchConsolidator.ActivityRetry
	activityRetry.MaximumAttempts = 3
	policy := s.batchConsolidator.getShadowStatsActivityRetryPolicy(activityRetry)

	require.NotNil(policy)
	require.Equal(int32(0), policy.MaximumAttempts)
	require.Equal(activityRetry.BackoffCoefficient, policy.BackoffCoefficient)
	require.Equal(activityRetry.InitialInterval, policy.InitialInterval)
	require.Equal(activityRetry.MaximumInterval, policy.MaximumInterval)
	require.Equal(int32(3), activityRetry.MaximumAttempts)
}

func (s *batchConsolidatorTestSuite) mockEmptyShadowStats() {
	s.env.OnActivity(activity.ActivityBatchConsolidatorStats, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorStatsRequest) (*activity.BatchConsolidatorStatsResponse, error) {
			return &activity.BatchConsolidatorStatsResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		})
}

func (s *batchConsolidatorTestSuite) mockAutoConsolidateLatestHeight(height uint64) {
	s.env.OnActivity(activity.ActivityBatchConsolidatorLatestBlock, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.BatchConsolidatorLatestBlockRequest) (*activity.BatchConsolidatorLatestBlockResponse, error) {
			return &activity.BatchConsolidatorLatestBlockResponse{
				Tag:    request.Tag,
				Height: height,
			}, nil
		})
}
