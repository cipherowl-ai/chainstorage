package workflow

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

func (s *batchConsolidatorTestSuite) TestRepairExistingCSCBStopsAfterVerifiedCanary() {
	require := testutil.Require(s.T())
	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	s.mockAutoConsolidateLatestHeight(2000)
	var requests []*activity.BatchConsolidatorRequest
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			return &activity.BatchConsolidatorResponse{
				StartHeight:        1000,
				EndHeight:          1100,
				ScannedBlocks:      100,
				ConsolidatedBlocks: 100,
				ObjectKey:          "consolidated/clean.cscb.zstd",
				OldObjectKey:       "consolidated/dirty.cscb.zstd",
				RepairedObjects:    1,
				PendingOldDeletion: true,
			}, nil
		}).Once()

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeRepairExistingCSCB,
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1100,
		MaxBlocks:   100,
	})
	require.NoError(err)
	require.Equal([]*activity.BatchConsolidatorRequest{{
		Mode:        config.ConsolidationModeRepairExistingCSCB,
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1100,
		MaxBlocks:   100,
	}}, requests)
}

func (s *batchConsolidatorTestSuite) TestRepairExistingCSCBDeletesThenFindsNextExactObject() {
	require := testutil.Require(s.T())
	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	s.mockAutoConsolidateLatestHeight(2000)
	var requests []*activity.BatchConsolidatorRequest
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			if len(requests) == 1 {
				return &activity.BatchConsolidatorResponse{
					StartHeight:        1000,
					EndHeight:          1100,
					ScannedBlocks:      100,
					ConsolidatedBlocks: 100,
					ObjectKey:          "consolidated/clean.cscb.zstd",
					OldObjectKey:       "consolidated/dirty.cscb.zstd",
					RepairedObjects:    1,
				}, nil
			}
			return &activity.BatchConsolidatorResponse{
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			}, nil
		}).Twice()

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:             config.ConsolidationModeRepairExistingCSCB,
		Tag:              2,
		StartHeight:      900,
		EndHeight:        1200,
		MaxBlocks:        100,
		DeleteOldObjects: true,
	})
	require.NoError(err)
	require.Len(requests, 2)
	for _, request := range requests {
		require.Equal(&activity.BatchConsolidatorRequest{
			Mode:             config.ConsolidationModeRepairExistingCSCB,
			Tag:              2,
			StartHeight:      900,
			EndHeight:        1200,
			MaxBlocks:        100,
			DeleteOldObjects: true,
		}, request)
	}
}

func (s *batchConsolidatorTestSuite) TestRepairExistingCSCBRequiresSerialExecution() {
	require := testutil.Require(s.T())

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeRepairExistingCSCB,
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1100,
		MaxBlocks:   100,
		Parallelism: 2,
	})
	require.Error(err)
	require.Contains(err.Error(), "repair_existing_cscb requires parallelism=1")
}
