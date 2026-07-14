package workflow

import (
	"context"
	"encoding/hex"

	"github.com/stretchr/testify/mock"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

func (s *batchConsolidatorTestSuite) TestRepairExistingCSCBCompletesThenChecksForNextObject() {
	require := testutil.Require(s.T())
	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	s.mockAutoConsolidateLatestHeight(2000)
	var requests []*activity.BatchConsolidatorRequest
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			if len(requests) > 1 {
				return &activity.BatchConsolidatorResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
				}, nil
			}
			return &activity.BatchConsolidatorResponse{
				StartHeight:        1000,
				EndHeight:          1100,
				ScannedBlocks:      100,
				ConsolidatedBlocks: 100,
				ObjectKey:          "consolidated/clean.cscb.zstd",
				OldObjectKey:       "consolidated/dirty.cscb.zstd",
				RepairedObjects:    1,
			}, nil
		}).Twice()

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeRepairExistingCSCB,
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1100,
		MaxBlocks:   100,
	})
	require.NoError(err)
	require.Len(requests, 2)
	for _, request := range requests {
		require.Equal(config.ConsolidationModeRepairExistingCSCB, request.Mode)
		require.Equal(uint32(2), request.Tag)
		require.Equal(uint64(1000), request.StartHeight)
		require.Equal(uint64(1100), request.EndHeight)
		require.Equal(uint64(100), request.MaxBlocks)
		require.True(isRepairExecutionKey(request.RepairExecutionKey))
	}
	require.NotEqual(requests[0].RepairExecutionKey, requests[1].RepairExecutionKey)
}

func isRepairExecutionKey(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
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
