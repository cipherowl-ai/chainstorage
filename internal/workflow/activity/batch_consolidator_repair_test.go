package activity

import (
	"context"

	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepair"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBRestoresRebuildsAndVerifies() {
	require := testutil.Require(s.T())
	records, blocks := makeConsolidatorFixture(1, 1000)
	request := &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeRepairExistingCSCB,
		Tag:         2,
		StartHeight: 900,
		EndHeight:   1200,
		MaxBlocks:   100,
	}
	oldKey := "consolidated/dirty.cscb.zstd"
	newKey := "consolidated/clean.cscb.zstd"
	prepared := repairActivityManifest(cscbrepair.StatePrepared, oldKey, "")
	restored := repairActivityManifest(cscbrepair.StateRestored, oldKey, "")
	verified := repairActivityManifest(cscbrepair.StateVerified, oldKey, newKey)
	fake := &repairActivityFake{
		prepared: prepared,
		restored: restored,
		verified: verified,
	}
	s.batchConsolidator.repairer = fake
	s.batchConsolidator.config.AWS.Storage.Consolidation.LocalSpillDir = s.T().TempDir()

	gomock.InOrder(
		s.metaStorage.EXPECT().
			PromoteBlockConsolidationShadows(gomock.Any(), uint32(2), uint64(1000), uint64(1001), uint64(100), config.DefaultSingleBlockObjectRetention).
			Return(&metastorage.ConsolidationPromotionResult{}, nil),
		s.metaStorage.EXPECT().
			GetBlocksMissingConsolidationShadow(gomock.Any(), uint32(2), uint64(1000), uint64(1001), uint64(100)).
			Return(records, nil),
		s.blobStorage.EXPECT().Download(gomock.Any(), records[0].Metadata).Return(blocks[0], nil),
		s.blobStorage.EXPECT().
			UploadConsolidated(gomock.Any(), gomock.Any()).
			Return(newKey, makeConsolidatorPlacements(records), nil),
		s.metaStorage.EXPECT().PersistBlockConsolidationShadows(gomock.Any(), gomock.Any()).Return(nil),
		s.metaStorage.EXPECT().
			PromoteBlockConsolidationShadows(gomock.Any(), uint32(2), uint64(1000), uint64(1001), uint64(100), config.DefaultSingleBlockObjectRetention).
			Return(&metastorage.ConsolidationPromotionResult{Blocks: 1}, nil),
	)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight:        1000,
		EndHeight:          1001,
		ScannedBlocks:      1,
		ConsolidatedBlocks: 1,
		PromotedBlocks:     1,
		ObjectKey:          newKey,
		OldObjectKey:       oldKey,
		RepairedObjects:    1,
		PendingOldDeletion: true,
	}, response)
	require.Equal([]string{"prepare", "restore", "verify"}, fake.calls)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBDeletesOnlyAfterVerified() {
	require := testutil.Require(s.T())
	oldKey := "consolidated/dirty.cscb.zstd"
	newKey := "consolidated/clean.cscb.zstd"
	verified := repairActivityManifest(cscbrepair.StateVerified, oldKey, newKey)
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, newKey)
	fake := &repairActivityFake{prepared: verified, completed: completed}
	s.batchConsolidator.repairer = fake

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:             config.ConsolidationModeRepairExistingCSCB,
		Tag:              2,
		StartHeight:      900,
		EndHeight:        1200,
		MaxBlocks:        100,
		DeleteOldObjects: true,
	})
	require.NoError(err)
	require.Equal(uint64(1), response.RepairedObjects)
	require.False(response.PendingOldDeletion)
	require.Equal([]string{"prepare", "delete"}, fake.calls)
}

func repairActivityManifest(state cscbrepair.State, oldKey string, newKey string) *cscbrepair.Manifest {
	return &cscbrepair.Manifest{
		ID:                       42,
		Tag:                      2,
		State:                    state,
		StartHeight:              1000,
		EndHeight:                1001,
		CanonicalBlockCount:      1,
		TotalBlockCount:          1,
		OldConsolidatedObjectKey: oldKey,
		NewConsolidatedObjectKey: newKey,
	}
}

type repairActivityFake struct {
	cscbrepair.Repairer
	prepared  *cscbrepair.Manifest
	restored  *cscbrepair.Manifest
	verified  *cscbrepair.Manifest
	completed *cscbrepair.Manifest
	calls     []string
}

func (f *repairActivityFake) PrepareNext(context.Context, uint32, uint64, uint64, uint64, cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "prepare")
	return f.prepared, nil
}

func (f *repairActivityFake) Restore(context.Context, int64, cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "restore")
	return f.restored, nil
}

func (f *repairActivityFake) VerifyRebuilt(context.Context, int64, cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "verify")
	return f.verified, nil
}

func (f *repairActivityFake) DeleteOldObject(context.Context, int64) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "delete")
	return f.completed, nil
}
