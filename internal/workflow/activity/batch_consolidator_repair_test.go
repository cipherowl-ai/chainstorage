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
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	}
	oldKey := "consolidated/dirty.cscb.zstd"
	newKey := "consolidated/clean.cscb.zstd"
	prepared := repairActivityManifest(cscbrepair.StatePrepared, oldKey, "")
	restored := repairActivityManifest(cscbrepair.StateRestored, oldKey, "")
	verified := repairActivityManifest(cscbrepair.StateVerified, oldKey, newKey)
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, newKey)
	fake := &repairActivityFake{
		prepared:  prepared,
		restored:  restored,
		verified:  verified,
		completed: completed,
	}
	s.batchConsolidator.repairer = fake
	s.batchConsolidator.config.AWS.Storage.Consolidation.LocalSpillDir = s.T().TempDir()

	gomock.InOrder(
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
	}, response)
	require.Equal([]string{"prepare", "restore", "verify", "complete"}, fake.calls)
	require.Equal(testRepairExecutionKey, fake.executionKey)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBRejectsNonExactRebuildBeforeUpload() {
	require := testutil.Require(s.T())
	records, _ := makeConsolidatorFixture(1, 1000)
	oldKey := "consolidated/dirty.cscb.zstd"
	restored := repairActivityManifest(cscbrepair.StateRestored, oldKey, "")
	restored.Blocks[0].BlockMetadataID = 999
	fake := &repairActivityFake{prepared: restored}
	s.batchConsolidator.repairer = fake

	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), uint32(2), uint64(1000), uint64(1001), uint64(100)).
		Return(records, nil)

	_, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	})
	require.ErrorContains(err, "unexpected metadata_id=10")
	require.Equal([]string{"prepare"}, fake.calls)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBResumesAfterShadowsPersisted() {
	require := testutil.Require(s.T())
	oldKey := "consolidated/dirty.cscb.zstd"
	newKey := "consolidated/clean.cscb.zstd"
	restored := repairActivityManifest(cscbrepair.StateRestored, oldKey, "")
	verified := repairActivityManifest(cscbrepair.StateVerified, oldKey, newKey)
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, newKey)
	fake := &repairActivityFake{prepared: restored, verified: verified, completed: completed}
	s.batchConsolidator.repairer = fake

	gomock.InOrder(
		s.metaStorage.EXPECT().
			GetBlocksMissingConsolidationShadow(gomock.Any(), uint32(2), uint64(1000), uint64(1001), uint64(100)).
			Return(nil, nil),
		s.metaStorage.EXPECT().
			PromoteBlockConsolidationShadows(gomock.Any(), uint32(2), uint64(1000), uint64(1001), uint64(100), config.DefaultSingleBlockObjectRetention).
			Return(&metastorage.ConsolidationPromotionResult{Blocks: 1}, nil),
	)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	})
	require.NoError(err)
	require.Equal(uint64(1), response.RepairedObjects)
	require.Equal([]string{"prepare", "verify", "complete"}, fake.calls)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBCompletesAllNonCanonicalObjectWithoutRebuild() {
	require := testutil.Require(s.T())
	oldKey := "consolidated/dirty.cscb.zstd"
	prepared := repairActivityManifest(cscbrepair.StatePrepared, oldKey, "")
	prepared.CanonicalBlockCount = 0
	restored := repairActivityManifest(cscbrepair.StateRestored, oldKey, "")
	restored.CanonicalBlockCount = 0
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, "")
	completed.CanonicalBlockCount = 0
	fake := &repairActivityFake{prepared: prepared, restored: restored, completed: completed}
	s.batchConsolidator.repairer = fake

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	})
	require.NoError(err)
	require.Equal(uint64(1), response.RepairedObjects)
	require.Zero(response.ConsolidatedBlocks)
	require.Empty(response.ObjectKey)
	require.Equal([]string{"prepare", "restore", "complete"}, fake.calls)
	require.Equal(testRepairExecutionKey, fake.executionKey)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBRecordsAlreadyCleanObjectWithoutRebuild() {
	require := testutil.Require(s.T())
	oldKey := "consolidated/already-clean.cscb.zstd"
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, "")
	completed.Outcome = cscbrepair.OutcomeAlreadyCleanStorageNeutral
	fake := &repairActivityFake{prepared: completed}
	s.batchConsolidator.repairer = fake

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	})
	require.NoError(err)
	require.Equal(uint64(1), response.RepairedObjects)
	require.Equal(uint64(1), response.ScannedBlocks)
	require.Zero(response.ConsolidatedBlocks)
	require.Zero(response.PromotedBlocks)
	require.Equal(oldKey, response.ObjectKey)
	require.Equal([]string{"prepare"}, fake.calls)
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
		Blocks: []cscbrepair.Block{{
			BlockMetadataID:      10,
			Canonical:            true,
			Tag:                  2,
			Height:               1000,
			Hash:                 "hash-1000",
			SingleBlockObjectKey: "single-block/1000.gzip",
		}},
	}
}

type repairActivityFake struct {
	cscbrepair.Repairer
	prepared     *cscbrepair.Manifest
	restored     *cscbrepair.Manifest
	verified     *cscbrepair.Manifest
	completed    *cscbrepair.Manifest
	calls        []string
	executionKey string
}

func (f *repairActivityFake) PrepareNext(_ context.Context, executionKey string, _ uint32, _ uint64, _ uint64, _ uint64, _ cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "prepare")
	f.executionKey = executionKey
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

func (f *repairActivityFake) Complete(context.Context, int64, cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "complete")
	return f.completed, nil
}

const testRepairExecutionKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
