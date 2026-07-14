package cscbrepair

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/retirement"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestDeleteOldObjectResumesAfterDeleteBeforeDatabaseCompletion(t *testing.T) {
	manifest := verifiedRepairManifest()
	repository := &deleteResumeRepository{
		manifest:         manifest,
		failNextComplete: true,
	}
	store := &deleteResumeObjectStore{
		oldKey: manifest.OldConsolidatedObjectKey,
		newKey: manifest.NewConsolidatedObjectKey,
		oldTopology: retirement.ObjectVersionTopology{Versions: []retirement.ObjectVersion{{
			VersionID: manifest.OldConsolidatedObjectVersion.VersionID,
			ETag:      manifest.OldConsolidatedObjectVersion.ETag,
			Bytes:     manifest.OldConsolidatedObjectVersion.Bytes,
			IsLatest:  true,
		}}},
		newTopology: topologyFor(manifest.NewConsolidatedObjectVersion),
	}
	repairer := NewRepairer(repository, store, manifest.Bucket)

	_, err := repairer.DeleteOldObject(context.Background(), manifest.ID)
	require.ErrorContains(t, err, "database completion unavailable")
	require.Equal(t, 1, store.deleteCalls)
	require.Empty(t, store.oldTopology.Versions)
	require.Equal(t, StateVerified, repository.manifest.State)

	completed, err := repairer.DeleteOldObject(context.Background(), manifest.ID)
	require.NoError(t, err)
	require.Equal(t, StateCompleted, completed.State)
	require.Equal(t, 1, store.deleteCalls, "resume must not issue a second delete")
	require.Equal(t, 2, repository.completeCalls)
}

func TestDeleteOldObjectRejectsVersionTopologyDrift(t *testing.T) {
	manifest := verifiedRepairManifest()
	repository := &deleteResumeRepository{manifest: manifest}
	store := &deleteResumeObjectStore{
		oldKey: manifest.OldConsolidatedObjectKey,
		newKey: manifest.NewConsolidatedObjectKey,
		oldTopology: retirement.ObjectVersionTopology{Versions: []retirement.ObjectVersion{{
			VersionID: "unexpected-version",
			ETag:      manifest.OldConsolidatedObjectVersion.ETag,
			Bytes:     manifest.OldConsolidatedObjectVersion.Bytes,
			IsLatest:  true,
		}}},
		newTopology: topologyFor(manifest.NewConsolidatedObjectVersion),
	}
	repairer := NewRepairer(repository, store, manifest.Bucket)

	_, err := repairer.DeleteOldObject(context.Background(), manifest.ID)
	require.ErrorContains(t, err, "topology changed before deletion")
	require.Zero(t, store.deleteCalls)
	require.Zero(t, repository.completeCalls)
}

func TestDeleteOldObjectRejectsRebuiltVersionTopologyDrift(t *testing.T) {
	manifest := verifiedRepairManifest()
	repository := &deleteResumeRepository{manifest: manifest}
	store := &deleteResumeObjectStore{
		oldKey:      manifest.OldConsolidatedObjectKey,
		newKey:      manifest.NewConsolidatedObjectKey,
		oldTopology: topologyFor(manifest.OldConsolidatedObjectVersion),
		newTopology: retirement.ObjectVersionTopology{Versions: []retirement.ObjectVersion{{
			VersionID: "unexpected-clean-version",
			ETag:      manifest.NewConsolidatedObjectVersion.ETag,
			Bytes:     manifest.NewConsolidatedObjectVersion.Bytes,
			IsLatest:  true,
		}}},
	}
	repairer := NewRepairer(repository, store, manifest.Bucket)

	_, err := repairer.DeleteOldObject(context.Background(), manifest.ID)
	require.ErrorContains(t, err, "rebuilt CSCB changed before old-object deletion")
	require.Zero(t, store.deleteCalls)
	require.Zero(t, repository.completeCalls)
}

func TestValidateCandidateRejectsUnsafeSingleBlockState(t *testing.T) {
	valid := validRepairCandidate()
	require.NoError(t, validateCandidate(valid, 1000, 1001, 1))

	tests := map[string]func(*Manifest){
		"retirement fence":         func(candidate *Manifest) { candidate.Blocks[0].RetirementFenced = true },
		"retirement manifest":      func(candidate *Manifest) { candidate.Blocks[0].RetirementManifestExists = true },
		"deleted single block":     func(candidate *Manifest) { candidate.Blocks[0].SingleBlockObjectDeleted = true },
		"missing single block key": func(candidate *Manifest) { candidate.Blocks[0].SingleBlockObjectKey = "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := validRepairCandidate()
			mutate(candidate)
			require.ErrorContains(t, validateCandidate(candidate, 1000, 1001, 1), "blocked or incomplete")
		})
	}
}

func TestPrepareNextRejectsPendingRepairOutsideApprovedRange(t *testing.T) {
	pending := validRepairCandidate()
	pending.State = StatePrepared
	repository := &pendingRepairRepository{pending: pending}
	repairer := NewRepairer(repository, &deleteResumeObjectStore{}, "repair-bucket")

	_, err := repairer.PrepareNext(context.Background(), 2, 2000, 2001, 1, nil)
	require.ErrorContains(t, err, "pending CSCB repair does not fit the approved request")
	require.ErrorContains(t, err, "exceeds approved range")
}

func TestValidateRebuiltMetadataRequiresFreshRetentionAndCleanNonCanonicalRows(t *testing.T) {
	restoredAt := time.Now().UTC()
	validatedAt := restoredAt.Add(time.Second)
	retentionStartedAt := restoredAt.Add(2 * time.Second)
	deleteAfter := retentionStartedAt.Add(72 * time.Hour)
	manifest := &Manifest{
		ID:                  8,
		RestoredAt:          &restoredAt,
		CanonicalBlockCount: 1,
		Blocks: []Block{
			{
				BlockMetadataID:           1,
				Canonical:                 true,
				ActiveObjectKey:           "consolidated/clean.cscb.zstd",
				ActiveObjectFormat:        int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
				NewConsolidatedObjectKey:  "consolidated/clean.cscb.zstd",
				NewByteLength:             100,
				NewUncompressedLength:     200,
				NewValidatedAt:            &validatedAt,
				NewRetentionStartedAt:     &retentionStartedAt,
				NewSingleBlockDeleteAfter: &deleteAfter,
			},
			{
				BlockMetadataID:      2,
				SingleBlockObjectKey: "single/2.gzip",
				ActiveObjectKey:      "single/2.gzip",
				ActiveObjectFormat:   int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK),
			},
		},
	}
	require.NoError(t, validateRebuiltMetadata(manifest, "consolidated/clean.cscb.zstd"))

	staleStartedAt := restoredAt.Add(-time.Second)
	manifest.Blocks[0].NewRetentionStartedAt = &staleStartedAt
	require.ErrorContains(t, validateRebuiltMetadata(manifest, "consolidated/clean.cscb.zstd"), "freshly promoted")

	manifest.Blocks[0].NewRetentionStartedAt = &retentionStartedAt
	manifest.Blocks[1].NewRetentionStartedAt = &retentionStartedAt
	require.ErrorContains(t, validateRebuiltMetadata(manifest, "consolidated/clean.cscb.zstd"), "did not remain on single-block storage")

	manifest.Blocks[1].NewRetentionStartedAt = nil
	tooSoon := retentionStartedAt.Add(71*time.Hour + 59*time.Minute)
	manifest.Blocks[0].NewSingleBlockDeleteAfter = &tooSoon
	require.ErrorContains(t, validateRebuiltMetadata(manifest, "consolidated/clean.cscb.zstd"), "required 72-hour")
}

func validRepairCandidate() *Manifest {
	return &Manifest{
		Tag:                      2,
		OldConsolidatedObjectKey: "consolidated/dirty.cscb.zstd",
		StartHeight:              1000,
		EndHeight:                1001,
		CanonicalBlockCount:      1,
		TotalBlockCount:          1,
		Blocks: []Block{{
			BlockMetadataID:          1,
			Canonical:                true,
			Tag:                      2,
			Height:                   1000,
			SingleBlockObjectKey:     "single/1000.gzip",
			OldConsolidatedObjectKey: "consolidated/dirty.cscb.zstd",
			OldByteLength:            100,
			OldUncompressedLength:    200,
		}},
	}
}

func verifiedRepairManifest() *Manifest {
	return &Manifest{
		ID:                       7,
		State:                    StateVerified,
		Bucket:                   "repair-bucket",
		OldConsolidatedObjectKey: "consolidated/dirty.cscb.zstd",
		OldConsolidatedObjectVersion: ObjectVersion{
			VersionID: "dirty-version",
			ETag:      "dirty-etag",
			Bytes:     1234,
		},
		NewConsolidatedObjectKey: "consolidated/clean.cscb.zstd",
		NewConsolidatedObjectVersion: ObjectVersion{
			VersionID: "clean-version",
			ETag:      "clean-etag",
			Bytes:     1200,
		},
	}
}

type deleteResumeRepository struct {
	Repository
	manifest         *Manifest
	failNextComplete bool
	completeCalls    int
}

type pendingRepairRepository struct {
	Repository
	pending *Manifest
}

func (r *pendingRepairRepository) FindPending(context.Context, uint32) (*Manifest, error) {
	return r.pending, nil
}

func (r *deleteResumeRepository) Get(context.Context, int64) (*Manifest, error) {
	return r.manifest, nil
}

func (r *deleteResumeRepository) Complete(_ context.Context, _ int64, outcome string) (*Manifest, error) {
	r.completeCalls++
	if r.failNextComplete {
		r.failNextComplete = false
		return nil, xerrors.New("database completion unavailable")
	}
	r.manifest.State = StateCompleted
	r.manifest.Outcome = outcome
	return r.manifest, nil
}

type deleteResumeObjectStore struct {
	retirement.ObjectStore
	oldKey      string
	newKey      string
	oldTopology retirement.ObjectVersionTopology
	newTopology retirement.ObjectVersionTopology
	deleteCalls int
}

func (s *deleteResumeObjectStore) ListObjectVersions(_ context.Context, _ string, key string) (retirement.ObjectVersionTopology, error) {
	switch key {
	case s.oldKey:
		return s.oldTopology, nil
	case s.newKey:
		return s.newTopology, nil
	default:
		return retirement.ObjectVersionTopology{}, xerrors.Errorf("unexpected object key %q", key)
	}
}

func (s *deleteResumeObjectStore) DeleteObjectVersion(_ context.Context, _ string, key string, versionID string) error {
	s.deleteCalls++
	if key != s.oldKey || len(s.oldTopology.Versions) != 1 || s.oldTopology.Versions[0].VersionID != versionID {
		return xerrors.New("unexpected version deletion")
	}
	s.oldTopology = retirement.ObjectVersionTopology{}
	return nil
}

func topologyFor(object ObjectVersion) retirement.ObjectVersionTopology {
	return retirement.ObjectVersionTopology{Versions: []retirement.ObjectVersion{{
		VersionID: object.VersionID,
		ETag:      object.ETag,
		Bytes:     object.Bytes,
		IsLatest:  true,
	}}}
}
