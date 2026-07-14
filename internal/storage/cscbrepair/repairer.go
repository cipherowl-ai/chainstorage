package cscbrepair

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/retirement"
)

const completedOutcome = "old_consolidated_object_version_deleted"

type repairerImpl struct {
	repository Repository
	store      retirement.ObjectStore
	bucket     string
}

func NewRepairer(repository Repository, store retirement.ObjectStore, bucket string) Repairer {
	return &repairerImpl{
		repository: repository,
		store:      store,
		bucket:     bucket,
	}
}

func (r *repairerImpl) PrepareNext(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	maxBlocks uint64,
	progress Progress,
) (*Manifest, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	pending, err := r.repository.FindPending(ctx, tag)
	if err != nil {
		return nil, err
	}
	if pending != nil {
		if err := validateCandidate(pending, startHeight, endHeight, maxBlocks); err != nil {
			return nil, xerrors.Errorf("pending CSCB repair does not fit the approved request: %w", err)
		}
		return pending, nil
	}
	candidate, err := r.repository.FindNextCandidate(ctx, tag, startHeight, endHeight)
	if err != nil || candidate == nil {
		return candidate, err
	}
	if err := validateCandidate(candidate, startHeight, endHeight, maxBlocks); err != nil {
		return nil, err
	}

	oldVersion, err := r.inspectExactObjectVersion(ctx, candidate.OldConsolidatedObjectKey)
	if err != nil {
		return nil, xerrors.Errorf("failed to pin dirty CSCB object: %w", err)
	}
	candidate.State = StatePrepared
	candidate.Bucket = r.bucket
	candidate.OldConsolidatedObjectVersion = oldVersion
	verifier := retirement.NewPinnedPayloadVerifier(r.store)
	for i := range candidate.Blocks {
		block := &candidate.Blocks[i]
		singleVersion, err := r.inspectExactObjectVersion(ctx, block.SingleBlockObjectKey)
		if err != nil {
			return nil, xerrors.Errorf("failed to pin single-block object at height %d: %w", block.Height, err)
		}
		block.SingleBlockObjectVersion = singleVersion
		payload := makePayloadCandidate(candidate, block, candidate.OldConsolidatedObjectKey, oldVersion)
		singleInspection, err := verifier.InspectSingleBlock(ctx, payload)
		if err != nil {
			return nil, xerrors.Errorf("failed to verify pinned single-block payload at height %d: %w", block.Height, err)
		}
		oldInspection, err := verifier.InspectConsolidated(ctx, payload)
		if err != nil {
			return nil, xerrors.Errorf("failed to verify dirty CSCB payload at height %d: %w", block.Height, err)
		}
		if !oldInspection.HasStoragePlacement {
			return nil, xerrors.Errorf(
				"CSCB repair candidate is already storage-neutral at height %d object=%q",
				block.Height,
				candidate.OldConsolidatedObjectKey,
			)
		}
		if singleInspection.CanonicalSHA256 != oldInspection.CanonicalSHA256 {
			return nil, xerrors.Errorf("single-block and dirty CSCB payloads differ at height %d", block.Height)
		}
		block.PayloadSHA256 = singleInspection.CanonicalSHA256
		reportProgress(progress, "prepare_payload_verified", i+1, len(candidate.Blocks), block.Height)
	}
	candidate.RowSetSHA256 = rowSetSHA256(candidate)
	prepared, err := r.repository.Prepare(ctx, candidate)
	if err != nil {
		return nil, xerrors.Errorf("failed to persist CSCB repair manifest: %w", err)
	}
	reportProgress(progress, "prepared", len(candidate.Blocks), len(candidate.Blocks), candidate.EndHeight-1)
	return prepared, nil
}

func (r *repairerImpl) Restore(ctx context.Context, repairID int64, progress Progress) (*Manifest, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	manifest, err := r.repository.Get(ctx, repairID)
	if err != nil {
		return nil, err
	}
	if manifest.State != StatePrepared {
		return manifest, nil
	}
	restored, err := r.repository.RestoreToSingleBlock(ctx, repairID, func(locked *Manifest) error {
		if err := r.requirePinnedObjectVersion(ctx, locked.OldConsolidatedObjectKey, locked.OldConsolidatedObjectVersion); err != nil {
			return xerrors.Errorf("dirty CSCB changed before restore: %w", err)
		}
		for i := range locked.Blocks {
			block := &locked.Blocks[i]
			if err := r.requirePinnedObjectVersion(ctx, block.SingleBlockObjectKey, block.SingleBlockObjectVersion); err != nil {
				return xerrors.Errorf("single-block object changed before restore at height %d: %w", block.Height, err)
			}
			reportProgress(progress, "restore_object_revalidated", i+1, len(locked.Blocks), block.Height)
		}
		return nil
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to restore single-block metadata: %w", err)
	}
	reportProgress(progress, "restored", len(restored.Blocks), len(restored.Blocks), restored.EndHeight-1)
	return restored, nil
}

func (r *repairerImpl) Get(ctx context.Context, repairID int64) (*Manifest, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	return r.repository.Get(ctx, repairID)
}

func (r *repairerImpl) VerifyRebuilt(ctx context.Context, repairID int64, progress Progress) (*Manifest, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	manifest, err := r.repository.GetRebuilt(ctx, repairID)
	if err != nil {
		return nil, err
	}
	if manifest.State == StateVerified || manifest.State == StateCompleted {
		return manifest, nil
	}
	if manifest.State != StateRestored {
		return nil, xerrors.Errorf("CSCB repair must be restored before payload verification: id=%d state=%s", manifest.ID, manifest.State)
	}
	newObjectKey, err := rebuiltObjectKey(manifest)
	if err != nil {
		return nil, err
	}
	newVersion, err := r.inspectExactObjectVersion(ctx, newObjectKey)
	if err != nil {
		return nil, xerrors.Errorf("failed to pin rebuilt CSCB object: %w", err)
	}
	verifier := retirement.NewPinnedPayloadVerifier(r.store)
	verifiedCanonical := 0
	for i := range manifest.Blocks {
		block := &manifest.Blocks[i]
		if err := r.requirePinnedObjectVersion(ctx, block.SingleBlockObjectKey, block.SingleBlockObjectVersion); err != nil {
			return nil, xerrors.Errorf("single-block object changed during repair at height %d: %w", block.Height, err)
		}
		payload := makePayloadCandidate(manifest, block, newObjectKey, newVersion)
		singleInspection, err := verifier.InspectSingleBlock(ctx, payload)
		if err != nil {
			return nil, xerrors.Errorf("failed to re-read pinned single-block payload at height %d: %w", block.Height, err)
		}
		if singleInspection.CanonicalSHA256 != block.PayloadSHA256 {
			return nil, xerrors.Errorf("single-block payload digest changed during repair at height %d", block.Height)
		}
		if block.Canonical {
			cleanInspection, err := verifier.InspectConsolidated(ctx, payload)
			if err != nil {
				return nil, xerrors.Errorf("failed to verify rebuilt CSCB payload at height %d: %w", block.Height, err)
			}
			if cleanInspection.HasStoragePlacement {
				return nil, xerrors.Errorf("rebuilt CSCB retains storage placement at height %d", block.Height)
			}
			if cleanInspection.CanonicalSHA256 != block.PayloadSHA256 {
				return nil, xerrors.Errorf("rebuilt CSCB payload differs from pinned single-block payload at height %d", block.Height)
			}
			verifiedCanonical++
		}
		reportProgress(progress, "rebuilt_payload_verified", i+1, len(manifest.Blocks), block.Height)
	}
	if uint64(verifiedCanonical) != manifest.CanonicalBlockCount {
		return nil, xerrors.Errorf(
			"rebuilt CSCB canonical verification count mismatch: expected=%d actual=%d",
			manifest.CanonicalBlockCount,
			verifiedCanonical,
		)
	}
	verified, err := r.repository.RecordVerified(ctx, manifest.ID, newObjectKey, newVersion)
	if err != nil {
		return nil, xerrors.Errorf("failed to record verified CSCB repair: %w", err)
	}
	reportProgress(progress, "verified", len(manifest.Blocks), len(manifest.Blocks), manifest.EndHeight-1)
	return verified, nil
}

func (r *repairerImpl) DeleteOldObject(ctx context.Context, repairID int64) (*Manifest, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	manifest, err := r.repository.Get(ctx, repairID)
	if err != nil {
		return nil, err
	}
	if manifest.State == StateCompleted {
		return manifest, nil
	}
	if manifest.State != StateVerified {
		return nil, xerrors.Errorf("CSCB repair must be verified before old object deletion: id=%d state=%s", manifest.ID, manifest.State)
	}
	topology, err := r.store.ListObjectVersions(ctx, manifest.Bucket, manifest.OldConsolidatedObjectKey)
	if err != nil {
		return nil, err
	}
	if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
		return r.repository.Complete(ctx, manifest.ID, completedOutcome)
	}
	if err := r.requirePinnedObjectVersion(
		ctx,
		manifest.NewConsolidatedObjectKey,
		manifest.NewConsolidatedObjectVersion,
	); err != nil {
		return nil, xerrors.Errorf("rebuilt CSCB changed before old-object deletion: %w", err)
	}
	if err := requireExactTopology(topology, manifest.OldConsolidatedObjectVersion); err != nil {
		return nil, xerrors.Errorf("dirty CSCB topology changed before deletion: %w", err)
	}
	if err := r.store.DeleteObjectVersion(
		ctx,
		manifest.Bucket,
		manifest.OldConsolidatedObjectKey,
		manifest.OldConsolidatedObjectVersion.VersionID,
	); err != nil {
		return nil, xerrors.Errorf("failed to delete pinned dirty CSCB version: %w", err)
	}
	topology, err = r.store.ListObjectVersions(ctx, manifest.Bucket, manifest.OldConsolidatedObjectKey)
	if err != nil {
		return nil, xerrors.Errorf("failed to verify dirty CSCB deletion: %w", err)
	}
	if len(topology.Versions) != 0 || len(topology.DeleteMarkers) != 0 {
		return nil, xerrors.Errorf(
			"dirty CSCB still has versions after exact-version deletion: versions=%d delete_markers=%d",
			len(topology.Versions),
			len(topology.DeleteMarkers),
		)
	}
	return r.repository.Complete(ctx, manifest.ID, completedOutcome)
}

func (r *repairerImpl) validate() error {
	if r == nil || r.repository == nil || r.store == nil || r.bucket == "" {
		return xerrors.New("CSCB repair requires a repository, object store, and bucket")
	}
	return nil
}

func validateCandidate(manifest *Manifest, startHeight uint64, endHeight uint64, maxBlocks uint64) error {
	if manifest == nil || manifest.OldConsolidatedObjectKey == "" || len(manifest.Blocks) == 0 {
		return xerrors.New("active CSCB repair candidate is required")
	}
	if maxBlocks == 0 || manifest.TotalBlockCount > maxBlocks || manifest.CanonicalBlockCount > maxBlocks {
		return xerrors.Errorf(
			"CSCB repair candidate exceeds one consolidation object: total=%d canonical=%d max_blocks=%d",
			manifest.TotalBlockCount,
			manifest.CanonicalBlockCount,
			maxBlocks,
		)
	}
	if manifest.CanonicalBlockCount == 0 || manifest.TotalBlockCount != uint64(len(manifest.Blocks)) {
		return xerrors.New("CSCB repair candidate must contain at least one canonical row and a complete row set")
	}
	if manifest.StartHeight < startHeight || manifest.EndHeight > endHeight {
		return xerrors.Errorf(
			"CSCB repair object range [%d, %d) exceeds approved range [%d, %d)",
			manifest.StartHeight,
			manifest.EndHeight,
			startHeight,
			endHeight,
		)
	}
	for _, block := range manifest.Blocks {
		if block.Skipped || block.RetirementFenced || block.RetirementManifestExists ||
			block.SingleBlockObjectKey == "" || block.SingleBlockObjectDeleted ||
			block.OldConsolidatedObjectKey != manifest.OldConsolidatedObjectKey ||
			block.OldByteLength == 0 || block.OldUncompressedLength == 0 {
			return xerrors.Errorf("CSCB repair candidate is blocked or incomplete at metadata_id=%d height=%d", block.BlockMetadataID, block.Height)
		}
	}
	return nil
}

func (r *repairerImpl) inspectExactObjectVersion(ctx context.Context, key string) (ObjectVersion, error) {
	topology, err := r.store.ListObjectVersions(ctx, r.bucket, key)
	if err != nil {
		return ObjectVersion{}, err
	}
	if len(topology.Versions) != 1 || len(topology.DeleteMarkers) != 0 {
		return ObjectVersion{}, xerrors.Errorf(
			"object must have exactly one data version and zero delete markers: key=%q versions=%d delete_markers=%d",
			key,
			len(topology.Versions),
			len(topology.DeleteMarkers),
		)
	}
	version := topology.Versions[0]
	result := ObjectVersion{VersionID: version.VersionID, ETag: version.ETag, Bytes: version.Bytes}
	if !version.IsLatest || !immutableVersionID(result.VersionID) || result.ETag == "" || result.Bytes == 0 {
		return ObjectVersion{}, xerrors.Errorf("object current version is not immutable and complete: key=%q", key)
	}
	return result, nil
}

func (r *repairerImpl) requirePinnedObjectVersion(ctx context.Context, key string, expected ObjectVersion) error {
	topology, err := r.store.ListObjectVersions(ctx, r.bucket, key)
	if err != nil {
		return err
	}
	return requireExactTopology(topology, expected)
}

func requireExactTopology(topology retirement.ObjectVersionTopology, expected ObjectVersion) error {
	if len(topology.Versions) != 1 || len(topology.DeleteMarkers) != 0 {
		return xerrors.Errorf(
			"expected one data version and zero delete markers, got versions=%d delete_markers=%d",
			len(topology.Versions),
			len(topology.DeleteMarkers),
		)
	}
	version := topology.Versions[0]
	if !version.IsLatest || version.VersionID != expected.VersionID || version.ETag != expected.ETag || version.Bytes != expected.Bytes {
		return xerrors.Errorf(
			"pinned object version changed: version=%q etag=%q bytes=%d latest=%t",
			version.VersionID,
			version.ETag,
			version.Bytes,
			version.IsLatest,
		)
	}
	return nil
}

func rebuiltObjectKey(manifest *Manifest) (string, error) {
	objectKey := ""
	for _, block := range manifest.Blocks {
		if !block.Canonical {
			continue
		}
		if block.NewConsolidatedObjectKey == "" || block.NewConsolidatedObjectKey != block.ActiveObjectKey {
			return "", xerrors.Errorf("rebuilt CSCB placement is incomplete at height %d", block.Height)
		}
		if objectKey == "" {
			objectKey = block.NewConsolidatedObjectKey
			continue
		}
		if objectKey != block.NewConsolidatedObjectKey {
			return "", xerrors.Errorf("one dirty CSCB repair produced multiple new objects: %q and %q", objectKey, block.NewConsolidatedObjectKey)
		}
	}
	if objectKey == "" {
		return "", xerrors.New("rebuilt CSCB object key is missing")
	}
	if objectKey == manifest.OldConsolidatedObjectKey {
		return "", xerrors.Errorf("rebuilt CSCB reused dirty object key %q", objectKey)
	}
	return objectKey, nil
}

func makePayloadCandidate(
	manifest *Manifest,
	block *Block,
	consolidatedKey string,
	consolidatedVersion ObjectVersion,
) retirement.Candidate {
	return retirement.Candidate{
		Bucket:             manifest.Bucket,
		Key:                block.SingleBlockObjectKey,
		VersionID:          block.SingleBlockObjectVersion.VersionID,
		Height:             block.Height,
		Hash:               block.Hash,
		ConsolidatedKey:    consolidatedKey,
		Tag:                block.Tag,
		CSCBVersionID:      consolidatedVersion.VersionID,
		ByteOffset:         blockPlacementOffset(block, consolidatedKey, manifest.OldConsolidatedObjectKey),
		ByteLength:         blockPlacementLength(block, consolidatedKey, manifest.OldConsolidatedObjectKey),
		UncompressedLength: blockPlacementUncompressedLength(block, consolidatedKey, manifest.OldConsolidatedObjectKey),
	}
}

func blockPlacementOffset(block *Block, objectKey string, oldObjectKey string) uint64 {
	if objectKey == oldObjectKey {
		return block.OldByteOffset
	}
	return block.NewByteOffset
}

func blockPlacementLength(block *Block, objectKey string, oldObjectKey string) uint64 {
	if objectKey == oldObjectKey {
		return block.OldByteLength
	}
	return block.NewByteLength
}

func blockPlacementUncompressedLength(block *Block, objectKey string, oldObjectKey string) uint64 {
	if objectKey == oldObjectKey {
		return block.OldUncompressedLength
	}
	return block.NewUncompressedLength
}

func rowSetSHA256(manifest *Manifest) string {
	digest := sha256.New()
	for _, block := range manifest.Blocks {
		_, _ = fmt.Fprintf(
			digest,
			"%d\x1f%t\x1f%d\x1f%d\x1f%d:%s\x1f%d:%s\x1f%d:%s\x1f%d:%s\x1f%d\x1f%d\x1f%d\n",
			block.BlockMetadataID,
			block.Canonical,
			block.Tag,
			block.Height,
			len(block.Hash),
			block.Hash,
			len(block.SingleBlockObjectKey),
			block.SingleBlockObjectKey,
			len(block.SingleBlockObjectVersion.VersionID),
			block.SingleBlockObjectVersion.VersionID,
			len(block.PayloadSHA256),
			block.PayloadSHA256,
			block.OldByteOffset,
			block.OldByteLength,
			block.OldUncompressedLength,
		)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func immutableVersionID(versionID string) bool {
	value := strings.TrimSpace(versionID)
	return value != "" && !strings.EqualFold(value, "null")
}

var _ Repairer = (*repairerImpl)(nil)
