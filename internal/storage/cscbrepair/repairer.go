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

const (
	completedOutcome                = "old_consolidated_object_retained_unreferenced"
	alreadyCleanOutcome             = OutcomeAlreadyCleanStorageNeutral
	maxSingleBlockVersionsToInspect = 10
)

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

func (r *repairerImpl) ListCandidates(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	limit int,
) ([]string, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, xerrors.New("CSCB repair candidate limit must be positive")
	}
	return r.repository.ListCandidateObjectKeys(ctx, tag, startHeight, endHeight, limit)
}

func (r *repairerImpl) PrepareNext(
	ctx context.Context,
	executionKey string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	maxBlocks uint64,
	progress Progress,
) (*Manifest, error) {
	return r.prepare(ctx, executionKey, tag, startHeight, endHeight, maxBlocks, "", progress)
}

func (r *repairerImpl) PrepareObject(
	ctx context.Context,
	executionKey string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	maxBlocks uint64,
	objectKey string,
	progress Progress,
) (*Manifest, error) {
	if objectKey == "" {
		return nil, xerrors.New("CSCB repair object key is required")
	}
	return r.prepare(ctx, executionKey, tag, startHeight, endHeight, maxBlocks, objectKey, progress)
}

func (r *repairerImpl) prepare(
	ctx context.Context,
	executionKey string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	maxBlocks uint64,
	objectKey string,
	progress Progress,
) (*Manifest, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if !validExecutionKey(executionKey) {
		return nil, xerrors.New("valid CSCB repair execution key is required")
	}
	bound, found, err := r.repository.FindByExecutionKey(ctx, executionKey)
	if err != nil {
		return nil, err
	}
	if found {
		if bound == nil {
			return nil, nil
		}
		if bound.Tag != tag {
			return nil, xerrors.Errorf("bound CSCB repair tag changed: expected=%d actual=%d", tag, bound.Tag)
		}
		if objectKey != "" && bound.OldConsolidatedObjectKey != objectKey {
			return nil, xerrors.Errorf(
				"bound CSCB repair object changed: expected=%q actual=%q",
				objectKey,
				bound.OldConsolidatedObjectKey,
			)
		}
		if err := validateApprovedManifest(bound, startHeight, endHeight, maxBlocks); err != nil {
			return nil, xerrors.Errorf("bound CSCB repair does not fit the approved request: %w", err)
		}
		return r.inspectFencedCandidate(ctx, bound, progress)
	}
	var pending *Manifest
	if objectKey == "" {
		pending, err = r.repository.FindPending(ctx, tag)
	} else {
		pending, err = r.repository.FindByObjectKey(ctx, tag, objectKey)
	}
	if err != nil {
		return nil, err
	}
	if pending != nil {
		if err := validateApprovedManifest(pending, startHeight, endHeight, maxBlocks); err != nil {
			return nil, xerrors.Errorf("pending CSCB repair does not fit the approved request: %w", err)
		}
		bound, err := r.repository.BindExecutionKey(ctx, executionKey, pending.ID)
		if err != nil {
			return nil, err
		}
		return r.inspectFencedCandidate(ctx, bound, progress)
	}
	var candidate *Manifest
	if objectKey == "" {
		candidate, err = r.repository.FindNextCandidate(ctx, tag, startHeight, endHeight)
	} else {
		candidate, err = r.repository.FindCandidateByObjectKey(ctx, tag, objectKey)
	}
	if err != nil {
		return nil, err
	}
	if candidate == nil {
		if objectKey != "" {
			return nil, xerrors.Errorf("CSCB repair candidate is no longer available: object=%q", objectKey)
		}
		return r.repository.BindNoCandidateExecution(ctx, executionKey, tag, startHeight, endHeight)
	}
	if err := validateCandidate(candidate, startHeight, endHeight, maxBlocks); err != nil {
		return nil, err
	}
	candidate.State = StatePreparing
	candidate.Bucket = r.bucket
	for i := range candidate.Blocks {
		candidate.Blocks[i].SingleBlockObjectKeySHA256 = objectKeySHA256(candidate.Blocks[i].SingleBlockObjectKey)
	}
	candidate.RowSetSHA256 = rowSetSHA256(candidate)
	fenced, err := r.repository.FenceCandidate(ctx, candidate)
	if err != nil {
		return nil, xerrors.Errorf("failed to persist CSCB repair fence: %w", err)
	}
	bound, err = r.repository.BindExecutionKey(ctx, executionKey, fenced.ID)
	if err != nil {
		return nil, xerrors.Errorf("failed to bind CSCB repair execution: %w", err)
	}
	return r.inspectFencedCandidate(ctx, bound, progress)
}

func (r *repairerImpl) inspectFencedCandidate(
	ctx context.Context,
	manifest *Manifest,
	progress Progress,
) (*Manifest, error) {
	if manifest == nil || manifest.State != StatePreparing {
		return manifest, nil
	}
	oldVersion, err := r.inspectExactObjectVersion(ctx, manifest.OldConsolidatedObjectKey)
	if err != nil {
		return nil, xerrors.Errorf("failed to pin CSCB repair source object: %w", err)
	}
	blocks := append([]Block(nil), manifest.Blocks...)
	verifier := retirement.NewPinnedPayloadVerifier(r.store)
	hasStoragePlacement := false
	for i := range blocks {
		block := &blocks[i]
		if objectKeySHA256(block.SingleBlockObjectKey) != block.SingleBlockObjectKeySHA256 {
			return nil, xerrors.Errorf("single-block object key audit digest changed at height %d", block.Height)
		}
		singleVersion, err := r.inspectSingleBlockObjectVersion(ctx, retirement.Candidate{
			Bucket: manifest.Bucket,
			Key:    block.SingleBlockObjectKey,
			Height: block.Height,
			Hash:   block.Hash,
			Tag:    block.Tag,
		})
		if err != nil {
			return nil, xerrors.Errorf("failed to pin single-block object at height %d: %w", block.Height, err)
		}
		block.SingleBlockObjectVersion = singleVersion
		payload := makePayloadCandidate(manifest, block, manifest.OldConsolidatedObjectKey, oldVersion)
		singleInspection, err := verifier.InspectSingleBlock(ctx, payload)
		if err != nil {
			return nil, xerrors.Errorf("failed to verify pinned single-block payload at height %d: %w", block.Height, err)
		}
		oldInspection, err := verifier.InspectConsolidated(ctx, payload)
		if err != nil {
			return nil, xerrors.Errorf("failed to verify source CSCB payload at height %d: %w", block.Height, err)
		}
		hasStoragePlacement = hasStoragePlacement || oldInspection.HasStoragePlacement
		if singleInspection.SemanticSHA256 != oldInspection.SemanticSHA256 {
			return nil, xerrors.Errorf("single-block and CSCB payloads differ at height %d", block.Height)
		}
		block.PayloadSHA256 = singleInspection.CanonicalSHA256
		reportProgress(progress, "prepare_payload_verified", i+1, len(blocks), block.Height)
	}
	inspected, err := r.repository.RecordInspection(ctx, manifest.ID, oldVersion, blocks, !hasStoragePlacement)
	if err != nil {
		return nil, xerrors.Errorf("failed to persist CSCB repair inspection: %w", err)
	}
	stage := "prepared"
	if inspected.State == StateCompleted && inspected.Outcome == alreadyCleanOutcome {
		stage = "already_clean_storage_neutral"
	}
	reportProgress(progress, stage, len(inspected.Blocks), len(inspected.Blocks), inspected.EndHeight-1)
	return inspected, nil
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
	if err := r.requirePinnedObjectVersion(ctx, manifest.OldConsolidatedObjectKey, manifest.OldConsolidatedObjectVersion); err != nil {
		return nil, xerrors.Errorf("CSCB repair source changed before restore: %w", err)
	}
	for i := range manifest.Blocks {
		block := &manifest.Blocks[i]
		if err := r.requirePinnedSingleBlockObjectVersion(ctx, block.SingleBlockObjectKey, block.SingleBlockObjectVersion); err != nil {
			return nil, xerrors.Errorf("single-block object changed before restore at height %d: %w", block.Height, err)
		}
		reportProgress(progress, "restore_object_revalidated", i+1, len(manifest.Blocks), block.Height)
	}
	restored, err := r.repository.RestoreToSingleBlock(ctx, repairID)
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
		if err := r.requirePinnedSingleBlockObjectVersion(ctx, block.SingleBlockObjectKey, block.SingleBlockObjectVersion); err != nil {
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

func (r *repairerImpl) Complete(ctx context.Context, repairID int64, progress Progress) (*Manifest, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	manifest, err := r.repository.GetRebuilt(ctx, repairID)
	if err != nil {
		return nil, err
	}
	if manifest.State == StateCompleted {
		return manifest, nil
	}
	if manifest.CanonicalBlockCount == 0 {
		if manifest.State != StateRestored {
			return nil, xerrors.Errorf("zero-canonical CSCB repair must be restored before completion: id=%d state=%s", manifest.ID, manifest.State)
		}
	} else if manifest.State != StateVerified {
		return nil, xerrors.Errorf("CSCB repair must be verified before completion: id=%d state=%s", manifest.ID, manifest.State)
	}
	if err := r.requirePinnedObjectVersion(ctx, manifest.OldConsolidatedObjectKey, manifest.OldConsolidatedObjectVersion); err != nil {
		return nil, xerrors.Errorf("retained CSCB repair source changed before completion: %w", err)
	}
	if manifest.CanonicalBlockCount > 0 {
		if err := r.requirePinnedObjectVersion(
			ctx,
			manifest.NewConsolidatedObjectKey,
			manifest.NewConsolidatedObjectVersion,
		); err != nil {
			return nil, xerrors.Errorf("rebuilt CSCB changed before repair completion: %w", err)
		}
	}

	verifier := retirement.NewPinnedPayloadVerifier(r.store)
	for i := range manifest.Blocks {
		block := &manifest.Blocks[i]
		if err := r.requirePinnedSingleBlockObjectVersion(ctx, block.SingleBlockObjectKey, block.SingleBlockObjectVersion); err != nil {
			return nil, xerrors.Errorf("single-block object changed before completion at height %d: %w", block.Height, err)
		}
		payload := makePayloadCandidate(manifest, block, manifest.NewConsolidatedObjectKey, manifest.NewConsolidatedObjectVersion)
		singleInspection, err := verifier.InspectSingleBlock(ctx, payload)
		if err != nil {
			return nil, xerrors.Errorf("failed to verify retained single-block payload at height %d: %w", block.Height, err)
		}
		if singleInspection.CanonicalSHA256 != block.PayloadSHA256 {
			return nil, xerrors.Errorf("single-block payload digest changed before completion at height %d", block.Height)
		}
		if block.Canonical {
			cleanInspection, err := verifier.InspectConsolidated(ctx, payload)
			if err != nil {
				return nil, xerrors.Errorf("failed to verify rebuilt CSCB payload before completion at height %d: %w", block.Height, err)
			}
			if cleanInspection.HasStoragePlacement || cleanInspection.CanonicalSHA256 != block.PayloadSHA256 {
				return nil, xerrors.Errorf("rebuilt CSCB payload is not storage-neutral and identical at height %d", block.Height)
			}
		}
		reportProgress(progress, "completion_payload_verified", i+1, len(manifest.Blocks), block.Height)
	}
	completed, err := r.repository.CompleteRetainingOldObject(ctx, manifest.ID, completedOutcome)
	if err != nil {
		return nil, err
	}
	reportProgress(progress, "completed", len(completed.Blocks), len(completed.Blocks), completed.EndHeight-1)
	return completed, nil
}

func (r *repairerImpl) validate() error {
	if r == nil || r.repository == nil || r.store == nil || r.bucket == "" {
		return xerrors.New("CSCB repair requires a repository, object store, and bucket")
	}
	return nil
}

func validateCandidate(manifest *Manifest, startHeight uint64, endHeight uint64, maxBlocks uint64) error {
	if err := validateApprovedManifest(manifest, startHeight, endHeight, maxBlocks); err != nil {
		return err
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

func validateApprovedManifest(manifest *Manifest, startHeight uint64, endHeight uint64, maxBlocks uint64) error {
	if manifest == nil || manifest.OldConsolidatedObjectKey == "" || len(manifest.Blocks) == 0 {
		return xerrors.New("active CSCB repair manifest is required")
	}
	if maxBlocks == 0 || manifest.TotalBlockCount > maxBlocks || manifest.CanonicalBlockCount > maxBlocks {
		return xerrors.Errorf(
			"CSCB repair manifest exceeds one consolidation object: total=%d canonical=%d max_blocks=%d",
			manifest.TotalBlockCount,
			manifest.CanonicalBlockCount,
			maxBlocks,
		)
	}
	if manifest.TotalBlockCount == 0 || manifest.TotalBlockCount != uint64(len(manifest.Blocks)) {
		return xerrors.New("CSCB repair manifest must contain a complete non-empty row set")
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
	return nil
}

func validExecutionKey(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
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

func (r *repairerImpl) inspectSingleBlockObjectVersion(
	ctx context.Context,
	candidate retirement.Candidate,
) (ObjectVersion, error) {
	topology, err := r.store.ListObjectVersions(ctx, r.bucket, candidate.Key)
	if err != nil {
		return ObjectVersion{}, err
	}
	current, err := currentSingleBlockObjectVersion(topology)
	if err != nil {
		return ObjectVersion{}, xerrors.Errorf("unsafe single-block object topology: key=%q: %w", candidate.Key, err)
	}
	if len(topology.Versions) == 1 {
		return current, nil
	}
	if len(topology.Versions) > maxSingleBlockVersionsToInspect {
		return ObjectVersion{}, xerrors.Errorf(
			"too many single-block object versions to safely inspect: key=%q count=%d max=%d",
			candidate.Key,
			len(topology.Versions),
			maxSingleBlockVersionsToInspect,
		)
	}

	// Retries historically created redundant single-block versions whose raw
	// Solana JSON serialization can differ while representing the same block.
	// Verify every immutable version through the semantic block digest before
	// pinning the current one.
	verifier := retirement.NewPinnedPayloadVerifier(r.store)
	currentCandidate := candidate
	currentCandidate.VersionID = current.VersionID
	currentInspection, err := verifier.InspectSingleBlock(ctx, currentCandidate)
	if err != nil {
		return ObjectVersion{}, xerrors.Errorf("failed to read current single-block object version %q: %w", current.VersionID, err)
	}
	for _, version := range topology.Versions {
		if version.VersionID == current.VersionID {
			continue
		}
		historicalCandidate := candidate
		historicalCandidate.VersionID = version.VersionID
		historicalInspection, err := verifier.InspectSingleBlock(ctx, historicalCandidate)
		if err != nil {
			return ObjectVersion{}, xerrors.Errorf("failed to read historical single-block object version %q: %w", version.VersionID, err)
		}
		if historicalInspection.SemanticSHA256 != currentInspection.SemanticSHA256 {
			return ObjectVersion{}, xerrors.Errorf(
				"single-block object versions contain different semantic block payloads: current=%q historical=%q",
				current.VersionID,
				version.VersionID,
			)
		}
	}
	return current, nil
}

func (r *repairerImpl) requirePinnedObjectVersion(ctx context.Context, key string, expected ObjectVersion) error {
	topology, err := r.store.ListObjectVersions(ctx, r.bucket, key)
	if err != nil {
		return err
	}
	return requireExactTopology(topology, expected)
}

func (r *repairerImpl) requirePinnedSingleBlockObjectVersion(ctx context.Context, key string, expected ObjectVersion) error {
	topology, err := r.store.ListObjectVersions(ctx, r.bucket, key)
	if err != nil {
		return err
	}
	current, err := currentSingleBlockObjectVersion(topology)
	if err != nil {
		return err
	}
	if current != expected {
		return xerrors.Errorf(
			"pinned single-block object current version changed: version=%q etag=%q bytes=%d",
			current.VersionID,
			current.ETag,
			current.Bytes,
		)
	}
	return nil
}

func currentSingleBlockObjectVersion(topology retirement.ObjectVersionTopology) (ObjectVersion, error) {
	if len(topology.Versions) == 0 || len(topology.DeleteMarkers) != 0 {
		return ObjectVersion{}, xerrors.Errorf(
			"expected at least one data version and zero delete markers, got versions=%d delete_markers=%d",
			len(topology.Versions),
			len(topology.DeleteMarkers),
		)
	}

	var current ObjectVersion
	currentFound := false
	for _, version := range topology.Versions {
		candidate := ObjectVersion{VersionID: version.VersionID, ETag: version.ETag, Bytes: version.Bytes}
		if !immutableVersionID(candidate.VersionID) || candidate.ETag == "" || candidate.Bytes == 0 {
			return ObjectVersion{}, xerrors.Errorf("single-block object version is not immutable and complete: version=%q", candidate.VersionID)
		}
		if version.IsLatest {
			if currentFound {
				return ObjectVersion{}, xerrors.New("single-block object has multiple latest data versions")
			}
			current = candidate
			currentFound = true
		}
	}
	if !currentFound {
		return ObjectVersion{}, xerrors.New("single-block object has no latest data version")
	}
	return current, nil
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
			"%d\x1f%t\x1f%d\x1f%d\x1f%d:%s\x1f%d:%s\x1f%s\x1f%d\x1f%d\x1f%d\n",
			block.BlockMetadataID,
			block.Canonical,
			block.Tag,
			block.Height,
			len(block.Hash),
			block.Hash,
			len(block.SingleBlockObjectKey),
			block.SingleBlockObjectKey,
			block.SingleBlockObjectKeySHA256,
			block.OldByteOffset,
			block.OldByteLength,
			block.OldUncompressedLength,
		)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func objectKeySHA256(objectKey string) string {
	digest := sha256.Sum256([]byte(objectKey))
	return hex.EncodeToString(digest[:])
}

func immutableVersionID(versionID string) bool {
	value := strings.TrimSpace(versionID)
	return value != "" && !strings.EqualFold(value, "null")
}

var _ Repairer = (*repairerImpl)(nil)
