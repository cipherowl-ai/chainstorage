package retirement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	sha256HexLength                   = sha256.Size * 2
	cscbFormatMetadataKey             = "chainstorage-format"
	cscbCompressionScopeMetadataKey   = "chainstorage-compression-scope"
	cscbUncompressedLengthMetadataKey = "chainstorage-uncompressed-length"
	cscbFormatMetadataValue           = "cscb"
	cscbCompressionScopeMetadataValue = "batch-chunked"
)

type Planner struct {
	repo            Repository
	store           ObjectStore
	verifierFactory func() blockPayloadVerifier
}

func NewPlanner(repo Repository, store ObjectStore) *Planner {
	return &Planner{
		repo:  repo,
		store: store,
		verifierFactory: func() blockPayloadVerifier {
			return newPayloadVerifier(store)
		},
	}
}

func (p *Planner) Plan(ctx context.Context, req PlanRequest) (*Report, error) {
	if err := normalizeRequest(&req); err != nil {
		return nil, err
	}
	rows, err := p.repo.ListMetadataRows(ctx, req.Tag, req.StartHeight, req.EndHeight, req.Limit)
	if err != nil {
		return nil, err
	}

	report := newReport(req)
	cscbHeads := make(map[string]ObjectHead)
	legacyTopologies := make(map[string]ObjectVersionTopology)
	verifier := p.verifierFactory()
	for _, row := range rows {
		item := p.planRow(ctx, req, row, cscbHeads, legacyTopologies, verifier)
		report.Items = append(report.Items, item)
	}
	report.Summary = summarize(report.Items)
	return report, nil
}

func (p *Planner) Apply(ctx context.Context, req PlanRequest, report *Report) error {
	if report == nil {
		return xerrors.New("report is required")
	}
	if err := normalizeRequest(&req); err != nil {
		return err
	}
	if !req.Execute {
		return xerrors.New("execute must be enabled to apply a retirement report")
	}
	if isProduction(req.Environment) && !req.ProductionDeleteEnabled {
		return xerrors.New("production deletion requires explicit production-delete confirmation")
	}
	if err := validateApplyReport(req, report); err != nil {
		return err
	}

	verifier := p.verifierFactory()
	var firstErr error
	halted := false
	for i := range report.Items {
		item := &report.Items[i]
		if item.Action != ActionDeleteObjectVersion {
			continue
		}
		if halted {
			item.Action = ActionSkip
			item.SkipReason = SkipNotAttemptedAfterFailure
			continue
		}
		if reason, err := p.applyCandidate(ctx, req, item, verifier); err != nil {
			item.Action = ActionSkip
			item.SkipReason = reason
			item.RetirementOutcome = reason
			if outcomeErr := p.repo.RecordRetirementOutcome(ctx, item.BlockMetadataID, reason, time.Now().UTC()); outcomeErr != nil {
				err = errors.Join(err, outcomeErr)
			}
			if firstErr == nil {
				firstErr = err
			}
			halted = true
		}
	}
	report.Summary = summarize(report.Items)
	return firstErr
}

func (p *Planner) Reconcile(ctx context.Context, req PlanRequest) (*Report, error) {
	if err := normalizeRequest(&req); err != nil {
		return nil, err
	}
	if req.Execute && isProduction(req.Environment) && !req.ProductionDeleteEnabled {
		return nil, xerrors.New("production reconciliation requires explicit production-delete confirmation")
	}
	manifests, err := p.repo.ListPendingRetirements(ctx, req.Tag, req.StartHeight, req.EndHeight, req.Limit)
	if err != nil {
		return nil, err
	}
	report := newReport(req)
	verifier := p.verifierFactory()
	var firstErr error
	halted := false
	for _, manifest := range manifests {
		item := candidateFromManifest(manifest)
		if halted {
			item.Action = ActionSkip
			item.SkipReason = SkipNotAttemptedAfterFailure
			report.Items = append(report.Items, item)
			continue
		}
		if manifest.Bucket != req.Bucket || !requestAllowsCandidate(req, item) ||
			manifest.State != RetirementStateEligible && manifest.State != RetirementStateDeleting ||
			!isSHA256Hex(manifest.LegacyObjectKeySHA256) || !isSHA256Hex(manifest.PayloadSHA256) {
			item.Action = ActionSkip
			item.SkipReason = SkipMetadataChanged
			report.Items = append(report.Items, item)
			continue
		}
		if !approvalMatches(req) {
			item.Action = ActionSkip
			item.SkipReason = SkipChainRangeNotApproved
			report.Items = append(report.Items, item)
			continue
		}
		if req.FallbackErrorCount > 0 {
			item.Action = ActionSkip
			item.SkipReason = SkipActiveFallbackOrReadErrors
			report.Items = append(report.Items, item)
			continue
		}
		if !req.ClientMigrationApproved {
			item.Action = ActionSkip
			item.SkipReason = SkipFileClientsNotApproved
			report.Items = append(report.Items, item)
			continue
		}
		if !req.Execute {
			item.Action = ActionReportOnly
			report.Items = append(report.Items, item)
			continue
		}
		if reason, err := p.reconcileManifest(ctx, req, &item, manifest, verifier); err != nil {
			item.Action = ActionSkip
			item.SkipReason = reason
			item.RetirementOutcome = reason
			if outcomeErr := p.repo.RecordRetirementOutcome(ctx, item.BlockMetadataID, reason, time.Now().UTC()); outcomeErr != nil {
				err = errors.Join(err, outcomeErr)
			}
			if firstErr == nil {
				firstErr = err
			}
			halted = true
		}
		report.Items = append(report.Items, item)
	}
	report.Summary = summarize(report.Items)
	return report, firstErr
}

func (p *Planner) planRow(
	ctx context.Context,
	req PlanRequest,
	row MetadataRow,
	cscbHeads map[string]ObjectHead,
	legacyTopologies map[string]ObjectVersionTopology,
	verifier blockPayloadVerifier,
) Candidate {
	item := Candidate{
		Bucket:             req.Bucket,
		Key:                row.LegacyObjectKey,
		BlockMetadataID:    row.BlockMetadataID,
		Tag:                row.Tag,
		Height:             row.Height,
		Hash:               row.Hash,
		ByteOffset:         row.PrimaryByteOffset,
		ByteLength:         row.PrimaryByteLength,
		UncompressedLength: row.PrimaryUncompressedLength,
		Action:             ActionSkip,
	}
	if row.Retirement != nil {
		item.RetirementState = row.Retirement.State
		item.RetirementAttempts = row.Retirement.AttemptCount
		item.RetirementOutcome = row.Retirement.Outcome
		if row.Retirement.State == RetirementStateDeletedVerified {
			item.Key = ""
			item.VersionID = firstString(row.Retirement.LegacyObjectVersionIDs)
			item.LegacyETag = row.Retirement.LegacyObjectETag
			item.LegacyBytes = row.Retirement.LegacyObjectBytes
			item.ConsolidatedKey = row.Retirement.ConsolidatedObjectKey
			item.CSCBVersionID = row.Retirement.ConsolidatedObjectVersionID
			item.CSCBETag = row.Retirement.ConsolidatedObjectETag
			item.PayloadSHA256 = row.Retirement.PayloadSHA256
			item.Action = ActionAlreadyDeleted
			item.SkipReason = SkipRetirementAlreadyFinalized
			return item
		}
	}
	if row.Skipped {
		item.SkipReason = SkipSkippedBlock
		return item
	}
	if row.Shadow == nil {
		item.SkipReason = SkipMissingConsolidationShadow
		return item
	}
	if row.LegacyObjectKey == "" {
		item.SkipReason = SkipMissingLegacyKey
		return item
	}
	shadow := row.Shadow
	item.ConsolidatedKey = shadow.ConsolidatedObjectKey
	item.ValidatedAt = shadow.ValidatedAt
	item.RetiredAt = shadow.LegacyObjectRetiredAt
	if shadow.LegacyObjectRetireAfter != nil {
		eligibleAt := *shadow.LegacyObjectRetireAfter
		item.EligibleAt = &eligibleAt
	}
	if shadow.ValidatedAt == nil {
		item.SkipReason = SkipValidationNotPassed
		return item
	}
	if !isPrimaryConsolidated(row) {
		item.SkipReason = SkipActiveMetadataStillLegacy
		return item
	}
	if !validShadowReference(row, shadow) {
		item.SkipReason = SkipInvalidMetadataReference
		return item
	}
	if shadow.LegacyObjectRetiredAt == nil || shadow.LegacyObjectRetireAfter == nil {
		item.SkipReason = SkipMissingRetirementMarker
		return item
	}
	if item.EligibleAt != nil && req.Now.Before(*item.EligibleAt) {
		item.SkipReason = SkipRetentionPeriodActive
		return item
	}
	if !approvalMatches(req) {
		item.SkipReason = SkipChainRangeNotApproved
		return item
	}
	if req.FallbackErrorCount > 0 {
		item.SkipReason = SkipActiveFallbackOrReadErrors
		return item
	}
	if !req.ClientMigrationApproved {
		item.SkipReason = SkipFileClientsNotApproved
		return item
	}

	topology, ok := legacyTopologies[row.LegacyObjectKey]
	if !ok {
		var err error
		topology, err = p.store.ListObjectVersions(ctx, req.Bucket, row.LegacyObjectKey)
		if err != nil {
			item.SkipReason = SkipObjectInspectionFailed
			return item
		}
		legacyTopologies[row.LegacyObjectKey] = topology
	}
	item.LegacyVersions = len(topology.Versions)
	item.DeleteMarkers = len(topology.DeleteMarkers)
	if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
		item.SkipReason = SkipLegacyObjectMissing
		return item
	}
	if len(topology.Versions) != 1 || len(topology.DeleteMarkers) != 0 || !topology.Versions[0].IsLatest {
		item.SkipReason = SkipUnsafeLegacyVersionTopology
		return item
	}
	version := topology.Versions[0]
	item.VersionID = version.VersionID
	item.LegacyETag = version.ETag
	item.LegacyBytes = version.Bytes
	if item.VersionID == "" || item.LegacyETag == "" {
		item.SkipReason = SkipLegacyVersionIDUnavailable
		return item
	}

	head, ok := cscbHeads[shadow.ConsolidatedObjectKey]
	if !ok {
		var err error
		head, err = p.store.HeadObject(ctx, req.Bucket, shadow.ConsolidatedObjectKey)
		if err != nil {
			item.SkipReason = SkipObjectInspectionFailed
			return item
		}
		cscbHeads[shadow.ConsolidatedObjectKey] = head
	}
	if !head.Exists || head.DeleteMark || head.VersionID == "" || head.ETag == "" {
		item.SkipReason = SkipMissingCSCBObject
		return item
	}
	item.CSCBVersionID = head.VersionID
	item.CSCBETag = head.ETag
	logicalBytes, ok := cscbLogicalPayloadBytes(head)
	if head.Bytes == 0 || !ok || shadow.ByteOffset > logicalBytes || shadow.ByteLength > logicalBytes-shadow.ByteOffset {
		item.SkipReason = SkipInvalidMetadataReference
		return item
	}
	item.LegacyKeySHA256 = keySHA256(item.Key)
	payloadSHA256, err := verifier.Verify(ctx, item)
	if err != nil {
		item.SkipReason = SkipLegacyPayloadMismatch
		return item
	}
	item.PayloadSHA256 = payloadSHA256

	if req.Execute {
		if isProduction(req.Environment) && !req.ProductionDeleteEnabled {
			item.SkipReason = SkipProductionDeletionDisabled
			return item
		}
		item.Action = ActionDeleteObjectVersion
		item.SkipReason = ""
		return item
	}
	item.Action = ActionReportOnly
	item.SkipReason = ""
	return item
}

func (p *Planner) applyCandidate(ctx context.Context, req PlanRequest, item *Candidate, verifier blockPayloadVerifier) (string, error) {
	payloadSHA256, reason, err := p.revalidateCandidate(ctx, req, *item, verifier)
	if err != nil {
		return reason, err
	}
	item.PayloadSHA256 = payloadSHA256
	item.LegacyKeySHA256 = keySHA256(item.Key)
	manifest := manifestFromCandidate(*item, req.Now)
	if err := p.repo.PrepareRetirement(ctx, manifest); err != nil {
		return SkipMetadataChanged, err
	}
	if err := p.repo.MarkRetirementDeleting(ctx, item.BlockMetadataID, time.Now().UTC()); err != nil {
		return SkipMetadataChanged, err
	}
	item.RetirementState = RetirementStateDeleting
	item.RetirementAttempts++
	item.RetirementOutcome = "delete_started"
	preDeleteSHA256, reason, err := p.revalidateCandidate(ctx, req, *item, verifier)
	if err != nil {
		return reason, err
	}
	if preDeleteSHA256 != item.PayloadSHA256 {
		return SkipLegacyPayloadMismatch, xerrors.Errorf("payload digest changed immediately before retirement: block_metadata_id=%d", item.BlockMetadataID)
	}
	if err := p.store.DeleteObjectVersion(ctx, item.Bucket, item.Key, item.VersionID); err != nil {
		return SkipVersionedDeleteFailed, err
	}
	return p.finalizeVerifiedDeletion(ctx, req, item, item.PayloadSHA256, verifier)
}

func (p *Planner) reconcileManifest(
	ctx context.Context,
	req PlanRequest,
	item *Candidate,
	manifest RetirementManifest,
	verifier blockPayloadVerifier,
) (string, error) {
	if manifest.LegacyObjectKey == "" || keySHA256(manifest.LegacyObjectKey) != manifest.LegacyObjectKeySHA256 {
		return SkipMetadataChanged, xerrors.Errorf("retirement manifest legacy key hash mismatch: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	topology, err := p.store.ListObjectVersions(ctx, manifest.Bucket, manifest.LegacyObjectKey)
	if err != nil {
		return SkipObjectInspectionFailed, err
	}
	if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
		if manifest.State != RetirementStateDeleting {
			return SkipMetadataChanged, xerrors.Errorf("legacy object is absent while retirement state is %s", manifest.State)
		}
		return p.finalizeVerifiedDeletion(ctx, req, item, manifest.PayloadSHA256, verifier)
	}
	if !topologyMatchesManifest(topology, manifest) {
		return SkipUnsafeLegacyVersionTopology, xerrors.Errorf("pending retirement topology changed: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	payloadSHA256, reason, err := p.revalidateCandidate(ctx, req, *item, verifier)
	if err != nil {
		return reason, err
	}
	if payloadSHA256 != manifest.PayloadSHA256 {
		return SkipLegacyPayloadMismatch, xerrors.Errorf("pending retirement payload digest changed: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if err := p.repo.MarkRetirementDeleting(ctx, manifest.BlockMetadataID, time.Now().UTC()); err != nil {
		return SkipMetadataChanged, err
	}
	item.RetirementState = RetirementStateDeleting
	item.RetirementAttempts++
	item.RetirementOutcome = "delete_started"
	preDeleteSHA256, reason, err := p.revalidateCandidate(ctx, req, *item, verifier)
	if err != nil {
		return reason, err
	}
	if preDeleteSHA256 != manifest.PayloadSHA256 {
		return SkipLegacyPayloadMismatch, xerrors.Errorf("pending retirement payload digest changed immediately before deletion: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if err := p.store.DeleteObjectVersion(ctx, item.Bucket, item.Key, item.VersionID); err != nil {
		return SkipVersionedDeleteFailed, err
	}
	return p.finalizeVerifiedDeletion(ctx, req, item, manifest.PayloadSHA256, verifier)
}

func (p *Planner) finalizeVerifiedDeletion(
	ctx context.Context,
	req PlanRequest,
	item *Candidate,
	expectedPayloadSHA256 string,
	verifier blockPayloadVerifier,
) (string, error) {
	if err := p.verifyLegacyDeleted(ctx, item.Bucket, item.Key); err != nil {
		return SkipPostDeleteVerificationFailed, err
	}
	payloadSHA256, reason, err := p.revalidateConsolidatedCandidate(ctx, req, *item, verifier)
	if err != nil {
		return reason, err
	}
	if payloadSHA256 != expectedPayloadSHA256 {
		return SkipLegacyPayloadMismatch, xerrors.Errorf("pinned CSCB payload digest changed after legacy deletion: block_metadata_id=%d", item.BlockMetadataID)
	}
	deletedAt := time.Now().UTC()
	if err := p.repo.FinalizeRetirement(ctx, item.BlockMetadataID, deletedAt, ActionDeletedVerified); err != nil {
		return SkipPostDeleteVerificationFailed, err
	}
	item.Action = ActionDeletedVerified
	item.SkipReason = ""
	item.RetirementState = RetirementStateDeletedVerified
	item.RetirementOutcome = ActionDeletedVerified
	item.Key = ""
	item.LegacyETag = ""
	return "", nil
}

func (p *Planner) revalidateCandidate(ctx context.Context, req PlanRequest, candidate Candidate, verifier blockPayloadVerifier) (string, string, error) {
	if _, reason, err := p.revalidateMetadataAndCSCB(ctx, req, candidate, true); err != nil {
		return "", reason, err
	}
	topology, err := p.store.ListObjectVersions(ctx, candidate.Bucket, candidate.Key)
	if err != nil {
		return "", SkipObjectInspectionFailed, err
	}
	if !topologyMatchesCandidate(topology, candidate) {
		return "", SkipUnsafeLegacyVersionTopology, xerrors.Errorf("legacy object version topology changed before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	digest, err := verifier.Verify(ctx, candidate)
	if err != nil {
		return "", SkipLegacyPayloadMismatch, err
	}
	return digest, "", nil
}

func (p *Planner) revalidateConsolidatedCandidate(ctx context.Context, req PlanRequest, candidate Candidate, verifier blockPayloadVerifier) (string, string, error) {
	if _, reason, err := p.revalidateMetadataAndCSCB(ctx, req, candidate, false); err != nil {
		return "", reason, err
	}
	digest, err := verifier.VerifyConsolidated(ctx, candidate)
	if err != nil {
		return "", SkipLegacyPayloadMismatch, err
	}
	return digest, "", nil
}

func (p *Planner) revalidateMetadataAndCSCB(ctx context.Context, req PlanRequest, candidate Candidate, requireCanonical bool) (MetadataRow, string, error) {
	row, err := p.repo.GetMetadataRow(ctx, candidate.BlockMetadataID)
	if err != nil {
		return MetadataRow{}, SkipMetadataChanged, err
	}
	if !candidateMatchesRow(req, candidate, row, requireCanonical) {
		return MetadataRow{}, SkipMetadataChanged, xerrors.Errorf("canonical, shadow, or retirement metadata changed before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	current, err := p.store.HeadObject(ctx, candidate.Bucket, candidate.ConsolidatedKey)
	if err != nil {
		return MetadataRow{}, SkipObjectInspectionFailed, err
	}
	pinned, err := p.store.HeadObjectVersion(ctx, candidate.Bucket, candidate.ConsolidatedKey, candidate.CSCBVersionID)
	if err != nil {
		return MetadataRow{}, SkipObjectInspectionFailed, err
	}
	if !current.Exists || current.DeleteMark || current.VersionID != candidate.CSCBVersionID || current.ETag != candidate.CSCBETag ||
		!pinned.Exists || pinned.DeleteMark || pinned.VersionID != candidate.CSCBVersionID || pinned.ETag != candidate.CSCBETag {
		return MetadataRow{}, SkipCSCBObjectChanged, xerrors.Errorf("pinned CSCB object changed before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	logicalBytes, ok := cscbLogicalPayloadBytes(pinned)
	if pinned.Bytes == 0 || !ok || candidate.ByteOffset > logicalBytes || candidate.ByteLength > logicalBytes-candidate.ByteOffset {
		return MetadataRow{}, SkipCSCBObjectChanged, xerrors.Errorf("pinned CSCB metadata is invalid before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	return row, "", nil
}

func (p *Planner) verifyLegacyDeleted(ctx context.Context, bucket string, key string) error {
	topology, err := p.store.ListObjectVersions(ctx, bucket, key)
	if err != nil {
		return err
	}
	if len(topology.Versions) != 0 || len(topology.DeleteMarkers) != 0 {
		return xerrors.Errorf("legacy key still has versions after delete: versions=%d markers=%d", len(topology.Versions), len(topology.DeleteMarkers))
	}
	head, err := p.store.HeadObject(ctx, bucket, key)
	if err != nil {
		return err
	}
	if head.Exists {
		return xerrors.New("legacy key still resolves after deleting its pinned version")
	}
	return nil
}

func candidateMatchesRow(req PlanRequest, candidate Candidate, row MetadataRow, requireCanonical bool) bool {
	if row.Shadow == nil || row.Shadow.ValidatedAt == nil || row.Shadow.LegacyObjectRetiredAt == nil ||
		row.Shadow.LegacyObjectRetireAfter == nil || row.Shadow.LegacyObjectDeletedAt != nil ||
		req.Now.Before(*row.Shadow.LegacyObjectRetireAfter) {
		return false
	}
	return (!requireCanonical || row.Canonical) &&
		!row.Skipped &&
		approvalMatches(req) &&
		req.FallbackErrorCount == 0 &&
		req.ClientMigrationApproved &&
		requestAllowsCandidate(req, candidate) &&
		row.BlockMetadataID == candidate.BlockMetadataID &&
		row.Tag == candidate.Tag &&
		row.Height == candidate.Height &&
		row.Hash == candidate.Hash &&
		row.LegacyObjectKey == candidate.Key &&
		row.PrimaryObjectKey == candidate.ConsolidatedKey &&
		row.PrimaryObjectFormat == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH &&
		row.PrimaryByteOffset == candidate.ByteOffset &&
		row.PrimaryByteLength == candidate.ByteLength &&
		row.PrimaryUncompressedLength == candidate.UncompressedLength &&
		validShadowReference(row, row.Shadow)
}

func requestAllowsCandidate(req PlanRequest, candidate Candidate) bool {
	return candidate.Bucket == req.Bucket &&
		candidate.Tag == req.Tag &&
		candidate.Height >= req.StartHeight &&
		candidate.Height < req.EndHeight
}

func validateApplyReport(req PlanRequest, report *Report) error {
	if report.DryRun ||
		report.Environment != req.Environment ||
		report.Blockchain != req.Blockchain ||
		report.Network != req.Network ||
		report.Sidechain != req.Sidechain ||
		report.Bucket != req.Bucket ||
		report.Tag != req.Tag ||
		report.StartHeight != req.StartHeight ||
		report.EndHeight != req.EndHeight ||
		report.Approval != req.Approval ||
		report.SafetyGates.ClientMigrationApproved != req.ClientMigrationApproved ||
		report.SafetyGates.FallbackReadErrors != req.FallbackErrorCount ||
		report.SafetyGates.ProductionDeleteEnabled != req.ProductionDeleteEnabled ||
		report.SafetyGates.VersionedDeleteMode != "exact_single_object_version_only" {
		return xerrors.New("retirement report does not match the execution request")
	}
	if !approvalMatches(req) || req.FallbackErrorCount != 0 || !req.ClientMigrationApproved {
		return xerrors.New("retirement execution safety gates are not satisfied")
	}
	for _, item := range report.Items {
		if item.Action != ActionDeleteObjectVersion {
			continue
		}
		if !requestAllowsCandidate(req, item) || item.BlockMetadataID <= 0 || item.Key == "" ||
			item.LegacyKeySHA256 != keySHA256(item.Key) || item.VersionID == "" || item.LegacyETag == "" ||
			item.LegacyBytes == 0 || item.LegacyVersions != 1 || item.DeleteMarkers != 0 ||
			item.ConsolidatedKey == "" || item.CSCBVersionID == "" || item.CSCBETag == "" ||
			item.ByteLength == 0 || item.UncompressedLength == 0 || !isSHA256Hex(item.PayloadSHA256) {
			return xerrors.Errorf("retirement report contains an invalid delete candidate: block_metadata_id=%d", item.BlockMetadataID)
		}
	}
	return nil
}

func topologyMatchesCandidate(topology ObjectVersionTopology, candidate Candidate) bool {
	return len(topology.Versions) == 1 &&
		len(topology.DeleteMarkers) == 0 &&
		topology.Versions[0].IsLatest &&
		topology.Versions[0].VersionID == candidate.VersionID &&
		topology.Versions[0].ETag == candidate.LegacyETag &&
		topology.Versions[0].Bytes == candidate.LegacyBytes
}

func topologyMatchesManifest(topology ObjectVersionTopology, manifest RetirementManifest) bool {
	return len(manifest.LegacyObjectVersionIDs) == 1 &&
		len(topology.Versions) == 1 &&
		len(topology.DeleteMarkers) == 0 &&
		topology.Versions[0].IsLatest &&
		topology.Versions[0].VersionID == manifest.LegacyObjectVersionIDs[0] &&
		topology.Versions[0].ETag == manifest.LegacyObjectETag &&
		topology.Versions[0].Bytes == manifest.LegacyObjectBytes
}

func manifestFromCandidate(candidate Candidate, preparedAt time.Time) RetirementManifest {
	return RetirementManifest{
		BlockMetadataID:                candidate.BlockMetadataID,
		Tag:                            candidate.Tag,
		Height:                         candidate.Height,
		Hash:                           candidate.Hash,
		State:                          RetirementStateEligible,
		Bucket:                         candidate.Bucket,
		LegacyObjectKey:                candidate.Key,
		LegacyObjectKeySHA256:          keySHA256(candidate.Key),
		LegacyObjectVersionIDs:         []string{candidate.VersionID},
		LegacyObjectETag:               candidate.LegacyETag,
		LegacyObjectBytes:              candidate.LegacyBytes,
		ConsolidatedObjectKey:          candidate.ConsolidatedKey,
		ConsolidatedObjectVersionID:    candidate.CSCBVersionID,
		ConsolidatedObjectETag:         candidate.CSCBETag,
		ConsolidatedByteOffset:         candidate.ByteOffset,
		ConsolidatedByteLength:         candidate.ByteLength,
		ConsolidatedUncompressedLength: candidate.UncompressedLength,
		PayloadSHA256:                  candidate.PayloadSHA256,
		PreparedAt:                     preparedAt,
	}
}

func candidateFromManifest(manifest RetirementManifest) Candidate {
	return Candidate{
		Bucket:             manifest.Bucket,
		Key:                manifest.LegacyObjectKey,
		VersionID:          firstString(manifest.LegacyObjectVersionIDs),
		BlockMetadataID:    manifest.BlockMetadataID,
		Tag:                manifest.Tag,
		Height:             manifest.Height,
		Hash:               manifest.Hash,
		LegacyBytes:        manifest.LegacyObjectBytes,
		LegacyETag:         manifest.LegacyObjectETag,
		LegacyKeySHA256:    manifest.LegacyObjectKeySHA256,
		LegacyVersions:     len(manifest.LegacyObjectVersionIDs),
		ConsolidatedKey:    manifest.ConsolidatedObjectKey,
		CSCBVersionID:      manifest.ConsolidatedObjectVersionID,
		CSCBETag:           manifest.ConsolidatedObjectETag,
		ByteOffset:         manifest.ConsolidatedByteOffset,
		ByteLength:         manifest.ConsolidatedByteLength,
		UncompressedLength: manifest.ConsolidatedUncompressedLength,
		PayloadSHA256:      manifest.PayloadSHA256,
		RetirementState:    manifest.State,
		RetirementAttempts: manifest.AttemptCount,
		RetirementOutcome:  manifest.Outcome,
		Action:             ActionDeleteObjectVersion,
	}
}

func keySHA256(key string) string {
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])
}

func isSHA256Hex(value string) bool {
	if len(value) != sha256HexLength {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func newReport(req PlanRequest) *Report {
	return &Report{
		GeneratedAt: req.Now,
		DryRun:      !req.Execute,
		Environment: req.Environment,
		Blockchain:  req.Blockchain,
		Network:     req.Network,
		Sidechain:   req.Sidechain,
		Bucket:      req.Bucket,
		Tag:         req.Tag,
		StartHeight: req.StartHeight,
		EndHeight:   req.EndHeight,
		Approval:    req.Approval,
		SafetyGates: SafetyGates{
			ClientMigrationApproved: req.ClientMigrationApproved,
			FallbackReadErrors:      req.FallbackErrorCount,
			VersionedDeleteMode:     "exact_single_object_version_only",
			ProductionDeleteEnabled: req.ProductionDeleteEnabled,
		},
		Items: make([]Candidate, 0),
	}
}

func normalizeRequest(req *PlanRequest) error {
	if req == nil {
		return xerrors.New("request is required")
	}
	if req.EndHeight <= req.StartHeight {
		return xerrors.Errorf("end height must be greater than start height: start=%d end=%d", req.StartHeight, req.EndHeight)
	}
	if req.Bucket == "" {
		return xerrors.New("bucket is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	return nil
}

func validShadowReference(row MetadataRow, shadow *ConsolidationShadow) bool {
	if shadow == nil {
		return false
	}
	if shadow.Tag != row.Tag || shadow.Height != row.Height || shadow.Hash != row.Hash {
		return false
	}
	if shadow.LegacyObjectKey != row.LegacyObjectKey {
		return false
	}
	if !isPrimaryConsolidated(row) {
		return false
	}
	if row.PrimaryObjectKey != shadow.ConsolidatedObjectKey ||
		row.PrimaryObjectFormat != shadow.ObjectFormat ||
		row.PrimaryByteOffset != shadow.ByteOffset ||
		row.PrimaryByteLength != shadow.ByteLength ||
		row.PrimaryUncompressedLength != shadow.UncompressedLength {
		return false
	}
	if shadow.ConsolidatedObjectKey == "" {
		return false
	}
	if shadow.ObjectFormat != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH {
		return false
	}
	return shadow.ByteLength != 0
}

func cscbLogicalPayloadBytes(head ObjectHead) (uint64, bool) {
	format, ok := objectMetadataValue(head.Metadata, cscbFormatMetadataKey)
	if !ok || !strings.EqualFold(strings.TrimSpace(format), cscbFormatMetadataValue) {
		return 0, false
	}
	scope, ok := objectMetadataValue(head.Metadata, cscbCompressionScopeMetadataKey)
	if !ok || !strings.EqualFold(strings.TrimSpace(scope), cscbCompressionScopeMetadataValue) {
		return 0, false
	}
	value, ok := objectMetadataValue(head.Metadata, cscbUncompressedLengthMetadataKey)
	if !ok {
		return 0, false
	}
	logicalBytes, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil || logicalBytes == 0 {
		return 0, false
	}
	return logicalBytes, true
}

func objectMetadataValue(metadata map[string]string, name string) (string, bool) {
	value, ok := metadata[name]
	if ok {
		return value, true
	}
	for key, candidate := range metadata {
		if strings.EqualFold(key, name) {
			return candidate, true
		}
	}
	return "", false
}

func isPrimaryConsolidated(row MetadataRow) bool {
	return row.PrimaryObjectFormat == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH &&
		row.PrimaryObjectKey != "" &&
		row.PrimaryByteLength > 0
}

func approvalMatches(req PlanRequest) bool {
	return normalizeApprovalChain(req.Approval.Chain) == normalizeApprovalChain(actualChain(req)) &&
		req.Approval.StartHeight == req.StartHeight &&
		req.Approval.EndHeight == req.EndHeight
}

func actualChain(req PlanRequest) string {
	parts := []string{req.Blockchain, req.Network}
	if req.Sidechain != "" {
		parts = append(parts, req.Sidechain)
	}
	return strings.Join(parts, "-")
}

func normalizeApprovalChain(value string) string {
	return strings.ToLower(strings.ReplaceAll(value, "_", "-"))
}

func isProduction(env string) bool {
	env = strings.ToLower(env)
	return env == "production" || env == "prod"
}

func summarize(items []Candidate) Summary {
	var summary Summary
	summary.TotalRows = len(items)
	for _, item := range items {
		summary.LegacyBytes += item.LegacyBytes
		if item.DeleteMarkers > 0 || item.SkipReason == SkipLegacyCurrentDeleteMarker {
			summary.DeleteMarkerRows++
		}
		switch item.Action {
		case ActionReportOnly, ActionDeleteObjectVersion:
			summary.EligibleRows++
			summary.EligibleBytes += item.LegacyBytes
		case ActionDeletedObjectVersion, ActionDeletedVerified, ActionAlreadyDeleted:
			summary.DeletedRows++
		default:
			summary.SkippedRows++
		}
	}
	return summary
}
