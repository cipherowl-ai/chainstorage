package retirement

import (
	"context"
	cryptorand "crypto/rand"
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
	retirementClaimTokenBytes         = 16
	retirementClaimLease              = 15 * time.Minute
	retirementSafetyQuiescencePeriod  = 15 * time.Minute
	s3MutableNullVersionID            = "null"
)

var unverifiedRetentionSafetySHA256 = keySHA256("unverified-retention-safety-configuration")

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
	singleBlockTopologies := make(map[string]ObjectVersionTopology)
	writeOncePolicies := make(map[string]bool)
	verifier := p.verifierFactory()
	for _, row := range rows {
		item := p.planRow(ctx, req, row, cscbHeads, singleBlockTopologies, writeOncePolicies, verifier)
		report.Items = append(report.Items, item)
	}
	report.SafetyGates.CSCBWriteOncePolicyVerified = writeOncePolicyGateVerified(report.Items)
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
	if err := p.preflightApplyRetentionSafety(ctx, report); err != nil {
		report.SafetyGates.CSCBWriteOncePolicyVerified = writeOncePolicyGateVerified(report.Items)
		report.Summary = summarize(report.Items)
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
		reason, err := p.applyCandidate(ctx, req, item, verifier)
		if reason != "" {
			if item.RetirementState != RetirementStateDeletedPendingVerification {
				item.Action = ActionSkip
			}
			item.SkipReason = reason
			item.RetirementOutcome = reason
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			halted = true
		}
	}
	report.SafetyGates.CSCBWriteOncePolicyVerified = writeOncePolicyGateVerified(report.Items)
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
			manifest.State != RetirementStateEligible &&
				manifest.State != RetirementStateDeleting &&
				manifest.State != RetirementStateDeletedPendingVerification ||
			!isSHA256Hex(manifest.SingleBlockObjectKeySHA256) || !isSHA256Hex(manifest.PayloadSHA256) {
			markCandidateBlocked(&item, SkipMetadataChanged)
			report.Items = append(report.Items, item)
			continue
		}
		if !approvalMatches(req) {
			markCandidateBlocked(&item, SkipChainRangeNotApproved)
			report.Items = append(report.Items, item)
			continue
		}
		if req.FallbackErrorCount > 0 {
			markCandidateBlocked(&item, SkipActiveFallbackOrReadErrors)
			report.Items = append(report.Items, item)
			continue
		}
		if !req.ClientMigrationApproved {
			markCandidateBlocked(&item, SkipFileClientsNotApproved)
			report.Items = append(report.Items, item)
			continue
		}
		if !req.Execute {
			if reason, err := p.inspectCSCBRetentionSafety(ctx, &item); err != nil {
				markCandidateBlocked(&item, reason)
				report.Items = append(report.Items, item)
				continue
			}
			if item.RetirementState == RetirementStateDeletedPendingVerification {
				item.Action = ActionDeletedObjectVersion
				item.SkipReason = SkipRetirementVerificationPending
			} else {
				item.Action = ActionReportOnly
			}
			report.Items = append(report.Items, item)
			continue
		}
		if reason, err := p.verifyCSCBRetentionSafety(ctx, &item); err != nil {
			markCandidateBlocked(&item, reason)
			item.RetirementOutcome = reason
			report.Items = append(report.Items, item)
			if firstErr == nil {
				firstErr = err
			}
			if reason != SkipCSCBSafetyQuiescenceActive {
				halted = true
			}
			continue
		}
		reason, err := p.reconcileManifest(ctx, req, &item, manifest)
		if reason != "" {
			if item.RetirementState != RetirementStateDeletedPendingVerification {
				item.Action = ActionSkip
			}
			item.SkipReason = reason
			item.RetirementOutcome = reason
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			halted = true
		}
		report.Items = append(report.Items, item)
	}
	report.SafetyGates.CSCBWriteOncePolicyVerified = writeOncePolicyGateVerified(report.Items)
	report.Summary = summarize(report.Items)
	return report, firstErr
}

func (p *Planner) planRow(
	ctx context.Context,
	req PlanRequest,
	row MetadataRow,
	cscbHeads map[string]ObjectHead,
	singleBlockTopologies map[string]ObjectVersionTopology,
	writeOncePolicies map[string]bool,
	verifier blockPayloadVerifier,
) Candidate {
	item := Candidate{
		Bucket:             req.Bucket,
		Key:                row.SingleBlockObjectKey,
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
			if !finalizedRetirementMatchesRow(row) {
				item.SkipReason = SkipMetadataChanged
				return item
			}
			item.Key = ""
			item.VersionID = firstString(row.Retirement.SingleBlockObjectVersionIDs)
			item.SingleBlockETag = row.Retirement.SingleBlockObjectETag
			item.SingleBlockBytes = row.Retirement.SingleBlockObjectBytes
			item.ConsolidatedKey = row.Retirement.ConsolidatedObjectKey
			item.CSCBVersionID = row.Retirement.ConsolidatedObjectVersionID
			item.CSCBETag = row.Retirement.ConsolidatedObjectETag
			item.PayloadSHA256 = row.Retirement.PayloadSHA256
			item.SingleBlockDeletedAt = row.Retirement.DeletedAt
			item.RetirementVerifiedAt = row.Retirement.VerifiedAt
			item.Action = ActionAlreadyDeleted
			item.SkipReason = SkipRetirementAlreadyFinalized
			return item
		}
		if row.Retirement.State == RetirementStateDeletedPendingVerification {
			if !pendingVerificationRetirementMatchesRow(row) {
				item.SkipReason = SkipMetadataChanged
				return item
			}
			item.Key = ""
			item.VersionID = firstString(row.Retirement.SingleBlockObjectVersionIDs)
			item.SingleBlockBytes = row.Retirement.SingleBlockObjectBytes
			item.ConsolidatedKey = row.Retirement.ConsolidatedObjectKey
			item.CSCBVersionID = row.Retirement.ConsolidatedObjectVersionID
			item.CSCBETag = row.Retirement.ConsolidatedObjectETag
			item.PayloadSHA256 = row.Retirement.PayloadSHA256
			item.SingleBlockDeletedAt = row.Retirement.DeletedAt
			item.Action = ActionDeletedObjectVersion
			item.SkipReason = SkipRetirementVerificationPending
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
	if row.SingleBlockObjectKey == "" {
		item.SkipReason = SkipMissingSingleBlockKey
		return item
	}
	shadow := row.Shadow
	item.ConsolidatedKey = shadow.ConsolidatedObjectKey
	item.ValidatedAt = shadow.ValidatedAt
	item.RetiredAt = shadow.SingleBlockRetentionStartedAt
	if shadow.SingleBlockDeleteAfter != nil {
		eligibleAt := *shadow.SingleBlockDeleteAfter
		item.EligibleAt = &eligibleAt
	}
	if shadow.ValidatedAt == nil {
		item.SkipReason = SkipValidationNotPassed
		return item
	}
	if !isPrimaryConsolidated(row) {
		item.SkipReason = SkipActiveMetadataStillSingleBlock
		return item
	}
	if !validShadowReference(row, shadow) {
		item.SkipReason = SkipInvalidMetadataReference
		return item
	}
	if shadow.SingleBlockRetentionStartedAt == nil || shadow.SingleBlockDeleteAfter == nil {
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

	topology, ok := singleBlockTopologies[row.SingleBlockObjectKey]
	if !ok {
		var err error
		topology, err = p.store.ListObjectVersions(ctx, req.Bucket, row.SingleBlockObjectKey)
		if err != nil {
			item.SkipReason = SkipObjectInspectionFailed
			return item
		}
		singleBlockTopologies[row.SingleBlockObjectKey] = topology
	}
	item.SingleBlockVersions = len(topology.Versions)
	item.DeleteMarkers = len(topology.DeleteMarkers)
	if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
		item.SkipReason = SkipSingleBlockObjectMissing
		return item
	}
	if len(topology.Versions) != 1 || len(topology.DeleteMarkers) != 0 || !topology.Versions[0].IsLatest {
		item.SkipReason = SkipUnsafeSingleBlockVersionTopology
		return item
	}
	version := topology.Versions[0]
	item.VersionID = version.VersionID
	item.SingleBlockETag = version.ETag
	item.SingleBlockBytes = version.Bytes
	if !isImmutableVersionID(item.VersionID) || item.SingleBlockETag == "" {
		item.SkipReason = SkipSingleBlockVersionIDUnavailable
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
	if !head.Exists || head.DeleteMark || !isImmutableVersionID(head.VersionID) || head.ETag == "" {
		item.SkipReason = SkipMissingCSCBObject
		return item
	}
	if head.Expiration != "" {
		item.SkipReason = SkipCSCBLifecycleExpirationActive
		return item
	}
	item.CSCBVersionID = head.VersionID
	item.CSCBETag = head.ETag
	logicalBytes, ok := cscbLogicalPayloadBytes(head)
	if head.Bytes == 0 || !ok || shadow.ByteOffset > logicalBytes || shadow.ByteLength > logicalBytes-shadow.ByteOffset {
		item.SkipReason = SkipInvalidMetadataReference
		return item
	}
	item.SingleBlockKeySHA256 = keySHA256(item.Key)
	payloadSHA256, err := verifier.Verify(ctx, item)
	if err != nil {
		item.SkipReason = SkipSingleBlockPayloadMismatch
		return item
	}
	item.PayloadSHA256 = payloadSHA256
	writeOncePolicyVerified, checked := writeOncePolicies[shadow.ConsolidatedObjectKey]
	if !checked {
		_, policyErr := p.store.InspectObjectRetentionSafety(ctx, req.Bucket, shadow.ConsolidatedObjectKey)
		writeOncePolicyVerified = policyErr == nil
		writeOncePolicies[shadow.ConsolidatedObjectKey] = writeOncePolicyVerified
	}
	item.CSCBWriteOncePolicy = writeOncePolicyVerified
	if !writeOncePolicyVerified {
		item.SkipReason = SkipCSCBWriteOncePolicyUnverified
		return item
	}

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
	item.SingleBlockKeySHA256 = keySHA256(item.Key)
	manifest := manifestFromCandidate(*item, req.Now)
	if err := p.repo.PrepareRetirement(ctx, manifest); err != nil {
		return SkipMetadataChanged, err
	}
	return p.withRetirementClaim(ctx, item, func(claimToken string) (string, error) {
		preDeleteSHA256, reason, err := p.revalidateCandidate(ctx, req, *item, verifier)
		if err != nil {
			return reason, err
		}
		if preDeleteSHA256 != item.PayloadSHA256 {
			return SkipSingleBlockPayloadMismatch, xerrors.Errorf("payload digest changed immediately before retirement: block_metadata_id=%d", item.BlockMetadataID)
		}
		if err := p.renewRetirementClaim(ctx, item.BlockMetadataID, claimToken); err != nil {
			return SkipRetirementClaimActive, err
		}
		if reason, err := p.verifyCSCBRetentionSafety(ctx, item); err != nil {
			return reason, err
		}
		if err := p.store.DeleteObjectVersion(ctx, item.Bucket, item.Key, item.VersionID); err != nil {
			return SkipVersionedDeleteFailed, err
		}
		return p.completeObjectDeletion(ctx, req, item, claimToken, item.PayloadSHA256)
	})
}

func (p *Planner) reconcileManifest(
	ctx context.Context,
	req PlanRequest,
	item *Candidate,
	manifest RetirementManifest,
) (string, error) {
	if manifest.State == RetirementStateDeletedPendingVerification {
		if manifest.SingleBlockObjectKey != "" || manifest.SingleBlockObjectETag != "" || manifest.DeletedAt == nil || manifest.VerifiedAt != nil {
			return SkipMetadataChanged, xerrors.Errorf("pending-verification retirement manifest is inconsistent: block_metadata_id=%d", manifest.BlockMetadataID)
		}
		return p.withRetirementClaim(ctx, item, func(claimToken string) (string, error) {
			if reason, err := p.verifyCSCBRetentionSafety(ctx, item); err != nil {
				return reason, err
			}
			return p.finalizeRecordedDeletion(ctx, req, item, claimToken, manifest.PayloadSHA256)
		})
	}
	if manifest.SingleBlockObjectKey == "" || keySHA256(manifest.SingleBlockObjectKey) != manifest.SingleBlockObjectKeySHA256 {
		return SkipMetadataChanged, xerrors.Errorf("retirement manifest single-block key hash mismatch: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if manifest.State == RetirementStateEligible {
		topology, err := p.store.ListObjectVersions(ctx, manifest.Bucket, manifest.SingleBlockObjectKey)
		if err != nil {
			return SkipObjectInspectionFailed, err
		}
		if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
			return SkipMetadataChanged, xerrors.Errorf("single-block object is absent before any delete attempt: block_metadata_id=%d", manifest.BlockMetadataID)
		}
	}
	return p.withRetirementClaim(ctx, item, func(claimToken string) (string, error) {
		topology, err := p.store.ListObjectVersions(ctx, manifest.Bucket, manifest.SingleBlockObjectKey)
		if err != nil {
			return SkipObjectInspectionFailed, err
		}
		if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
			if manifest.State != RetirementStateDeleting {
				return SkipMetadataChanged, xerrors.Errorf("single-block object is absent while retirement state is %s", manifest.State)
			}
			if reason, err := p.verifyCSCBRetentionSafety(ctx, item); err != nil {
				return reason, err
			}
			return p.completeObjectDeletion(ctx, req, item, claimToken, manifest.PayloadSHA256)
		}
		if !topologyMatchesManifest(topology, manifest) {
			return SkipUnsafeSingleBlockVersionTopology, xerrors.Errorf("pending retirement topology changed: block_metadata_id=%d", manifest.BlockMetadataID)
		}
		verifier := p.verifierFactory()
		payloadSHA256, reason, err := p.revalidateCandidate(ctx, req, *item, verifier)
		if err != nil {
			return reason, err
		}
		if payloadSHA256 != manifest.PayloadSHA256 {
			return SkipSingleBlockPayloadMismatch, xerrors.Errorf("pending retirement payload digest changed: block_metadata_id=%d", manifest.BlockMetadataID)
		}
		preDeleteSHA256, reason, err := p.revalidateCandidate(ctx, req, *item, verifier)
		if err != nil {
			return reason, err
		}
		if preDeleteSHA256 != manifest.PayloadSHA256 {
			return SkipSingleBlockPayloadMismatch, xerrors.Errorf("pending retirement payload digest changed immediately before deletion: block_metadata_id=%d", manifest.BlockMetadataID)
		}
		if err := p.renewRetirementClaim(ctx, item.BlockMetadataID, claimToken); err != nil {
			return SkipRetirementClaimActive, err
		}
		if reason, err := p.verifyCSCBRetentionSafety(ctx, item); err != nil {
			return reason, err
		}
		if err := p.store.DeleteObjectVersion(ctx, item.Bucket, item.Key, item.VersionID); err != nil {
			return SkipVersionedDeleteFailed, err
		}
		return p.completeObjectDeletion(ctx, req, item, claimToken, manifest.PayloadSHA256)
	})
}

func (p *Planner) verifyCSCBRetentionSafety(ctx context.Context, item *Candidate) (string, error) {
	item.CSCBWriteOncePolicy = false
	snapshot, inspectionErr := p.store.InspectObjectRetentionSafety(ctx, item.Bucket, item.ConsolidatedKey)
	configurationSHA256 := snapshot.ConfigurationSHA256
	if inspectionErr != nil || !isSHA256Hex(configurationSHA256) {
		configurationSHA256 = unverifiedRetentionSafetySHA256
	}
	firstObservedAt, observedAt, observationErr := p.repo.ObserveRetentionSafety(
		ctx,
		item.Bucket,
		item.ConsolidatedKey,
		configurationSHA256,
	)
	if observationErr != nil {
		return SkipCSCBWriteOncePolicyUnverified, errors.Join(inspectionErr, observationErr)
	}
	if inspectionErr != nil {
		return SkipCSCBWriteOncePolicyUnverified, inspectionErr
	}
	item.CSCBWriteOncePolicy = true
	if observedAt.Sub(firstObservedAt) < retirementSafetyQuiescencePeriod {
		return SkipCSCBSafetyQuiescenceActive, xerrors.Errorf(
			"CSCB retention safety configuration has not remained stable for %s: bucket=%s key=%s first_observed_at=%s observed_at=%s",
			retirementSafetyQuiescencePeriod,
			item.Bucket,
			item.ConsolidatedKey,
			firstObservedAt.Format(time.RFC3339Nano),
			observedAt.Format(time.RFC3339Nano),
		)
	}
	return "", nil
}

func (p *Planner) inspectCSCBRetentionSafety(ctx context.Context, item *Candidate) (string, error) {
	item.CSCBWriteOncePolicy = false
	if _, err := p.store.InspectObjectRetentionSafety(ctx, item.Bucket, item.ConsolidatedKey); err != nil {
		return SkipCSCBWriteOncePolicyUnverified, err
	}
	item.CSCBWriteOncePolicy = true
	return "", nil
}

func (p *Planner) preflightApplyRetentionSafety(ctx context.Context, report *Report) error {
	type safetyResult struct {
		reason string
		err    error
	}
	results := make(map[string]safetyResult)
	var firstErr error
	for i := range report.Items {
		item := &report.Items[i]
		if item.Action != ActionDeleteObjectVersion {
			continue
		}
		result, ok := results[item.ConsolidatedKey]
		if !ok {
			result.reason, result.err = p.verifyCSCBRetentionSafety(ctx, item)
			results[item.ConsolidatedKey] = result
		} else if result.err == nil {
			item.CSCBWriteOncePolicy = true
		}
		if result.err == nil {
			continue
		}
		item.Action = ActionSkip
		item.SkipReason = result.reason
		item.RetirementOutcome = result.reason
		if firstErr == nil {
			firstErr = result.err
		}
	}
	return firstErr
}

func (p *Planner) withRetirementClaim(
	ctx context.Context,
	item *Candidate,
	operation func(claimToken string) (string, error),
) (reason string, err error) {
	claimToken, err := newRetirementClaimToken()
	if err != nil {
		return SkipMetadataChanged, err
	}
	claimedAt := time.Now().UTC()
	if err := p.repo.ClaimRetirement(ctx, item.BlockMetadataID, claimToken, claimedAt, claimedAt.Add(retirementClaimLease)); err != nil {
		if errors.Is(err, ErrRetirementClaimUnavailable) {
			return SkipRetirementClaimActive, nil
		}
		return SkipMetadataChanged, err
	}
	if item.RetirementState == RetirementStateDeletedPendingVerification {
		item.RetirementOutcome = "verification_started"
	} else {
		item.RetirementState = RetirementStateDeleting
		item.RetirementOutcome = "delete_started"
	}
	item.RetirementAttempts++

	claimOwned := true
	defer func() {
		if err == nil || !claimOwned {
			return
		}
		outcome := reason
		if outcome == "" {
			outcome = SkipMetadataChanged
		}
		if outcomeErr := p.repo.RecordRetirementOutcome(ctx, item.BlockMetadataID, claimToken, outcome, time.Now().UTC()); outcomeErr != nil {
			err = errors.Join(err, outcomeErr)
		}
	}()

	reason, err = operation(claimToken)
	if err == nil {
		claimOwned = false
	}
	return reason, err
}

func (p *Planner) renewRetirementClaim(ctx context.Context, blockMetadataID int64, claimToken string) error {
	renewedAt := time.Now().UTC()
	return p.repo.RenewRetirementClaim(ctx, blockMetadataID, claimToken, renewedAt, renewedAt.Add(retirementClaimLease))
}

func (p *Planner) completeObjectDeletion(
	ctx context.Context,
	req PlanRequest,
	item *Candidate,
	claimToken string,
	expectedPayloadSHA256 string,
) (string, error) {
	if err := p.verifySingleBlockDeleted(ctx, item.Bucket, item.Key); err != nil {
		return SkipPostDeleteVerificationFailed, err
	}
	deletedAt, err := p.repo.RecordRetirementObjectDeleted(
		ctx,
		item.BlockMetadataID,
		claimToken,
		ActionDeletedObjectVersion,
	)
	if err != nil {
		return SkipPostDeleteVerificationFailed, err
	}
	item.Action = ActionDeletedObjectVersion
	item.SkipReason = ""
	item.RetirementState = RetirementStateDeletedPendingVerification
	item.RetirementOutcome = ActionDeletedObjectVersion
	item.SingleBlockDeletedAt = &deletedAt
	item.Key = ""
	item.SingleBlockETag = ""
	return p.finalizeRecordedDeletion(ctx, req, item, claimToken, expectedPayloadSHA256)
}

func (p *Planner) finalizeRecordedDeletion(
	ctx context.Context,
	req PlanRequest,
	item *Candidate,
	claimToken string,
	expectedPayloadSHA256 string,
) (string, error) {
	if err := p.renewRetirementClaim(ctx, item.BlockMetadataID, claimToken); err != nil {
		return SkipRetirementClaimActive, err
	}
	verifier := p.verifierFactory()
	payloadSHA256, reason, err := p.revalidateConsolidatedCandidate(ctx, req, *item, verifier)
	if err != nil {
		return reason, err
	}
	if payloadSHA256 != expectedPayloadSHA256 {
		return SkipSingleBlockPayloadMismatch, xerrors.Errorf("pinned CSCB payload digest changed after single-block deletion: block_metadata_id=%d", item.BlockMetadataID)
	}
	verifiedAt, err := p.repo.FinalizeRetirement(ctx, item.BlockMetadataID, claimToken, ActionDeletedVerified)
	if err != nil {
		return SkipPostDeleteVerificationFailed, err
	}
	item.Action = ActionDeletedVerified
	item.SkipReason = ""
	item.RetirementState = RetirementStateDeletedVerified
	item.RetirementOutcome = ActionDeletedVerified
	item.RetirementVerifiedAt = &verifiedAt
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
		return "", SkipUnsafeSingleBlockVersionTopology, xerrors.Errorf("single-block object version topology changed before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	digest, err := verifier.Verify(ctx, candidate)
	if err != nil {
		return "", SkipSingleBlockPayloadMismatch, err
	}
	return digest, "", nil
}

func (p *Planner) revalidateConsolidatedCandidate(ctx context.Context, req PlanRequest, candidate Candidate, verifier blockPayloadVerifier) (string, string, error) {
	if _, reason, err := p.revalidateMetadataAndCSCB(ctx, req, candidate, false); err != nil {
		return "", reason, err
	}
	digest, err := verifier.VerifyConsolidated(ctx, candidate)
	if err != nil {
		return "", SkipSingleBlockPayloadMismatch, err
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
	if !current.Exists || current.DeleteMark || !isImmutableVersionID(current.VersionID) || current.VersionID != candidate.CSCBVersionID || current.ETag != candidate.CSCBETag ||
		!pinned.Exists || pinned.DeleteMark || pinned.VersionID != candidate.CSCBVersionID || pinned.ETag != candidate.CSCBETag {
		return MetadataRow{}, SkipCSCBObjectChanged, xerrors.Errorf("pinned CSCB object changed before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	if current.Expiration != "" || pinned.Expiration != "" {
		return MetadataRow{}, SkipCSCBLifecycleExpirationActive, xerrors.Errorf("pinned CSCB object has active lifecycle expiration: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	logicalBytes, ok := cscbLogicalPayloadBytes(pinned)
	if pinned.Bytes == 0 || !ok || candidate.ByteOffset > logicalBytes || candidate.ByteLength > logicalBytes-candidate.ByteOffset {
		return MetadataRow{}, SkipCSCBObjectChanged, xerrors.Errorf("pinned CSCB metadata is invalid before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	return row, "", nil
}

func (p *Planner) verifySingleBlockDeleted(ctx context.Context, bucket string, key string) error {
	topology, err := p.store.ListObjectVersions(ctx, bucket, key)
	if err != nil {
		return err
	}
	if len(topology.Versions) != 0 || len(topology.DeleteMarkers) != 0 {
		return xerrors.Errorf("single-block key still has versions after delete: versions=%d markers=%d", len(topology.Versions), len(topology.DeleteMarkers))
	}
	head, err := p.store.HeadObject(ctx, bucket, key)
	if err != nil {
		return err
	}
	if head.Exists {
		return xerrors.New("single-block key still resolves after deleting its pinned version")
	}
	return nil
}

func candidateMatchesRow(req PlanRequest, candidate Candidate, row MetadataRow, requireCanonical bool) bool {
	if candidate.RetirementState == RetirementStateDeletedPendingVerification {
		return pendingVerificationCandidateMatchesRow(req, candidate, row, requireCanonical)
	}
	if row.Shadow == nil || row.Shadow.ValidatedAt == nil || row.Shadow.SingleBlockRetentionStartedAt == nil ||
		row.Shadow.SingleBlockDeleteAfter == nil || row.Shadow.SingleBlockObjectDeletedAt != nil ||
		req.Now.Before(*row.Shadow.SingleBlockDeleteAfter) {
		return false
	}
	retirementMatches := row.Retirement == nil || pendingRetirementMatchesCandidate(row, candidate)
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
		row.SingleBlockObjectKey == candidate.Key &&
		row.PrimaryObjectKey == candidate.ConsolidatedKey &&
		row.PrimaryObjectFormat == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH &&
		row.PrimaryByteOffset == candidate.ByteOffset &&
		row.PrimaryByteLength == candidate.ByteLength &&
		row.PrimaryUncompressedLength == candidate.UncompressedLength &&
		retirementMatches &&
		validShadowReference(row, row.Shadow)
}

func pendingVerificationCandidateMatchesRow(req PlanRequest, candidate Candidate, row MetadataRow, requireCanonical bool) bool {
	manifest := row.Retirement
	if manifest == nil || manifest.State != RetirementStateDeletedPendingVerification ||
		row.Shadow == nil || row.RetirementFencedAt == nil ||
		row.SingleBlockObjectKey != "" || row.Shadow.SingleBlockObjectKey != "" ||
		row.Shadow.ValidatedAt == nil || row.Shadow.SingleBlockRetentionStartedAt == nil ||
		row.Shadow.SingleBlockDeleteAfter == nil || row.Shadow.SingleBlockObjectDeletedAt == nil ||
		manifest.SingleBlockObjectKey != "" || manifest.SingleBlockObjectETag != "" ||
		manifest.DeletedAt == nil || manifest.VerifiedAt != nil ||
		!row.Shadow.SingleBlockObjectDeletedAt.Equal(*manifest.DeletedAt) {
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
		row.PrimaryObjectKey == candidate.ConsolidatedKey &&
		row.PrimaryObjectFormat == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH &&
		row.PrimaryByteOffset == candidate.ByteOffset &&
		row.PrimaryByteLength == candidate.ByteLength &&
		row.PrimaryUncompressedLength == candidate.UncompressedLength &&
		pendingRetirementMatchesCandidate(row, candidate) &&
		validShadowReference(row, row.Shadow)
}

func pendingRetirementMatchesCandidate(row MetadataRow, candidate Candidate) bool {
	manifest := row.Retirement
	if manifest == nil || row.RetirementFencedAt == nil ||
		manifest.State != RetirementStateEligible && manifest.State != RetirementStateDeleting && manifest.State != RetirementStateDeletedPendingVerification {
		return false
	}
	return manifest.BlockMetadataID == candidate.BlockMetadataID &&
		manifest.Tag == candidate.Tag &&
		manifest.Height == candidate.Height &&
		manifest.Hash == candidate.Hash &&
		manifest.Bucket == candidate.Bucket &&
		manifest.SingleBlockObjectKey == candidate.Key &&
		manifest.SingleBlockObjectKeySHA256 == candidate.SingleBlockKeySHA256 &&
		len(manifest.SingleBlockObjectVersionIDs) == 1 &&
		manifest.SingleBlockObjectVersionIDs[0] == candidate.VersionID &&
		manifest.SingleBlockObjectETag == candidate.SingleBlockETag &&
		manifest.SingleBlockObjectBytes == candidate.SingleBlockBytes &&
		manifest.ConsolidatedObjectKey == candidate.ConsolidatedKey &&
		manifest.ConsolidatedObjectVersionID == candidate.CSCBVersionID &&
		manifest.ConsolidatedObjectETag == candidate.CSCBETag &&
		manifest.ConsolidatedByteOffset == candidate.ByteOffset &&
		manifest.ConsolidatedByteLength == candidate.ByteLength &&
		manifest.ConsolidatedUncompressedLength == candidate.UncompressedLength &&
		manifest.PayloadSHA256 == candidate.PayloadSHA256
}

func pendingVerificationRetirementMatchesRow(row MetadataRow) bool {
	manifest := row.Retirement
	return manifest != nil &&
		manifest.State == RetirementStateDeletedPendingVerification &&
		manifest.SingleBlockObjectKey == "" &&
		manifest.SingleBlockObjectETag == "" &&
		manifest.DeletedAt != nil &&
		manifest.VerifiedAt == nil &&
		manifest.ClaimToken != "" &&
		manifest.ClaimExpiresAt != nil &&
		row.Shadow != nil &&
		row.RetirementFencedAt != nil &&
		row.SingleBlockObjectKey == "" &&
		row.Shadow.SingleBlockObjectKey == "" &&
		row.Shadow.SingleBlockObjectDeletedAt != nil &&
		row.Shadow.SingleBlockObjectDeletedAt.Equal(*manifest.DeletedAt) &&
		manifest.BlockMetadataID == row.BlockMetadataID &&
		manifest.Tag == row.Tag &&
		manifest.Height == row.Height &&
		manifest.Hash == row.Hash &&
		manifest.ConsolidatedObjectKey == row.PrimaryObjectKey &&
		manifest.ConsolidatedByteOffset == row.PrimaryByteOffset &&
		manifest.ConsolidatedByteLength == row.PrimaryByteLength &&
		manifest.ConsolidatedUncompressedLength == row.PrimaryUncompressedLength &&
		validShadowReference(row, row.Shadow)
}

func finalizedRetirementMatchesRow(row MetadataRow) bool {
	manifest := row.Retirement
	if manifest == nil || row.Shadow == nil || row.RetirementFencedAt == nil ||
		manifest.State != RetirementStateDeletedVerified ||
		manifest.SingleBlockObjectKey != "" || manifest.SingleBlockObjectETag != "" ||
		manifest.ClaimToken != "" || manifest.ClaimExpiresAt != nil ||
		manifest.DeletedAt == nil || manifest.VerifiedAt == nil || manifest.Outcome == "" ||
		row.SingleBlockObjectKey != "" || row.Shadow.SingleBlockObjectKey != "" ||
		row.Shadow.SingleBlockObjectDeletedAt == nil ||
		!row.Shadow.SingleBlockObjectDeletedAt.Equal(*manifest.DeletedAt) {
		return false
	}
	return manifest.BlockMetadataID == row.BlockMetadataID &&
		manifest.Tag == row.Tag &&
		manifest.Height == row.Height &&
		manifest.Hash == row.Hash &&
		manifest.ConsolidatedObjectKey == row.PrimaryObjectKey &&
		manifest.ConsolidatedByteOffset == row.PrimaryByteOffset &&
		manifest.ConsolidatedByteLength == row.PrimaryByteLength &&
		manifest.ConsolidatedUncompressedLength == row.PrimaryUncompressedLength &&
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
		report.SafetyGates.VersionedDeleteMode != "exact_single_object_version_only" ||
		report.SafetyGates.CSCBWriteOncePolicyMode != cscbWriteOncePolicyMode ||
		!report.SafetyGates.CSCBWriteOncePolicyVerified {
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
			item.SingleBlockKeySHA256 != keySHA256(item.Key) || !isImmutableVersionID(item.VersionID) || item.SingleBlockETag == "" ||
			item.SingleBlockBytes == 0 || item.SingleBlockVersions != 1 || item.DeleteMarkers != 0 ||
			item.ConsolidatedKey == "" || !isImmutableVersionID(item.CSCBVersionID) || item.CSCBETag == "" ||
			!item.CSCBWriteOncePolicy ||
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
		isImmutableVersionID(topology.Versions[0].VersionID) &&
		topology.Versions[0].VersionID == candidate.VersionID &&
		topology.Versions[0].ETag == candidate.SingleBlockETag &&
		topology.Versions[0].Bytes == candidate.SingleBlockBytes
}

func topologyMatchesManifest(topology ObjectVersionTopology, manifest RetirementManifest) bool {
	return len(manifest.SingleBlockObjectVersionIDs) == 1 &&
		isImmutableVersionID(manifest.SingleBlockObjectVersionIDs[0]) &&
		len(topology.Versions) == 1 &&
		len(topology.DeleteMarkers) == 0 &&
		topology.Versions[0].IsLatest &&
		topology.Versions[0].VersionID == manifest.SingleBlockObjectVersionIDs[0] &&
		topology.Versions[0].ETag == manifest.SingleBlockObjectETag &&
		topology.Versions[0].Bytes == manifest.SingleBlockObjectBytes
}

func isImmutableVersionID(versionID string) bool {
	return versionID != "" && versionID != s3MutableNullVersionID
}

func manifestFromCandidate(candidate Candidate, preparedAt time.Time) RetirementManifest {
	return RetirementManifest{
		BlockMetadataID:                candidate.BlockMetadataID,
		Tag:                            candidate.Tag,
		Height:                         candidate.Height,
		Hash:                           candidate.Hash,
		State:                          RetirementStateEligible,
		Bucket:                         candidate.Bucket,
		SingleBlockObjectKey:           candidate.Key,
		SingleBlockObjectKeySHA256:     keySHA256(candidate.Key),
		SingleBlockObjectVersionIDs:    []string{candidate.VersionID},
		SingleBlockObjectETag:          candidate.SingleBlockETag,
		SingleBlockObjectBytes:         candidate.SingleBlockBytes,
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
	candidate := Candidate{
		Bucket:               manifest.Bucket,
		Key:                  manifest.SingleBlockObjectKey,
		VersionID:            firstString(manifest.SingleBlockObjectVersionIDs),
		BlockMetadataID:      manifest.BlockMetadataID,
		Tag:                  manifest.Tag,
		Height:               manifest.Height,
		Hash:                 manifest.Hash,
		SingleBlockBytes:     manifest.SingleBlockObjectBytes,
		SingleBlockETag:      manifest.SingleBlockObjectETag,
		SingleBlockKeySHA256: manifest.SingleBlockObjectKeySHA256,
		SingleBlockVersions:  len(manifest.SingleBlockObjectVersionIDs),
		ConsolidatedKey:      manifest.ConsolidatedObjectKey,
		CSCBVersionID:        manifest.ConsolidatedObjectVersionID,
		CSCBETag:             manifest.ConsolidatedObjectETag,
		ByteOffset:           manifest.ConsolidatedByteOffset,
		ByteLength:           manifest.ConsolidatedByteLength,
		UncompressedLength:   manifest.ConsolidatedUncompressedLength,
		PayloadSHA256:        manifest.PayloadSHA256,
		RetirementState:      manifest.State,
		RetirementAttempts:   manifest.AttemptCount,
		RetirementOutcome:    manifest.Outcome,
		SingleBlockDeletedAt: manifest.DeletedAt,
		RetirementVerifiedAt: manifest.VerifiedAt,
		Action:               ActionDeleteObjectVersion,
	}
	if manifest.State == RetirementStateDeletedPendingVerification {
		candidate.Action = ActionDeletedObjectVersion
	}
	return candidate
}

func markCandidateBlocked(candidate *Candidate, reason string) {
	if candidate.RetirementState == RetirementStateDeletedPendingVerification {
		candidate.Action = ActionDeletedObjectVersion
	} else {
		candidate.Action = ActionSkip
	}
	candidate.SkipReason = reason
}

func newRetirementClaimToken() (string, error) {
	value := make([]byte, retirementClaimTokenBytes)
	if _, err := cryptorand.Read(value); err != nil {
		return "", xerrors.Errorf("failed to generate retirement claim token: %w", err)
	}
	return hex.EncodeToString(value), nil
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
			CSCBWriteOncePolicyMode: cscbWriteOncePolicyMode,
			ProductionDeleteEnabled: req.ProductionDeleteEnabled,
		},
		Items: make([]Candidate, 0),
	}
}

func writeOncePolicyGateVerified(items []Candidate) bool {
	eligible := false
	for _, item := range items {
		if item.Action != ActionReportOnly && item.Action != ActionDeleteObjectVersion &&
			item.Action != ActionDeletedObjectVersion && item.Action != ActionDeletedVerified {
			continue
		}
		eligible = true
		if !item.CSCBWriteOncePolicy {
			return false
		}
	}
	return eligible
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
	if shadow.SingleBlockObjectKey != row.SingleBlockObjectKey {
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
		summary.SingleBlockBytes += item.SingleBlockBytes
		if item.DeleteMarkers > 0 || item.SkipReason == SkipSingleBlockCurrentDeleteMarker {
			summary.DeleteMarkerRows++
		}
		switch item.Action {
		case ActionReportOnly, ActionDeleteObjectVersion:
			summary.EligibleRows++
			summary.EligibleBytes += item.SingleBlockBytes
		case ActionDeletedObjectVersion, ActionDeletedVerified, ActionAlreadyDeleted:
			summary.DeletedRows++
		default:
			summary.SkippedRows++
		}
	}
	return summary
}
