package retirement

import (
	"context"
	"strconv"
	"strings"
	"time"

	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	cscbFormatMetadataKey             = "chainstorage-format"
	cscbCompressionScopeMetadataKey   = "chainstorage-compression-scope"
	cscbUncompressedLengthMetadataKey = "chainstorage-uncompressed-length"
	cscbFormatMetadataValue           = "cscb"
	cscbCompressionScopeMetadataValue = "batch-chunked"
)

type Planner struct {
	repo  Repository
	store ObjectStore
}

func NewPlanner(repo Repository, store ObjectStore) *Planner {
	return &Planner{
		repo:  repo,
		store: store,
	}
}

func (p *Planner) Plan(ctx context.Context, req PlanRequest) (*Report, error) {
	if req.EndHeight <= req.StartHeight {
		return nil, xerrors.Errorf("end height must be greater than start height: start=%d end=%d", req.StartHeight, req.EndHeight)
	}
	if req.Bucket == "" {
		return nil, xerrors.New("bucket is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	rows, err := p.repo.ListMetadataRows(ctx, req.Tag, req.StartHeight, req.EndHeight, req.Limit)
	if err != nil {
		return nil, err
	}

	report := &Report{
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
			VersionedDeleteMode:     "explicit_object_version_only",
			ProductionDeleteEnabled: false,
		},
		Items: make([]Candidate, 0, len(rows)),
	}

	cscbHeads := make(map[string]ObjectHead)
	legacyVersions := make(map[string]ObjectVersion)
	for _, row := range rows {
		item := p.planRow(ctx, req, row, cscbHeads, legacyVersions)
		report.Items = append(report.Items, item)
	}
	report.Summary = summarize(report.Items)
	return report, nil
}

func (p *Planner) Apply(ctx context.Context, req PlanRequest, report *Report) error {
	if report == nil {
		return xerrors.New("report is required")
	}
	if isProduction(req.Environment) {
		return xerrors.New("production deletion is disabled; run dry-run/report only")
	}
	for i := range report.Items {
		item := &report.Items[i]
		if item.Action != ActionDeleteObjectVersion {
			continue
		}
		if item.VersionID == "" {
			item.Action = ActionSkip
			item.SkipReason = SkipLegacyVersionIDUnavailable
			continue
		}
		if err := p.store.DeleteObjectVersion(ctx, item.Bucket, item.Key, item.VersionID); err != nil {
			item.Action = ActionSkip
			item.SkipReason = SkipVersionedDeleteFailed
			return xerrors.Errorf("failed to delete object version (bucket=%s key=%s version_id=%s): %w", item.Bucket, item.Key, item.VersionID, err)
		}
		item.Action = ActionDeletedObjectVersion
	}
	report.Summary = summarize(report.Items)
	return nil
}

func (p *Planner) planRow(
	ctx context.Context,
	req PlanRequest,
	row MetadataRow,
	cscbHeads map[string]ObjectHead,
	legacyVersions map[string]ObjectVersion,
) Candidate {
	item := Candidate{
		Bucket: req.Bucket,
		Key:    row.LegacyObjectKey,
		Height: row.Height,
		Hash:   row.Hash,
		Action: ActionSkip,
	}

	if row.Skipped {
		item.SkipReason = SkipSkippedBlock
		return item
	}
	if row.LegacyObjectKey == "" {
		item.SkipReason = SkipMissingLegacyKey
		return item
	}
	if row.Shadow == nil {
		item.SkipReason = SkipMissingConsolidationShadow
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

	version, ok := legacyVersions[row.LegacyObjectKey]
	if !ok {
		var err error
		version, err = p.store.CurrentObjectVersion(ctx, req.Bucket, row.LegacyObjectKey)
		if err != nil {
			item.SkipReason = SkipObjectInspectionFailed
			return item
		}
		legacyVersions[row.LegacyObjectKey] = version
	}
	if !version.Exists {
		item.SkipReason = SkipLegacyObjectMissing
		return item
	}
	if version.CurrentDeleteMarker {
		item.SkipReason = SkipLegacyCurrentDeleteMarker
		return item
	}
	item.VersionID = version.VersionID
	item.LegacyBytes = version.Bytes
	if item.VersionID == "" {
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
	if !head.Exists || head.DeleteMark {
		item.SkipReason = SkipMissingCSCBObject
		return item
	}
	// CSCB placements address the logical payload, not the compressed S3 object.
	logicalBytes, ok := cscbLogicalPayloadBytes(head)
	if head.Bytes == 0 || !ok || shadow.ByteOffset > logicalBytes || shadow.ByteLength > logicalBytes-shadow.ByteOffset {
		item.SkipReason = SkipInvalidMetadataReference
		return item
	}

	if req.Execute {
		if isProduction(req.Environment) {
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
	if shadow.ByteLength == 0 {
		return false
	}
	return true
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
		if item.SkipReason == SkipLegacyCurrentDeleteMarker {
			summary.DeleteMarkerRows++
		}
		if item.Action == ActionReportOnly || item.Action == ActionDeleteObjectVersion || item.Action == ActionDeletedObjectVersion {
			summary.EligibleRows++
			summary.EligibleBytes += item.LegacyBytes
			continue
		}
		summary.SkippedRows++
	}
	return summary
}
