package retirement

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type fakeRepo struct {
	rows []MetadataRow
}

func (r *fakeRepo) ListMetadataRows(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]MetadataRow, error) {
	if limit > 0 && uint64(len(r.rows)) > limit {
		return r.rows[:limit], nil
	}
	return r.rows, nil
}

type fakeStore struct {
	heads        map[string]ObjectHead
	versions     map[string]ObjectVersion
	headCalls    map[string]int
	versionCalls map[string]int
	deleted      []Candidate
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		heads:        make(map[string]ObjectHead),
		versions:     make(map[string]ObjectVersion),
		headCalls:    make(map[string]int),
		versionCalls: make(map[string]int),
	}
}

func (s *fakeStore) HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error) {
	s.headCalls[key]++
	return s.heads[key], nil
}

func (s *fakeStore) CurrentObjectVersion(ctx context.Context, bucket string, key string) (ObjectVersion, error) {
	s.versionCalls[key]++
	return s.versions[key], nil
}

func (s *fakeStore) DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string) error {
	s.deleted = append(s.deleted, Candidate{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	})
	return nil
}

func TestPlannerPlan_NotEligibleGracePeriod(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-time.Hour)
	store := newFakeStore()
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	planner := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

	report, err := planner.Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipGracePeriodActive, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
	require.Zero(store.versionCalls[row.LegacyObjectKey])
}

func TestPlannerPlan_MissingCSCBObject(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/missing.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.versions[row.LegacyObjectKey] = ObjectVersion{Exists: true, VersionID: "legacy-v1", Bytes: 42}
	planner := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

	report, err := planner.Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipMissingCSCBObject, report.Items[0].SkipReason)
	require.Equal("legacy-v1", report.Items[0].VersionID)
	require.Equal(uint64(42), report.Items[0].LegacyBytes)
}

func TestPlannerPlan_InvalidMetadataReference(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	row.Shadow.LegacyObjectKey = "legacy/stale.zstd"
	store := newFakeStore()
	planner := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

	report, err := planner.Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipInvalidMetadataReference, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
}

func TestPlannerPlan_ActiveLegacyMetadataIsNeverRetired(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := activeLegacyTestRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.versions[row.LegacyObjectKey] = ObjectVersion{Exists: true, VersionID: "legacy-v1", Bytes: 42}
	store.heads[row.Shadow.ConsolidatedObjectKey] = ObjectHead{Exists: true, Bytes: 1024}

	report, err := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipActiveMetadataStillLegacy, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
	require.Zero(store.versionCalls[row.LegacyObjectKey])
}

func TestPlannerPlan_ValidationAndApprovalGates(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)

	t.Run("validation not passed", func(t *testing.T) {
		row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
		row.Shadow.ValidatedAt = nil
		store := newFakeStore()
		report, err := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
		require.NoError(err)
		require.Len(report.Items, 1)
		require.Equal(ActionSkip, report.Items[0].Action)
		require.Equal(SkipValidationNotPassed, report.Items[0].SkipReason)
		require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
		require.Zero(store.versionCalls[row.LegacyObjectKey])
	})

	t.Run("range not approved", func(t *testing.T) {
		row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
		store := newFakeStore()
		req := testRequest(now, false)
		req.Approval.EndHeight = req.EndHeight - 1
		report, err := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), req)
		require.NoError(err)
		require.Len(report.Items, 1)
		require.Equal(ActionSkip, report.Items[0].Action)
		require.Equal(SkipChainRangeNotApproved, report.Items[0].SkipReason)
		require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
		require.Zero(store.versionCalls[row.LegacyObjectKey])
	})
}

func TestPlannerPlan_FallbackAndClientMigrationGates(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)

	t.Run("fallback errors", func(t *testing.T) {
		req := testRequest(now, false)
		req.FallbackErrorCount = 1
		report, err := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, newFakeStore()).Plan(context.Background(), req)
		require.NoError(err)
		require.Equal(SkipActiveFallbackOrReadErrors, report.Items[0].SkipReason)
	})

	t.Run("file clients not approved", func(t *testing.T) {
		req := testRequest(now, false)
		req.ClientMigrationApproved = false
		report, err := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, newFakeStore()).Plan(context.Background(), req)
		require.NoError(err)
		require.Equal(SkipFileClientsNotApproved, report.Items[0].SkipReason)
	})
}

func TestPlannerPlan_SkippedReorgAndPromotedRows(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	skipped := testRow(100, "hash-100", "", "consolidated/canary.cscb.zstd", validatedAt)
	skipped.Skipped = true

	stale := testRow(101, "hash-101", "legacy/101.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	stale.Shadow.Hash = "hash-before-reorg"

	promoted := testRow(102, "hash-102", "consolidated/primary.cscb.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	promoted.PrimaryObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	promoted.PrimaryByteLength = 123
	promoted.PrimaryObjectKey = "consolidated/primary.cscb.zstd"

	report, err := NewPlanner(&fakeRepo{rows: []MetadataRow{skipped, stale, promoted}}, newFakeStore()).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 3)
	require.Equal(SkipSkippedBlock, report.Items[0].SkipReason)
	require.Equal(SkipInvalidMetadataReference, report.Items[1].SkipReason)
	require.Equal(SkipInvalidMetadataReference, report.Items[2].SkipReason)
}

func TestPlannerPlan_PromotedRowUsesShadowLegacyKeyAndRetireAfter(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-time.Hour)
	retiredAt := now.Add(-2 * time.Minute)
	retireAfter := now.Add(-time.Minute)
	legacyKey := "legacy/102.zstd"
	cscbKey := "consolidated/primary.cscb.zstd"
	row := testRow(102, "hash-102", legacyKey, cscbKey, validatedAt)
	row.Shadow.LegacyObjectRetiredAt = &retiredAt
	row.Shadow.LegacyObjectRetireAfter = &retireAfter
	store := newFakeStore()
	store.versions[legacyKey] = ObjectVersion{Exists: true, VersionID: "legacy-v1", Bytes: 42}
	store.heads[cscbKey] = ObjectHead{Exists: true, Bytes: 1024}

	report, err := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionReportOnly, report.Items[0].Action)
	require.Equal(legacyKey, report.Items[0].Key)
	require.Equal(cscbKey, report.Items[0].ConsolidatedKey)
	require.Equal(retiredAt, *report.Items[0].RetiredAt)
	require.Equal(retireAfter, *report.Items[0].EligibleAt)
	require.Empty(report.Items[0].SkipReason)
}

func TestPlannerPlan_VersionedDeleteMarkerBehavior(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.versions[row.LegacyObjectKey] = ObjectVersion{Exists: true, CurrentDeleteMarker: true, VersionID: "delete-marker-v1"}
	report, err := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipLegacyCurrentDeleteMarker, report.Items[0].SkipReason)
	require.Equal(1, report.Summary.DeleteMarkerRows)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
}

func TestPlannerPlan_DryRunOutput(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.versions[row.LegacyObjectKey] = ObjectVersion{Exists: true, VersionID: "legacy-v1", Bytes: 42}
	store.heads[row.Shadow.ConsolidatedObjectKey] = ObjectHead{Exists: true, Bytes: 1024}
	report, err := NewPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)

	var buf bytes.Buffer
	require.NoError(WriteReportJSON(&buf, report))
	var decoded Report
	require.NoError(json.Unmarshal(buf.Bytes(), &decoded))
	require.True(decoded.DryRun)
	require.Equal("production", decoded.Environment)
	require.Equal("solana", decoded.Blockchain)
	require.Equal("mainnet", decoded.Network)
	require.Equal(uint64(428058000), decoded.StartHeight)
	require.Equal(uint64(428059000), decoded.EndHeight)
	require.Equal(1, decoded.Summary.EligibleRows)
	require.Equal(ActionReportOnly, decoded.Items[0].Action)
	require.Equal("legacy-v1", decoded.Items[0].VersionID)
	require.Empty(decoded.Items[0].SkipReason)
}

func TestPlannerPlan_ExactSolanaCanaryReportShape(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	const (
		startHeight     = uint64(428058000)
		endHeight       = uint64(428059000)
		cscbKey         = "BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/consolidated/v=7/shard=00000000042805000000-00000000042806000000/00000000000428058000-00000000000428059000-deadbeef.cscb.zstd"
		bucket          = "co-chainstorage-solana-mainnet-prod"
		legacyBytesEach = uint64(123)
	)

	rows := make([]MetadataRow, 0, endHeight-startHeight)
	store := newFakeStore()
	store.heads[cscbKey] = ObjectHead{Exists: true, Bytes: 2 << 20}
	for height := startHeight; height < endHeight; height++ {
		hash := fmt.Sprintf("solana-hash-%d", height)
		key := fmt.Sprintf("BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/0/%d/%s.zstd", height, hash)
		rows = append(rows, testRow(height, hash, key, cscbKey, validatedAt))
		store.versions[key] = ObjectVersion{
			Exists:    true,
			VersionID: fmt.Sprintf("legacy-version-%d", height),
			Bytes:     legacyBytesEach,
		}
	}
	req := testRequest(now, false)
	req.Bucket = bucket
	req.StartHeight = startHeight
	req.EndHeight = endHeight
	report, err := NewPlanner(&fakeRepo{rows: rows}, store).Plan(context.Background(), req)
	require.NoError(err)

	require.Equal(bucket, report.Bucket)
	require.Equal(startHeight, report.StartHeight)
	require.Equal(endHeight, report.EndHeight)
	require.Len(report.Items, 1000)
	require.Equal(1000, report.Summary.TotalRows)
	require.Equal(1000, report.Summary.EligibleRows)
	require.Zero(report.Summary.SkippedRows)
	require.Equal(uint64(1000)*legacyBytesEach, report.Summary.LegacyBytes)
	require.Equal(uint64(1000)*legacyBytesEach, report.Summary.EligibleBytes)
	require.Equal(1, store.headCalls[cscbKey])

	first := report.Items[0]
	require.Equal(startHeight, first.Height)
	require.Equal("solana-hash-428058000", first.Hash)
	require.Equal("legacy-version-428058000", first.VersionID)
	require.Equal(legacyBytesEach, first.LegacyBytes)
	require.Equal(cscbKey, first.ConsolidatedKey)
	require.Equal(ActionReportOnly, first.Action)
	require.Empty(first.SkipReason)

	last := report.Items[len(report.Items)-1]
	require.Equal(endHeight-1, last.Height)
	require.Equal("solana-hash-428058999", last.Hash)
	require.Equal("legacy-version-428058999", last.VersionID)
	require.Equal(cscbKey, last.ConsolidatedKey)
	require.Equal(ActionReportOnly, last.Action)
}

func TestPlannerApply_ProductionDisabledAndNonProdDeletesVersionIDOnly(t *testing.T) {
	require := require.New(t)
	store := newFakeStore()
	planner := NewPlanner(&fakeRepo{}, store)
	report := &Report{Items: []Candidate{{
		Bucket:    "bucket",
		Key:       "legacy/100.zstd",
		VersionID: "legacy-v1",
		Action:    ActionDeleteObjectVersion,
	}}}

	err := planner.Apply(context.Background(), PlanRequest{Environment: "production"}, report)
	require.Error(err)
	require.Empty(store.deleted)

	err = planner.Apply(context.Background(), PlanRequest{Environment: "development"}, report)
	require.NoError(err)
	require.Len(store.deleted, 1)
	require.Equal("legacy-v1", store.deleted[0].VersionID)
	require.Equal(ActionDeletedObjectVersion, report.Items[0].Action)
}

func testRequest(now time.Time, execute bool) PlanRequest {
	return PlanRequest{
		Environment:             "production",
		Blockchain:              "solana",
		Network:                 "mainnet",
		Bucket:                  "co-chainstorage-solana-mainnet-prod",
		Tag:                     0,
		StartHeight:             428058000,
		EndHeight:               428059000,
		Now:                     now,
		GracePeriod:             7 * 24 * time.Hour,
		Execute:                 execute,
		ClientMigrationApproved: true,
		Approval: Approval{
			Chain:       "solana-mainnet",
			StartHeight: 428058000,
			EndHeight:   428059000,
		},
	}
}

func testRow(height uint64, hash string, legacyKey string, cscbKey string, validatedAt time.Time) MetadataRow {
	return MetadataRow{
		BlockMetadataID:           int64(height),
		Tag:                       0,
		Height:                    height,
		Hash:                      hash,
		PrimaryObjectKey:          cscbKey,
		LegacyObjectKey:           legacyKey,
		PrimaryObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		PrimaryByteOffset:         100,
		PrimaryByteLength:         200,
		PrimaryUncompressedLength: 300,
		Shadow: &ConsolidationShadow{
			Tag:                   0,
			Height:                height,
			Hash:                  hash,
			LegacyObjectKey:       legacyKey,
			ConsolidatedObjectKey: cscbKey,
			ObjectFormat:          api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:            100,
			ByteLength:            200,
			UncompressedLength:    300,
			ValidatedAt:           &validatedAt,
			FormatVersion:         1,
		},
	}
}

func activeLegacyTestRow(height uint64, hash string, legacyKey string, cscbKey string, validatedAt time.Time) MetadataRow {
	row := testRow(height, hash, legacyKey, cscbKey, validatedAt)
	row.PrimaryObjectKey = legacyKey
	row.PrimaryObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK
	row.PrimaryByteOffset = 0
	row.PrimaryByteLength = 0
	row.PrimaryUncompressedLength = 0
	return row
}
