package retirement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type fakeRepo struct {
	rows          []MetadataRow
	manifests     map[int64]RetirementManifest
	finalizeErr   error
	finalizeCalls int
}

func (r *fakeRepo) ListMetadataRows(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]MetadataRow, error) {
	if limit > 0 && uint64(len(r.rows)) > limit {
		return r.rows[:limit], nil
	}
	return r.rows, nil
}

func (r *fakeRepo) GetMetadataRow(ctx context.Context, blockMetadataID int64) (MetadataRow, error) {
	for _, row := range r.rows {
		if row.BlockMetadataID == blockMetadataID {
			return row, nil
		}
	}
	return MetadataRow{}, fmt.Errorf("metadata row not found: %d", blockMetadataID)
}

func (r *fakeRepo) PrepareRetirement(ctx context.Context, manifest RetirementManifest) error {
	if r.manifests == nil {
		r.manifests = make(map[int64]RetirementManifest)
	}
	if existing, ok := r.manifests[manifest.BlockMetadataID]; ok && !samePreparedManifest(existing, manifest) {
		return errors.New("manifest conflict")
	}
	r.manifests[manifest.BlockMetadataID] = manifest
	return nil
}

func (r *fakeRepo) MarkRetirementDeleting(ctx context.Context, blockMetadataID int64, startedAt time.Time) error {
	manifest, ok := r.manifests[blockMetadataID]
	if !ok {
		return errors.New("manifest not found")
	}
	if manifest.State != RetirementStateEligible && manifest.State != RetirementStateDeleting {
		return fmt.Errorf("invalid state: %s", manifest.State)
	}
	manifest.State = RetirementStateDeleting
	manifest.DeleteStartedAt = &startedAt
	manifest.LastAttemptAt = &startedAt
	manifest.AttemptCount++
	manifest.Outcome = "delete_started"
	r.manifests[blockMetadataID] = manifest
	return nil
}

func (r *fakeRepo) RecordRetirementOutcome(ctx context.Context, blockMetadataID int64, outcome string, attemptedAt time.Time) error {
	manifest, ok := r.manifests[blockMetadataID]
	if !ok || manifest.State == RetirementStateDeletedVerified {
		return nil
	}
	manifest.Outcome = outcome
	manifest.LastAttemptAt = &attemptedAt
	r.manifests[blockMetadataID] = manifest
	return nil
}

func (r *fakeRepo) FinalizeRetirement(ctx context.Context, blockMetadataID int64, deletedAt time.Time, outcome string) error {
	r.finalizeCalls++
	if r.finalizeErr != nil {
		return r.finalizeErr
	}
	manifest, ok := r.manifests[blockMetadataID]
	if !ok || manifest.State != RetirementStateDeleting {
		return errors.New("manifest is not deleting")
	}
	manifest.State = RetirementStateDeletedVerified
	manifest.LegacyObjectKey = ""
	manifest.LegacyObjectETag = ""
	manifest.DeletedAt = &deletedAt
	manifest.Outcome = outcome
	r.manifests[blockMetadataID] = manifest
	for i := range r.rows {
		if r.rows[i].BlockMetadataID != blockMetadataID {
			continue
		}
		r.rows[i].LegacyObjectKey = ""
		if r.rows[i].Shadow != nil {
			r.rows[i].Shadow.LegacyObjectKey = ""
			r.rows[i].Shadow.LegacyObjectDeletedAt = &deletedAt
		}
		r.rows[i].Retirement = &manifest
	}
	return nil
}

func (r *fakeRepo) ListPendingRetirements(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]RetirementManifest, error) {
	var manifests []RetirementManifest
	for _, manifest := range r.manifests {
		if manifest.Tag == tag && manifest.Height >= startHeight && manifest.Height < endHeight &&
			(manifest.State == RetirementStateEligible || manifest.State == RetirementStateDeleting) {
			manifests = append(manifests, manifest)
		}
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].Height < manifests[j].Height })
	if limit > 0 && uint64(len(manifests)) > limit {
		manifests = manifests[:limit]
	}
	return manifests, nil
}

type fakeStore struct {
	heads         map[string]ObjectHead
	versionHeads  map[string]ObjectHead
	topologies    map[string]ObjectVersionTopology
	objects       map[string][]byte
	headCalls     map[string]int
	versionCalls  map[string]int
	deleted       []Candidate
	deleteMutates bool
	listHook      func(key string, call int) ObjectVersionTopology
	headHook      func(key string, call int) ObjectHead
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		heads:        make(map[string]ObjectHead),
		versionHeads: make(map[string]ObjectHead),
		topologies:   make(map[string]ObjectVersionTopology),
		objects:      make(map[string][]byte),
		headCalls:    make(map[string]int),
		versionCalls: make(map[string]int),
	}
}

func (s *fakeStore) HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error) {
	s.headCalls[key]++
	if s.headHook != nil {
		return s.headHook(key, s.headCalls[key]), nil
	}
	return s.heads[key], nil
}

func (s *fakeStore) HeadObjectVersion(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error) {
	return s.versionHeads[versionObjectKey(key, versionID)], nil
}

func (s *fakeStore) ListObjectVersions(ctx context.Context, bucket string, key string) (ObjectVersionTopology, error) {
	s.versionCalls[key]++
	if s.listHook != nil {
		return s.listHook(key, s.versionCalls[key]), nil
	}
	return s.topologies[key], nil
}

func (s *fakeStore) ReadObjectVersion(ctx context.Context, bucket string, key string, versionID string) ([]byte, error) {
	return append([]byte(nil), s.objects[versionObjectKey(key, versionID)]...), nil
}

func (s *fakeStore) ReadObjectVersionRange(ctx context.Context, bucket string, key string, versionID string, offset uint64, length uint64) ([]byte, error) {
	data := s.objects[versionObjectKey(key, versionID)]
	if offset >= uint64(len(data)) {
		return nil, errors.New("range starts after object")
	}
	end := offset + length
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	return append([]byte(nil), data[offset:end]...), nil
}

func (s *fakeStore) DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string) error {
	s.deleted = append(s.deleted, Candidate{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	})
	if s.deleteMutates {
		delete(s.topologies, key)
		delete(s.heads, key)
	}
	return nil
}

type fakePayloadVerifier struct {
	digest             string
	consolidatedDigest string
	verifyErr          error
	consolidatedErr    error
	verifyCalls        int
	consolidatedCalls  int
	verifyHook         func(call int) (string, error)
}

func (v *fakePayloadVerifier) Verify(ctx context.Context, candidate Candidate) (string, error) {
	v.verifyCalls++
	if v.verifyHook != nil {
		return v.verifyHook(v.verifyCalls)
	}
	return v.digest, v.verifyErr
}

func (v *fakePayloadVerifier) VerifyConsolidated(ctx context.Context, candidate Candidate) (string, error) {
	v.consolidatedCalls++
	return v.consolidatedDigest, v.consolidatedErr
}

func newTestPlanner(repo *fakeRepo, store *fakeStore) (*Planner, *fakePayloadVerifier) {
	digest := strings.Repeat("a", sha256HexLength)
	verifier := &fakePayloadVerifier{digest: digest, consolidatedDigest: digest}
	planner := &Planner{repo: repo, store: store}
	planner.verifierFactory = func() blockPayloadVerifier { return verifier }
	return planner, verifier
}

func testPlanner(repo *fakeRepo, store *fakeStore) *Planner {
	planner, _ := newTestPlanner(repo, store)
	return planner
}

func versionObjectKey(key string, versionID string) string {
	return key + "\x00" + versionID
}

func safeTopology(versionID string, etag string, bytes uint64) ObjectVersionTopology {
	return ObjectVersionTopology{Versions: []ObjectVersion{{
		VersionID: versionID,
		ETag:      etag,
		Bytes:     bytes,
		IsLatest:  true,
	}}}
}

func TestPlannerPlan_NotEligibleRetentionPeriod(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-time.Hour)
	store := newFakeStore()
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	planner := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

	report, err := planner.Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipRetentionPeriodActive, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
	require.Zero(store.versionCalls[row.LegacyObjectKey])
}

func TestPlannerPlan_MissingRetirementMarkerIsNeverEligible(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-30 * 24 * time.Hour)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	row.Shadow.LegacyObjectRetiredAt = nil
	row.Shadow.LegacyObjectRetireAfter = nil
	store := newFakeStore()

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipMissingRetirementMarker, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
	require.Zero(store.versionCalls[row.LegacyObjectKey])
}

func TestPlannerPlan_MissingCSCBObject(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/missing.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.topologies[row.LegacyObjectKey] = safeTopology("legacy-v1", "legacy-etag", 42)
	planner := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

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
	planner := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

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
	store.topologies[row.LegacyObjectKey] = safeTopology("legacy-v1", "legacy-etag", 42)
	store.heads[row.Shadow.ConsolidatedObjectKey] = cscbObjectHead(1024, 1024)

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
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
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
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
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), req)
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
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, newFakeStore()).Plan(context.Background(), req)
		require.NoError(err)
		require.Equal(SkipActiveFallbackOrReadErrors, report.Items[0].SkipReason)
	})

	t.Run("file clients not approved", func(t *testing.T) {
		req := testRequest(now, false)
		req.ClientMigrationApproved = false
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, newFakeStore()).Plan(context.Background(), req)
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

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{skipped, stale, promoted}}, newFakeStore()).Plan(context.Background(), testRequest(now, false))
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
	store.topologies[legacyKey] = safeTopology("legacy-v1", "legacy-etag", 42)
	store.heads[cscbKey] = cscbObjectHead(1024, 1024)

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
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
	store.topologies[row.LegacyObjectKey] = ObjectVersionTopology{DeleteMarkers: []ObjectDeleteMarker{{
		VersionID: "delete-marker-v1",
		IsLatest:  true,
	}}}
	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipUnsafeLegacyVersionTopology, report.Items[0].SkipReason)
	require.Equal(1, report.Summary.DeleteMarkerRows)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
}

func TestPlannerPlan_RejectsNonSingletonLegacyTopology(t *testing.T) {
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	for _, test := range []struct {
		name     string
		topology ObjectVersionTopology
	}{
		{
			name: "older version exists",
			topology: ObjectVersionTopology{Versions: []ObjectVersion{
				{VersionID: "legacy-v2", ETag: "etag-v2", Bytes: 42, IsLatest: true},
				{VersionID: "legacy-v1", ETag: "etag-v1", Bytes: 41, IsLatest: false},
			}},
		},
		{
			name: "single version is not latest",
			topology: ObjectVersionTopology{Versions: []ObjectVersion{
				{VersionID: "legacy-v1", ETag: "etag-v1", Bytes: 42, IsLatest: false},
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
			store := newFakeStore()
			store.topologies[row.LegacyObjectKey] = test.topology
			report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
			require.NoError(err)
			require.Equal(ActionSkip, report.Items[0].Action)
			require.Equal(SkipUnsafeLegacyVersionTopology, report.Items[0].SkipReason)
			require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
		})
	}
}

func TestPlannerPlan_RequiresIndependentPayloadComparison(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	store := newFakeStore()
	store.topologies[row.LegacyObjectKey] = safeTopology("legacy-v1", "legacy-etag", 42)
	store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)
	planner, verifier := newTestPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)
	verifier.verifyErr = errors.New("payloads differ")

	report, err := planner.Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipLegacyPayloadMismatch, report.Items[0].SkipReason)
	require.Empty(report.Items[0].PayloadSHA256)
}

func TestPlannerPlan_DryRunOutput(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.topologies[row.LegacyObjectKey] = safeTopology("legacy-v1", "legacy-etag", 42)
	store.heads[row.Shadow.ConsolidatedObjectKey] = cscbObjectHead(1024, 1024)
	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
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
	store.heads[cscbKey] = cscbObjectHead(512<<10, 2<<20)
	for height := startHeight; height < endHeight; height++ {
		hash := fmt.Sprintf("solana-hash-%d", height)
		key := fmt.Sprintf("BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/0/%d/%s.zstd", height, hash)
		rows = append(rows, testRow(height, hash, key, cscbKey, validatedAt))
		store.topologies[key] = safeTopology(
			fmt.Sprintf("legacy-version-%d", height),
			fmt.Sprintf("legacy-etag-%d", height),
			legacyBytesEach,
		)
	}
	req := testRequest(now, false)
	req.Bucket = bucket
	req.StartHeight = startHeight
	req.EndHeight = endHeight
	report, err := testPlanner(&fakeRepo{rows: rows}, store).Plan(context.Background(), req)
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

func TestPlannerApply_RequiresProductionGateAndFinalizesMetadata(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(428058000, "hash-428058000", "legacy/428058000.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{row}}
	store := newFakeStore()
	store.deleteMutates = true
	store.topologies[row.LegacyObjectKey] = safeTopology("legacy-v1", "legacy-etag", 42)
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[row.PrimaryObjectKey] = cscbHead
	store.versionHeads[versionObjectKey(row.PrimaryObjectKey, cscbHead.VersionID)] = cscbHead
	planner := testPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Equal(ActionDeleteObjectVersion, report.Items[0].Action)

	blockedReq := req
	blockedReq.ProductionDeleteEnabled = false
	err = planner.Apply(context.Background(), blockedReq, report)
	require.Error(err)
	require.Empty(store.deleted)

	err = planner.Apply(context.Background(), req, report)
	require.NoError(err)
	require.Len(store.deleted, 1)
	require.Equal("legacy-v1", store.deleted[0].VersionID)
	require.Equal(ActionDeletedVerified, report.Items[0].Action)
	require.Empty(report.Items[0].Key)
	manifest := repo.manifests[row.BlockMetadataID]
	require.Equal(RetirementStateDeletedVerified, manifest.State)
	require.Empty(manifest.LegacyObjectKey)
	require.NotNil(manifest.DeletedAt)
	require.Empty(repo.rows[0].LegacyObjectKey)
	require.Empty(repo.rows[0].Shadow.LegacyObjectKey)
	require.NotNil(repo.rows[0].Shadow.LegacyObjectDeletedAt)

	dryRunReq := req
	dryRunReq.Execute = false
	dryRunReq.ProductionDeleteEnabled = false
	finalReport, err := planner.Plan(context.Background(), dryRunReq)
	require.NoError(err)
	require.Equal(ActionAlreadyDeleted, finalReport.Items[0].Action)
	require.Empty(finalReport.Items[0].Key)
	require.Equal(keySHA256(row.LegacyObjectKey), manifest.LegacyObjectKeySHA256)
}

func TestPlannerApply_RejectsTamperedReportBeforeManifestWrite(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(req PlanRequest, report *Report)
	}{
		{
			name: "legacy key",
			mutate: func(req PlanRequest, report *Report) {
				report.Items[0].Key = "legacy/other.zstd"
			},
		},
		{
			name: "height outside approval",
			mutate: func(req PlanRequest, report *Report) {
				report.Items[0].Height = req.EndHeight
			},
		},
		{
			name: "dry-run report",
			mutate: func(req PlanRequest, report *Report) {
				report.DryRun = true
			},
		},
		{
			name: "production gate mismatch",
			mutate: func(req PlanRequest, report *Report) {
				report.SafetyGates.ProductionDeleteEnabled = false
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
			test.mutate(req, report)
			err := planner.Apply(context.Background(), req, report)
			require.Error(err)
			require.Empty(repo.manifests)
			require.Empty(store.deleted)
		})
	}
}

func TestPlannerApply_RevalidatesTopologyAfterManifestPersistence(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	safe := store.topologies[report.Items[0].Key]
	store.listHook = func(key string, call int) ObjectVersionTopology {
		if call < 3 {
			return safe
		}
		return ObjectVersionTopology{Versions: []ObjectVersion{
			safe.Versions[0],
			{VersionID: "unexpected-v2", ETag: "unexpected-etag", Bytes: 1, IsLatest: false},
		}}
	}

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Empty(store.deleted)
	require.Equal(SkipUnsafeLegacyVersionTopology, report.Items[0].SkipReason)
	manifest := repo.manifests[report.Items[0].BlockMetadataID]
	require.Equal(RetirementStateDeleting, manifest.State)
	require.NotEmpty(manifest.LegacyObjectKey)
	require.Equal(1, manifest.AttemptCount)
	require.Equal(SkipUnsafeLegacyVersionTopology, manifest.Outcome)
}

func TestPlannerApply_StopsAfterFirstFailure(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	first := testRow(428058000, "hash-428058000", "legacy/428058000.zstd", "consolidated/first.cscb.zstd", now.Add(-8*24*time.Hour))
	second := testRow(428058001, "hash-428058001", "legacy/428058001.zstd", "consolidated/second.cscb.zstd", now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{first, second}}
	store := newFakeStore()
	store.deleteMutates = true
	for _, row := range repo.rows {
		store.topologies[row.LegacyObjectKey] = safeTopology("legacy-v1", "legacy-etag", 42)
		head := cscbObjectHead(1024, 1024)
		store.heads[row.PrimaryObjectKey] = head
		store.versionHeads[versionObjectKey(row.PrimaryObjectKey, head.VersionID)] = head
	}
	planner := testPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Len(report.Items, 2)

	firstSafe := store.topologies[first.LegacyObjectKey]
	store.listHook = func(key string, call int) ObjectVersionTopology {
		if key != first.LegacyObjectKey || call < 3 {
			return store.topologies[key]
		}
		return ObjectVersionTopology{Versions: []ObjectVersion{
			firstSafe.Versions[0],
			{VersionID: "unexpected-v2", ETag: "unexpected-etag", Bytes: 1},
		}}
	}

	err = planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Empty(store.deleted)
	require.Equal(SkipUnsafeLegacyVersionTopology, report.Items[0].SkipReason)
	require.Equal(SkipNotAttemptedAfterFailure, report.Items[1].SkipReason)
	_, secondManifestExists := repo.manifests[second.BlockMetadataID]
	require.False(secondManifestExists)
}

func TestPlannerApply_DoesNotFinalizeWhenCSCBChangesAfterLegacyDelete(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	store.headHook = func(key string, call int) ObjectHead {
		head := store.heads[key]
		if key == row.PrimaryObjectKey && call >= 4 {
			head.VersionID = "replacement-version"
			head.ETag = "replacement-etag"
		}
		return head
	}

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Len(store.deleted, 1)
	require.Equal(SkipCSCBObjectChanged, report.Items[0].SkipReason)
	require.Equal(RetirementStateDeleting, repo.manifests[row.BlockMetadataID].State)
	require.Equal(row.LegacyObjectKey, repo.rows[0].LegacyObjectKey)
	require.Zero(repo.finalizeCalls)
}

func TestPlannerApply_RejectsBlockThatIsNoLongerCanonical(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	repo.rows[0].Canonical = false

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Equal(SkipMetadataChanged, report.Items[0].SkipReason)
	require.Empty(repo.manifests)
	require.Empty(store.deleted)
}

func TestPlannerReconcile_CompletesCrashAfterS3Deletion(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, verifier, req, report := newExecutableTestFixture(t)
	repo.finalizeErr = errors.New("database unavailable")

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Equal(SkipPostDeleteVerificationFailed, report.Items[0].SkipReason)
	require.Len(store.deleted, 1)
	require.Equal(RetirementStateDeleting, repo.manifests[row.BlockMetadataID].State)
	require.Equal(SkipPostDeleteVerificationFailed, repo.manifests[row.BlockMetadataID].Outcome)
	require.Equal(row.LegacyObjectKey, repo.rows[0].LegacyObjectKey)

	repo.finalizeErr = nil
	repo.rows[0].Canonical = false
	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.NoError(err)
	require.Len(reconcileReport.Items, 1)
	require.Equal(ActionDeletedVerified, reconcileReport.Items[0].Action)
	require.Equal(1, reconcileReport.Summary.DeletedRows)
	require.Len(store.deleted, 1)
	require.Equal(2, verifier.consolidatedCalls)
	require.Equal(RetirementStateDeletedVerified, repo.manifests[row.BlockMetadataID].State)
	require.Empty(repo.rows[0].LegacyObjectKey)
	require.Empty(repo.rows[0].Shadow.LegacyObjectKey)
	require.NotNil(repo.rows[0].Shadow.LegacyObjectDeletedAt)
}

func TestPlannerReconcile_DoesNotFinalizeExternallyMissingEligibleObject(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	manifest := manifestFromCandidate(report.Items[0], req.Now)
	require.NoError(repo.PrepareRetirement(context.Background(), manifest))
	delete(store.topologies, row.LegacyObjectKey)

	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.Error(err)
	require.Len(reconcileReport.Items, 1)
	require.Equal(ActionSkip, reconcileReport.Items[0].Action)
	require.Equal(SkipMetadataChanged, reconcileReport.Items[0].SkipReason)
	require.Equal(RetirementStateEligible, repo.manifests[row.BlockMetadataID].State)
	require.Equal(row.LegacyObjectKey, repo.rows[0].LegacyObjectKey)
	require.Zero(repo.finalizeCalls)
}

func TestPlannerPlan_UsesCSCBUncompressedLengthForPlacementBounds(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	row.PrimaryByteOffset = 900
	row.PrimaryByteLength = 100
	row.Shadow.ByteOffset = 900
	row.Shadow.ByteLength = 100
	store := newFakeStore()
	store.topologies[row.LegacyObjectKey] = safeTopology("legacy-v1", "legacy-etag", 42)
	store.heads[row.Shadow.ConsolidatedObjectKey] = cscbObjectHead(100, 1000)

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(ActionReportOnly, report.Items[0].Action)
	require.Empty(report.Items[0].SkipReason)
}

func TestPlannerPlan_InvalidCSCBUncompressedLengthFailsClosed(t *testing.T) {
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)

	for _, test := range []struct {
		name            string
		compressedBytes uint64
		metadata        map[string]string
	}{
		{name: "missing", compressedBytes: 1024, metadata: nil},
		{name: "zero compressed bytes", compressedBytes: 0, metadata: cscbObjectMetadata("100", cscbFormatMetadataValue, cscbCompressionScopeMetadataValue)},
		{name: "wrong format", compressedBytes: 1024, metadata: cscbObjectMetadata("100", "not-cscb", cscbCompressionScopeMetadataValue)},
		{name: "wrong compression scope", compressedBytes: 1024, metadata: cscbObjectMetadata("100", cscbFormatMetadataValue, "whole-object")},
		{name: "malformed length", compressedBytes: 1024, metadata: cscbObjectMetadata("invalid", cscbFormatMetadataValue, cscbCompressionScopeMetadataValue)},
		{name: "zero length", compressedBytes: 1024, metadata: cscbObjectMetadata("0", cscbFormatMetadataValue, cscbCompressionScopeMetadataValue)},
	} {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			row := testRow(100, "hash-100", "legacy/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
			store := newFakeStore()
			store.topologies[row.LegacyObjectKey] = safeTopology("legacy-v1", "legacy-etag", 42)
			store.heads[row.Shadow.ConsolidatedObjectKey] = ObjectHead{
				Exists:    true,
				Bytes:     test.compressedBytes,
				VersionID: "cscb-v1",
				ETag:      "cscb-etag",
				Metadata:  test.metadata,
			}

			report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
			require.NoError(err)
			require.Equal(ActionSkip, report.Items[0].Action)
			require.Equal(SkipInvalidMetadataReference, report.Items[0].SkipReason)
		})
	}
}

func newExecutableTestFixture(t *testing.T) (
	MetadataRow,
	*fakeRepo,
	*fakeStore,
	*Planner,
	*fakePayloadVerifier,
	PlanRequest,
	*Report,
) {
	t.Helper()
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(
		428058000,
		"hash-428058000",
		"legacy/428058000.zstd",
		"consolidated/canary.cscb.zstd",
		now.Add(-8*24*time.Hour),
	)
	repo := &fakeRepo{rows: []MetadataRow{row}}
	store := newFakeStore()
	store.deleteMutates = true
	store.topologies[row.LegacyObjectKey] = safeTopology("legacy-v1", "legacy-etag", 42)
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[row.PrimaryObjectKey] = cscbHead
	store.versionHeads[versionObjectKey(row.PrimaryObjectKey, cscbHead.VersionID)] = cscbHead
	planner, verifier := newTestPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionDeleteObjectVersion, report.Items[0].Action)
	return row, repo, store, planner, verifier, req, report
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
	retiredAt := validatedAt
	retireAfter := validatedAt.Add(72 * time.Hour)
	return MetadataRow{
		BlockMetadataID:           int64(height),
		Canonical:                 true,
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
			Tag:                     0,
			Height:                  height,
			Hash:                    hash,
			LegacyObjectKey:         legacyKey,
			ConsolidatedObjectKey:   cscbKey,
			ObjectFormat:            api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:              100,
			ByteLength:              200,
			UncompressedLength:      300,
			ValidatedAt:             &validatedAt,
			LegacyObjectRetiredAt:   &retiredAt,
			LegacyObjectRetireAfter: &retireAfter,
			FormatVersion:           1,
		},
	}
}

func cscbObjectHead(compressedBytes uint64, uncompressedBytes uint64) ObjectHead {
	return ObjectHead{
		Exists:    true,
		Bytes:     compressedBytes,
		VersionID: "cscb-v1",
		ETag:      "cscb-etag",
		Metadata: cscbObjectMetadata(
			fmt.Sprintf("%d", uncompressedBytes),
			cscbFormatMetadataValue,
			cscbCompressionScopeMetadataValue,
		),
	}
}

func cscbObjectMetadata(uncompressedLength string, format string, compressionScope string) map[string]string {
	return map[string]string{
		cscbFormatMetadataKey:             format,
		cscbCompressionScopeMetadataKey:   compressionScope,
		cscbUncompressedLengthMetadataKey: uncompressedLength,
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
