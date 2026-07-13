package retirement

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestIntegrationPostgresRepositoryRetirementStateMachine(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retirement integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_000_000_000 + unique%100_000_000)
	height := uint64(8_000_000_000 + unique%100_000_000)
	// Exercise the complete lifecycle for SQL NULL hashes. Repository reads
	// normalize NULL to an empty string, while writes use NULLIF consistently.
	hash := ""
	legacyKey := fmt.Sprintf("legacy/%d.gzip", height)
	cscbKey := fmt.Sprintf("consolidated/%d.cscb.gzip", height)
	validatedAt := time.Now().UTC().Add(-96 * time.Hour)
	retiredAt := validatedAt
	retireAfter := retiredAt.Add(72 * time.Hour)
	var blockMetadataID int64
	err = db.QueryRowContext(ctx, `
		INSERT INTO block_metadata (
			height, tag, hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length
		) VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6, FALSE, $7, $8, $9, $10)
		RETURNING id`,
		height,
		tag,
		hash,
		height-1,
		cscbKey,
		time.Now().UTC().Unix(),
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		0,
		128,
		128,
	).Scan(&blockMetadataID)
	require.NoError(err)
	defer func() {
		_, _ = db.ExecContext(ctx, `ALTER TABLE block_legacy_object_retirement DISABLE TRIGGER block_legacy_object_retirement_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_legacy_object_retirement WHERE block_metadata_id = $1`, blockMetadataID)
		_, _ = db.ExecContext(ctx, `ALTER TABLE block_legacy_object_retirement ENABLE TRIGGER block_legacy_object_retirement_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE block_metadata_id = $1`, blockMetadataID)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE block_metadata_id = $1`, blockMetadataID)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, blockMetadataID)
	}()
	_, err = db.ExecContext(ctx, `
		INSERT INTO canonical_blocks (height, block_metadata_id, tag)
		VALUES ($1, $2, $3)`, height, blockMetadataID, tag)
	require.NoError(err)
	_, err = db.ExecContext(ctx, `
		INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, legacy_object_key_main,
			consolidated_object_key_main, object_format, byte_offset, byte_length,
			uncompressed_length, validated_at, legacy_object_retired_at, legacy_object_retire_after
		) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		blockMetadataID,
		tag,
		height,
		hash,
		legacyKey,
		cscbKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		0,
		128,
		128,
		validatedAt,
		retiredAt,
		retireAfter,
	)
	require.NoError(err)

	repo := NewPostgresRepository(db)
	preparedAt := time.Now().UTC().Add(7 * 24 * time.Hour)
	manifest := RetirementManifest{
		BlockMetadataID:                blockMetadataID,
		Tag:                            tag,
		Height:                         height,
		Hash:                           hash,
		State:                          RetirementStateEligible,
		Bucket:                         "integration-bucket",
		LegacyObjectKey:                legacyKey,
		LegacyObjectKeySHA256:          keySHA256(legacyKey),
		LegacyObjectVersionIDs:         []string{"legacy-v1"},
		LegacyObjectETag:               "legacy-etag",
		LegacyObjectBytes:              256,
		ConsolidatedObjectKey:          cscbKey,
		ConsolidatedObjectVersionID:    "cscb-v1",
		ConsolidatedObjectETag:         "cscb-etag",
		ConsolidatedByteOffset:         0,
		ConsolidatedByteLength:         128,
		ConsolidatedUncompressedLength: 128,
		PayloadSHA256:                  keySHA256("payload"),
		PreparedAt:                     preparedAt,
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO block_legacy_object_retirement (
			block_metadata_id, tag, height, hash, state, bucket,
			legacy_object_key_main, legacy_object_key_sha256, legacy_object_version_ids,
			legacy_object_etag, legacy_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, prepared_at
		) VALUES ($1, $2, $3, NULLIF($4, ''), 'deleted_verified', $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, CURRENT_TIMESTAMP)`,
		blockMetadataID, tag, height, hash, manifest.Bucket, legacyKey, manifest.LegacyObjectKeySHA256,
		pq.Array(manifest.LegacyObjectVersionIDs), manifest.LegacyObjectETag, manifest.LegacyObjectBytes,
		cscbKey, manifest.ConsolidatedObjectVersionID, manifest.ConsolidatedObjectETag,
		manifest.ConsolidatedByteOffset, manifest.ConsolidatedByteLength, manifest.ConsolidatedUncompressedLength,
		manifest.PayloadSHA256,
	)
	require.Error(err)
	require.Contains(err.Error(), "must be inserted in eligible state")
	_, err = db.ExecContext(ctx, `UPDATE block_consolidation_shadow SET legacy_object_retire_after = CURRENT_TIMESTAMP + INTERVAL '1 hour' WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	err = repo.PrepareRetirement(ctx, manifest)
	require.Error(err)
	require.Contains(err.Error(), "failed to lock canonical retirement metadata")
	_, err = db.ExecContext(ctx, `UPDATE block_consolidation_shadow SET legacy_object_retire_after = $2 WHERE block_metadata_id = $1`, blockMetadataID, retireAfter)
	require.NoError(err)
	require.NoError(repo.PrepareRetirement(ctx, manifest))
	_, err = db.ExecContext(ctx, `DELETE FROM block_legacy_object_retirement WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "audit manifests cannot be deleted")
	_, err = db.ExecContext(ctx, `
		INSERT INTO block_legacy_object_retirement (
			block_metadata_id, tag, height, hash, state, bucket,
			legacy_object_key_main, legacy_object_key_sha256, legacy_object_version_ids,
			legacy_object_etag, legacy_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, prepared_at
		) VALUES ($1, $2, $3, NULLIF($4, ''), 'eligible', $5, $6, $7, ARRAY['null'], $8, $9,
			$10, $11, $12, $13, $14, $15, $16, CURRENT_TIMESTAMP)
		ON CONFLICT (block_metadata_id) DO NOTHING`,
		blockMetadataID, tag, height, hash, manifest.Bucket, legacyKey, manifest.LegacyObjectKeySHA256,
		manifest.LegacyObjectETag, manifest.LegacyObjectBytes, cscbKey, manifest.ConsolidatedObjectVersionID,
		manifest.ConsolidatedObjectETag, manifest.ConsolidatedByteOffset, manifest.ConsolidatedByteLength,
		manifest.ConsolidatedUncompressedLength, manifest.PayloadSHA256,
	)
	require.Error(err)
	require.Contains(err.Error(), "block_legacy_object_retirement_immutable_version_ids_check")
	_, err = db.ExecContext(ctx, `
		INSERT INTO block_legacy_object_retirement (
			block_metadata_id, tag, height, hash, state, bucket,
			legacy_object_key_main, legacy_object_key_sha256, legacy_object_version_ids,
			legacy_object_etag, legacy_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, prepared_at
		) VALUES ($1, $2, $3, NULLIF($4, ''), 'eligible', $5, $6, $7, ARRAY[NULL]::TEXT[], $8, $9,
			$10, $11, $12, $13, $14, $15, $16, CURRENT_TIMESTAMP)
		ON CONFLICT (block_metadata_id) DO NOTHING`,
		blockMetadataID, tag, height, hash, manifest.Bucket, legacyKey, manifest.LegacyObjectKeySHA256,
		manifest.LegacyObjectETag, manifest.LegacyObjectBytes, cscbKey, manifest.ConsolidatedObjectVersionID,
		manifest.ConsolidatedObjectETag, manifest.ConsolidatedByteOffset, manifest.ConsolidatedByteLength,
		manifest.ConsolidatedUncompressedLength, manifest.PayloadSHA256,
	)
	require.Error(err)
	require.Contains(err.Error(), "block_legacy_object_retirement_immutable_version_ids_check")
	firstObservedAt, observedAt, err := repo.ObserveRetentionSafety(ctx, manifest.Bucket, cscbKey, keySHA256("safe-configuration-v1"))
	require.NoError(err)
	require.Equal(firstObservedAt, observedAt)
	sameFirstObservedAt, laterObservedAt, err := repo.ObserveRetentionSafety(ctx, manifest.Bucket, cscbKey, keySHA256("safe-configuration-v1"))
	require.NoError(err)
	require.Equal(firstObservedAt, sameFirstObservedAt)
	require.False(laterObservedAt.Before(observedAt))
	resetFirstObservedAt, resetObservedAt, err := repo.ObserveRetentionSafety(ctx, manifest.Bucket, cscbKey, keySHA256("safe-configuration-v2"))
	require.NoError(err)
	require.Equal(resetFirstObservedAt, resetObservedAt)
	require.False(resetFirstObservedAt.Before(laterObservedAt))
	_, err = db.ExecContext(ctx, `UPDATE block_legacy_object_retirement SET state = 'deleted_verified' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "invalid legacy retirement transition")
	_, err = db.ExecContext(ctx, `UPDATE block_legacy_object_retirement SET consolidated_object_etag = 'mutated' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot change pinned retirement manifest fields")

	startedAt := preparedAt.Add(24 * time.Hour)
	claimToken := "integration-claim"
	require.NoError(repo.ClaimRetirement(ctx, blockMetadataID, claimToken, startedAt, startedAt.Add(time.Hour)))
	var databaseClaimExpiresAt time.Time
	require.NoError(db.QueryRowContext(ctx, `SELECT claim_expires_at FROM block_legacy_object_retirement WHERE block_metadata_id = $1`, blockMetadataID).Scan(&databaseClaimExpiresAt))
	require.WithinDuration(time.Now().UTC().Add(time.Hour), databaseClaimExpiresAt, 5*time.Second)
	_, err = db.ExecContext(ctx, `UPDATE block_legacy_object_retirement SET claim_token = 'stolen-claim' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot replace an active legacy retirement claim")
	_, err = db.ExecContext(ctx, `UPDATE block_legacy_object_retirement SET state = 'deleted_pending_verification', claim_token = 'stolen-claim' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot change claim owner while recording legacy object deletion")
	err = repo.ClaimRetirement(ctx, blockMetadataID, "competing-claim", startedAt.Add(48*time.Hour), startedAt.Add(49*time.Hour))
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	err = repo.RenewRetirementClaim(ctx, blockMetadataID, "competing-claim", startedAt.Add(time.Second), startedAt.Add(2*time.Hour))
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	require.NoError(repo.RenewRetirementClaim(ctx, blockMetadataID, claimToken, startedAt.Add(time.Second), startedAt.Add(2*time.Hour)))
	_, err = db.ExecContext(ctx, `UPDATE block_legacy_object_retirement SET claim_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	_, err = repo.RecordRetirementObjectDeleted(ctx, blockMetadataID, claimToken, ActionDeletedObjectVersion)
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	claimToken = "replacement-claim"
	require.NoError(repo.ClaimRetirement(ctx, blockMetadataID, claimToken, time.Now().UTC(), time.Now().UTC().Add(time.Hour)))
	_, err = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	postReorgRow, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.False(postReorgRow.Canonical)
	require.Equal(legacyKey, postReorgRow.LegacyObjectKey)
	_, err = repo.RecordRetirementObjectDeleted(ctx, blockMetadataID, "competing-claim", ActionDeletedObjectVersion)
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	_, err = db.ExecContext(ctx, `UPDATE block_consolidation_shadow SET byte_length = 127 WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	_, err = repo.RecordRetirementObjectDeleted(ctx, blockMetadataID, claimToken, ActionDeletedObjectVersion)
	require.Error(err)
	require.Contains(err.Error(), "CSCB metadata changed")
	failedRow, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.Equal(legacyKey, failedRow.LegacyObjectKey)
	require.Equal(RetirementStateDeleting, failedRow.Retirement.State)
	_, err = db.ExecContext(ctx, `UPDATE block_consolidation_shadow SET byte_length = 128 WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	deletedAt, err := repo.RecordRetirementObjectDeleted(ctx, blockMetadataID, claimToken, ActionDeletedObjectVersion)
	require.NoError(err)
	require.WithinDuration(time.Now().UTC(), deletedAt, 5*time.Second)

	pendingRow, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.Empty(pendingRow.LegacyObjectKey)
	require.Empty(pendingRow.Shadow.LegacyObjectKey)
	require.NotNil(pendingRow.Shadow.LegacyObjectDeletedAt)
	require.Equal(RetirementStateDeletedPendingVerification, pendingRow.Retirement.State)
	require.Empty(pendingRow.Retirement.LegacyObjectKey)
	require.Equal(ActionDeletedObjectVersion, pendingRow.Retirement.Outcome)
	require.NotNil(pendingRow.Retirement.DeletedAt)
	require.Nil(pendingRow.Retirement.VerifiedAt)
	require.Equal(claimToken, pendingRow.Retirement.ClaimToken)

	_, err = db.ExecContext(ctx, `UPDATE block_legacy_object_retirement SET consolidated_object_version_id = 'mutated' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot change pinned retirement manifest fields")
	_, err = db.ExecContext(ctx, `UPDATE block_legacy_object_retirement SET state = 'eligible' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "invalid legacy retirement transition")

	_, err = repo.FinalizeRetirement(ctx, blockMetadataID, "competing-claim", ActionDeletedVerified)
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	verifiedAt, err := repo.FinalizeRetirement(ctx, blockMetadataID, claimToken, ActionDeletedVerified)
	require.NoError(err)
	idempotentVerifiedAt, err := repo.FinalizeRetirement(ctx, blockMetadataID, "idempotent-claim", ActionDeletedVerified)
	require.NoError(err)
	require.Equal(verifiedAt, idempotentVerifiedAt)

	row, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.False(row.Canonical)
	require.Empty(row.Hash)
	require.Empty(row.LegacyObjectKey)
	require.NotNil(row.Shadow)
	require.Empty(row.Shadow.Hash)
	require.Empty(row.Shadow.LegacyObjectKey)
	require.NotNil(row.Shadow.LegacyObjectDeletedAt)
	require.WithinDuration(deletedAt, *row.Shadow.LegacyObjectDeletedAt, time.Microsecond)
	require.Equal(cscbKey, row.Shadow.ConsolidatedObjectKey)
	require.Equal(cscbKey, row.PrimaryObjectKey)
	require.NotNil(row.Retirement)
	require.WithinDuration(time.Now().UTC(), row.Retirement.PreparedAt, 5*time.Second)
	require.Equal(RetirementStateDeletedVerified, row.Retirement.State)
	require.Empty(row.Retirement.LegacyObjectKey)
	require.Equal(keySHA256(legacyKey), row.Retirement.LegacyObjectKeySHA256)
	require.Equal([]string{"legacy-v1"}, row.Retirement.LegacyObjectVersionIDs)
	require.Empty(row.Retirement.LegacyObjectETag)
	require.Empty(row.Retirement.ClaimToken)
	require.Nil(row.Retirement.ClaimExpiresAt)
	require.NotNil(row.Retirement.DeleteStartedAt)
	require.Equal(2, row.Retirement.AttemptCount)
	require.NotNil(row.Retirement.LastAttemptAt)
	require.NotNil(row.Retirement.DeletedAt)
	require.NotNil(row.Retirement.VerifiedAt)
	require.Equal(ActionDeletedVerified, row.Retirement.Outcome)
	require.WithinDuration(deletedAt, *row.Retirement.DeletedAt, time.Microsecond)
	require.WithinDuration(verifiedAt, *row.Retirement.VerifiedAt, time.Microsecond)
	_, err = db.ExecContext(ctx, `UPDATE block_legacy_object_retirement SET outcome = 'mutated' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot change a verified legacy retirement")

	pending, err := repo.ListPendingRetirements(ctx, tag, height, height+1, 0)
	require.NoError(err)
	require.Empty(pending)
}

func openRetirementIntegrationDB(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)
	if cfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(cfg.ConnectTimeout.Seconds()))
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
