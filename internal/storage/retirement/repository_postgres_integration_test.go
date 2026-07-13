package retirement

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestIntegrationPostgresRepositoryFinalizeRetirementClearsLegacyPathAtomically(t *testing.T) {
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
	hash := fmt.Sprintf("retirement-%x", unique)
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
		) VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, $8, $9, $10)
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
		_, _ = db.ExecContext(ctx, `DELETE FROM block_legacy_object_retirement WHERE block_metadata_id = $1`, blockMetadataID)
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
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
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
	preparedAt := time.Now().UTC()
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
	require.NoError(repo.PrepareRetirement(ctx, manifest))
	startedAt := preparedAt.Add(time.Second)
	require.NoError(repo.MarkRetirementDeleting(ctx, blockMetadataID, startedAt))
	_, err = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	postReorgRow, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.False(postReorgRow.Canonical)
	require.Equal(legacyKey, postReorgRow.LegacyObjectKey)
	deletedAt := startedAt.Add(time.Second)
	_, err = db.ExecContext(ctx, `UPDATE block_metadata SET byte_length = 127 WHERE id = $1`, blockMetadataID)
	require.NoError(err)
	err = repo.FinalizeRetirement(ctx, blockMetadataID, deletedAt, ActionDeletedVerified)
	require.Error(err)
	require.Contains(err.Error(), "CSCB metadata changed")
	failedRow, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.Equal(legacyKey, failedRow.LegacyObjectKey)
	require.Equal(RetirementStateDeleting, failedRow.Retirement.State)
	_, err = db.ExecContext(ctx, `UPDATE block_metadata SET byte_length = 128 WHERE id = $1`, blockMetadataID)
	require.NoError(err)
	require.NoError(repo.FinalizeRetirement(ctx, blockMetadataID, deletedAt, ActionDeletedVerified))
	require.NoError(repo.FinalizeRetirement(ctx, blockMetadataID, deletedAt.Add(time.Minute), ActionDeletedVerified))

	row, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.False(row.Canonical)
	require.Empty(row.LegacyObjectKey)
	require.NotNil(row.Shadow)
	require.Empty(row.Shadow.LegacyObjectKey)
	require.NotNil(row.Shadow.LegacyObjectDeletedAt)
	require.WithinDuration(deletedAt, *row.Shadow.LegacyObjectDeletedAt, time.Microsecond)
	require.Equal(cscbKey, row.Shadow.ConsolidatedObjectKey)
	require.Equal(cscbKey, row.PrimaryObjectKey)
	require.NotNil(row.Retirement)
	require.Equal(RetirementStateDeletedVerified, row.Retirement.State)
	require.Empty(row.Retirement.LegacyObjectKey)
	require.Equal(keySHA256(legacyKey), row.Retirement.LegacyObjectKeySHA256)
	require.Equal([]string{"legacy-v1"}, row.Retirement.LegacyObjectVersionIDs)
	require.Empty(row.Retirement.LegacyObjectETag)
	require.NotNil(row.Retirement.DeleteStartedAt)
	require.Equal(1, row.Retirement.AttemptCount)
	require.NotNil(row.Retirement.LastAttemptAt)
	require.NotNil(row.Retirement.DeletedAt)
	require.Equal(ActionDeletedVerified, row.Retirement.Outcome)
	require.WithinDuration(deletedAt, *row.Retirement.DeletedAt, time.Microsecond)

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
