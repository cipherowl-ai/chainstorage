package retirement

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const metadataRowColumns = `
	bm.id,
	(cb.block_metadata_id IS NOT NULL) AS is_canonical,
	bm.tag,
	bm.height,
	COALESCE(bm.hash, ''),
	bm.skipped,
	COALESCE(bm.object_key_main, ''),
	bm.object_format,
	bm.byte_offset,
	bm.byte_length,
	bm.uncompressed_length,
	shadow.block_metadata_id,
	shadow.tag,
	shadow.height,
	shadow.hash,
	shadow.legacy_object_key_main,
	shadow.consolidated_object_key_main,
	shadow.object_format,
	shadow.byte_offset,
	shadow.byte_length,
	shadow.uncompressed_length,
	shadow.validated_at,
	shadow.legacy_object_retired_at,
	shadow.legacy_object_retire_after,
	shadow.legacy_object_deleted_at,
	shadow.format_version,
	retirement.block_metadata_id,
	retirement.state,
	retirement.bucket,
	retirement.legacy_object_key_main,
	retirement.legacy_object_key_sha256,
	retirement.legacy_object_version_ids,
	retirement.legacy_object_etag,
	retirement.legacy_object_bytes,
	retirement.consolidated_object_key_main,
	retirement.consolidated_object_version_id,
	retirement.consolidated_object_etag,
	retirement.consolidated_byte_offset,
	retirement.consolidated_byte_length,
	retirement.consolidated_uncompressed_length,
	retirement.payload_sha256,
	retirement.outcome,
	retirement.attempt_count,
	retirement.prepared_at,
	retirement.delete_started_at,
	retirement.last_attempt_at,
	retirement.deleted_at`

const metadataRowFrom = `
	FROM canonical_blocks cb
	JOIN block_metadata bm ON bm.id = cb.block_metadata_id AND bm.tag = cb.tag AND bm.height = cb.height
	LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
	LEFT JOIN block_legacy_object_retirement retirement ON retirement.block_metadata_id = bm.id`

const metadataRowByIDFrom = `
	FROM block_metadata bm
	LEFT JOIN canonical_blocks cb ON cb.block_metadata_id = bm.id AND cb.tag = bm.tag AND cb.height = bm.height
	LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
	LEFT JOIN block_legacy_object_retirement retirement ON retirement.block_metadata_id = bm.id`

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) ListMetadataRows(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]MetadataRow, error) {
	if r.db == nil {
		return nil, xerrors.New("postgres db is required")
	}
	if endHeight <= startHeight {
		return nil, nil
	}
	if limit == 0 {
		limit = endHeight - startHeight
	}

	query := fmt.Sprintf(`
		SELECT %s
		%s
		WHERE cb.tag = $1
			AND cb.height >= $2
			AND cb.height < $3
		ORDER BY cb.height ASC
		LIMIT $4`, metadataRowColumns, metadataRowFrom)
	rows, err := r.db.QueryContext(ctx, query, tag, startHeight, endHeight, limit)
	if err != nil {
		return nil, xerrors.Errorf("failed to query retirement metadata rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]MetadataRow, 0)
	for rows.Next() {
		row, err := scanMetadataRow(rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan retirement metadata row: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate retirement metadata rows: %w", err)
	}
	return result, nil
}

func (r *PostgresRepository) GetMetadataRow(ctx context.Context, blockMetadataID int64) (MetadataRow, error) {
	if r.db == nil {
		return MetadataRow{}, xerrors.New("postgres db is required")
	}
	query := fmt.Sprintf(`
		SELECT %s
		%s
		WHERE bm.id = $1`, metadataRowColumns, metadataRowByIDFrom)
	row, err := scanMetadataRow(r.db.QueryRowContext(ctx, query, blockMetadataID))
	if err != nil {
		if err == sql.ErrNoRows {
			return MetadataRow{}, xerrors.Errorf("retirement metadata row not found: block_metadata_id=%d", blockMetadataID)
		}
		return MetadataRow{}, xerrors.Errorf("failed to get retirement metadata row: %w", err)
	}
	return row, nil
}

func (r *PostgresRepository) PrepareRetirement(ctx context.Context, manifest RetirementManifest) error {
	if r.db == nil {
		return xerrors.New("postgres db is required")
	}
	if manifest.State != RetirementStateEligible {
		return xerrors.Errorf("prepared retirement must be eligible, got %q", manifest.State)
	}
	if err := validatePreparedManifest(manifest); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("failed to begin retirement preparation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const lockMetadata = `
		SELECT bm.id
		FROM canonical_blocks cb
		JOIN block_metadata bm ON bm.id = cb.block_metadata_id AND bm.tag = cb.tag AND bm.height = cb.height
		JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		WHERE bm.id = $1
		FOR UPDATE OF cb, bm, shadow`
	var lockedID int64
	if err := tx.QueryRowContext(ctx, lockMetadata, manifest.BlockMetadataID).Scan(&lockedID); err != nil {
		return xerrors.Errorf("failed to lock canonical retirement metadata: %w", err)
	}
	query := fmt.Sprintf(`
		SELECT %s
		%s
		WHERE bm.id = $1`, metadataRowColumns, metadataRowFrom)
	row, err := scanMetadataRow(tx.QueryRowContext(ctx, query, lockedID))
	if err != nil {
		return xerrors.Errorf("failed to lock retirement metadata row: %w", err)
	}
	if err := validateManifestMetadata(row, manifest); err != nil {
		return err
	}

	const insert = `
		INSERT INTO block_legacy_object_retirement (
			block_metadata_id, tag, height, hash, state, bucket,
			legacy_object_key_main, legacy_object_key_sha256, legacy_object_version_ids,
			legacy_object_etag, legacy_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, outcome, prepared_at, updated_at
		)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $20)
		ON CONFLICT (block_metadata_id) DO NOTHING`
	if _, err := tx.ExecContext(ctx, insert,
		manifest.BlockMetadataID,
		manifest.Tag,
		manifest.Height,
		manifest.Hash,
		manifest.State,
		manifest.Bucket,
		manifest.LegacyObjectKey,
		manifest.LegacyObjectKeySHA256,
		pq.Array(manifest.LegacyObjectVersionIDs),
		manifest.LegacyObjectETag,
		manifest.LegacyObjectBytes,
		manifest.ConsolidatedObjectKey,
		manifest.ConsolidatedObjectVersionID,
		manifest.ConsolidatedObjectETag,
		manifest.ConsolidatedByteOffset,
		manifest.ConsolidatedByteLength,
		manifest.ConsolidatedUncompressedLength,
		manifest.PayloadSHA256,
		manifest.Outcome,
		manifest.PreparedAt,
	); err != nil {
		return xerrors.Errorf("failed to persist retirement manifest: %w", err)
	}

	existing, err := getManifestForUpdate(ctx, tx, manifest.BlockMetadataID)
	if err != nil {
		return err
	}
	if !samePreparedManifest(existing, manifest) {
		return xerrors.Errorf("retirement manifest conflict: block_metadata_id=%d state=%s", manifest.BlockMetadataID, existing.State)
	}
	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("failed to commit retirement manifest: %w", err)
	}
	return nil
}

func (r *PostgresRepository) MarkRetirementDeleting(ctx context.Context, blockMetadataID int64, startedAt time.Time) error {
	if r.db == nil {
		return xerrors.New("postgres db is required")
	}
	const query = `
		UPDATE block_legacy_object_retirement
		SET state = $2,
			delete_started_at = COALESCE(delete_started_at, $3),
			last_attempt_at = $3,
			attempt_count = attempt_count + 1,
			outcome = 'delete_started',
			updated_at = $3
		WHERE block_metadata_id = $1
			AND state IN ($4, $2)`
	result, err := r.db.ExecContext(ctx, query, blockMetadataID, RetirementStateDeleting, startedAt, RetirementStateEligible)
	if err != nil {
		return xerrors.Errorf("failed to mark retirement deleting: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return xerrors.Errorf("failed to inspect retirement deleting transition: %w", err)
	}
	if rows == 1 {
		return nil
	}
	return xerrors.Errorf("invalid retirement transition to deleting: block_metadata_id=%d", blockMetadataID)
}

func (r *PostgresRepository) RecordRetirementOutcome(ctx context.Context, blockMetadataID int64, outcome string, attemptedAt time.Time) error {
	if r.db == nil {
		return xerrors.New("postgres db is required")
	}
	if outcome == "" {
		return xerrors.New("retirement outcome is required")
	}
	const query = `
		UPDATE block_legacy_object_retirement
		SET outcome = $2,
			last_attempt_at = $3,
			updated_at = $3
		WHERE block_metadata_id = $1
			AND state IN ($4, $5)`
	_, err := r.db.ExecContext(ctx, query, blockMetadataID, outcome, attemptedAt, RetirementStateEligible, RetirementStateDeleting)
	if err != nil {
		return xerrors.Errorf("failed to record retirement outcome: %w", err)
	}
	return nil
}

func (r *PostgresRepository) FinalizeRetirement(ctx context.Context, blockMetadataID int64, deletedAt time.Time, outcome string) error {
	if r.db == nil {
		return xerrors.New("postgres db is required")
	}
	if deletedAt.IsZero() || outcome == "" {
		return xerrors.New("deleted timestamp and outcome are required for retirement finalization")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("failed to begin retirement finalization: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	metadata, err := lockFinalizationMetadata(ctx, tx, blockMetadataID)
	if err != nil {
		return err
	}
	manifest, err := getManifestForUpdate(ctx, tx, blockMetadataID)
	if err != nil {
		return err
	}
	if !finalizationMetadataMatchesManifest(metadata, manifest) {
		return xerrors.Errorf("active or shadow CSCB metadata changed before retirement finalization: block_metadata_id=%d", blockMetadataID)
	}
	if manifest.State == RetirementStateDeletedVerified {
		if manifest.LegacyObjectKey != "" || manifest.LegacyObjectETag != "" || manifest.Outcome == "" ||
			manifest.DeleteStartedAt == nil || manifest.LastAttemptAt == nil || manifest.DeletedAt == nil ||
			metadata.shadowLegacyKey.Valid || !metadata.shadowDeletedAt.Valid ||
			!metadata.shadowDeletedAt.Time.Equal(*manifest.DeletedAt) {
			return xerrors.Errorf("finalized retirement metadata is inconsistent: block_metadata_id=%d", blockMetadataID)
		}
		if err := tx.Commit(); err != nil {
			return xerrors.Errorf("failed to commit idempotent retirement finalization: %w", err)
		}
		return nil
	}
	if manifest.State != RetirementStateDeleting {
		return xerrors.Errorf("retirement must be deleting before finalization: block_metadata_id=%d state=%s", blockMetadataID, manifest.State)
	}
	if !metadata.shadowLegacyKey.Valid || metadata.shadowLegacyKey.String != manifest.LegacyObjectKey || metadata.shadowDeletedAt.Valid {
		return xerrors.Errorf("legacy shadow metadata changed before retirement finalization: block_metadata_id=%d", blockMetadataID)
	}

	const clearShadow = `
		UPDATE block_consolidation_shadow
		SET legacy_object_key_main = NULL,
			legacy_object_deleted_at = $3
		WHERE block_metadata_id = $1
			AND legacy_object_key_main = $2
			AND legacy_object_deleted_at IS NULL`
	result, err := tx.ExecContext(ctx, clearShadow, blockMetadataID, manifest.LegacyObjectKey, deletedAt)
	if err != nil {
		return xerrors.Errorf("failed to clear retired shadow path: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return xerrors.Errorf("retired shadow path clear guard failed: block_metadata_id=%d rows=%d err=%v", blockMetadataID, rows, err)
	}

	const finalize = `
		UPDATE block_legacy_object_retirement
		SET state = $2,
			legacy_object_key_main = NULL,
			legacy_object_etag = '',
			outcome = $3,
			deleted_at = $4,
			updated_at = $4
		WHERE block_metadata_id = $1
			AND state = $5`
	result, err = tx.ExecContext(ctx, finalize, blockMetadataID, RetirementStateDeletedVerified, outcome, deletedAt, RetirementStateDeleting)
	if err != nil {
		return xerrors.Errorf("failed to finalize retirement manifest: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return xerrors.Errorf("retirement finalization guard failed: block_metadata_id=%d rows=%d err=%v", blockMetadataID, rows, err)
	}
	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("failed to commit retirement finalization: %w", err)
	}
	return nil
}

type finalizationMetadata struct {
	shadowLegacyKey          sql.NullString
	shadowDeletedAt          sql.NullTime
	shadowConsolidatedKey    string
	shadowObjectFormat       int64
	shadowByteOffset         int64
	shadowByteLength         int64
	shadowUncompressedLength int64
	shadowValidatedAt        sql.NullTime
	primaryObjectKey         sql.NullString
	primaryObjectFormat      int64
	primaryByteOffset        sql.NullInt64
	primaryByteLength        sql.NullInt64
	primaryUncompressed      sql.NullInt64
	primarySkipped           bool
}

func lockFinalizationMetadata(ctx context.Context, tx *sql.Tx, blockMetadataID int64) (finalizationMetadata, error) {
	const query = `
		SELECT
			shadow.legacy_object_key_main,
			shadow.legacy_object_deleted_at,
			shadow.consolidated_object_key_main,
			shadow.object_format,
			shadow.byte_offset,
			shadow.byte_length,
			shadow.uncompressed_length,
			shadow.validated_at,
			bm.object_key_main,
			bm.object_format,
			bm.byte_offset,
			bm.byte_length,
			bm.uncompressed_length,
			bm.skipped
		FROM block_consolidation_shadow shadow
		JOIN block_metadata bm ON bm.id = shadow.block_metadata_id
		WHERE shadow.block_metadata_id = $1
		FOR UPDATE OF shadow, bm`
	var metadata finalizationMetadata
	if err := tx.QueryRowContext(ctx, query, blockMetadataID).Scan(
		&metadata.shadowLegacyKey,
		&metadata.shadowDeletedAt,
		&metadata.shadowConsolidatedKey,
		&metadata.shadowObjectFormat,
		&metadata.shadowByteOffset,
		&metadata.shadowByteLength,
		&metadata.shadowUncompressedLength,
		&metadata.shadowValidatedAt,
		&metadata.primaryObjectKey,
		&metadata.primaryObjectFormat,
		&metadata.primaryByteOffset,
		&metadata.primaryByteLength,
		&metadata.primaryUncompressed,
		&metadata.primarySkipped,
	); err != nil {
		return finalizationMetadata{}, xerrors.Errorf("failed to lock retirement finalization metadata: %w", err)
	}
	return metadata, nil
}

func finalizationMetadataMatchesManifest(metadata finalizationMetadata, manifest RetirementManifest) bool {
	return metadata.shadowValidatedAt.Valid &&
		metadata.shadowConsolidatedKey == manifest.ConsolidatedObjectKey &&
		metadata.shadowObjectFormat == int64(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH) &&
		metadata.shadowByteOffset >= 0 && uint64(metadata.shadowByteOffset) == manifest.ConsolidatedByteOffset &&
		metadata.shadowByteLength > 0 && uint64(metadata.shadowByteLength) == manifest.ConsolidatedByteLength &&
		metadata.shadowUncompressedLength > 0 && uint64(metadata.shadowUncompressedLength) == manifest.ConsolidatedUncompressedLength &&
		!metadata.primarySkipped &&
		metadata.primaryObjectKey.Valid && metadata.primaryObjectKey.String == manifest.ConsolidatedObjectKey &&
		metadata.primaryObjectFormat == int64(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH) &&
		metadata.primaryByteOffset.Valid && metadata.primaryByteOffset.Int64 >= 0 && uint64(metadata.primaryByteOffset.Int64) == manifest.ConsolidatedByteOffset &&
		metadata.primaryByteLength.Valid && metadata.primaryByteLength.Int64 > 0 && uint64(metadata.primaryByteLength.Int64) == manifest.ConsolidatedByteLength &&
		metadata.primaryUncompressed.Valid && metadata.primaryUncompressed.Int64 > 0 && uint64(metadata.primaryUncompressed.Int64) == manifest.ConsolidatedUncompressedLength
}

func (r *PostgresRepository) ListPendingRetirements(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]RetirementManifest, error) {
	if r.db == nil {
		return nil, xerrors.New("postgres db is required")
	}
	if endHeight <= startHeight {
		return nil, nil
	}
	if limit == 0 {
		limit = endHeight - startHeight
	}
	const query = `
		SELECT
			block_metadata_id, tag, height, COALESCE(hash, ''), state, bucket,
			legacy_object_key_main, legacy_object_key_sha256, legacy_object_version_ids,
			legacy_object_etag, legacy_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, outcome, attempt_count, prepared_at, delete_started_at, last_attempt_at, deleted_at
		FROM block_legacy_object_retirement
		WHERE tag = $1
			AND height >= $2
			AND height < $3
			AND state IN ($4, $5)
		ORDER BY height ASC
		LIMIT $6`
	rows, err := r.db.QueryContext(ctx, query, tag, startHeight, endHeight, RetirementStateEligible, RetirementStateDeleting, limit)
	if err != nil {
		return nil, xerrors.Errorf("failed to list pending retirements: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := make([]RetirementManifest, 0)
	for rows.Next() {
		manifest, err := scanManifest(rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan pending retirement: %w", err)
		}
		result = append(result, manifest)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate pending retirements: %w", err)
	}
	return result, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanMetadataRow(source scanner) (MetadataRow, error) {
	var row MetadataRow
	var primaryObjectFormat int64
	var primaryByteOffset sql.NullInt64
	var primaryByteLength sql.NullInt64
	var primaryUncompressedLength sql.NullInt64
	var shadowBlockMetadataID sql.NullInt64
	var shadowTag sql.NullInt64
	var shadowHeight sql.NullInt64
	var shadowHash sql.NullString
	var shadowLegacyKey sql.NullString
	var shadowConsolidatedKey sql.NullString
	var shadowObjectFormat sql.NullInt64
	var shadowByteOffset sql.NullInt64
	var shadowByteLength sql.NullInt64
	var shadowUncompressedLength sql.NullInt64
	var shadowValidatedAt sql.NullTime
	var shadowLegacyObjectRetiredAt sql.NullTime
	var shadowLegacyObjectRetireAfter sql.NullTime
	var shadowLegacyObjectDeletedAt sql.NullTime
	var shadowFormatVersion sql.NullInt64
	var retirementBlockMetadataID sql.NullInt64
	var retirementState sql.NullString
	var retirementBucket sql.NullString
	var retirementLegacyKey sql.NullString
	var retirementLegacyKeySHA256 sql.NullString
	var retirementVersionIDs pq.StringArray
	var retirementLegacyETag sql.NullString
	var retirementLegacyBytes sql.NullInt64
	var retirementConsolidatedKey sql.NullString
	var retirementConsolidatedVersionID sql.NullString
	var retirementConsolidatedETag sql.NullString
	var retirementByteOffset sql.NullInt64
	var retirementByteLength sql.NullInt64
	var retirementUncompressedLength sql.NullInt64
	var retirementPayloadSHA256 sql.NullString
	var retirementOutcome sql.NullString
	var retirementAttemptCount sql.NullInt64
	var retirementPreparedAt sql.NullTime
	var retirementDeleteStartedAt sql.NullTime
	var retirementLastAttemptAt sql.NullTime
	var retirementDeletedAt sql.NullTime
	if err := source.Scan(
		&row.BlockMetadataID,
		&row.Canonical,
		&row.Tag,
		&row.Height,
		&row.Hash,
		&row.Skipped,
		&row.PrimaryObjectKey,
		&primaryObjectFormat,
		&primaryByteOffset,
		&primaryByteLength,
		&primaryUncompressedLength,
		&shadowBlockMetadataID,
		&shadowTag,
		&shadowHeight,
		&shadowHash,
		&shadowLegacyKey,
		&shadowConsolidatedKey,
		&shadowObjectFormat,
		&shadowByteOffset,
		&shadowByteLength,
		&shadowUncompressedLength,
		&shadowValidatedAt,
		&shadowLegacyObjectRetiredAt,
		&shadowLegacyObjectRetireAfter,
		&shadowLegacyObjectDeletedAt,
		&shadowFormatVersion,
		&retirementBlockMetadataID,
		&retirementState,
		&retirementBucket,
		&retirementLegacyKey,
		&retirementLegacyKeySHA256,
		&retirementVersionIDs,
		&retirementLegacyETag,
		&retirementLegacyBytes,
		&retirementConsolidatedKey,
		&retirementConsolidatedVersionID,
		&retirementConsolidatedETag,
		&retirementByteOffset,
		&retirementByteLength,
		&retirementUncompressedLength,
		&retirementPayloadSHA256,
		&retirementOutcome,
		&retirementAttemptCount,
		&retirementPreparedAt,
		&retirementDeleteStartedAt,
		&retirementLastAttemptAt,
		&retirementDeletedAt,
	); err != nil {
		return MetadataRow{}, err
	}
	row.PrimaryObjectFormat = api.BlockObjectFormat(primaryObjectFormat)
	row.PrimaryByteOffset = nullableUint64(primaryByteOffset)
	row.PrimaryByteLength = nullableUint64(primaryByteLength)
	row.PrimaryUncompressedLength = nullableUint64(primaryUncompressedLength)
	if shadowBlockMetadataID.Valid {
		row.LegacyObjectKey = shadowLegacyKey.String
		row.Shadow = &ConsolidationShadow{
			Tag:                   uint32(shadowTag.Int64),
			Height:                uint64(shadowHeight.Int64),
			Hash:                  shadowHash.String,
			LegacyObjectKey:       shadowLegacyKey.String,
			ConsolidatedObjectKey: shadowConsolidatedKey.String,
			ObjectFormat:          api.BlockObjectFormat(shadowObjectFormat.Int64),
			ByteOffset:            nullableUint64(shadowByteOffset),
			ByteLength:            nullableUint64(shadowByteLength),
			UncompressedLength:    nullableUint64(shadowUncompressedLength),
			FormatVersion:         int(shadowFormatVersion.Int64),
		}
		row.Shadow.ValidatedAt = nullableTime(shadowValidatedAt)
		row.Shadow.LegacyObjectRetiredAt = nullableTime(shadowLegacyObjectRetiredAt)
		row.Shadow.LegacyObjectRetireAfter = nullableTime(shadowLegacyObjectRetireAfter)
		row.Shadow.LegacyObjectDeletedAt = nullableTime(shadowLegacyObjectDeletedAt)
	}
	if retirementBlockMetadataID.Valid {
		row.Retirement = &RetirementManifest{
			BlockMetadataID:                retirementBlockMetadataID.Int64,
			Tag:                            row.Tag,
			Height:                         row.Height,
			Hash:                           row.Hash,
			State:                          retirementState.String,
			Bucket:                         retirementBucket.String,
			LegacyObjectKey:                retirementLegacyKey.String,
			LegacyObjectKeySHA256:          retirementLegacyKeySHA256.String,
			LegacyObjectVersionIDs:         append([]string(nil), retirementVersionIDs...),
			LegacyObjectETag:               retirementLegacyETag.String,
			LegacyObjectBytes:              nullableUint64(retirementLegacyBytes),
			ConsolidatedObjectKey:          retirementConsolidatedKey.String,
			ConsolidatedObjectVersionID:    retirementConsolidatedVersionID.String,
			ConsolidatedObjectETag:         retirementConsolidatedETag.String,
			ConsolidatedByteOffset:         nullableUint64(retirementByteOffset),
			ConsolidatedByteLength:         nullableUint64(retirementByteLength),
			ConsolidatedUncompressedLength: nullableUint64(retirementUncompressedLength),
			PayloadSHA256:                  retirementPayloadSHA256.String,
			Outcome:                        retirementOutcome.String,
			AttemptCount:                   int(retirementAttemptCount.Int64),
			PreparedAt:                     retirementPreparedAt.Time,
			DeleteStartedAt:                nullableTime(retirementDeleteStartedAt),
			LastAttemptAt:                  nullableTime(retirementLastAttemptAt),
			DeletedAt:                      nullableTime(retirementDeletedAt),
		}
	}
	return row, nil
}

func getManifestForUpdate(ctx context.Context, tx *sql.Tx, blockMetadataID int64) (RetirementManifest, error) {
	const query = `
		SELECT
			block_metadata_id, tag, height, COALESCE(hash, ''), state, bucket,
			legacy_object_key_main, legacy_object_key_sha256, legacy_object_version_ids,
			legacy_object_etag, legacy_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, outcome, attempt_count, prepared_at, delete_started_at, last_attempt_at, deleted_at
		FROM block_legacy_object_retirement
		WHERE block_metadata_id = $1
		FOR UPDATE`
	manifest, err := scanManifest(tx.QueryRowContext(ctx, query, blockMetadataID))
	if err != nil {
		return RetirementManifest{}, xerrors.Errorf("failed to lock retirement manifest: %w", err)
	}
	return manifest, nil
}

func scanManifest(source scanner) (RetirementManifest, error) {
	var manifest RetirementManifest
	var hash sql.NullString
	var legacyKey sql.NullString
	var versionIDs pq.StringArray
	var legacyBytes int64
	var byteOffset int64
	var byteLength int64
	var uncompressedLength int64
	var attemptCount int
	var deleteStartedAt sql.NullTime
	var lastAttemptAt sql.NullTime
	var deletedAt sql.NullTime
	if err := source.Scan(
		&manifest.BlockMetadataID,
		&manifest.Tag,
		&manifest.Height,
		&hash,
		&manifest.State,
		&manifest.Bucket,
		&legacyKey,
		&manifest.LegacyObjectKeySHA256,
		&versionIDs,
		&manifest.LegacyObjectETag,
		&legacyBytes,
		&manifest.ConsolidatedObjectKey,
		&manifest.ConsolidatedObjectVersionID,
		&manifest.ConsolidatedObjectETag,
		&byteOffset,
		&byteLength,
		&uncompressedLength,
		&manifest.PayloadSHA256,
		&manifest.Outcome,
		&attemptCount,
		&manifest.PreparedAt,
		&deleteStartedAt,
		&lastAttemptAt,
		&deletedAt,
	); err != nil {
		return RetirementManifest{}, err
	}
	if legacyBytes < 0 || byteOffset < 0 || byteLength < 0 || uncompressedLength < 0 || attemptCount < 0 {
		return RetirementManifest{}, xerrors.New("retirement manifest contains negative byte fields")
	}
	manifest.Hash = hash.String
	manifest.LegacyObjectKey = legacyKey.String
	manifest.LegacyObjectVersionIDs = append([]string(nil), versionIDs...)
	manifest.LegacyObjectBytes = uint64(legacyBytes)
	manifest.ConsolidatedByteOffset = uint64(byteOffset)
	manifest.ConsolidatedByteLength = uint64(byteLength)
	manifest.ConsolidatedUncompressedLength = uint64(uncompressedLength)
	manifest.AttemptCount = attemptCount
	manifest.DeleteStartedAt = nullableTime(deleteStartedAt)
	manifest.LastAttemptAt = nullableTime(lastAttemptAt)
	manifest.DeletedAt = nullableTime(deletedAt)
	return manifest, nil
}

func validateManifestMetadata(row MetadataRow, manifest RetirementManifest) error {
	if !row.Canonical || row.BlockMetadataID != manifest.BlockMetadataID || row.Tag != manifest.Tag || row.Height != manifest.Height || row.Hash != manifest.Hash {
		return xerrors.Errorf("canonical metadata changed before retirement: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if row.Skipped || row.Shadow == nil || !validShadowReference(row, row.Shadow) || row.LegacyObjectKey != manifest.LegacyObjectKey {
		return xerrors.Errorf("legacy shadow metadata changed before retirement: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if row.Shadow.ValidatedAt == nil || row.Shadow.LegacyObjectRetiredAt == nil || row.Shadow.LegacyObjectRetireAfter == nil ||
		row.Shadow.LegacyObjectDeletedAt != nil || manifest.PreparedAt.Before(*row.Shadow.LegacyObjectRetireAfter) {
		return xerrors.Errorf("retirement eligibility changed before manifest persistence: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if row.PrimaryObjectKey != manifest.ConsolidatedObjectKey ||
		row.PrimaryObjectFormat != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH ||
		row.PrimaryByteOffset != manifest.ConsolidatedByteOffset ||
		row.PrimaryByteLength != manifest.ConsolidatedByteLength ||
		row.PrimaryUncompressedLength != manifest.ConsolidatedUncompressedLength {
		return xerrors.Errorf("consolidated metadata changed before retirement: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	return nil
}

func validatePreparedManifest(manifest RetirementManifest) error {
	if manifest.BlockMetadataID <= 0 || manifest.Bucket == "" || manifest.LegacyObjectKey == "" ||
		manifest.LegacyObjectKeySHA256 != keySHA256(manifest.LegacyObjectKey) ||
		len(manifest.LegacyObjectVersionIDs) != 1 || manifest.LegacyObjectVersionIDs[0] == "" ||
		manifest.LegacyObjectETag == "" || manifest.LegacyObjectBytes == 0 ||
		manifest.ConsolidatedObjectKey == "" || manifest.ConsolidatedObjectVersionID == "" || manifest.ConsolidatedObjectETag == "" ||
		manifest.ConsolidatedByteLength == 0 || manifest.ConsolidatedUncompressedLength == 0 ||
		!isSHA256Hex(manifest.PayloadSHA256) || manifest.PreparedAt.IsZero() {
		return xerrors.Errorf("retirement manifest is incomplete: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	return nil
}

func samePreparedManifest(existing RetirementManifest, expected RetirementManifest) bool {
	if existing.State != RetirementStateEligible && existing.State != RetirementStateDeleting {
		return false
	}
	return existing.BlockMetadataID == expected.BlockMetadataID &&
		existing.Tag == expected.Tag &&
		existing.Height == expected.Height &&
		existing.Hash == expected.Hash &&
		existing.Bucket == expected.Bucket &&
		existing.LegacyObjectKey == expected.LegacyObjectKey &&
		existing.LegacyObjectKeySHA256 == expected.LegacyObjectKeySHA256 &&
		sameStrings(existing.LegacyObjectVersionIDs, expected.LegacyObjectVersionIDs) &&
		existing.LegacyObjectETag == expected.LegacyObjectETag &&
		existing.LegacyObjectBytes == expected.LegacyObjectBytes &&
		existing.ConsolidatedObjectKey == expected.ConsolidatedObjectKey &&
		existing.ConsolidatedObjectVersionID == expected.ConsolidatedObjectVersionID &&
		existing.ConsolidatedObjectETag == expected.ConsolidatedObjectETag &&
		existing.ConsolidatedByteOffset == expected.ConsolidatedByteOffset &&
		existing.ConsolidatedByteLength == expected.ConsolidatedByteLength &&
		existing.ConsolidatedUncompressedLength == expected.ConsolidatedUncompressedLength &&
		existing.PayloadSHA256 == expected.PayloadSHA256
}

func sameStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func nullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	copy := value.Time
	return &copy
}

func nullableUint64(value sql.NullInt64) uint64 {
	if !value.Valid || value.Int64 < 0 {
		return 0
	}
	return uint64(value.Int64)
}
