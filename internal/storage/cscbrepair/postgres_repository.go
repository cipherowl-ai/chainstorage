package cscbrepair

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/lib/pq"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/retirementlock"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const manifestColumns = `
	id, tag, state, bucket,
	old_consolidated_object_key_main, old_consolidated_object_version_id,
	old_consolidated_object_etag, old_consolidated_object_bytes,
	start_height, end_height, canonical_block_count, total_block_count, row_set_sha256,
	new_consolidated_object_key_main, new_consolidated_object_version_id,
	new_consolidated_object_etag, new_consolidated_object_bytes,
	outcome, prepared_at, restored_at, verified_at,
	old_object_deleted_at, completed_at`

type (
	PostgresRepository struct {
		db *sql.DB
	}

	rowScanner interface {
		Scan(dest ...any) error
	}

	queryer interface {
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}
)

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) FindByExecutionKey(
	ctx context.Context,
	executionKey string,
) (*Manifest, bool, error) {
	if err := r.validateDB(); err != nil {
		return nil, false, err
	}
	if !validExecutionKey(executionKey) {
		return nil, false, xerrors.New("valid CSCB repair execution key is required")
	}
	var repairID sql.NullInt64
	if err := r.db.QueryRowContext(ctx, `
		SELECT repair_id
		FROM cscb_repair_execution
		WHERE execution_key = $1`, executionKey).Scan(&repairID); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, xerrors.Errorf("failed to find CSCB repair execution binding: %w", err)
	}
	if !repairID.Valid {
		return nil, true, nil
	}
	manifest, err := loadManifest(ctx, r.db, repairID.Int64, false)
	if err != nil {
		return nil, false, err
	}
	return manifest, true, nil
}

func (r *PostgresRepository) FindPending(
	ctx context.Context,
	tag uint32,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
			SELECT %s
			FROM cscb_repair_manifest
			WHERE tag = $1
				AND state <> $2
			ORDER BY id ASC
			LIMIT 1`, manifestColumns)
	manifest, err := scanManifest(r.db.QueryRowContext(ctx, query, tag, StateCompleted))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to find pending CSCB repair: %w", err)
	}
	if err := loadManifestBlocks(ctx, r.db, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func (r *PostgresRepository) FindNextCandidate(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if endHeight <= startHeight {
		return nil, nil
	}
	const keyQuery = `
		SELECT bm.object_key_main
		FROM canonical_blocks cb
		JOIN block_metadata bm ON bm.id = cb.block_metadata_id
			AND bm.tag = cb.tag
			AND bm.height = cb.height
		WHERE cb.tag = $1
			AND cb.height >= $2
			AND cb.height < $3
			AND bm.skipped = FALSE
			AND bm.object_format = $4
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND NOT EXISTS (
				SELECT 1
				FROM cscb_repair_manifest repair
				WHERE repair.tag = bm.tag
					AND (
						repair.old_consolidated_object_key_main = bm.object_key_main
						OR repair.new_consolidated_object_key_main = bm.object_key_main
					)
			)
		GROUP BY bm.object_key_main
		ORDER BY MAX(cb.height) DESC, bm.object_key_main DESC
		LIMIT 1`
	var objectKey string
	if err := r.db.QueryRowContext(
		ctx,
		keyQuery,
		tag,
		startHeight,
		endHeight,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
	).Scan(&objectKey); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to find next active CSCB repair candidate: %w", err)
	}
	manifest, err := loadCandidateByKey(ctx, r.db, tag, objectKey)
	if err != nil {
		return nil, err
	}
	return manifest, nil
}

func (r *PostgresRepository) Prepare(ctx context.Context, manifest *Manifest) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if err := validatePreparedInput(manifest); err != nil {
		return nil, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair preparation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockCandidateRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	if err := lockConsolidatedObjectKey(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}
	current, err := loadCandidateByKey(ctx, tx, manifest.Tag, manifest.OldConsolidatedObjectKey)
	if err != nil {
		return nil, err
	}
	if err := compareCandidateRows(current, manifest); err != nil {
		return nil, err
	}

	const insertManifest = `
		INSERT INTO cscb_repair_manifest (
			tag, state, bucket,
			old_consolidated_object_key_main, old_consolidated_object_version_id,
			old_consolidated_object_etag, old_consolidated_object_bytes,
			start_height, end_height, canonical_block_count, total_block_count, row_set_sha256
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (tag, old_consolidated_object_key_main) DO NOTHING
		RETURNING id`
	var repairID int64
	err = tx.QueryRowContext(
		ctx,
		insertManifest,
		manifest.Tag,
		StatePrepared,
		manifest.Bucket,
		manifest.OldConsolidatedObjectKey,
		manifest.OldConsolidatedObjectVersion.VersionID,
		manifest.OldConsolidatedObjectVersion.ETag,
		manifest.OldConsolidatedObjectVersion.Bytes,
		manifest.StartHeight,
		manifest.EndHeight,
		manifest.CanonicalBlockCount,
		manifest.TotalBlockCount,
		manifest.RowSetSHA256,
	).Scan(&repairID)
	inserted := err == nil
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("failed to insert CSCB repair manifest: %w", err)
	}

	if inserted {
		const insertBlock = `
			INSERT INTO cscb_repair_block (
				repair_id, block_metadata_id, canonical, tag, height, hash,
				single_block_object_key_main, single_block_object_version_id,
				single_block_object_etag, single_block_object_bytes, payload_sha256,
				old_byte_offset, old_byte_length, old_uncompressed_length
			) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8, $9, $10, $11, $12, $13, $14)`
		for _, block := range manifest.Blocks {
			if _, err := tx.ExecContext(
				ctx,
				insertBlock,
				repairID,
				block.BlockMetadataID,
				block.Canonical,
				block.Tag,
				block.Height,
				block.Hash,
				block.SingleBlockObjectKey,
				block.SingleBlockObjectVersion.VersionID,
				block.SingleBlockObjectVersion.ETag,
				block.SingleBlockObjectVersion.Bytes,
				block.PayloadSHA256,
				block.OldByteOffset,
				block.OldByteLength,
				block.OldUncompressedLength,
			); err != nil {
				return nil, xerrors.Errorf("failed to insert CSCB repair block metadata_id=%d: %w", block.BlockMetadataID, err)
			}
		}
	} else {
		const existingID = `
			SELECT id
			FROM cscb_repair_manifest
			WHERE tag = $1 AND old_consolidated_object_key_main = $2`
		if err := tx.QueryRowContext(ctx, existingID, manifest.Tag, manifest.OldConsolidatedObjectKey).Scan(&repairID); err != nil {
			return nil, xerrors.Errorf("failed to load concurrent CSCB repair manifest: %w", err)
		}
	}

	prepared, err := loadManifest(ctx, tx, repairID, false)
	if err != nil {
		return nil, err
	}
	if !samePreparedManifest(prepared, manifest) {
		return nil, xerrors.Errorf("CSCB repair manifest conflict for old object %q", manifest.OldConsolidatedObjectKey)
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair preparation: %w", err)
	}
	return prepared, nil
}

func (r *PostgresRepository) BindExecutionKey(
	ctx context.Context,
	executionKey string,
	repairID int64,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if !validExecutionKey(executionKey) || repairID <= 0 {
		return nil, xerrors.New("valid CSCB repair execution binding is required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair execution binding: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insert = `
		INSERT INTO cscb_repair_execution (execution_key, repair_id)
		VALUES ($1, $2)
		ON CONFLICT (execution_key) DO NOTHING`
	if _, err := tx.ExecContext(ctx, insert, executionKey, repairID); err != nil {
		return nil, xerrors.Errorf("failed to bind CSCB repair execution: %w", err)
	}
	var boundRepairID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT repair_id
		FROM cscb_repair_execution
		WHERE execution_key = $1
		FOR UPDATE`, executionKey).Scan(&boundRepairID); err != nil {
		return nil, xerrors.Errorf("failed to load CSCB repair execution binding: %w", err)
	}
	if !boundRepairID.Valid {
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit CSCB repair no-candidate execution binding: %w", err)
		}
		return nil, nil
	}
	if boundRepairID.Int64 != repairID {
		return nil, xerrors.Errorf(
			"CSCB repair execution key is already bound to repair id %d, requested %d",
			boundRepairID.Int64,
			repairID,
		)
	}
	manifest, err := loadManifest(ctx, tx, boundRepairID.Int64, false)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair execution binding: %w", err)
	}
	return manifest, nil
}

func (r *PostgresRepository) BindNoCandidateExecution(
	ctx context.Context,
	executionKey string,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if !validExecutionKey(executionKey) {
		return nil, xerrors.New("valid CSCB repair execution key is required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair no-candidate execution binding: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO cscb_repair_execution (execution_key, repair_id)
		VALUES ($1, NULL)
		ON CONFLICT (execution_key) DO NOTHING`, executionKey); err != nil {
		return nil, xerrors.Errorf("failed to bind CSCB repair no-candidate execution: %w", err)
	}
	var boundRepairID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT repair_id
		FROM cscb_repair_execution
		WHERE execution_key = $1
		FOR UPDATE`, executionKey).Scan(&boundRepairID); err != nil {
		return nil, xerrors.Errorf("failed to load CSCB repair no-candidate execution binding: %w", err)
	}
	var manifest *Manifest
	if boundRepairID.Valid {
		manifest, err = loadManifest(ctx, tx, boundRepairID.Int64, false)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair no-candidate execution binding: %w", err)
	}
	return manifest, nil
}

func (r *PostgresRepository) Get(ctx context.Context, repairID int64) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	return loadManifest(ctx, r.db, repairID, false)
}

func (r *PostgresRepository) RestoreToSingleBlock(
	ctx context.Context,
	repairID int64,
	validate func(*Manifest) error,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair restore: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	manifest, err := loadManifest(ctx, tx, repairID, true)
	if err != nil {
		return nil, err
	}
	if manifest.State != StatePrepared {
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit idempotent CSCB repair restore: %w", err)
		}
		return manifest, nil
	}
	if err := lockCandidateRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	current, err := loadCandidateByKey(ctx, tx, manifest.Tag, manifest.OldConsolidatedObjectKey)
	if err != nil {
		return nil, err
	}
	if err := compareCandidateRows(current, manifest); err != nil {
		return nil, err
	}

	const unrelatedMissing = `
		SELECT bm.id
		FROM canonical_blocks cb
		JOIN block_metadata bm ON bm.id = cb.block_metadata_id
			AND bm.tag = cb.tag
			AND bm.height = cb.height
		WHERE cb.tag = $1
			AND cb.height >= $2
			AND cb.height < $3
			AND bm.skipped = FALSE
			AND bm.object_format = $4
			AND bm.byte_length IS NULL
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND NOT EXISTS (
				SELECT 1 FROM cscb_repair_block block
				WHERE block.repair_id = $5 AND block.block_metadata_id = bm.id
			)
			AND NOT EXISTS (
				SELECT 1 FROM block_consolidation_shadow shadow
				WHERE shadow.block_metadata_id = bm.id
					AND shadow.single_block_object_key_main = bm.object_key_main
					AND shadow.validated_at IS NOT NULL
			)
		LIMIT 1`
	var unrelatedID int64
	err = tx.QueryRowContext(
		ctx,
		unrelatedMissing,
		manifest.Tag,
		manifest.StartHeight,
		manifest.EndHeight,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
		manifest.ID,
	).Scan(&unrelatedID)
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("failed to check CSCB repair range isolation: %w", err)
	}
	if err == nil {
		return nil, xerrors.Errorf("CSCB repair range contains unrelated unconsolidated metadata_id=%d", unrelatedID)
	}

	if validate != nil {
		if err := validate(manifest); err != nil {
			return nil, err
		}
	}

	const restoreMetadata = `
		UPDATE block_metadata bm
		SET object_key_main = block.single_block_object_key_main,
			object_format = $2,
			byte_offset = NULL,
			byte_length = NULL,
			uncompressed_length = NULL
		FROM cscb_repair_block block
		WHERE block.repair_id = $1
			AND bm.id = block.block_metadata_id
			AND bm.tag = block.tag
			AND bm.height = block.height
			AND bm.hash IS NOT DISTINCT FROM block.hash
			AND bm.skipped = FALSE
			AND bm.single_block_retention_fenced_at IS NULL
			AND bm.object_key_main = $3
			AND bm.object_format = $4
			AND bm.byte_offset = block.old_byte_offset
			AND bm.byte_length = block.old_byte_length
			AND bm.uncompressed_length = block.old_uncompressed_length`
	result, err := tx.ExecContext(
		ctx,
		restoreMetadata,
		manifest.ID,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
		manifest.OldConsolidatedObjectKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to restore active single-block metadata: %w", err)
	}
	if err := requireRowsAffected(result, manifest.TotalBlockCount, "restore active single-block metadata"); err != nil {
		return nil, err
	}

	const deleteOldShadows = `
		DELETE FROM block_consolidation_shadow shadow
		USING cscb_repair_block block
		WHERE block.repair_id = $1
			AND shadow.block_metadata_id = block.block_metadata_id
			AND shadow.tag = block.tag
			AND shadow.height = block.height
			AND shadow.hash IS NOT DISTINCT FROM block.hash
			AND shadow.single_block_object_key_main = block.single_block_object_key_main
			AND shadow.single_block_object_deleted_at IS NULL
			AND shadow.consolidated_object_key_main = $2
			AND shadow.object_format = $3
			AND shadow.byte_offset = block.old_byte_offset
			AND shadow.byte_length = block.old_byte_length
			AND shadow.uncompressed_length = block.old_uncompressed_length`
	result, err = tx.ExecContext(
		ctx,
		deleteOldShadows,
		manifest.ID,
		manifest.OldConsolidatedObjectKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to remove invalidated CSCB shadows: %w", err)
	}
	if err := requireRowsAffected(result, manifest.TotalBlockCount, "remove invalidated CSCB shadows"); err != nil {
		return nil, err
	}

	const markRestored = `
		UPDATE cscb_repair_manifest
		SET state = $2,
			restored_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $1 AND state = $3`
	result, err = tx.ExecContext(ctx, markRestored, manifest.ID, StateRestored, StatePrepared)
	if err != nil {
		return nil, xerrors.Errorf("failed to mark CSCB repair restored: %w", err)
	}
	if err := requireRowsAffected(result, 1, "mark CSCB repair restored"); err != nil {
		return nil, err
	}
	restored, err := loadManifest(ctx, tx, manifest.ID, false)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair restore: %w", err)
	}
	return restored, nil
}

func (r *PostgresRepository) GetRebuilt(ctx context.Context, repairID int64) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	manifest, err := loadManifest(ctx, r.db, repairID, false)
	if err != nil {
		return nil, err
	}
	if err := loadRebuiltBlocks(ctx, r.db, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func (r *PostgresRepository) RecordVerified(
	ctx context.Context,
	repairID int64,
	objectKey string,
	object ObjectVersion,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if objectKey == "" || object.VersionID == "" || object.ETag == "" || object.Bytes == 0 {
		return nil, xerrors.New("verified CSCB object identity is required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin recording verified CSCB repair: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	manifest, err := loadManifest(ctx, tx, repairID, true)
	if err != nil {
		return nil, err
	}
	if manifest.State == StateVerified || manifest.State == StateCompleted {
		if manifest.NewConsolidatedObjectKey != objectKey ||
			manifest.NewConsolidatedObjectVersion.VersionID != object.VersionID ||
			manifest.NewConsolidatedObjectVersion.ETag != object.ETag ||
			manifest.NewConsolidatedObjectVersion.Bytes != object.Bytes {
			return nil, xerrors.Errorf("verified CSCB repair identity changed for repair id %d", manifest.ID)
		}
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit idempotent verified CSCB repair: %w", err)
		}
		return manifest, nil
	}
	if manifest.State != StateRestored {
		return nil, xerrors.Errorf("CSCB repair must be restored before verification: id=%d state=%s", manifest.ID, manifest.State)
	}
	if err := lockRepairRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	if err := lockConsolidatedObjectKey(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}
	if err := loadRebuiltBlocks(ctx, tx, manifest); err != nil {
		return nil, err
	}
	if err := validateRebuiltMetadata(manifest, objectKey); err != nil {
		return nil, err
	}
	if err := requireNoOldReferences(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}

	const update = `
		UPDATE cscb_repair_manifest
		SET state = $2,
			new_consolidated_object_key_main = $3,
			new_consolidated_object_version_id = $4,
			new_consolidated_object_etag = $5,
			new_consolidated_object_bytes = $6,
			verified_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $1 AND state = $7`
	result, err := tx.ExecContext(
		ctx,
		update,
		manifest.ID,
		StateVerified,
		objectKey,
		object.VersionID,
		object.ETag,
		object.Bytes,
		StateRestored,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to record verified CSCB repair: %w", err)
	}
	if err := requireRowsAffected(result, 1, "record verified CSCB repair"); err != nil {
		return nil, err
	}
	verified, err := loadManifest(ctx, tx, manifest.ID, false)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit verified CSCB repair: %w", err)
	}
	return verified, nil
}

func (r *PostgresRepository) CompleteWithOldObjectDeletion(
	ctx context.Context,
	repairID int64,
	outcome string,
	deleteObject DeleteOldObject,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if outcome == "" || deleteObject == nil {
		return nil, xerrors.New("CSCB repair completion outcome and old-object deletion are required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	manifest, err := loadManifest(ctx, tx, repairID, true)
	if err != nil {
		return nil, err
	}
	if manifest.State == StateCompleted {
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit idempotent CSCB repair completion: %w", err)
		}
		return manifest, nil
	}
	if manifest.State != StateVerified {
		return nil, xerrors.Errorf("CSCB repair must be verified before completion: id=%d state=%s", manifest.ID, manifest.State)
	}
	if err := lockRepairRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	if err := lockConsolidatedObjectKey(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}
	if err := requireNoOldReferences(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}
	if err := deleteObject(manifest); err != nil {
		return nil, err
	}
	const update = `
		UPDATE cscb_repair_manifest
		SET state = $2,
			outcome = $3,
			old_object_deleted_at = clock_timestamp(),
			completed_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $1 AND state = $4`
	result, err := tx.ExecContext(ctx, update, manifest.ID, StateCompleted, outcome, StateVerified)
	if err != nil {
		return nil, xerrors.Errorf("failed to complete CSCB repair: %w", err)
	}
	if err := requireRowsAffected(result, 1, "complete CSCB repair"); err != nil {
		return nil, err
	}
	completed, err := loadManifest(ctx, tx, manifest.ID, false)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair completion: %w", err)
	}
	return completed, nil
}

func (r *PostgresRepository) validateDB() error {
	if r == nil || r.db == nil {
		return xerrors.New("postgres db is required for CSCB repair")
	}
	return nil
}

func loadCandidateByKey(ctx context.Context, q queryer, tag uint32, objectKey string) (*Manifest, error) {
	const query = `
		SELECT
			bm.id,
			EXISTS (
				SELECT 1 FROM canonical_blocks cb
				WHERE cb.block_metadata_id = bm.id AND cb.tag = bm.tag AND cb.height = bm.height
			) AS canonical,
			bm.tag,
			bm.height,
			COALESCE(bm.hash, ''),
			bm.skipped,
			(bm.single_block_retention_fenced_at IS NOT NULL),
			(retirement.block_metadata_id IS NOT NULL),
			COALESCE(shadow.single_block_object_key_main, ''),
			(shadow.single_block_object_deleted_at IS NOT NULL),
			COALESCE(shadow.consolidated_object_key_main, ''),
			bm.byte_offset,
			bm.byte_length,
			bm.uncompressed_length
		FROM block_metadata bm
		LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		LEFT JOIN block_single_block_retention retirement ON retirement.block_metadata_id = bm.id
		WHERE bm.tag = $1
			AND bm.object_key_main = $2
			AND bm.object_format = $3
		ORDER BY bm.height ASC, bm.id ASC`
	rows, err := q.QueryContext(ctx, query, tag, objectKey, api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH)
	if err != nil {
		return nil, xerrors.Errorf("failed to query active CSCB repair rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	manifest := &Manifest{
		Tag:                      tag,
		OldConsolidatedObjectKey: objectKey,
		StartHeight:              ^uint64(0),
	}
	for rows.Next() {
		var block Block
		var oldOffset, oldLength, oldUncompressed sql.NullInt64
		if err := rows.Scan(
			&block.BlockMetadataID,
			&block.Canonical,
			&block.Tag,
			&block.Height,
			&block.Hash,
			&block.Skipped,
			&block.RetirementFenced,
			&block.RetirementManifestExists,
			&block.SingleBlockObjectKey,
			&block.SingleBlockObjectDeleted,
			&block.OldConsolidatedObjectKey,
			&oldOffset,
			&oldLength,
			&oldUncompressed,
		); err != nil {
			return nil, xerrors.Errorf("failed to scan active CSCB repair row: %w", err)
		}
		if oldOffset.Valid && oldOffset.Int64 >= 0 {
			block.OldByteOffset = uint64(oldOffset.Int64)
		}
		if oldLength.Valid && oldLength.Int64 >= 0 {
			block.OldByteLength = uint64(oldLength.Int64)
		}
		if oldUncompressed.Valid && oldUncompressed.Int64 >= 0 {
			block.OldUncompressedLength = uint64(oldUncompressed.Int64)
		}
		manifest.Blocks = append(manifest.Blocks, block)
		manifest.TotalBlockCount++
		if block.Canonical {
			manifest.CanonicalBlockCount++
		}
		if block.Height < manifest.StartHeight {
			manifest.StartHeight = block.Height
		}
		if block.Height == ^uint64(0) {
			return nil, xerrors.New("CSCB repair row height overflows exclusive end height")
		}
		if block.Height+1 > manifest.EndHeight {
			manifest.EndHeight = block.Height + 1
		}
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate active CSCB repair rows: %w", err)
	}
	if len(manifest.Blocks) == 0 {
		return nil, xerrors.Errorf("active CSCB repair object %q has no metadata references", objectKey)
	}
	return manifest, nil
}

func loadManifest(ctx context.Context, q queryer, repairID int64, forUpdate bool) (*Manifest, error) {
	suffix := ""
	if forUpdate {
		suffix = " FOR UPDATE"
	}
	query := fmt.Sprintf(`SELECT %s FROM cscb_repair_manifest WHERE id = $1%s`, manifestColumns, suffix)
	manifest, err := scanManifest(q.QueryRowContext(ctx, query, repairID))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, xerrors.Errorf("CSCB repair manifest not found: id=%d", repairID)
		}
		return nil, xerrors.Errorf("failed to load CSCB repair manifest: %w", err)
	}
	if err := loadManifestBlocks(ctx, q, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func scanManifest(row rowScanner) (*Manifest, error) {
	var manifest Manifest
	var state string
	var newKey, newVersionID, newETag sql.NullString
	var newBytes sql.NullInt64
	var restoredAt, verifiedAt, oldDeletedAt, completedAt sql.NullTime
	if err := row.Scan(
		&manifest.ID,
		&manifest.Tag,
		&state,
		&manifest.Bucket,
		&manifest.OldConsolidatedObjectKey,
		&manifest.OldConsolidatedObjectVersion.VersionID,
		&manifest.OldConsolidatedObjectVersion.ETag,
		&manifest.OldConsolidatedObjectVersion.Bytes,
		&manifest.StartHeight,
		&manifest.EndHeight,
		&manifest.CanonicalBlockCount,
		&manifest.TotalBlockCount,
		&manifest.RowSetSHA256,
		&newKey,
		&newVersionID,
		&newETag,
		&newBytes,
		&manifest.Outcome,
		&manifest.PreparedAt,
		&restoredAt,
		&verifiedAt,
		&oldDeletedAt,
		&completedAt,
	); err != nil {
		return nil, err
	}
	manifest.State = State(state)
	manifest.NewConsolidatedObjectKey = newKey.String
	manifest.NewConsolidatedObjectVersion.VersionID = newVersionID.String
	manifest.NewConsolidatedObjectVersion.ETag = newETag.String
	if newBytes.Valid && newBytes.Int64 > 0 {
		manifest.NewConsolidatedObjectVersion.Bytes = uint64(newBytes.Int64)
	}
	manifest.RestoredAt = nullableTime(restoredAt)
	manifest.VerifiedAt = nullableTime(verifiedAt)
	manifest.OldObjectDeletedAt = nullableTime(oldDeletedAt)
	manifest.CompletedAt = nullableTime(completedAt)
	return &manifest, nil
}

func loadManifestBlocks(ctx context.Context, q queryer, manifest *Manifest) error {
	const query = `
		SELECT block_metadata_id, canonical, tag, height, COALESCE(hash, ''),
			single_block_object_key_main, single_block_object_version_id,
			single_block_object_etag, single_block_object_bytes, payload_sha256,
			old_byte_offset, old_byte_length, old_uncompressed_length
		FROM cscb_repair_block
		WHERE repair_id = $1
		ORDER BY height ASC, block_metadata_id ASC`
	rows, err := q.QueryContext(ctx, query, manifest.ID)
	if err != nil {
		return xerrors.Errorf("failed to query CSCB repair blocks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	manifest.Blocks = nil
	for rows.Next() {
		var block Block
		block.OldConsolidatedObjectKey = manifest.OldConsolidatedObjectKey
		if err := rows.Scan(
			&block.BlockMetadataID,
			&block.Canonical,
			&block.Tag,
			&block.Height,
			&block.Hash,
			&block.SingleBlockObjectKey,
			&block.SingleBlockObjectVersion.VersionID,
			&block.SingleBlockObjectVersion.ETag,
			&block.SingleBlockObjectVersion.Bytes,
			&block.PayloadSHA256,
			&block.OldByteOffset,
			&block.OldByteLength,
			&block.OldUncompressedLength,
		); err != nil {
			return xerrors.Errorf("failed to scan CSCB repair block: %w", err)
		}
		manifest.Blocks = append(manifest.Blocks, block)
	}
	if err := rows.Err(); err != nil {
		return xerrors.Errorf("failed to iterate CSCB repair blocks: %w", err)
	}
	if manifest.ID != 0 && uint64(len(manifest.Blocks)) != manifest.TotalBlockCount {
		return xerrors.Errorf(
			"CSCB repair manifest block count mismatch: id=%d expected=%d actual=%d",
			manifest.ID,
			manifest.TotalBlockCount,
			len(manifest.Blocks),
		)
	}
	return nil
}

func loadRebuiltBlocks(ctx context.Context, q queryer, manifest *Manifest) error {
	const query = `
		SELECT
			block.block_metadata_id,
			EXISTS (
				SELECT 1 FROM canonical_blocks cb
				WHERE cb.block_metadata_id = bm.id AND cb.tag = bm.tag AND cb.height = bm.height
			) AS canonical,
			COALESCE(bm.object_key_main, ''),
			bm.object_format,
			bm.byte_offset,
			bm.byte_length,
			bm.uncompressed_length,
			COALESCE(shadow.consolidated_object_key_main, ''),
			shadow.validated_at,
			shadow.single_block_retention_started_at,
			shadow.single_block_delete_after
		FROM cscb_repair_block block
		JOIN block_metadata bm ON bm.id = block.block_metadata_id
		LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		WHERE block.repair_id = $1
		ORDER BY block.height ASC, block.block_metadata_id ASC`
	rows, err := q.QueryContext(ctx, query, manifest.ID)
	if err != nil {
		return xerrors.Errorf("failed to query rebuilt CSCB repair rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	blocksByID := make(map[int64]*Block, len(manifest.Blocks))
	for i := range manifest.Blocks {
		blocksByID[manifest.Blocks[i].BlockMetadataID] = &manifest.Blocks[i]
	}
	seen := 0
	for rows.Next() {
		var id int64
		var canonical bool
		var activeOffset, activeLength, activeUncompressed sql.NullInt64
		var validatedAt, retentionStartedAt, deleteAfter sql.NullTime
		var activeKey, newKey string
		var objectFormat int32
		if err := rows.Scan(
			&id,
			&canonical,
			&activeKey,
			&objectFormat,
			&activeOffset,
			&activeLength,
			&activeUncompressed,
			&newKey,
			&validatedAt,
			&retentionStartedAt,
			&deleteAfter,
		); err != nil {
			return xerrors.Errorf("failed to scan rebuilt CSCB repair row: %w", err)
		}
		block, ok := blocksByID[id]
		if !ok {
			return xerrors.Errorf("rebuilt CSCB repair returned unexpected metadata_id=%d", id)
		}
		if canonical != block.Canonical {
			return xerrors.Errorf("canonical identity changed during CSCB repair for metadata_id=%d", id)
		}
		block.ActiveObjectKey = activeKey
		block.ActiveObjectFormat = objectFormat
		block.NewConsolidatedObjectKey = newKey
		block.NewByteOffset = nullableUint64(activeOffset)
		block.NewByteLength = nullableUint64(activeLength)
		block.NewUncompressedLength = nullableUint64(activeUncompressed)
		block.NewValidatedAt = nullableTime(validatedAt)
		block.NewRetentionStartedAt = nullableTime(retentionStartedAt)
		block.NewSingleBlockDeleteAfter = nullableTime(deleteAfter)
		seen++
	}
	if err := rows.Err(); err != nil {
		return xerrors.Errorf("failed to iterate rebuilt CSCB repair rows: %w", err)
	}
	if uint64(seen) != manifest.TotalBlockCount {
		return xerrors.Errorf("rebuilt CSCB repair row count mismatch: expected=%d actual=%d", manifest.TotalBlockCount, seen)
	}
	return nil
}

func lockCandidateRows(ctx context.Context, tx *sql.Tx, blocks []Block) error {
	ordered := append([]Block(nil), blocks...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Tag != ordered[j].Tag {
			return ordered[i].Tag < ordered[j].Tag
		}
		if ordered[i].Height != ordered[j].Height {
			return ordered[i].Height < ordered[j].Height
		}
		if ordered[i].Hash != ordered[j].Hash {
			return ordered[i].Hash < ordered[j].Hash
		}
		return ordered[i].BlockMetadataID < ordered[j].BlockMetadataID
	})
	ids := make([]int64, 0, len(ordered))
	for _, block := range ordered {
		if err := retirementlock.Acquire(ctx, tx, block.Tag, block.Height, block.Hash); err != nil {
			return err
		}
		ids = append(ids, block.BlockMetadataID)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT bm.id
		FROM block_metadata bm
		JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		WHERE bm.id = ANY($1)
		ORDER BY bm.id
		FOR UPDATE OF bm, shadow`, pq.Array(ids))
	if err != nil {
		return xerrors.Errorf("failed to lock CSCB repair metadata rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return xerrors.Errorf("failed to iterate locked CSCB repair rows: %w", err)
	}
	if count != len(ids) {
		return xerrors.Errorf("CSCB repair lock count mismatch: expected=%d actual=%d", len(ids), count)
	}
	return nil
}

func lockConsolidatedObjectKey(ctx context.Context, tx *sql.Tx, objectKey string) error {
	if objectKey == "" {
		return xerrors.New("consolidated object key is required for CSCB repair lock")
	}
	if _, err := tx.ExecContext(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 1))`,
		objectKey,
	); err != nil {
		return xerrors.Errorf("failed to acquire CSCB repair object lock: %w", err)
	}
	return nil
}

func lockRepairRows(ctx context.Context, tx *sql.Tx, blocks []Block) error {
	ordered := append([]Block(nil), blocks...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Tag != ordered[j].Tag {
			return ordered[i].Tag < ordered[j].Tag
		}
		if ordered[i].Height != ordered[j].Height {
			return ordered[i].Height < ordered[j].Height
		}
		if ordered[i].Hash != ordered[j].Hash {
			return ordered[i].Hash < ordered[j].Hash
		}
		return ordered[i].BlockMetadataID < ordered[j].BlockMetadataID
	})
	ids := make([]int64, 0, len(ordered))
	for _, block := range ordered {
		if err := retirementlock.Acquire(ctx, tx, block.Tag, block.Height, block.Hash); err != nil {
			return err
		}
		ids = append(ids, block.BlockMetadataID)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM block_metadata
		WHERE id = ANY($1)
		ORDER BY id
		FOR UPDATE`, pq.Array(ids))
	if err != nil {
		return xerrors.Errorf("failed to lock rebuilt block metadata: %w", err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return xerrors.Errorf("failed to iterate locked rebuilt block metadata: %w", err)
	}
	if err := rows.Close(); err != nil {
		return xerrors.Errorf("failed to close rebuilt block metadata rows: %w", err)
	}
	if count != len(ids) {
		return xerrors.Errorf("rebuilt metadata lock count mismatch: expected=%d actual=%d", len(ids), count)
	}
	shadowRows, err := tx.QueryContext(ctx, `
		SELECT block_metadata_id FROM block_consolidation_shadow
		WHERE block_metadata_id = ANY($1)
		ORDER BY block_metadata_id
		FOR UPDATE`, pq.Array(ids))
	if err != nil {
		return xerrors.Errorf("failed to lock rebuilt consolidation shadows: %w", err)
	}
	defer func() { _ = shadowRows.Close() }()
	for shadowRows.Next() {
	}
	return shadowRows.Err()
}

func compareCandidateRows(current *Manifest, pinned *Manifest) error {
	if current == nil || pinned == nil {
		return xerrors.New("CSCB repair candidate is required")
	}
	if current.Tag != pinned.Tag ||
		current.OldConsolidatedObjectKey != pinned.OldConsolidatedObjectKey ||
		current.StartHeight != pinned.StartHeight ||
		current.EndHeight != pinned.EndHeight ||
		current.CanonicalBlockCount != pinned.CanonicalBlockCount ||
		current.TotalBlockCount != pinned.TotalBlockCount ||
		len(current.Blocks) != len(pinned.Blocks) {
		return xerrors.Errorf("active CSCB repair candidate changed for old object %q", pinned.OldConsolidatedObjectKey)
	}
	for i := range pinned.Blocks {
		a := current.Blocks[i]
		b := pinned.Blocks[i]
		if a.BlockMetadataID != b.BlockMetadataID ||
			a.Canonical != b.Canonical ||
			a.Tag != b.Tag ||
			a.Height != b.Height ||
			a.Hash != b.Hash ||
			a.Skipped ||
			a.RetirementFenced ||
			a.RetirementManifestExists ||
			a.SingleBlockObjectKey != b.SingleBlockObjectKey ||
			a.SingleBlockObjectDeleted ||
			a.OldConsolidatedObjectKey != b.OldConsolidatedObjectKey ||
			a.OldByteOffset != b.OldByteOffset ||
			a.OldByteLength != b.OldByteLength ||
			a.OldUncompressedLength != b.OldUncompressedLength {
			return xerrors.Errorf("active CSCB repair row changed for metadata_id=%d", b.BlockMetadataID)
		}
	}
	return nil
}

func validateRebuiltMetadata(manifest *Manifest, objectKey string) error {
	if manifest.RestoredAt == nil {
		return xerrors.Errorf("CSCB repair restore timestamp is missing: id=%d", manifest.ID)
	}
	canonicalCount := uint64(0)
	for _, block := range manifest.Blocks {
		if !block.Canonical {
			if block.ActiveObjectKey != block.SingleBlockObjectKey ||
				block.ActiveObjectFormat != int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK) ||
				block.NewConsolidatedObjectKey != "" ||
				block.NewByteOffset != 0 ||
				block.NewByteLength != 0 ||
				block.NewUncompressedLength != 0 ||
				block.NewValidatedAt != nil ||
				block.NewRetentionStartedAt != nil ||
				block.NewSingleBlockDeleteAfter != nil {
				return xerrors.Errorf("non-canonical row did not remain on single-block storage: metadata_id=%d", block.BlockMetadataID)
			}
			continue
		}
		canonicalCount++
		if block.ActiveObjectKey != objectKey ||
			block.NewConsolidatedObjectKey != objectKey ||
			block.ActiveObjectFormat != int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH) ||
			block.NewByteLength == 0 ||
			block.NewUncompressedLength == 0 ||
			block.NewValidatedAt == nil ||
			block.NewRetentionStartedAt == nil ||
			block.NewSingleBlockDeleteAfter == nil ||
			block.NewRetentionStartedAt.Before(*manifest.RestoredAt) ||
			!block.NewSingleBlockDeleteAfter.After(*block.NewRetentionStartedAt) {
			return xerrors.Errorf("canonical row is not on a freshly promoted CSCB: metadata_id=%d", block.BlockMetadataID)
		}
		if block.NewSingleBlockDeleteAfter.Before(block.NewRetentionStartedAt.Add(72 * time.Hour)) {
			return xerrors.Errorf(
				"canonical row does not have the required 72-hour single-block retention: metadata_id=%d",
				block.BlockMetadataID,
			)
		}
	}
	if canonicalCount != manifest.CanonicalBlockCount {
		return xerrors.Errorf("rebuilt canonical block count mismatch: expected=%d actual=%d", manifest.CanonicalBlockCount, canonicalCount)
	}
	if objectKey == manifest.OldConsolidatedObjectKey {
		return xerrors.Errorf("rebuilt CSCB reused dirty object key %q", objectKey)
	}
	return nil
}

func requireNoOldReferences(ctx context.Context, q queryer, objectKey string) error {
	const query = `
		SELECT
			(SELECT COUNT(*) FROM block_metadata WHERE object_key_main = $1)
			+
			(SELECT COUNT(*) FROM block_consolidation_shadow WHERE consolidated_object_key_main = $1)`
	var references uint64
	if err := q.QueryRowContext(ctx, query, objectKey).Scan(&references); err != nil {
		return xerrors.Errorf("failed to count old CSCB references: %w", err)
	}
	if references != 0 {
		return xerrors.Errorf("old CSCB %q still has %d database references", objectKey, references)
	}
	return nil
}

func validatePreparedInput(manifest *Manifest) error {
	if manifest == nil || manifest.Tag == 0 || manifest.Bucket == "" ||
		manifest.OldConsolidatedObjectKey == "" ||
		manifest.OldConsolidatedObjectVersion.VersionID == "" ||
		manifest.OldConsolidatedObjectVersion.ETag == "" ||
		manifest.OldConsolidatedObjectVersion.Bytes == 0 ||
		manifest.EndHeight <= manifest.StartHeight ||
		manifest.CanonicalBlockCount == 0 ||
		manifest.TotalBlockCount != uint64(len(manifest.Blocks)) ||
		len(manifest.RowSetSHA256) != 64 {
		return xerrors.New("complete pinned CSCB repair manifest is required")
	}
	for _, block := range manifest.Blocks {
		if block.BlockMetadataID == 0 || block.Tag != manifest.Tag || block.SingleBlockObjectKey == "" ||
			block.SingleBlockObjectVersion.VersionID == "" || block.SingleBlockObjectVersion.ETag == "" ||
			block.SingleBlockObjectVersion.Bytes == 0 || len(block.PayloadSHA256) != 64 ||
			block.OldConsolidatedObjectKey != manifest.OldConsolidatedObjectKey ||
			block.OldByteLength == 0 || block.OldUncompressedLength == 0 {
			return xerrors.Errorf("incomplete pinned CSCB repair block metadata_id=%d", block.BlockMetadataID)
		}
	}
	return nil
}

func samePreparedManifest(actual *Manifest, expected *Manifest) bool {
	if actual == nil || expected == nil ||
		actual.Tag != expected.Tag ||
		actual.State != StatePrepared ||
		actual.Bucket != expected.Bucket ||
		actual.OldConsolidatedObjectKey != expected.OldConsolidatedObjectKey ||
		actual.OldConsolidatedObjectVersion != expected.OldConsolidatedObjectVersion ||
		actual.StartHeight != expected.StartHeight ||
		actual.EndHeight != expected.EndHeight ||
		actual.CanonicalBlockCount != expected.CanonicalBlockCount ||
		actual.TotalBlockCount != expected.TotalBlockCount ||
		actual.RowSetSHA256 != expected.RowSetSHA256 ||
		len(actual.Blocks) != len(expected.Blocks) {
		return false
	}
	for i := range actual.Blocks {
		a := actual.Blocks[i]
		b := expected.Blocks[i]
		if a.BlockMetadataID != b.BlockMetadataID || a.Canonical != b.Canonical ||
			a.Tag != b.Tag || a.Height != b.Height || a.Hash != b.Hash ||
			a.SingleBlockObjectKey != b.SingleBlockObjectKey ||
			a.SingleBlockObjectVersion != b.SingleBlockObjectVersion ||
			a.PayloadSHA256 != b.PayloadSHA256 ||
			a.OldByteOffset != b.OldByteOffset || a.OldByteLength != b.OldByteLength ||
			a.OldUncompressedLength != b.OldUncompressedLength {
			return false
		}
	}
	return true
}

func requireRowsAffected(result sql.Result, expected uint64, action string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return xerrors.Errorf("failed to inspect %s result: %w", action, err)
	}
	if uint64(rows) != expected {
		return xerrors.Errorf("%s guard failed: expected=%d actual=%d", action, expected, rows)
	}
	return nil
}

func nullableUint64(value sql.NullInt64) uint64 {
	if !value.Valid || value.Int64 < 0 {
		return 0
	}
	return uint64(value.Int64)
}

func nullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}
