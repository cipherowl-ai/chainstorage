package retirement

import (
	"context"
	"database/sql"

	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

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

	const query = `
		SELECT
			bm.id,
			bm.tag,
			bm.height,
			COALESCE(bm.hash, ''),
			bm.skipped,
			COALESCE(bm.object_key_main, ''),
			COALESCE(shadow.legacy_object_key_main, bm.object_key_main, ''),
			bm.object_format,
			bm.byte_offset,
			bm.byte_length,
			bm.uncompressed_length,
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
			shadow.format_version
		FROM canonical_blocks cb
		JOIN block_metadata bm ON bm.id = cb.block_metadata_id
		LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		WHERE cb.tag = $1
			AND cb.height >= $2
			AND cb.height < $3
		ORDER BY cb.height ASC
		LIMIT $4`

	rows, err := r.db.QueryContext(ctx, query, tag, startHeight, endHeight, limit)
	if err != nil {
		return nil, xerrors.Errorf("failed to query retirement metadata rows: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	result := make([]MetadataRow, 0)
	for rows.Next() {
		var row MetadataRow
		var primaryObjectFormat int64
		var primaryByteOffset sql.NullInt64
		var primaryByteLength sql.NullInt64
		var primaryUncompressedLength sql.NullInt64
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
		var shadowFormatVersion sql.NullInt64
		if err := rows.Scan(
			&row.BlockMetadataID,
			&row.Tag,
			&row.Height,
			&row.Hash,
			&row.Skipped,
			&row.PrimaryObjectKey,
			&row.LegacyObjectKey,
			&primaryObjectFormat,
			&primaryByteOffset,
			&primaryByteLength,
			&primaryUncompressedLength,
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
			&shadowFormatVersion,
		); err != nil {
			return nil, xerrors.Errorf("failed to scan retirement metadata row: %w", err)
		}
		row.PrimaryObjectFormat = api.BlockObjectFormat(primaryObjectFormat)
		row.PrimaryByteOffset = nullableUint64(primaryByteOffset)
		row.PrimaryByteLength = nullableUint64(primaryByteLength)
		row.PrimaryUncompressedLength = nullableUint64(primaryUncompressedLength)
		if shadowTag.Valid {
			shadow := &ConsolidationShadow{
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
			if shadowValidatedAt.Valid {
				value := shadowValidatedAt.Time
				shadow.ValidatedAt = &value
			}
			if shadowLegacyObjectRetiredAt.Valid {
				value := shadowLegacyObjectRetiredAt.Time
				shadow.LegacyObjectRetiredAt = &value
			}
			if shadowLegacyObjectRetireAfter.Valid {
				value := shadowLegacyObjectRetireAfter.Time
				shadow.LegacyObjectRetireAfter = &value
			}
			row.Shadow = shadow
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate retirement metadata rows: %w", err)
	}
	return result, nil
}

func nullableUint64(value sql.NullInt64) uint64 {
	if !value.Valid {
		return 0
	}
	return uint64(value.Int64)
}
