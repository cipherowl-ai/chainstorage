package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres/model"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	blockStorageImpl struct {
		db                               *sql.DB
		blockStartHeight                 uint64
		instrumentPersistBlockMetas      instrument.Instrument
		instrumentGetLatestBlock         instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlockByHash         instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlockByHeight       instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlocksByHeightRange instrument.InstrumentWithResult[[]*api.BlockMetadata]
		instrumentGetBlocksByHeights     instrument.InstrumentWithResult[[]*api.BlockMetadata]
		instrumentGetBlockByTimestamp    instrument.InstrumentWithResult[*api.BlockMetadata]
	}
)

const (
	blockMetadataColumns = `
		id, height, tag, hash, parent_hash, parent_height, object_key_main,
		timestamp, skipped, object_format, byte_offset, byte_length, uncompressed_length`
	canonicalBlockMetadataColumns = `
		bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.parent_height, bm.object_key_main,
		bm.timestamp, bm.skipped, bm.object_format, bm.byte_offset, bm.byte_length, bm.uncompressed_length`
)

func blockMetadataByHashQuery() string {
	return fmt.Sprintf(`
				SELECT %s
				FROM block_metadata
				WHERE tag = $1 AND height = $2 AND hash = $3 AND skipped = false
				LIMIT 1`, blockMetadataColumns)
}

func newBlockStorage(db *sql.DB, params Params) (internal.BlockStorage, error) {
	metrics := params.Metrics.SubScope("block_storage").Tagged(map[string]string{
		"storage_type": "postgres",
	})
	accessor := blockStorageImpl{
		db:                               db,
		blockStartHeight:                 params.Config.Chain.BlockStartHeight,
		instrumentPersistBlockMetas:      instrument.New(metrics, "persist_block_metas"),
		instrumentGetLatestBlock:         instrument.NewWithResult[*api.BlockMetadata](metrics, "get_latest_block"),
		instrumentGetBlockByHash:         instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_hash"),
		instrumentGetBlockByHeight:       instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_height"),
		instrumentGetBlocksByHeightRange: instrument.NewWithResult[[]*api.BlockMetadata](metrics, "get_blocks_by_height_range"),
		instrumentGetBlocksByHeights:     instrument.NewWithResult[[]*api.BlockMetadata](metrics, "get_blocks_by_heights"),
		instrumentGetBlockByTimestamp:    instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_timestamp"),
	}
	return &accessor, nil
}

func (b *blockStorageImpl) PersistBlockMetas(
	ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
	return b.instrumentPersistBlockMetas.Instrument(ctx, func(ctx context.Context) error {
		if len(blocks) == 0 {
			return nil
		}

		// Sort blocks by height for chain validation.
		// IMPORTANT: When multiple blocks have the same height (e.g., during a reorg), their relative
		// order after sorting is not guaranteed to be stable. However, this implementation follows the
		// "last block wins" principle - the last block processed for a given height will become the
		// canonical block for that height. This behavior is consistent with the DynamoDB implementation
		// where the last block overwrites the canonical entry.
		//
		// The canonical_blocks table uses "ON CONFLICT (height, tag) DO UPDATE" which means:
		// - If multiple blocks in the input have the same height, the last one processed will
		//   overwrite previous entries in canonical_blocks
		// - All blocks are still stored in block_metadata (allowing retrieval by specific hash)
		// - Only the last block for each height becomes the canonical one
		//
		// Callers should ensure that when multiple blocks exist for the same height, the desired
		// canonical block is placed last in the blocks array for that height.
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Height < blocks[j].Height
		})
		if err := parser.ValidateChain(blocks, lastBlock); err != nil {
			return xerrors.Errorf("failed to validate chain: %w", err)
		}

		// Create transaction with timeout context
		// Use a reasonable timeout for block persistence operations
		txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		tx, err := b.db.BeginTx(txCtx, nil)
		if err != nil {
			return xerrors.Errorf("failed to begin transaction: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					// Log the rollback error but don't override the original error
					// In a production environment, you might want to use a proper logger here
					// For now, we'll just ignore the rollback error as it's already a failure case
					_ = rollbackErr
				}
			}
		}()

		// Different queries for skipped vs non-skipped blocks due to different conflict resolution
		blockMetadataSkippedQuery := `
			INSERT INTO block_metadata (
				height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (tag, height) WHERE skipped = true DO UPDATE SET
				hash = EXCLUDED.hash,
				parent_hash = EXCLUDED.parent_hash,
				parent_height = EXCLUDED.parent_height,
				object_key_main = EXCLUDED.object_key_main,
				timestamp = EXCLUDED.timestamp,
				skipped = EXCLUDED.skipped,
				object_format = EXCLUDED.object_format,
				byte_offset = EXCLUDED.byte_offset,
				byte_length = EXCLUDED.byte_length,
				uncompressed_length = EXCLUDED.uncompressed_length
			RETURNING id`

		blockMetadataRegularQuery := `
			INSERT INTO block_metadata (
				height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (tag, hash) WHERE hash IS NOT NULL AND NOT skipped DO UPDATE SET
				parent_hash = EXCLUDED.parent_hash,
				parent_height = EXCLUDED.parent_height,
				object_key_main = EXCLUDED.object_key_main,
				timestamp = EXCLUDED.timestamp,
				skipped = EXCLUDED.skipped,
				object_format = EXCLUDED.object_format,
				byte_offset = EXCLUDED.byte_offset,
				byte_length = EXCLUDED.byte_length,
				uncompressed_length = EXCLUDED.uncompressed_length
			RETURNING id`

		// Simply insert or update canonical blocks like DynamoDB does
		// The "last write wins" behavior matches DynamoDB's TransactWriteItems
		// Chain validation happens in update_watermark activity, not here
		canonicalQuery := `
			INSERT INTO canonical_blocks (height, block_metadata_id, tag)
			VALUES ($1, $2, $3)
			ON CONFLICT (height, tag) DO UPDATE
			SET block_metadata_id = EXCLUDED.block_metadata_id`

		for _, block := range blocks {
			tsProto := block.GetTimestamp()
			var unixTimestamp int64
			if tsProto == nil { // special case for genesis block
				unixTimestamp = 0
			} else {
				unixTimestamp = tsProto.GetSeconds() // directly get seconds from protobuf timestamp
			}

			var parentHeight uint64
			if block.Height == 0 {
				// Genesis block has no parent, set parent height to 0
				parentHeight = 0
			} else {
				parentHeight = block.ParentHeight
			}

			var blockId int64
			var query string
			if block.Skipped {
				query = blockMetadataSkippedQuery
			} else {
				query = blockMetadataRegularQuery
			}

			byteOffset, byteLength, uncompressedLength := blockObjectByteFields(block)
			err = tx.QueryRowContext(txCtx, query,
				block.Height,
				block.Tag,
				block.Hash,
				block.ParentHash,
				parentHeight,
				block.ObjectKeyMain,
				unixTimestamp,
				block.Skipped,
				int32(block.GetObjectFormat()),
				byteOffset,
				byteLength,
				uncompressedLength,
			).Scan(&blockId)
			if err != nil {
				return xerrors.Errorf("failed to insert block metadata for height %d: %w", block.Height, err)
			}

			// Insert into canonical_blocks
			// Always insert/update canonical blocks like DynamoDB does
			_, err = tx.ExecContext(txCtx, canonicalQuery,
				block.Height,
				blockId,
				block.Tag,
			)
			if err != nil {
				return xerrors.Errorf("failed to insert canonical block for height %d: %w", block.Height, err)
			}
		}

		// Update watermark if requested
		// Set is_watermark=TRUE for the highest block to mark it as validated
		// This prevents canonical chain leakage to streamer before validation
		if updateWatermark && len(blocks) > 0 {
			highestBlock := blocks[len(blocks)-1]

			// Probabilistically clear old watermarks (1 in 5000 chance)
			// This prevents unbounded accumulation while keeping the operation rare enough
			// to have negligible performance impact
			if rand.Intn(5000) == 0 {
				// Clear all old watermarks for this tag, keeping only the current one
				// This is safe because only GetLatestBlock uses is_watermark filter,
				// and it only needs the highest watermarked block
				clearQuery := `
					UPDATE canonical_blocks
					SET is_watermark = FALSE
					WHERE tag = $1 AND is_watermark = TRUE`
				_, err = tx.ExecContext(txCtx, clearQuery, highestBlock.Tag)
				if err != nil {
					// Log but don't fail - cleanup is best-effort
					// In production, you might want to use a proper logger here
					_ = err
				}
			}

			// Set the new watermark
			watermarkQuery := `
				UPDATE canonical_blocks
				SET is_watermark = TRUE
				WHERE tag = $1 AND height = $2`
			_, err = tx.ExecContext(txCtx, watermarkQuery, highestBlock.Tag, highestBlock.Height)
			if err != nil {
				return xerrors.Errorf("failed to update watermark for height %d: %w", highestBlock.Height, err)
			}
		}

		// Commit transaction
		err = tx.Commit()
		if err != nil {
			return xerrors.Errorf("failed to commit transaction: %w", err)
		}
		committed = true
		return nil
	})
}

func (b *blockStorageImpl) GetLatestBlock(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
	return b.instrumentGetLatestBlock.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		// Get the latest canonical block by highest height
		// Only return blocks that have been validated (is_watermark=TRUE)
		// This prevents canonical chain leakage to streamer before validation
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.is_watermark = TRUE
			ORDER BY cb.height DESC
			LIMIT 1`, canonicalBlockMetadataColumns)
		row := b.db.QueryRowContext(ctx, query, tag)
		block, err := model.BlockMetadataFromCanonicalRow(b.db, row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("no latest block found: %w", errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get latest block: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlockByHash(ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error) {
	return b.instrumentGetBlockByHash.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		if err := b.validateHeight(height); err != nil {
			return nil, err
		}
		var row *sql.Row
		if blockHash == "" {
			// Get the canonical block at this height (could be regular or skipped)
			query := fmt.Sprintf(`
				SELECT %s
				FROM canonical_blocks cb
				JOIN block_metadata bm ON cb.block_metadata_id = bm.id
				WHERE cb.tag = $1 AND cb.height = $2
				LIMIT 1`, canonicalBlockMetadataColumns)
			row = b.db.QueryRowContext(ctx, query, tag, height)
		} else {
			// Query block_metadata directly for the specific hash
			row = b.db.QueryRowContext(ctx, blockMetadataByHashQuery(), tag, height, blockHash)
		}

		block, err := model.BlockMetadataFromRow(b.db, row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("block not found: %w", errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get block by hash: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlockByHeight(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
	return b.instrumentGetBlockByHeight.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		if err := b.validateHeight(height); err != nil {
			return nil, err
		}
		// Get block from canonical_blocks table (includes both regular and skipped blocks)
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height = $2
			LIMIT 1`, canonicalBlockMetadataColumns)
		row := b.db.QueryRowContext(ctx, query, tag, height)
		block, err := model.BlockMetadataFromCanonicalRow(b.db, row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("block at height %d not found: %w", height, errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get block by height: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*api.BlockMetadata, error) {
	return b.instrumentGetBlocksByHeightRange.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		if startHeight >= endHeight {
			return nil, errors.ErrOutOfRange
		}
		if err := b.validateHeight(startHeight); err != nil {
			return nil, err
		}

		// Get all blocks (canonical and skipped) from canonical_blocks table
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height >= $2 AND cb.height < $3
			ORDER BY cb.height ASC`, canonicalBlockMetadataColumns)
		rows, err := b.db.QueryContext(ctx, query, tag, startHeight, endHeight)
		if err != nil {
			return nil, xerrors.Errorf("failed to query blocks by height range: %w", err)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil && err == nil {
				err = xerrors.Errorf("failed to close rows: %w", closeErr)
			}
		}()

		blocks, err := model.BlockMetadataFromCanonicalRows(b.db, rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan block rows: %w", err)
		}

		// Check if we have all blocks in the range (no gaps)
		expectedCount := int(endHeight - startHeight)
		if len(blocks) != expectedCount {
			return nil, xerrors.Errorf("missing blocks in range [%d, %d): expected %d, got %d: %w",
				startHeight, endHeight, expectedCount, len(blocks), errors.ErrItemNotFound)
		}

		// Verify no gaps in heights
		for i, block := range blocks {
			expectedHeight := startHeight + uint64(i)
			if block.Height != expectedHeight {
				return nil, xerrors.Errorf("gap in block heights: expected %d, got %d: %w",
					expectedHeight, block.Height, errors.ErrItemNotFound)
			}
		}

		// Validate chain continuity (parent hash matching) like DynamoDB does
		// This is critical for detecting reorgs and triggering recovery logic
		if err = parser.ValidateChain(blocks, nil); err != nil {
			return nil, xerrors.Errorf("failed to validate chain: %w", err)
		}

		return blocks, nil
	})
}

func (b *blockStorageImpl) GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*api.BlockMetadata, error) {
	return b.instrumentGetBlocksByHeights.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		for _, height := range heights {
			if err := b.validateHeight(height); err != nil {
				return nil, err
			}
		}
		if len(heights) == 0 {
			return []*api.BlockMetadata{}, nil
		}
		// Build dynamic query with placeholders for IN clause
		placeholders := make([]string, len(heights))
		args := make([]interface{}, len(heights)+1)
		args[0] = tag // First argument is tag
		for i, height := range heights {
			placeholders[i] = fmt.Sprintf("$%d", i+2) // Start from $2 since $1 is tag
			args[i+1] = height
		}
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height IN (%s)
			ORDER BY cb.height ASC`,
			canonicalBlockMetadataColumns,
			strings.Join(placeholders, ", "))

		rows, err := b.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, xerrors.Errorf("failed to query blocks by heights: %w", err)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil && err == nil {
				err = xerrors.Errorf("failed to close rows: %w", closeErr)
			}
		}()

		blocks, err := model.BlockMetadataFromCanonicalRows(b.db, rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan block rows: %w", err)
		}

		// Verify we got all requested blocks and return them in the same order as requested
		blockMap := make(map[uint64]*api.BlockMetadata)
		for _, block := range blocks {
			blockMap[block.Height] = block
		}

		orderedBlocks := make([]*api.BlockMetadata, len(heights))
		for i, height := range heights {
			block, exists := blockMap[height]
			if !exists {
				return nil, xerrors.Errorf("block at height %d not found: %w", height, errors.ErrItemNotFound)
			}
			orderedBlocks[i] = block
		}

		return orderedBlocks, nil
	})
}

func (b *blockStorageImpl) validateHeight(height uint64) error {
	if height < b.blockStartHeight {
		return xerrors.Errorf("height(%d) should be no less than blockStartHeight(%d): %w",
			height, b.blockStartHeight, errors.ErrInvalidHeight)
	}
	return nil
}

// GetWatermarkCount returns the number of watermarked blocks for monitoring purposes
// This metric helps track watermark accumulation and determine if cleanup is needed
func (b *blockStorageImpl) GetWatermarkCount(ctx context.Context, tag uint32) (int64, error) {
	var count int64
	query := `SELECT COUNT(*) FROM canonical_blocks WHERE tag = $1 AND is_watermark = TRUE`
	err := b.db.QueryRowContext(ctx, query, tag).Scan(&count)
	if err != nil {
		return 0, xerrors.Errorf("failed to get watermark count: %w", err)
	}
	return count, nil
}

func (b *blockStorageImpl) GetBlockByTimestamp(ctx context.Context, tag uint32, timestamp uint64) (*api.BlockMetadata, error) {
	return b.instrumentGetBlockByTimestamp.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		// Query to get the latest block before or at the given timestamp
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND bm.timestamp <= $2
			ORDER BY bm.timestamp DESC, bm.height DESC
			LIMIT 1
		`, canonicalBlockMetadataColumns)

		var blockId int64
		var height uint64
		var blockTag uint32
		var hash, parentHash, objectKeyMain sql.NullString
		var parentHeight uint64
		var blockTimestamp int64
		var skipped bool
		var objectFormat int32
		var byteOffset, byteLength, uncompressedLength sql.NullInt64

		err := b.db.QueryRowContext(ctx, query, tag, timestamp).Scan(
			&blockId, &height, &blockTag, &hash, &parentHash, &parentHeight, &objectKeyMain, &blockTimestamp, &skipped,
			&objectFormat, &byteOffset, &byteLength, &uncompressedLength)
		if err != nil {
			if err == sql.ErrNoRows {
				fmt.Printf("[DEBUG] No block found for tag=%d, timestamp=%d\n", tag, timestamp)
				return nil, xerrors.Errorf("no block found before timestamp %d: %w", timestamp, errors.ErrItemNotFound)
			}
			fmt.Printf("[DEBUG] Failed to get block by timestamp: %v\n", err)
			return nil, xerrors.Errorf("failed to get block by timestamp: %w", err)
		}

		if !hash.Valid || blockTimestamp == 0 {
			fmt.Printf("[DEBUG] Invalid block data: height=%d, blockTimestamp=%d, hash.Valid=%v\n", height, blockTimestamp, hash.Valid)
			return nil, xerrors.Errorf("no block found before timestamp %d: %w", timestamp, errors.ErrItemNotFound)
		}

		return &api.BlockMetadata{
			Tag:                blockTag,
			Hash:               hash.String,
			ParentHash:         parentHash.String,
			Height:             height,
			ParentHeight:       parentHeight,
			ObjectKeyMain:      objectKeyMain.String,
			Timestamp:          utils.ToTimestamp(blockTimestamp),
			Skipped:            skipped,
			ObjectFormat:       api.BlockObjectFormat(objectFormat),
			ByteOffset:         uint64Value(byteOffset),
			ByteLength:         uint64Value(byteLength),
			UncompressedLength: uint64Value(uncompressedLength),
		}, nil
	})
}

func (b *blockStorageImpl) GetBlockConsolidationShadow(ctx context.Context, block *api.BlockMetadata) (*api.BlockMetadata, error) {
	shadows, err := b.GetBlocksConsolidationShadow(ctx, []*api.BlockMetadata{block})
	if err != nil {
		return nil, err
	}
	if len(shadows) == 0 || shadows[0] == nil {
		return nil, xerrors.Errorf("consolidation shadow not found: %w", errors.ErrItemNotFound)
	}
	return shadows[0], nil
}

func (b *blockStorageImpl) GetBlocksConsolidationShadow(ctx context.Context, blocks []*api.BlockMetadata) ([]*api.BlockMetadata, error) {
	shadows := make([]*api.BlockMetadata, len(blocks))
	placeholders := make([]string, 0, len(blocks))
	args := make([]interface{}, 0, len(blocks)*5)
	nextArg := 1
	for i, block := range blocks {
		if block.GetSkipped() {
			continue
		}
		placeholders = append(placeholders, fmt.Sprintf(
			"($%d::int, $%d::int, $%d::bigint, $%d::varchar, $%d::text)",
			nextArg,
			nextArg+1,
			nextArg+2,
			nextArg+3,
			nextArg+4,
		))
		args = append(args, i, block.GetTag(), block.GetHeight(), block.GetHash(), block.GetObjectKeyMain())
		nextArg += 5
	}
	if len(placeholders) == 0 {
		return shadows, nil
	}

	query := fmt.Sprintf(`
		WITH input(ord, tag, height, hash, legacy_object_key_main) AS (
			VALUES %s
		)
		SELECT
			input.ord,
			shadow.consolidated_object_key_main,
			shadow.object_format,
			shadow.byte_offset,
			shadow.byte_length,
			shadow.uncompressed_length
		FROM input
		LEFT JOIN LATERAL (
			SELECT consolidated_object_key_main, object_format, byte_offset, byte_length, uncompressed_length
			FROM block_consolidation_shadow
			WHERE tag = input.tag
				AND height = input.height
				AND hash = input.hash
				AND legacy_object_key_main = input.legacy_object_key_main
				AND validated_at IS NOT NULL
			ORDER BY created_at DESC
			LIMIT 1
		) shadow ON true
		ORDER BY input.ord ASC`, strings.Join(placeholders, ", "))
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get consolidation shadows: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var ord int
		var objectKey sql.NullString
		var objectFormat sql.NullInt64
		var byteOffset, byteLength sql.NullInt64
		var uncompressedLength sql.NullInt64
		if err := rows.Scan(
			&ord,
			&objectKey,
			&objectFormat,
			&byteOffset,
			&byteLength,
			&uncompressedLength,
		); err != nil {
			return nil, xerrors.Errorf("failed to scan consolidation shadow: %w", err)
		}
		if !objectKey.Valid {
			continue
		}
		if ord < 0 || ord >= len(blocks) {
			return nil, xerrors.Errorf("consolidation shadow input order out of range: %d", ord)
		}
		shadows[ord] = makeConsolidationShadowBlockMetadata(
			blocks[ord],
			objectKey.String,
			api.BlockObjectFormat(objectFormat.Int64),
			uint64Value(byteOffset),
			uint64Value(byteLength),
			uint64Value(uncompressedLength),
		)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate consolidation shadows: %w", err)
	}
	return shadows, nil
}

func (b *blockStorageImpl) GetBlocksMissingConsolidationShadow(ctx context.Context, tag uint32, startHeight, endHeight uint64, limit uint64) ([]*internal.BlockMetadataRecord, error) {
	if endHeight <= startHeight {
		return nil, nil
	}
	if limit == 0 {
		return nil, xerrors.New("consolidation shadow scan limit must be positive")
	}
	query := fmt.Sprintf(`
		SELECT %s
		FROM block_metadata bm
		JOIN canonical_blocks cb ON cb.block_metadata_id = bm.id
			AND cb.tag = bm.tag
			AND cb.height = bm.height
		WHERE bm.tag = $1
			AND bm.height >= $2
			AND bm.height < $3
			AND bm.skipped = false
			AND bm.byte_length IS NULL
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND NOT EXISTS (
				SELECT 1
				FROM block_consolidation_shadow shadow
				WHERE shadow.block_metadata_id = bm.id
					AND shadow.legacy_object_key_main = bm.object_key_main
					AND shadow.validated_at IS NOT NULL
			)
		ORDER BY bm.height ASC
		LIMIT $4`, canonicalBlockMetadataColumns)

	rows, err := b.db.QueryContext(ctx, query, tag, startHeight, endHeight, limit)
	if err != nil {
		return nil, xerrors.Errorf("failed to scan unconsolidated blocks: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	records := make([]*internal.BlockMetadataRecord, 0)
	for rows.Next() {
		var id int64
		var height uint64
		var blockTag uint32
		var hash, parentHash, objectKeyMain sql.NullString
		var parentHeight uint64
		var blockTimestamp int64
		var skipped bool
		var objectFormat int32
		var byteOffset, byteLength, uncompressedLength sql.NullInt64
		if err := rows.Scan(
			&id,
			&height,
			&blockTag,
			&hash,
			&parentHash,
			&parentHeight,
			&objectKeyMain,
			&blockTimestamp,
			&skipped,
			&objectFormat,
			&byteOffset,
			&byteLength,
			&uncompressedLength,
		); err != nil {
			return nil, xerrors.Errorf("failed to scan unconsolidated block: %w", err)
		}
		records = append(records, &internal.BlockMetadataRecord{
			ID: id,
			Metadata: &api.BlockMetadata{
				Tag:                blockTag,
				Hash:               hash.String,
				ParentHash:         parentHash.String,
				Height:             height,
				ParentHeight:       parentHeight,
				ObjectKeyMain:      objectKeyMain.String,
				Timestamp:          utils.ToTimestamp(blockTimestamp),
				Skipped:            skipped,
				ObjectFormat:       api.BlockObjectFormat(objectFormat),
				ByteOffset:         uint64Value(byteOffset),
				ByteLength:         uint64Value(byteLength),
				UncompressedLength: uint64Value(uncompressedLength),
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate unconsolidated blocks: %w", err)
	}
	return records, nil
}

func (b *blockStorageImpl) GetFirstBlockMissingConsolidationShadow(ctx context.Context, tag uint32, startHeight, endHeight uint64) (uint64, bool, error) {
	if endHeight <= startHeight {
		return 0, false, nil
	}
	const query = `
		SELECT bm.height
		FROM block_metadata bm
		JOIN canonical_blocks cb ON cb.block_metadata_id = bm.id
			AND cb.tag = bm.tag
			AND cb.height = bm.height
		WHERE bm.tag = $1
			AND bm.height >= $2
			AND bm.height < $3
			AND bm.skipped = false
			AND bm.byte_length IS NULL
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND NOT EXISTS (
				SELECT 1
				FROM block_consolidation_shadow shadow
				WHERE shadow.block_metadata_id = bm.id
					AND shadow.legacy_object_key_main = bm.object_key_main
					AND shadow.validated_at IS NOT NULL
			)
		ORDER BY bm.height ASC
		LIMIT 1`
	var height uint64
	if err := b.db.QueryRowContext(ctx, query, tag, startHeight, endHeight).Scan(&height); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, xerrors.Errorf("failed to get first block missing consolidation shadow: %w", err)
	}
	return height, true, nil
}

func (b *blockStorageImpl) GetBlockConsolidationShadowStats(ctx context.Context, tag uint32, startHeight, endHeight uint64) (*internal.ConsolidationShadowStats, error) {
	if endHeight <= startHeight {
		return &internal.ConsolidationShadowStats{}, nil
	}
	const query = `
		SELECT
			COUNT(DISTINCT shadow.consolidated_object_key_main),
			COUNT(*)
		FROM canonical_blocks cb
		JOIN block_metadata bm ON bm.id = cb.block_metadata_id
		JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
			AND shadow.legacy_object_key_main = bm.object_key_main
			AND shadow.tag = cb.tag
			AND shadow.height = cb.height
			AND shadow.hash = bm.hash
		WHERE cb.tag = $1
			AND cb.height >= $2
			AND cb.height < $3
			AND shadow.tag = $1
			AND shadow.height >= $2
			AND shadow.height < $3
			AND bm.skipped = false
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND shadow.validated_at IS NOT NULL
			AND shadow.consolidated_object_key_main IS NOT NULL
			AND shadow.consolidated_object_key_main <> ''`

	var objects, blocks uint64
	if err := b.db.QueryRowContext(ctx, query, tag, startHeight, endHeight).Scan(&objects, &blocks); err != nil {
		return nil, xerrors.Errorf("failed to get consolidation shadow stats: %w", err)
	}
	return &internal.ConsolidationShadowStats{
		Objects: objects,
		Blocks:  blocks,
	}, nil
}

func (b *blockStorageImpl) GetFirstPromotableBlockConsolidationShadow(ctx context.Context, tag uint32, startHeight, endHeight uint64) (uint64, bool, error) {
	if endHeight <= startHeight {
		return 0, false, nil
	}
	const query = `
		SELECT shadow.height
		FROM block_consolidation_shadow shadow
		JOIN block_metadata bm ON bm.id = shadow.block_metadata_id
			AND shadow.tag = bm.tag
			AND shadow.height = bm.height
			AND shadow.hash = bm.hash
			AND shadow.legacy_object_key_main = bm.object_key_main
		JOIN canonical_blocks cb ON cb.block_metadata_id = bm.id
			AND cb.tag = bm.tag
			AND cb.height = bm.height
		WHERE shadow.tag = $1
			AND shadow.height >= $2
			AND shadow.height < $3
			AND bm.skipped = false
			AND bm.byte_length IS NULL
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND shadow.validated_at IS NOT NULL
			AND shadow.consolidated_object_key_main IS NOT NULL
			AND shadow.consolidated_object_key_main <> ''
			AND shadow.object_format = $4
			AND shadow.byte_offset >= 0
			AND shadow.byte_length > 0
			AND shadow.uncompressed_length IS NOT NULL
			AND shadow.uncompressed_length > 0
		ORDER BY shadow.height ASC
		LIMIT 1`
	var height uint64
	if err := b.db.QueryRowContext(
		ctx,
		query,
		tag,
		startHeight,
		endHeight,
		int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
	).Scan(&height); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, xerrors.Errorf("failed to get first promotable consolidation shadow: %w", err)
	}
	return height, true, nil
}

func (b *blockStorageImpl) GetBlockConsolidationCursor(ctx context.Context, name string, tag uint32) (uint64, bool, error) {
	if name == "" {
		return 0, false, xerrors.New("block consolidation cursor name must be non-empty")
	}
	const query = `
		SELECT height
		FROM block_consolidation_cursor
		WHERE name = $1 AND tag = $2`
	var height uint64
	if err := b.db.QueryRowContext(ctx, query, name, tag).Scan(&height); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, xerrors.Errorf("failed to get block consolidation cursor %q for tag %d: %w", name, tag, err)
	}
	return height, true, nil
}

func (b *blockStorageImpl) SetBlockConsolidationCursor(ctx context.Context, name string, tag uint32, height uint64) error {
	if name == "" {
		return xerrors.New("block consolidation cursor name must be non-empty")
	}
	const query = `
		INSERT INTO block_consolidation_cursor (name, tag, height, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (name, tag) DO UPDATE SET
			height = GREATEST(block_consolidation_cursor.height, EXCLUDED.height),
			updated_at = NOW()`
	if _, err := b.db.ExecContext(ctx, query, name, tag, height); err != nil {
		return xerrors.Errorf("failed to set block consolidation cursor %q for tag %d to height %d: %w", name, tag, height, err)
	}
	return nil
}

func (b *blockStorageImpl) PersistBlockConsolidationShadows(ctx context.Context, placements []*internal.ConsolidationShadowPlacement) error {
	if len(placements) == 0 {
		return nil
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("failed to begin consolidation shadow transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	const query = `
		INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, legacy_object_key_main, consolidated_object_key_main,
			object_format, byte_offset, byte_length, uncompressed_length, validated_at
		)
		SELECT
			bm.id, bm.tag, bm.height, bm.hash, bm.object_key_main, $6,
			$7, $8, $9, $10, NOW()
		FROM block_metadata bm
		WHERE bm.id = $1
			AND bm.tag = $2
			AND bm.height = $3
			AND bm.hash = $4
			AND bm.object_key_main = $5
			AND bm.skipped = false
		ON CONFLICT (block_metadata_id) DO UPDATE SET
			tag = EXCLUDED.tag,
			height = EXCLUDED.height,
			hash = EXCLUDED.hash,
			legacy_object_key_main = EXCLUDED.legacy_object_key_main,
			consolidated_object_key_main = EXCLUDED.consolidated_object_key_main,
			object_format = EXCLUDED.object_format,
			byte_offset = EXCLUDED.byte_offset,
			byte_length = EXCLUDED.byte_length,
			uncompressed_length = EXCLUDED.uncompressed_length,
			validated_at = EXCLUDED.validated_at
		WHERE block_consolidation_shadow.legacy_object_key_main = EXCLUDED.legacy_object_key_main
		RETURNING block_metadata_id`

	for _, placement := range placements {
		if placement == nil {
			continue
		}
		var id int64
		err := tx.QueryRowContext(
			ctx,
			query,
			placement.BlockMetadataID,
			placement.Tag,
			placement.Height,
			placement.Hash,
			placement.LegacyObjectKeyMain,
			placement.ConsolidatedObjectKeyMain,
			int32(placement.ObjectFormat),
			placement.ByteOffset,
			placement.ByteLength,
			placement.UncompressedLength,
		).Scan(&id)
		if err != nil {
			if err == sql.ErrNoRows {
				return xerrors.Errorf("failed to persist consolidation shadow for metadata_id=%d height=%d: primary metadata identity mismatch", placement.BlockMetadataID, placement.Height)
			}
			return xerrors.Errorf("failed to persist consolidation shadow for metadata_id=%d height=%d: %w", placement.BlockMetadataID, placement.Height, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("failed to commit consolidation shadows: %w", err)
	}
	committed = true
	return nil
}

func (b *blockStorageImpl) PromoteBlockConsolidationShadows(ctx context.Context, tag uint32, startHeight, endHeight uint64, limit uint64) (*internal.ConsolidationPromotionResult, error) {
	if endHeight <= startHeight {
		return &internal.ConsolidationPromotionResult{}, nil
	}
	if limit == 0 {
		return nil, xerrors.New("consolidation promotion limit must be positive")
	}
	if err := b.validateHeight(startHeight); err != nil {
		return nil, err
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin consolidation promotion transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	const invalidShadowQuery = `
		SELECT shadow.block_metadata_id, shadow.height
		FROM canonical_blocks cb
		JOIN block_metadata bm ON bm.id = cb.block_metadata_id
		JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
			AND shadow.tag = bm.tag
			AND shadow.height = bm.height
			AND shadow.hash = bm.hash
			AND shadow.legacy_object_key_main = bm.object_key_main
		WHERE cb.tag = $1
			AND cb.height >= $2
			AND cb.height < $3
			AND bm.skipped = false
			AND bm.byte_length IS NULL
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND shadow.validated_at IS NOT NULL
			AND (
				shadow.consolidated_object_key_main IS NULL
				OR shadow.consolidated_object_key_main = ''
				OR shadow.object_format <> $4
				OR shadow.byte_offset < 0
				OR shadow.byte_length <= 0
				OR shadow.uncompressed_length IS NULL
				OR shadow.uncompressed_length <= 0
			)
		ORDER BY cb.height ASC
		LIMIT 1`
	var invalidMetadataID int64
	var invalidHeight uint64
	err = tx.QueryRowContext(
		ctx,
		invalidShadowQuery,
		tag,
		startHeight,
		endHeight,
		int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
	).Scan(&invalidMetadataID, &invalidHeight)
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("failed to validate consolidation promotion shadows: %w", err)
	}
	if err == nil {
		return nil, xerrors.Errorf("invalid consolidation shadow metadata for metadata_id=%d height=%d", invalidMetadataID, invalidHeight)
	}

	const promoteQuery = `
		WITH candidates AS (
			SELECT
				bm.id AS block_metadata_id,
				bm.tag,
				bm.height,
				bm.hash,
				bm.object_key_main AS legacy_object_key_main,
				shadow.consolidated_object_key_main,
				shadow.object_format,
				shadow.byte_offset,
				shadow.byte_length,
				shadow.uncompressed_length
			FROM canonical_blocks cb
			JOIN block_metadata bm ON bm.id = cb.block_metadata_id
			JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
				AND shadow.tag = bm.tag
				AND shadow.height = bm.height
				AND shadow.hash = bm.hash
				AND shadow.legacy_object_key_main = bm.object_key_main
			WHERE cb.tag = $1
				AND cb.height >= $2
				AND cb.height < $3
				AND bm.skipped = false
				AND bm.byte_length IS NULL
				AND bm.object_key_main IS NOT NULL
				AND bm.object_key_main <> ''
				AND shadow.validated_at IS NOT NULL
				AND shadow.consolidated_object_key_main IS NOT NULL
				AND shadow.consolidated_object_key_main <> ''
				AND shadow.object_format = $5
				AND shadow.byte_offset >= 0
				AND shadow.byte_length > 0
				AND shadow.uncompressed_length IS NOT NULL
				AND shadow.uncompressed_length > 0
			ORDER BY cb.height ASC
			LIMIT $4
			FOR UPDATE OF cb, bm, shadow
		),
		updated AS (
			UPDATE block_metadata bm
			SET
				object_key_main = candidates.consolidated_object_key_main,
				object_format = candidates.object_format,
				byte_offset = candidates.byte_offset,
				byte_length = candidates.byte_length,
				uncompressed_length = candidates.uncompressed_length
			FROM candidates
			WHERE bm.id = candidates.block_metadata_id
				AND bm.tag = candidates.tag
				AND bm.height = candidates.height
				AND bm.hash = candidates.hash
				AND bm.object_key_main = candidates.legacy_object_key_main
				AND bm.skipped = false
				AND bm.byte_length IS NULL
			RETURNING bm.id
		)
		SELECT
			(SELECT COUNT(*) FROM candidates),
			(SELECT COUNT(*) FROM updated)`

	var candidates, promoted uint64
	if err := tx.QueryRowContext(
		ctx,
		promoteQuery,
		tag,
		startHeight,
		endHeight,
		limit,
		int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
	).Scan(&candidates, &promoted); err != nil {
		return nil, xerrors.Errorf("failed to promote consolidation shadows: %w", err)
	}
	if candidates != promoted {
		return nil, xerrors.Errorf("consolidation promotion guard failed: selected %d rows but promoted %d", candidates, promoted)
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit consolidation promotion: %w", err)
	}
	committed = true
	return &internal.ConsolidationPromotionResult{
		Blocks: promoted,
	}, nil
}

func makeConsolidationShadowBlockMetadata(
	block *api.BlockMetadata,
	objectKey string,
	objectFormat api.BlockObjectFormat,
	byteOffset uint64,
	byteLength uint64,
	uncompressedLength uint64,
) *api.BlockMetadata {
	shadow := proto.Clone(block).(*api.BlockMetadata)
	shadow.ObjectKeyMain = objectKey
	shadow.ObjectFormat = objectFormat
	shadow.ByteOffset = byteOffset
	shadow.ByteLength = byteLength
	shadow.UncompressedLength = uncompressedLength
	return shadow
}

func uint64Value(value sql.NullInt64) uint64 {
	if !value.Valid {
		return 0
	}
	return uint64(value.Int64)
}

func blockObjectByteFields(block *api.BlockMetadata) (sql.NullInt64, sql.NullInt64, sql.NullInt64) {
	if block.GetObjectFormat() != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH {
		return sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(block.GetByteOffset()), Valid: true},
		sql.NullInt64{Int64: int64(block.GetByteLength()), Valid: true},
		sql.NullInt64{Int64: int64(block.GetUncompressedLength()), Valid: true}
}
