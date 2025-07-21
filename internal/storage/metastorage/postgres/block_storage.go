package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres/model"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
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
		// `updateWatermark` is ignored in Postgres implementation because we can always find the latest
		// block by querying the maximum height in canonical_blocks for a tag.
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
			INSERT INTO block_metadata (height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (tag, height) WHERE skipped = true DO UPDATE SET
				hash = EXCLUDED.hash,
				parent_hash = EXCLUDED.parent_hash,
				parent_height = EXCLUDED.parent_height,
				object_key_main = EXCLUDED.object_key_main,
				timestamp = EXCLUDED.timestamp,
				skipped = EXCLUDED.skipped
			RETURNING id`

		blockMetadataRegularQuery := `
			INSERT INTO block_metadata (height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (tag, hash) WHERE hash IS NOT NULL AND NOT skipped DO UPDATE SET
				parent_hash = EXCLUDED.parent_hash,
				parent_height = EXCLUDED.parent_height,
				object_key_main = EXCLUDED.object_key_main,
				timestamp = EXCLUDED.timestamp,
				skipped = EXCLUDED.skipped
			RETURNING id`

		canonicalQuery := `
			INSERT INTO canonical_blocks (height, block_metadata_id, tag)
			VALUES ($1, $2, $3)
			ON CONFLICT (height, tag) DO UPDATE
			SET block_metadata_id = EXCLUDED.block_metadata_id`

		for _, block := range blocks {
			tsProto := block.GetTimestamp()
			var goTime time.Time
			if tsProto == nil { // special case for genesis block
				goTime = time.Unix(0, 0)
			} else {
				goTime, err = ptypes.Timestamp(tsProto) // convert timestamp to time.Time
				if err != nil {
					return xerrors.Errorf("invalid block timestamp at height %d: %w", block.Height, err)
				}
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

			err = tx.QueryRowContext(txCtx, query,
				block.Height,
				block.Tag,
				block.Hash,
				block.ParentHash,
				parentHeight,
				block.ObjectKeyMain,
				goTime,
				block.Skipped,
			).Scan(&blockId)
			if err != nil {
				return xerrors.Errorf("failed to insert block metadata for height %d: %w", block.Height, err)
			}

			// Insert ALL blocks (including skipped) into canonical_blocks
			_, err = tx.ExecContext(txCtx, canonicalQuery,
				block.Height,
				blockId,
				block.Tag,
			)
			if err != nil {
				return xerrors.Errorf("failed to insert canonical block for height %d: %w", block.Height, err)
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
		query := `
			SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.parent_height, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1
			ORDER BY cb.height DESC
			LIMIT 1`
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
			query := `
				SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.parent_height, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
				FROM canonical_blocks cb
				JOIN block_metadata bm ON cb.block_metadata_id = bm.id
				WHERE cb.tag = $1 AND cb.height = $2
				LIMIT 1`
			row = b.db.QueryRowContext(ctx, query, tag, height)
		} else {
			// Query block_metadata directly for the specific hash
			query := `
				SELECT id, height, tag, hash, parent_hash, parent_height, object_key_main, 
					   EXTRACT(EPOCH FROM timestamp)::BIGINT, skipped
				FROM block_metadata
				WHERE tag = $1 AND height = $2 AND hash = $3
				LIMIT 1`
			row = b.db.QueryRowContext(ctx, query, tag, height, blockHash)
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
		query := `
			SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.parent_height, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height = $2
			LIMIT 1`
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
		query := `
			SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.parent_height, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height >= $2 AND cb.height < $3
			ORDER BY cb.height ASC`
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
			SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.parent_height, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height IN (%s)
			ORDER BY cb.height ASC`,
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

func (b *blockStorageImpl) GetBlockByTimestamp(ctx context.Context, tag uint32, timestamp uint64) (*api.BlockMetadata, error) {
	return b.instrumentGetBlockByTimestamp.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		// Convert Unix timestamp to time.Time
		targetTime := time.Unix(int64(timestamp), 0)

		// Query to get the latest block before or at the given timestamp
		query := `
			SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.parent_height, bm.object_key_main, 
				   bm.timestamp, bm.skipped
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND bm.timestamp <= $2
			ORDER BY bm.timestamp DESC, bm.height DESC
			LIMIT 1
		`

		var blockId int64
		var height uint64
		var blockTag uint32
		var hash, parentHash, objectKeyMain sql.NullString
		var parentHeight uint64
		var blockTime sql.NullTime
		var skipped bool

		err := b.db.QueryRowContext(ctx, query, tag, targetTime).Scan(
			&blockId, &height, &blockTag, &hash, &parentHash, &parentHeight, &objectKeyMain, &blockTime, &skipped)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("no block found before timestamp %d: %w", timestamp, errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get block by timestamp: %w", err)
		}

		// Convert time.Time back to protobuf timestamp
		var protoTimestamp *timestamppb.Timestamp
		if blockTime.Valid {
			protoTimestamp, err = ptypes.TimestampProto(blockTime.Time)
			if err != nil {
				return nil, xerrors.Errorf("failed to convert timestamp: %w", err)
			}
		}

		return &api.BlockMetadata{
			Tag:           blockTag,
			Hash:          hash.String,
			ParentHash:    parentHash.String,
			Height:        height,
			ParentHeight:  parentHeight,
			ObjectKeyMain: objectKeyMain.String,
			Timestamp:     protoTimestamp,
			Skipped:       skipped,
		}, nil
	})
}
