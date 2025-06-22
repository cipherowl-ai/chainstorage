package model

import (
	"database/sql"

	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// getParentHeightFromHash looks up the parent height by finding the block with the given parent hash
func getParentHeightFromHash(db *sql.DB, tag uint32, parentHash string, currentHeight uint64) (uint64, error) {
	if parentHash == "" {
		return 0, nil
	}

	query := `
		SELECT height 
		FROM block_metadata 
		WHERE tag = $1 AND hash = $2`

	var parentHeight uint64
	err := db.QueryRow(query, tag, parentHash).Scan(&parentHeight)
	if err != nil {
		if err == sql.ErrNoRows {
			// If parent not found, fall back to height - 1
			return currentHeight - 1, nil
		}
		return 0, err
	}
	return parentHeight, nil
}

// BlockMetadataFromRow converts a postgres row into a BlockMetadata proto
// Used for direct block_metadata table queries
// Schema: id, height, tag, hash, parent_hash, object_key_main, timestamp, skipped
func BlockMetadataFromRow(db *sql.DB, row *sql.Row) (*api.BlockMetadata, error) {
	var block api.BlockMetadata
	var timestamp int64
	var id int64 // We get this but don't need it in the result
	err := row.Scan(
		&id,
		&block.Height,
		&block.Tag,
		&block.Hash,
		&block.ParentHash,
		&block.ObjectKeyMain,
		&timestamp,
		&block.Skipped,
	)
	if err != nil {
		return nil, err
	}
	block.Timestamp = utils.ToTimestamp(timestamp)
	block.ParentHeight, err = getParentHeightFromHash(db, block.Tag, block.ParentHash, block.Height)
	if err != nil {
		return nil, err
	}
	return &block, nil
}

// BlockMetadataFromCanonicalRow converts a postgres row from canonical join into a BlockMetadata proto
// Used for queries that join canonical_blocks with block_metadata
// Schema: bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.object_key_main, bm.timestamp, bm.skipped
func BlockMetadataFromCanonicalRow(db *sql.DB, row *sql.Row) (*api.BlockMetadata, error) {
	var block api.BlockMetadata
	var timestamp int64
	var id int64 // block_metadata.id
	err := row.Scan(
		&id,
		&block.Height,
		&block.Tag,
		&block.Hash,
		&block.ParentHash,
		&block.ObjectKeyMain,
		&timestamp,
		&block.Skipped,
	)
	if err != nil {
		return nil, err
	}
	block.Timestamp = utils.ToTimestamp(timestamp)
	block.ParentHeight, err = getParentHeightFromHash(db, block.Tag, block.ParentHash, block.Height)
	if err != nil {
		return nil, err
	}
	return &block, nil
}

// BlockMetadataFromRows converts multiple postgres rows into BlockMetadata protos
// Used for direct block_metadata table queries
func BlockMetadataFromRows(db *sql.DB, rows *sql.Rows) ([]*api.BlockMetadata, error) {
	var blocks []*api.BlockMetadata
	for rows.Next() {
		var block api.BlockMetadata
		var timestamp int64
		var id int64
		err := rows.Scan(
			&id,
			&block.Height,
			&block.Tag,
			&block.Hash,
			&block.ParentHash,
			&block.ObjectKeyMain,
			&timestamp,
			&block.Skipped,
		)
		if err != nil {
			return nil, err
		}
		block.Timestamp = utils.ToTimestamp(timestamp)
		block.ParentHeight, err = getParentHeightFromHash(db, block.Tag, block.ParentHash, block.Height)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, &block)
	}
	return blocks, nil
}

// BlockMetadataFromCanonicalRows converts multiple postgres rows from canonical joins into BlockMetadata protos
// Used for queries that join canonical_blocks with block_metadata
func BlockMetadataFromCanonicalRows(db *sql.DB, rows *sql.Rows) ([]*api.BlockMetadata, error) {
	var blocks []*api.BlockMetadata
	for rows.Next() {
		var block api.BlockMetadata
		var timestamp int64
		var id int64 // block_metadata.id
		err := rows.Scan(
			&id,
			&block.Height,
			&block.Tag,
			&block.Hash,
			&block.ParentHash,
			&block.ObjectKeyMain,
			&timestamp,
			&block.Skipped,
		)
		if err != nil {
			return nil, err
		}
		block.Timestamp = utils.ToTimestamp(timestamp)
		block.ParentHeight, err = getParentHeightFromHash(db, block.Tag, block.ParentHash, block.Height)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, &block)
	}
	return blocks, nil
}
