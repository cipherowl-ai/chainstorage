package model

import (
	"database/sql"

	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// BlockMetadataFromRow converts a postgres row into a BlockMetadata proto
// Used for direct block_metadata table queries
// Schema: id, height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped
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
		&block.ParentHeight,
		&block.ObjectKeyMain,
		&timestamp,
		&block.Skipped,
	)
	if err != nil {
		return nil, err
	}
	block.Timestamp = utils.ToTimestamp(timestamp)
	return &block, nil
}

// BlockMetadataFromCanonicalRow converts a postgres row from canonical join into a BlockMetadata proto
// Used for queries that join canonical_blocks with block_metadata
// Schema: bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.parent_height, bm.object_key_main, bm.timestamp, bm.skipped
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
		&block.ParentHeight,
		&block.ObjectKeyMain,
		&timestamp,
		&block.Skipped,
	)
	if err != nil {
		return nil, err
	}
	block.Timestamp = utils.ToTimestamp(timestamp)
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
			&block.ParentHeight,
			&block.ObjectKeyMain,
			&timestamp,
			&block.Skipped,
		)
		if err != nil {
			return nil, err
		}
		block.Timestamp = utils.ToTimestamp(timestamp)
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
			&block.ParentHeight,
			&block.ObjectKeyMain,
			&timestamp,
			&block.Skipped,
		)
		if err != nil {
			return nil, err
		}
		block.Timestamp = utils.ToTimestamp(timestamp)
		blocks = append(blocks, &block)
	}
	return blocks, nil
}
