-- +goose NO TRANSACTION

-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_metadata_cscb_repair_candidate
    ON block_metadata (tag, height DESC, object_key_main, id)
    WHERE object_format = 1
        AND object_key_main IS NOT NULL
        AND object_key_main <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_metadata_object_key_reference
    ON block_metadata (object_key_main, id)
    WHERE object_key_main IS NOT NULL
        AND object_key_main <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_consolidation_shadow_object_key_reference
    ON block_consolidation_shadow (consolidated_object_key_main, block_metadata_id)
    WHERE consolidated_object_key_main IS NOT NULL
        AND consolidated_object_key_main <> '';

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_object_key_reference;
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_object_key_reference;
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_cscb_repair_candidate;
