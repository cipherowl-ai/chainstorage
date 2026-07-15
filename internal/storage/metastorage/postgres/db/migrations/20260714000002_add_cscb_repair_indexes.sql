-- +goose NO TRANSACTION

-- +goose Up
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_cscb_repair_candidate_new;
CREATE INDEX CONCURRENTLY idx_block_metadata_cscb_repair_candidate_new
    ON block_metadata (tag, height DESC, object_key_main, id)
    WHERE object_format = 1
        AND object_key_main IS NOT NULL
        AND object_key_main <> '';
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_cscb_repair_candidate;
ALTER INDEX idx_block_metadata_cscb_repair_candidate_new
    RENAME TO idx_block_metadata_cscb_repair_candidate;

-- CSCB repair only performs exact object-key reference checks. B-trees over
-- the full keys and row IDs consume unnecessary space and the block_metadata
-- index exceeds Aurora local scratch storage on large chains such as Solana.
-- Hash indexes preserve the required equality lookups without storing or
-- sorting the full keys. Build under a temporary name before replacing the
-- canonical index, and drop that temporary name first so retries replace an
-- invalid index left by a failed concurrent build.
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_object_key_reference_hash_new;
CREATE INDEX CONCURRENTLY idx_block_metadata_object_key_reference_hash_new
    ON block_metadata USING HASH (object_key_main)
    WHERE object_key_main IS NOT NULL
        AND object_key_main <> '';
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_object_key_reference;
ALTER INDEX idx_block_metadata_object_key_reference_hash_new
    RENAME TO idx_block_metadata_object_key_reference;

DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_object_key_reference_hash_new;
CREATE INDEX CONCURRENTLY idx_block_consolidation_shadow_object_key_reference_hash_new
    ON block_consolidation_shadow USING HASH (consolidated_object_key_main)
    WHERE consolidated_object_key_main IS NOT NULL
        AND consolidated_object_key_main <> '';
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_object_key_reference;
ALTER INDEX idx_block_consolidation_shadow_object_key_reference_hash_new
    RENAME TO idx_block_consolidation_shadow_object_key_reference;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_object_key_reference_hash_new;
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_object_key_reference;
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_object_key_reference_hash_new;
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_object_key_reference;
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_cscb_repair_candidate_new;
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_cscb_repair_candidate;
