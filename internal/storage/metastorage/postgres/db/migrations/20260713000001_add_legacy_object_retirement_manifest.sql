-- +goose Up
ALTER TABLE block_consolidation_shadow
    ALTER COLUMN legacy_object_key_main DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS legacy_object_deleted_at TIMESTAMPTZ;

ALTER TABLE block_consolidation_shadow
    ADD CONSTRAINT block_consolidation_shadow_legacy_retirement_state CHECK (
        (legacy_object_key_main IS NOT NULL AND legacy_object_deleted_at IS NULL)
        OR
        (legacy_object_key_main IS NULL AND legacy_object_deleted_at IS NOT NULL)
    ) NOT VALID;

CREATE TABLE block_legacy_object_retirement (
    block_metadata_id BIGINT PRIMARY KEY REFERENCES block_consolidation_shadow (block_metadata_id) ON DELETE RESTRICT,
    tag INTEGER NOT NULL,
    height BIGINT NOT NULL CHECK (height >= 0),
    hash VARCHAR(66),
    state TEXT NOT NULL CHECK (state IN ('eligible', 'deleting', 'deleted_verified')),
    bucket TEXT NOT NULL CHECK (bucket <> ''),
    legacy_object_key_main TEXT,
    legacy_object_key_sha256 CHAR(64) NOT NULL CHECK (legacy_object_key_sha256 ~ '^[0-9a-f]{64}$'),
    legacy_object_version_ids TEXT[] NOT NULL CHECK (cardinality(legacy_object_version_ids) = 1 AND legacy_object_version_ids[1] <> ''),
    legacy_object_etag TEXT NOT NULL,
    legacy_object_bytes BIGINT NOT NULL CHECK (legacy_object_bytes > 0),
    consolidated_object_key_main TEXT NOT NULL CHECK (consolidated_object_key_main <> ''),
    consolidated_object_version_id TEXT NOT NULL CHECK (consolidated_object_version_id <> ''),
    consolidated_object_etag TEXT NOT NULL CHECK (consolidated_object_etag <> ''),
    consolidated_byte_offset BIGINT NOT NULL CHECK (consolidated_byte_offset >= 0),
    consolidated_byte_length BIGINT NOT NULL CHECK (consolidated_byte_length > 0),
    consolidated_uncompressed_length BIGINT NOT NULL CHECK (consolidated_uncompressed_length > 0),
    payload_sha256 CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    outcome TEXT NOT NULL DEFAULT '',
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    prepared_at TIMESTAMPTZ NOT NULL,
    delete_started_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (state = 'eligible' AND legacy_object_key_main IS NOT NULL AND legacy_object_key_main <> '' AND legacy_object_etag <> '' AND delete_started_at IS NULL AND deleted_at IS NULL)
        OR
        (state = 'deleting' AND legacy_object_key_main IS NOT NULL AND legacy_object_key_main <> '' AND legacy_object_etag <> '' AND delete_started_at IS NOT NULL AND last_attempt_at IS NOT NULL AND attempt_count > 0 AND deleted_at IS NULL)
        OR
        (state = 'deleted_verified' AND legacy_object_key_main IS NULL AND legacy_object_etag = '' AND outcome <> '' AND delete_started_at IS NOT NULL AND last_attempt_at IS NOT NULL AND attempt_count > 0 AND deleted_at IS NOT NULL)
    )
);

CREATE INDEX idx_block_legacy_object_retirement_pending
    ON block_legacy_object_retirement (tag, height, block_metadata_id)
    WHERE state IN ('eligible', 'deleting');

-- +goose Down
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM block_legacy_object_retirement) THEN
        RAISE EXCEPTION 'cannot roll back legacy retirement migration while retirement manifests exist';
    END IF;
    IF EXISTS (SELECT 1 FROM block_consolidation_shadow WHERE legacy_object_key_main IS NULL) THEN
        RAISE EXCEPTION 'cannot roll back legacy retirement migration after a verified deletion';
    END IF;
END $$;

DROP INDEX IF EXISTS idx_block_legacy_object_retirement_pending;
DROP TABLE IF EXISTS block_legacy_object_retirement;

ALTER TABLE block_consolidation_shadow
    DROP CONSTRAINT IF EXISTS block_consolidation_shadow_legacy_retirement_state,
    DROP COLUMN IF EXISTS legacy_object_deleted_at,
    ALTER COLUMN legacy_object_key_main SET NOT NULL;
