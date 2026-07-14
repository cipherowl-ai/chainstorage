-- +goose Up
CREATE TABLE cscb_repair_manifest (
    id BIGSERIAL PRIMARY KEY,
    tag INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('prepared', 'restored', 'verified', 'completed')),
    bucket TEXT NOT NULL CHECK (bucket <> ''),
    old_consolidated_object_key_main TEXT NOT NULL CHECK (old_consolidated_object_key_main <> ''),
    old_consolidated_object_version_id TEXT NOT NULL CHECK (
        BTRIM(old_consolidated_object_version_id) <> ''
        AND LOWER(BTRIM(old_consolidated_object_version_id)) <> 'null'
    ),
    old_consolidated_object_etag TEXT NOT NULL CHECK (old_consolidated_object_etag <> ''),
    old_consolidated_object_bytes BIGINT NOT NULL CHECK (old_consolidated_object_bytes > 0),
    start_height BIGINT NOT NULL CHECK (start_height >= 0),
    end_height BIGINT NOT NULL CHECK (end_height > start_height),
    canonical_block_count BIGINT NOT NULL CHECK (canonical_block_count > 0),
    total_block_count BIGINT NOT NULL CHECK (total_block_count >= canonical_block_count),
    row_set_sha256 CHAR(64) NOT NULL CHECK (row_set_sha256 ~ '^[0-9a-f]{64}$'),
    new_consolidated_object_key_main TEXT,
    new_consolidated_object_version_id TEXT,
    new_consolidated_object_etag TEXT,
    new_consolidated_object_bytes BIGINT,
    outcome TEXT NOT NULL DEFAULT '',
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    restored_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    old_object_deleted_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tag, old_consolidated_object_key_main),
    CHECK (
        (
            state = 'prepared'
            AND restored_at IS NULL
            AND verified_at IS NULL
            AND old_object_deleted_at IS NULL
            AND completed_at IS NULL
            AND new_consolidated_object_key_main IS NULL
            AND new_consolidated_object_version_id IS NULL
            AND new_consolidated_object_etag IS NULL
            AND new_consolidated_object_bytes IS NULL
        )
        OR (
            state = 'restored'
            AND restored_at IS NOT NULL
            AND verified_at IS NULL
            AND old_object_deleted_at IS NULL
            AND completed_at IS NULL
            AND new_consolidated_object_key_main IS NULL
            AND new_consolidated_object_version_id IS NULL
            AND new_consolidated_object_etag IS NULL
            AND new_consolidated_object_bytes IS NULL
        )
        OR (
            state = 'verified'
            AND restored_at IS NOT NULL
            AND verified_at IS NOT NULL
            AND verified_at >= restored_at
            AND old_object_deleted_at IS NULL
            AND completed_at IS NULL
            AND new_consolidated_object_key_main IS NOT NULL
            AND new_consolidated_object_key_main <> ''
            AND new_consolidated_object_key_main <> old_consolidated_object_key_main
            AND new_consolidated_object_version_id IS NOT NULL
            AND BTRIM(new_consolidated_object_version_id) <> ''
            AND LOWER(BTRIM(new_consolidated_object_version_id)) <> 'null'
            AND new_consolidated_object_etag IS NOT NULL
            AND new_consolidated_object_etag <> ''
            AND new_consolidated_object_bytes IS NOT NULL
            AND new_consolidated_object_bytes > 0
        )
        OR (
            state = 'completed'
            AND restored_at IS NOT NULL
            AND verified_at IS NOT NULL
            AND verified_at >= restored_at
            AND old_object_deleted_at IS NOT NULL
            AND old_object_deleted_at >= verified_at
            AND completed_at IS NOT NULL
            AND completed_at >= old_object_deleted_at
            AND new_consolidated_object_key_main IS NOT NULL
            AND new_consolidated_object_key_main <> ''
            AND new_consolidated_object_key_main <> old_consolidated_object_key_main
            AND new_consolidated_object_version_id IS NOT NULL
            AND BTRIM(new_consolidated_object_version_id) <> ''
            AND LOWER(BTRIM(new_consolidated_object_version_id)) <> 'null'
            AND new_consolidated_object_etag IS NOT NULL
            AND new_consolidated_object_etag <> ''
            AND new_consolidated_object_bytes IS NOT NULL
            AND new_consolidated_object_bytes > 0
            AND outcome <> ''
        )
    )
);

CREATE TABLE cscb_repair_block (
    repair_id BIGINT NOT NULL REFERENCES cscb_repair_manifest (id) ON DELETE RESTRICT,
    block_metadata_id BIGINT NOT NULL REFERENCES block_metadata (id) ON DELETE RESTRICT,
    canonical BOOLEAN NOT NULL,
    tag INTEGER NOT NULL,
    height BIGINT NOT NULL CHECK (height >= 0),
    hash VARCHAR(66),
    single_block_object_key_main TEXT NOT NULL CHECK (single_block_object_key_main <> ''),
    single_block_object_version_id TEXT NOT NULL CHECK (
        BTRIM(single_block_object_version_id) <> ''
        AND LOWER(BTRIM(single_block_object_version_id)) <> 'null'
    ),
    single_block_object_etag TEXT NOT NULL CHECK (single_block_object_etag <> ''),
    single_block_object_bytes BIGINT NOT NULL CHECK (single_block_object_bytes > 0),
    payload_sha256 CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    old_byte_offset BIGINT NOT NULL CHECK (old_byte_offset >= 0),
    old_byte_length BIGINT NOT NULL CHECK (old_byte_length > 0),
    old_uncompressed_length BIGINT NOT NULL CHECK (old_uncompressed_length > 0),
    PRIMARY KEY (repair_id, block_metadata_id),
    UNIQUE (block_metadata_id)
);

CREATE INDEX idx_cscb_repair_manifest_pending
    ON cscb_repair_manifest (tag, end_height DESC, id)
    WHERE state <> 'completed';

CREATE INDEX idx_cscb_repair_block_repair_height
    ON cscb_repair_block (repair_id, height, block_metadata_id);

-- +goose StatementBegin
CREATE FUNCTION enforce_cscb_repair_manifest_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.tag IS DISTINCT FROM OLD.tag
        OR NEW.bucket IS DISTINCT FROM OLD.bucket
        OR NEW.old_consolidated_object_key_main IS DISTINCT FROM OLD.old_consolidated_object_key_main
        OR NEW.old_consolidated_object_version_id IS DISTINCT FROM OLD.old_consolidated_object_version_id
        OR NEW.old_consolidated_object_etag IS DISTINCT FROM OLD.old_consolidated_object_etag
        OR NEW.old_consolidated_object_bytes IS DISTINCT FROM OLD.old_consolidated_object_bytes
        OR NEW.start_height IS DISTINCT FROM OLD.start_height
        OR NEW.end_height IS DISTINCT FROM OLD.end_height
        OR NEW.canonical_block_count IS DISTINCT FROM OLD.canonical_block_count
        OR NEW.total_block_count IS DISTINCT FROM OLD.total_block_count
        OR NEW.row_set_sha256 IS DISTINCT FROM OLD.row_set_sha256
        OR NEW.prepared_at IS DISTINCT FROM OLD.prepared_at THEN
        RAISE EXCEPTION 'cannot change pinned CSCB repair manifest fields for repair id %', OLD.id;
    END IF;

    IF NOT (
        (OLD.state = 'prepared' AND NEW.state IN ('prepared', 'restored'))
        OR (OLD.state = 'restored' AND NEW.state IN ('restored', 'verified'))
        OR (OLD.state = 'verified' AND NEW.state IN ('verified', 'completed'))
        OR (OLD.state = 'completed' AND NEW.state = 'completed')
    ) THEN
        RAISE EXCEPTION 'invalid CSCB repair transition from % to % for repair id %', OLD.state, NEW.state, OLD.id;
    END IF;

    IF OLD.new_consolidated_object_key_main IS NOT NULL
        AND (
            NEW.new_consolidated_object_key_main IS DISTINCT FROM OLD.new_consolidated_object_key_main
            OR NEW.new_consolidated_object_version_id IS DISTINCT FROM OLD.new_consolidated_object_version_id
            OR NEW.new_consolidated_object_etag IS DISTINCT FROM OLD.new_consolidated_object_etag
            OR NEW.new_consolidated_object_bytes IS DISTINCT FROM OLD.new_consolidated_object_bytes
            OR NEW.verified_at IS DISTINCT FROM OLD.verified_at
        ) THEN
        RAISE EXCEPTION 'cannot change verified CSCB repair target for repair id %', OLD.id;
    END IF;

    IF OLD.restored_at IS NOT NULL
        AND NEW.restored_at IS DISTINCT FROM OLD.restored_at THEN
        RAISE EXCEPTION 'cannot change CSCB repair restore timestamp for repair id %', OLD.id;
    END IF;

    IF NEW.outcome IS DISTINCT FROM OLD.outcome
        AND NOT (
            OLD.state = 'verified'
            AND NEW.state = 'completed'
            AND OLD.outcome = ''
            AND NEW.outcome <> ''
        ) THEN
        RAISE EXCEPTION 'cannot change CSCB repair outcome outside completion for repair id %', OLD.id;
    END IF;

    IF OLD.old_object_deleted_at IS NOT NULL
        AND (
            NEW.old_object_deleted_at IS DISTINCT FROM OLD.old_object_deleted_at
            OR NEW.completed_at IS DISTINCT FROM OLD.completed_at
        ) THEN
        RAISE EXCEPTION 'cannot change completed CSCB repair timestamps for repair id %', OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cscb_repair_manifest_transition_trigger
BEFORE UPDATE ON cscb_repair_manifest
FOR EACH ROW
EXECUTE FUNCTION enforce_cscb_repair_manifest_transition();

-- +goose StatementBegin
CREATE FUNCTION prevent_cscb_repair_audit_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'CSCB repair audit rows are immutable';
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cscb_repair_block_update_trigger
BEFORE UPDATE ON cscb_repair_block
FOR EACH ROW
EXECUTE FUNCTION prevent_cscb_repair_audit_mutation();

CREATE TRIGGER cscb_repair_block_delete_trigger
BEFORE DELETE ON cscb_repair_block
FOR EACH ROW
EXECUTE FUNCTION prevent_cscb_repair_audit_mutation();

CREATE TRIGGER cscb_repair_manifest_delete_trigger
BEFORE DELETE ON cscb_repair_manifest
FOR EACH ROW
EXECUTE FUNCTION prevent_cscb_repair_audit_mutation();

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cscb_repair_manifest) THEN
        RAISE EXCEPTION 'cannot roll back CSCB repair migration while repair manifests exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS cscb_repair_manifest_delete_trigger ON cscb_repair_manifest;
DROP TRIGGER IF EXISTS cscb_repair_block_delete_trigger ON cscb_repair_block;
DROP TRIGGER IF EXISTS cscb_repair_block_update_trigger ON cscb_repair_block;
DROP FUNCTION IF EXISTS prevent_cscb_repair_audit_mutation();

DROP TRIGGER IF EXISTS cscb_repair_manifest_transition_trigger ON cscb_repair_manifest;
DROP FUNCTION IF EXISTS enforce_cscb_repair_manifest_transition();

DROP INDEX IF EXISTS idx_cscb_repair_block_repair_height;
DROP INDEX IF EXISTS idx_cscb_repair_manifest_pending;
DROP TABLE IF EXISTS cscb_repair_block;
DROP TABLE IF EXISTS cscb_repair_manifest;
