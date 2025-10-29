-- +goose Up
-- Add is_watermark column to canonical_blocks table
-- This column controls visibility of blocks to the streamer
-- Blocks are written with is_watermark=FALSE initially, then set to TRUE after validation
ALTER TABLE canonical_blocks ADD COLUMN is_watermark BOOLEAN NOT NULL DEFAULT FALSE;

-- Create partial index for efficient watermark queries
-- This index only includes watermarked blocks, keeping it small
CREATE INDEX idx_canonical_watermark ON canonical_blocks (tag, height DESC) WHERE is_watermark = TRUE;

-- Set watermark on the current highest block for each tag to prevent GetLatestBlock failures
-- This ensures continuity during migration by marking existing latest blocks as validated
UPDATE canonical_blocks cb1
SET is_watermark = TRUE
WHERE height = (
    SELECT MAX(height)
    FROM canonical_blocks cb2
    WHERE cb2.tag = cb1.tag
);

-- +goose Down
-- Drop the partial index first
DROP INDEX IF EXISTS idx_canonical_watermark;

-- Drop the is_watermark column
ALTER TABLE canonical_blocks DROP COLUMN IF EXISTS is_watermark;