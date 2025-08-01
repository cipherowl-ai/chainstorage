# Data Migration Tool

Tool to migrate blockchain data from DynamoDB to PostgreSQL with complete reorg support and data integrity preservation.

## Overview

The migration tool performs a comprehensive transfer of blockchain data:
- **Block metadata** from DynamoDB to PostgreSQL (`block_metadata` + `canonical_blocks` tables)
- **Events** from DynamoDB to PostgreSQL (`block_events` table)
- **Complete reorg data** including both canonical and non-canonical blocks
- **Event ID-based migration** for efficient sequential processing

**Critical Requirements**: 
1. Block metadata **must** be migrated before events (foreign key dependencies)
2. Migration preserves complete blockchain history including all reorg blocks
3. Canonical block identification is maintained through migration ordering

## Architecture

### Advanced Migration Strategy
The tool uses a sophisticated **height-by-height approach** that:

1. **Queries ALL blocks** at each height from DynamoDB (canonical + non-canonical)
2. **Migrates non-canonical blocks first** to preserve reorg history
3. **Migrates canonical block last** to ensure proper canonicality in PostgreSQL
4. **Uses event ID ranges** for efficient event migration

### Reorg Handling Design
- **DynamoDB**: Stores both actual block hash entries and "canonical" markers
- **PostgreSQL**: Uses separate `block_metadata` (all blocks) and `canonical_blocks` (canonical only) tables
- **Migration**: Preserves complete reorg history while maintaining canonical block identification

## Basic Usage

```bash
# Migrate both blocks and events for a height range
go run cmd/admin/*.go migrate \
  --env=local \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=1000000 \
  --end-height=1001000 \
  --tag=1 \
  --event-tag=0
```

## Command Line Flags

| Flag | Required | Description | Default |
|------|----------|-------------|---------|
| `--start-height` | ✅ | Start block height (inclusive) | - |
| `--end-height` | ✅ | End block height (exclusive) | - |
| `--env` | ✅ | Environment (local/development/production) | - |
| `--blockchain` | ✅ | Blockchain name (e.g., ethereum, base) | - |
| `--network` | ✅ | Network name (e.g., mainnet, testnet) | - |
| `--tag` | | Block tag for migration | 1 |
| `--event-tag` | | Event tag for migration | 0 |
| `--batch-size` | | Events per batch | 100 |
| `--skip-blocks` | | Skip block migration (events only) | false |
| `--skip-events` | | Skip event migration (blocks only) | false |

## Migration Phases

### Phase 1: Height-by-Height Block Migration
For each height in the range:

1. **Query ALL blocks** at height from DynamoDB using direct table queries
2. **Separate canonical vs non-canonical** blocks client-side
3. **Migrate non-canonical blocks first** (preserves reorg history)
4. **Migrate canonical block last** (ensures canonicality in PostgreSQL)

```sql
-- DynamoDB Query Pattern:
-- All blocks: BlockPid = "{tag}-{height}"
-- Canonical: BlockPid = "{tag}-{height}" AND BlockRid = "canonical"
-- Non-canonical: BlockPid = "{tag}-{height}" AND BlockRid != "canonical"
```

### Phase 2: Event ID-Based Migration
1. **Determine event ID range** from start/end heights
2. **Migrate events sequentially** by event ID in batches
3. **Establish foreign key relationships** to migrated block metadata
4. **Handle missing events gracefully** (logged as debug)

**CRITICAL REQUIREMENT for Events-Only Migration:**
When using `--skip-blocks` (events-only migration), the corresponding block metadata **must already exist** in PostgreSQL. Events depend on block metadata through foreign key constraints (`block_events.block_metadata_id` → `block_metadata.id`). 

If block metadata is missing, the migration will fail with an error. To resolve this:
1. First run migration with `--skip-events` to migrate block metadata
2. Then run migration with `--skip-blocks` to migrate events

```bash
# Step 1: Migrate blocks first
go run ./cmd/admin/*.go migrate \
  --env=local \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=1000000 \
  --end-height=1001000 \
  --skip-events

# Step 2: Migrate events (now that block metadata exists)
go run ./cmd/admin/*.go migrate \
  --env=local \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=1000000 \
  --end-height=1001000 \
  --skip-blocks
```

## PostgreSQL Schema Design

### Block Storage Tables
```sql
-- All blocks ever observed (append-only)
CREATE TABLE block_metadata (
    id BIGSERIAL PRIMARY KEY,
    height BIGINT NOT NULL,
    tag INT NOT NULL,
    hash VARCHAR(66),
    parent_hash VARCHAR(66),
    object_key_main VARCHAR(255),
    timestamp TIMESTAMPTZ NOT NULL,
    skipped BOOLEAN NOT NULL DEFAULT FALSE
);

-- Canonical block tracking (current "winner" at each height)
CREATE TABLE canonical_blocks (
    height BIGINT NOT NULL,
    block_metadata_id BIGINT NOT NULL,
    tag INT NOT NULL,
    PRIMARY KEY (height, tag),
    FOREIGN KEY (block_metadata_id) REFERENCES block_metadata (id)
);
```

### Event Storage Table
```sql
-- Blockchain state change events (append-only)
CREATE TABLE block_events (
    event_tag INT NOT NULL DEFAULT 0,
    event_sequence BIGINT NOT NULL,
    event_type event_type_enum NOT NULL,
    block_metadata_id BIGINT NOT NULL,
    height BIGINT NOT NULL,
    hash VARCHAR(66),
    PRIMARY KEY (event_tag, event_sequence),
    FOREIGN KEY (block_metadata_id) REFERENCES block_metadata (id)
);
```

## Complete Reorg Support

### DynamoDB Storage Pattern
For height with reorgs, DynamoDB contains:
```
BlockPid: "1-12345", BlockRid: "0xabc123..." (non-canonical block)
BlockPid: "1-12345", BlockRid: "0xdef456..." (another non-canonical block) 
BlockPid: "1-12345", BlockRid: "canonical"   (canonical marker pointing to winner)
```

### Migration Process
1. **Query all blocks** at height: `BlockPid = "1-12345"`
2. **Filter canonical vs non-canonical** client-side based on `BlockRid`
3. **Migrate non-canonical first**: All reorg blocks → `block_metadata` only
4. **Migrate canonical last**: Winner block → `block_metadata` + `canonical_blocks`

### PostgreSQL Result
```sql
-- block_metadata table (ALL blocks)
id | height | hash        | ...
1  | 12345  | 0xabc123... | ... (non-canonical)
2  | 12345  | 0xdef456... | ... (non-canonical)  
3  | 12345  | 0x789abc... | ... (canonical)

-- canonical_blocks table (canonical only)
height | block_metadata_id | tag
12345  | 3                 | 1   (points to canonical block)
```

## Schema Mapping Details

### DynamoDB → PostgreSQL Block Metadata
```
DynamoDB BlockMetaDataDDBEntry → PostgreSQL Tables
├── Hash → block_metadata.hash
├── ParentHash → block_metadata.parent_hash  
├── Height → block_metadata.height
├── Tag → block_metadata.tag
├── ObjectKeyMain → block_metadata.object_key_main
├── Timestamp → block_metadata.timestamp
├── Skipped → block_metadata.skipped
└── (canonical status) → canonical_blocks.block_metadata_id (if canonical)
```

### DynamoDB → PostgreSQL Events
```
DynamoDB VersionedEventDDBEntry → PostgreSQL block_events
├── EventId → event_sequence
├── BlockHeight → height
├── BlockHash → hash  
├── EventTag → event_tag
├── EventType → event_type
├── Sequence → event_sequence
└── (block reference) → block_metadata_id (via foreign key)
```

## Usage Examples

### Complete Migration
```bash
# Migrate both blocks and events with full reorg support
go run cmd/admin/*.go migrate \
  --env=local \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=18000000 \
  --end-height=18001000 \
  --tag=1 \
  --event-tag=3
```

### Block-Only Migration
```bash
# Migrate only block metadata (useful for preparing for event migration)
go run cmd/admin/*.go migrate \
  --env=local \
  --blockchain=base \
  --network=mainnet \
  --start-height=1000000 \
  --end-height=1001000 \
  --skip-events
```

### Event-Only Migration  
```bash
# IMPORTANT: Block metadata must already exist in PostgreSQL!
# Run this ONLY after blocks have been migrated for this height range

# Migrate only events (requires blocks already migrated)
go run cmd/admin/*.go migrate \
  --env=local \
  --blockchain=polygon \
  --network=mainnet \
  --start-height=50000000 \
  --end-height=50001000 \
  --skip-blocks \
  --event-tag=2
```

### Two-Step Migration (Recommended for Large Ranges)
```bash
# Step 1: Migrate block metadata only
go run cmd/admin/*.go migrate \
  --env=local \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=18000000 \
  --end-height=18001000 \
  --tag=1 \
  --skip-events

# Step 2: Migrate events only (now that blocks exist)
go run cmd/admin/*.go migrate \
  --env=local \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=18000000 \
  --end-height=18001000 \
  --tag=1 \
  --event-tag=3 \
  --skip-blocks
```

### Large Range with Custom Batch Size
```bash
# Migrate large range with smaller batches for memory efficiency
go run cmd/admin/*.go migrate \
  --env=production \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=15000000 \
  --end-height=16000000 \
  --batch-size=50 \
  --tag=2 \
  --event-tag=3
```

## Performance Characteristics

### Block Migration Performance
- **Height-by-height processing**: Ensures complete reorg capture
- **Progress logging**: Every 100 heights processed
- **Reorg statistics**: Reports non-canonical block counts
- **Memory efficient**: Processes one height at a time

### Event Migration Performance  
- **Event ID-based batching**: Sequential processing by event ID
- **Configurable batch size**: Default 100 events per batch
- **Progress tracking**: Every 1000 events migrated
- **Range optimization**: Determines exact event ID bounds

### Typical Performance
- **Blocks**: ~1000 heights/minute (varies by reorg frequency)
- **Events**: ~10,000 events/minute (varies by event density)
- **Memory usage**: Low due to streaming approach

## Error Handling & Recovery

### Common Issues

1. **Foreign key violations during event migration**
   ```
   Error: foreign key constraint "block_events_block_metadata_id_fkey"
   Solution: Ensure blocks were migrated first for the height range
   ```

2. **No canonical block found at height**
   ```
   Log: "No canonical block found at height X"
   Behavior: Logged as debug, migration continues (normal for sparse chains)
   ```

3. **Multiple canonical blocks at same height**
   ```
   Error: "multiple canonical blocks found for {tag}-{height}"
   Solution: Investigate DynamoDB data consistency
   ```

4. **Connection timeouts**
   ```
   Error: Database connection timeouts
   Solution: Check database connectivity, adjust timeout settings
   ```

### Recovery Strategy

The migration is **fully resumable** from any height:

```bash
# Resume from height 1500000 if migration failed at 1500500
go run cmd/admin/*.go migrate \
  --env=local \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=1500000 \
  --end-height=2000000
```

**PostgreSQL handles duplicates gracefully**: 
- Blocks: Unique constraints prevent duplicate inserts
- Events: Primary key constraints prevent duplicate events
- Canonical blocks: `ON CONFLICT` resolution ensures correctness

## Monitoring & Validation

### Progress Monitoring
```bash
# Monitor migration progress in logs
go run cmd/admin/*.go migrate ... | grep "progress"

# Expected output:
# Block migration progress: processed=100/1000 (10.00%) totalNonCanonicalBlocks=5
# Event migration progress: processed=1000/50000 (2.00%)
```

### Data Validation
```sql
-- Verify block counts match
SELECT COUNT(*) FROM block_metadata WHERE height BETWEEN 1000000 AND 1001000;
SELECT COUNT(*) FROM canonical_blocks WHERE height BETWEEN 1000000 AND 1001000;

-- Verify event counts and foreign key integrity
SELECT COUNT(*) FROM block_events WHERE height BETWEEN 1000000 AND 1001000;
SELECT COUNT(*) FROM block_events e 
JOIN block_metadata b ON e.block_metadata_id = b.id 
WHERE e.height BETWEEN 1000000 AND 1001000;
```

## Technical Implementation Notes

### Direct DynamoDB Queries
The migration uses **direct DynamoDB client queries** (not the MetaStorage interface) to:
- Access complete block data including non-canonical blocks
- Query by exact DynamoDB key patterns
- Ensure consistent reads for data integrity

### Canonical Block Ordering
Critical implementation detail:
1. **Non-canonical blocks migrated first** → Only appear in `block_metadata`
2. **Canonical block migrated last** → Appears in both tables
3. **PostgreSQL `PersistBlockMetas`** assumes last block is canonical
4. **Result**: Correct canonical block identification in PostgreSQL

### Event ID Strategy
- **Event IDs in DynamoDB** correspond to **event sequences in PostgreSQL**
- **Sequential migration** by event ID ensures proper ordering
- **Range-based queries** optimize DynamoDB read efficiency
- **Foreign key resolution** handles missing block references gracefully

### Environment Configuration
The tool automatically uses the correct storage backends based on environment:
- **Local**: LocalStack DynamoDB + Local PostgreSQL
- **Development/Production**: AWS DynamoDB + Managed PostgreSQL
- **Configuration**: Loaded from `config/chainstorage/{blockchain}/{network}/{env}.yml` 