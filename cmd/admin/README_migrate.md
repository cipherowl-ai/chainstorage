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

### Basic Parameters
| Flag | Required | Description | Default |
|------|----------|-------------|---------|
| `--start-height` | ✅ | Start block height (inclusive) | - |
| `--end-height` | ✅ | End block height (exclusive) | - |
| `--env` | ✅ | Environment (local/development/production) | - |
| `--blockchain` | ✅ | Blockchain name (e.g., ethereum, base) | - |
| `--network` | ✅ | Network name (e.g., mainnet, testnet) | - |
| `--tag` | | Block tag for migration | 1 |
| `--event-tag` | | Event tag for migration | 0 |

### Performance & Batch Parameters  
| Flag | Required | Description | Default |
|------|----------|-------------|---------|
| `--batch-size` | | Number of blocks to process in each workflow batch | 100 |
| `--mini-batch-size` | | Number of blocks to process in each activity mini-batch | batch-size/10 |
| `--checkpoint-size` | | Number of blocks to process before creating a workflow checkpoint | 10000 |
| `--parallelism` | | Number of parallel workers for processing mini-batches | 1 |
| `--backoff-interval` | | Time duration to wait between batches (e.g., '1s', '500ms') | - |

### Migration Mode Parameters
| Flag | Required | Description | Default |
|------|----------|-------------|---------|
| `--skip-blocks` | | Skip block migration (events only) | false |
| `--skip-events` | | Skip event migration (blocks only) | false |
| `--continuous-sync` | | Enable continuous sync mode (workflow only, not supported in direct mode) | false |
| `--sync-interval` | | Time duration to wait between continuous sync cycles (e.g., '1m', '30s') | 1m |

## Continuous Sync Mode

The migrator supports **continuous sync mode** for real-time data synchronization. This mode is designed for workflow-based migrations and enables:

- **Infinite loop operation**: Automatically restarts migration when current batch completes
- **Dynamic end height**: Sets new StartHeight to current EndHeight and resets EndHeight to 0 (sync to latest)  
- **Configurable sync intervals**: Wait duration between continuous sync cycles
- **Automatic workflow continuation**: Uses Temporal's ContinueAsNewError for seamless restarts

### Continuous Sync Process
1. Complete current migration batch (StartHeight → EndHeight)
2. Set new StartHeight = previous EndHeight
3. Reset EndHeight = 0 (query latest block from source)
4. Wait for SyncInterval duration  
5. Restart workflow with new parameters using ContinueAsNewError
6. Repeat indefinitely

### Validation Rules for Continuous Sync
- `EndHeight` must be 0 OR greater than `StartHeight` when `ContinuousSync` is enabled
- When `EndHeight = 0`, the tool automatically queries the latest block from DynamoDB
- `SyncInterval` defaults to 1 minute if not specified or invalid

### Continuous Sync Examples

```bash
# Basic continuous sync - syncs every minute from height 1000000 to latest
go run cmd/admin/*.go migrate \
  --env=local \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=1000000 \
  --tag=2 \
  --event-tag=3 \
  --continuous-sync

# High-performance continuous sync with custom parameters  
go run cmd/admin/*.go migrate \
  --env=production \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=18000000 \
  --tag=1 \
  --event-tag=0 \
  --continuous-sync \
  --sync-interval=30s \
  --batch-size=500 \
  --mini-batch-size=50 \
  --parallelism=4 \
  --checkpoint-size=5000
```

**Note**: Continuous sync is only available when using the Temporal workflow system. The direct migration command will show a warning and perform a one-time migration if `--continuous-sync` is specified.

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

### Large Range with Custom Performance Parameters
```bash
# Migrate large range with optimized batch sizes and parallelism
go run cmd/admin/*.go migrate \
  --env=production \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=15000000 \
  --end-height=16000000 \
  --batch-size=1000 \
  --mini-batch-size=100 \
  --parallelism=8 \
  --checkpoint-size=50000 \
  --tag=2 \
  --event-tag=3

# Memory-efficient migration with smaller batches
go run cmd/admin/*.go migrate \
  --env=production \
  --blockchain=ethereum \
  --network=mainnet \
  --start-height=15000000 \
  --end-height=16000000 \
  --batch-size=50 \
  --mini-batch-size=10 \
  --parallelism=2 \
  --backoff-interval=1s \
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

### Performance Tuning Parameters

#### Batch Size Configuration
- **`--batch-size`**: Controls workflow-level batching (default: 100)
  - Higher values = fewer checkpoints, more memory usage
  - Lower values = more checkpoints, less memory usage
  - Recommended: 100-1000 for most use cases

- **`--mini-batch-size`**: Controls activity-level batching (default: batch-size/10)
  - Used for parallel processing within a workflow batch  
  - Should be 5-20% of batch-size for optimal parallelism
  - Recommended: 10-100 for most use cases

#### Parallelism & Checkpointing
- **`--parallelism`**: Number of parallel workers (default: 1)
  - Higher values = faster processing, more resource usage
  - Limited by database connection pool and activity concurrency
  - Recommended: 2-8 for most use cases, up to 16 for high-throughput

- **`--checkpoint-size`**: Blocks processed before workflow checkpoint (default: 10000)
  - Higher values = fewer workflow restarts, more memory usage
  - Lower values = more frequent checkpoints, better recovery
  - Recommended: 5000-50000 depending on batch size

#### Timing Controls
- **`--backoff-interval`**: Wait time between batches
  - Useful for rate limiting to reduce database load
  - Format: Go duration string (e.g., "1s", "500ms", "2m")
  - Recommended: 100ms-5s for rate limiting scenarios

### Typical Performance
- **Blocks**: ~1000-5000 heights/minute (varies by reorg frequency and parallelism)
- **Events**: ~10,000-50,000 events/minute (varies by event density and parallelism)
- **Memory usage**: Low due to streaming approach, scales with batch size
- **Database connections**: Scales with parallelism setting

### Performance Examples

#### High Throughput Configuration
```bash
# Optimized for maximum throughput
--batch-size=2000 \
--mini-batch-size=200 \
--parallelism=8 \
--checkpoint-size=50000
```

#### Memory Efficient Configuration  
```bash
# Optimized for low memory usage
--batch-size=100 \
--mini-batch-size=20 \
--parallelism=2 \
--checkpoint-size=5000 \
--backoff-interval=1s
```

#### Balanced Configuration
```bash
# Good balance of performance and resource usage
--batch-size=500 \
--mini-batch-size=50 \
--parallelism=4 \
--checkpoint-size=20000
```

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