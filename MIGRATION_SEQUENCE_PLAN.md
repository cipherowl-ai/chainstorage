# Event-Driven Migration Plan (Final Version)

## Overview
Completely refactor the migrator to be event-driven by default. Events are the source of truth - they tell us which blocks to migrate and in what order.

## Core Principle
**Events are fetched by sequence number, blocks are extracted from BLOCK_ADDED events, and both are migrated together**

## Key Configuration Changes

### Workflow Config
```yaml
workflows:
  migrator:
    batch_size: 5000        # Number of event sequences per activity
    checkpoint_size: 50000  # Number of events before continue-as-new
    parallelism: 8          # Number of concurrent workers
```

## The Complete Flow

### 1. Workflow Level Changes
```go
// workflow/migrator.go
func (w *Migrator) execute(ctx workflow.Context, request *MigratorRequest) error {
    // Use StartEventSequence everywhere (no more StartHeight confusion)
    startSequence := request.StartEventSequence
    if startSequence == 0 && request.AutoResume {
        // Get latest event sequence from PostgreSQL
        latestEventResp, err := w.getLatestEventFromPostgres.Execute(ctx, 
            &activity.GetLatestEventFromPostgresRequest{EventTag: eventTag})
        if latestEventResp.Found {
            startSequence = latestEventResp.Sequence + 1
        }
    }
    
    // Get batch size from config
    batchSize := cfg.BatchSize // e.g., 5000 events per activity
    checkpointSize := cfg.CheckpointSize // e.g., 50000 events before continue-as-new
    
    // Get max event ID to know when to stop
    maxEventId := GetMaxEventIdFromDynamoDB(eventTag)
    
    // Track total events processed for checkpointing
    totalProcessed := int64(0)
    
    // Process in batches
    for currentSeq := startSequence; currentSeq <= maxEventId; currentSeq += batchSize {
        endSeq := min(currentSeq + batchSize, maxEventId + 1)
        
        // Call migrator activity with event sequences
        activityRequest := &MigratorRequest{
            StartEventSequence: currentSeq,
            EndEventSequence:   endSeq,
            EventTag:          eventTag,
            Tag:               tag,
            Parallelism:       parallelism,
        }
        
        response := w.migrator.Execute(ctx, activityRequest)
        totalProcessed += int64(response.EventsMigrated)
        
        // Checkpoint if needed
        if totalProcessed >= checkpointSize {
            newRequest := *request
            newRequest.StartEventSequence = endSeq
            logger.Info("Checkpoint reached, continuing as new", 
                zap.Int64("eventsProcessed", totalProcessed),
                zap.Int64("nextStartSequence", endSeq))
            return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
        }
    }
    
    return nil
}
```

### 2. Activity Level Changes (migrator.go)

#### 2.1 Updated MigratorRequest Structure
```go
type MigratorRequest struct {
    StartEventSequence int64  // Start event sequence (no more StartHeight!)
    EndEventSequence   int64  // End event sequence (exclusive)
    EventTag           uint32
    Tag                uint32
    Parallelism        int    // Number of concurrent workers
}
```

#### 2.2 Main Execute Function
```go
func (a *Migrator) execute(ctx context.Context, request *MigratorRequest) (*MigratorResponse, error) {
    startTime := time.Now()
    logger := a.getLogger(ctx).With(
        zap.Int64("startEventSequence", request.StartEventSequence),
        zap.Int64("endEventSequence", request.EndEventSequence),
        zap.Uint32("eventTag", request.EventTag),
        zap.Int("parallelism", request.Parallelism))
    
    logger.Info("Starting event-driven migration")
    
    // Add heartbeat mechanism
    heartbeatTicker := time.NewTicker(30 * time.Second)
    defer heartbeatTicker.Stop()
    
    go func() {
        for range heartbeatTicker.C {
            activity.RecordHeartbeat(ctx, fmt.Sprintf("Processing events [%d, %d), elapsed: %v",
                request.StartEventSequence, request.EndEventSequence, time.Since(startTime)))
        }
    }()
    
    // Create storage instances
    migrationData, err := a.createStorageInstances(ctx)
    if err != nil {
        return nil, xerrors.Errorf("failed to create storage instances: %w", err)
    }
    
    // Step 1: Fetch events by sequence (with parallelism)
    events, err := a.fetchEventsBySequence(ctx, logger, migrationData, request)
    if err != nil {
        return nil, xerrors.Errorf("failed to fetch events: %w", err)
    }
    
    if len(events) == 0 {
        logger.Info("No events found in sequence range")
        return &MigratorResponse{
            Success: true,
            Message: "No events to migrate",
        }, nil
    }
    
    // Step 2: Extract blocks from BLOCK_ADDED events
    blocksToMigrate := a.extractBlocksFromEvents(logger, events)
    
    logger.Info("Extracted blocks from events",
        zap.Int("totalEvents", len(events)),
        zap.Int("blocksToMigrate", len(blocksToMigrate)))
    
    // Step 3: Migrate blocks using existing fast/slow path
    blocksMigrated := 0
    if len(blocksToMigrate) > 0 {
        blocksMigrated, err = a.migrateExtractedBlocks(ctx, logger, migrationData, request, blocksToMigrate)
        if err != nil {
            return nil, xerrors.Errorf("failed to migrate blocks: %w", err)
        }
    }
    
    // Step 4: Migrate events
    eventsMigrated := 0
    if len(events) > 0 {
        eventsMigrated, err = a.persistEvents(ctx, logger, migrationData, request, events)
        if err != nil {
            return nil, xerrors.Errorf("failed to migrate events: %w", err)
        }
    }
    
    duration := time.Since(startTime)
    logger.Info("Event-driven migration completed",
        zap.Int("blocksMigrated", blocksMigrated),
        zap.Int("eventsMigrated", eventsMigrated),
        zap.Duration("duration", duration))
    
    return &MigratorResponse{
        BlocksMigrated: blocksMigrated,
        EventsMigrated: eventsMigrated,
        Success:        true,
        Message:        fmt.Sprintf("Migrated %d blocks and %d events in %v", 
                                   blocksMigrated, eventsMigrated, duration),
    }, nil
}
```

#### 2.3 Fetch Events by Sequence with Mini-Batches
```go
func (a *Migrator) fetchEventsBySequence(ctx context.Context, logger *zap.Logger, 
    data *MigrationData, request *MigratorRequest) ([]*model.EventEntry, error) {
    
    startSeq := request.StartEventSequence
    endSeq := request.EndEventSequence
    totalSequences := endSeq - startSeq
    
    // Calculate mini-batch size based on parallelism
    parallelism := request.Parallelism
    if parallelism <= 0 {
        parallelism = 1
    }
    
    // Mini-batch size = total sequences / parallelism
    miniBatchSize := (totalSequences + int64(parallelism) - 1) / int64(parallelism)
    
    logger.Info("Fetching events with parallelism",
        zap.Int64("startSeq", startSeq),
        zap.Int64("endSeq", endSeq),
        zap.Int64("totalSequences", totalSequences),
        zap.Int("parallelism", parallelism),
        zap.Int64("miniBatchSize", miniBatchSize))
    
    // Parallel fetch using workers
    type batchResult struct {
        events []*model.EventEntry
        err    error
        start  int64
        end    int64
    }
    
    inputChan := make(chan struct{start, end int64}, parallelism)
    resultChan := make(chan batchResult, parallelism)
    
    // Create work items
    for start := startSeq; start < endSeq; start += miniBatchSize {
        end := min(start + miniBatchSize, endSeq)
        inputChan <- struct{start, end int64}{start, end}
    }
    close(inputChan)
    
    // Start workers
    var wg sync.WaitGroup
    wg.Add(parallelism)
    
    for i := 0; i < parallelism; i++ {
        go func(workerID int) {
            defer wg.Done()
            for work := range inputChan {
                // Fetch events using GetEventsByEventIdRange
                events, err := data.SourceStorage.GetEventsByEventIdRange(
                    ctx, request.EventTag, work.start, work.end)
                
                resultChan <- batchResult{
                    events: events, 
                    err: err,
                    start: work.start,
                    end: work.end,
                }
            }
        }(i)
    }
    
    // Wait for workers and close result channel
    go func() {
        wg.Wait()
        close(resultChan)
    }()
    
    // Collect results
    var allEvents []*model.EventEntry
    missingRanges := []string{}
    
    for result := range resultChan {
        if result.err != nil {
            // Handle missing sequences gracefully
            if errors.Is(result.err, storage.ErrItemNotFound) {
                logger.Warn("Some event sequences not found in range",
                    zap.Int64("start", result.start),
                    zap.Int64("end", result.end))
                missingRanges = append(missingRanges, fmt.Sprintf("[%d,%d)", result.start, result.end))
                continue
            }
            return nil, xerrors.Errorf("failed to fetch events [%d,%d): %w", 
                                      result.start, result.end, result.err)
        }
        allEvents = append(allEvents, result.events...)
    }
    
    if len(missingRanges) > 0 {
        logger.Warn("Some event ranges had missing sequences",
            zap.Strings("missingRanges", missingRanges))
    }
    
    // Sort by EventId to ensure proper ordering
    sort.Slice(allEvents, func(i, j int) bool {
        return allEvents[i].EventId < allEvents[j].EventId
    })
    
    logger.Info("Fetched events successfully",
        zap.Int("totalEvents", len(allEvents)))
    
    return allEvents, nil
}
```

#### 2.4 Extract Blocks from Events
```go
type BlockToMigrate struct {
    Height     uint64
    Hash       string
    ParentHash string
    EventSeq   int64  // Event sequence for ordering
}

func (a *Migrator) extractBlocksFromEvents(logger *zap.Logger, 
    events []*model.EventEntry) []BlockToMigrate {
    
    var blocks []BlockToMigrate
    blockAddedCount := 0
    blockRemovedCount := 0
    
    for _, event := range events {
        switch event.EventType {
        case api.BlockchainEvent_BLOCK_ADDED:
            blocks = append(blocks, BlockToMigrate{
                Height:     event.BlockHeight,
                Hash:       event.BlockHash,
                ParentHash: event.ParentHash,
                EventSeq:   event.EventId,
            })
            blockAddedCount++
        case api.BlockchainEvent_BLOCK_REMOVED:
            blockRemovedCount++
        }
    }
    
    logger.Info("Extracted blocks from events",
        zap.Int("totalEvents", len(events)),
        zap.Int("blockAddedEvents", blockAddedCount),
        zap.Int("blockRemovedEvents", blockRemovedCount),
        zap.Int("blocksToMigrate", len(blocks)))
    
    return blocks
}
```

#### 2.5 Migrate Extracted Blocks (Reusing Fast/Slow Path)
```go
// Add a field to track last migrated block across batches
type Migrator struct {
    // ... existing fields ...
    lastMigratedBlock *api.BlockMetadata // Track last block for validation
}

func (a *Migrator) migrateExtractedBlocks(ctx context.Context, logger *zap.Logger,
    data *MigrationData, request *MigratorRequest, 
    blocksToMigrate []BlockToMigrate) (int, error) {
    
    if len(blocksToMigrate) == 0 {
        return 0, nil
    }
    
    // Detect reorgs in current batch
    hasReorgs := a.detectReorgs(blocksToMigrate)
    
    // Fetch actual block data from DynamoDB
    allBlocksWithInfo, err := a.fetchBlockData(ctx, logger, data, request, blocksToMigrate)
    if err != nil {
        return 0, xerrors.Errorf("failed to fetch block data: %w", err)
    }
    
    // Sort blocks: by height first, then by event sequence
    sort.Slice(allBlocksWithInfo, func(i, j int) bool {
        if allBlocksWithInfo[i].Height != allBlocksWithInfo[j].Height {
            return allBlocksWithInfo[i].Height < allBlocksWithInfo[j].Height
        }
        // For same height, order by event sequence (last one wins)
        return allBlocksWithInfo[i].EventSeq < allBlocksWithInfo[j].EventSeq
    })
    
    // REUSE EXISTING FAST/SLOW PATH LOGIC
    blocksPersistedCount := 0
    
    if !hasReorgs {
        // FAST PATH: No reorgs in current batch, can validate
        logger.Info("No reorgs detected, using fast bulk persist with validation")
        allBlocks := make([]*api.BlockMetadata, len(allBlocksWithInfo))
        for i, blockWithInfo := range allBlocksWithInfo {
            allBlocks[i] = blockWithInfo.BlockMetadata
        }
        
        // Get last block for validation
        var lastBlock *api.BlockMetadata
        if a.lastMigratedBlock != nil {
            lastBlock = a.lastMigratedBlock
            logger.Debug("Using last migrated block for validation",
                zap.Uint64("lastHeight", lastBlock.Height),
                zap.String("lastHash", lastBlock.Hash))
        } else if len(allBlocks) > 0 && allBlocks[0].Height > 0 {
            // Try to get previous block from PostgreSQL for first batch
            prevHeight := allBlocks[0].Height - 1
            prevBlock, err := data.DestStorage.GetBlockByHeight(ctx, request.Tag, prevHeight)
            if err == nil {
                lastBlock = prevBlock
                logger.Debug("Found previous block in PostgreSQL for validation",
                    zap.Uint64("prevHeight", prevHeight),
                    zap.String("prevHash", prevBlock.Hash))
            }
        }
        
        // Bulk persist with validation (lastBlock can be nil for genesis)
        err := data.DestStorage.PersistBlockMetas(ctx, false, allBlocks, lastBlock)
        if err != nil {
            logger.Error("Failed to bulk persist blocks",
                zap.Error(err),
                zap.Int("blockCount", len(allBlocks)))
            return 0, xerrors.Errorf("failed to bulk persist blocks: %w", err)
        }
        blocksPersistedCount = len(allBlocks)
        
        // Update last migrated block (last block in sorted array)
        a.lastMigratedBlock = allBlocks[len(allBlocks)-1]
        
    } else {
        // SLOW PATH: Has reorgs, persist one by one without validation
        logger.Info("Reorgs detected, persisting blocks one by one")
        
        var lastPersistedBlock *api.BlockMetadata
        for _, blockWithInfo := range allBlocksWithInfo {
            err := data.DestStorage.PersistBlockMetas(ctx, false,
                []*api.BlockMetadata{blockWithInfo.BlockMetadata}, nil)
            if err != nil {
                logger.Error("Failed to persist block",
                    zap.Uint64("height", blockWithInfo.Height),
                    zap.String("hash", blockWithInfo.Hash),
                    zap.Error(err))
                return blocksPersistedCount, xerrors.Errorf("failed to persist block: %w", err)
            }
            blocksPersistedCount++
            lastPersistedBlock = blockWithInfo.BlockMetadata
            
            // Log progress periodically
            if blocksPersistedCount%100 == 0 {
                logger.Debug("Progress update",
                    zap.Int("persisted", blocksPersistedCount),
                    zap.Int("total", len(allBlocksWithInfo)))
            }
        }
        
        // Update last migrated block (last one persisted, which is canonical due to ordering)
        if lastPersistedBlock != nil {
            a.lastMigratedBlock = lastPersistedBlock
        }
    }
    
    logger.Info("Blocks migrated successfully",
        zap.Int("blocksPersistedCount", blocksPersistedCount),
        zap.Bool("hadReorgs", hasReorgs),
        zap.Uint64("lastBlockHeight", a.lastMigratedBlock.Height),
        zap.String("lastBlockHash", a.lastMigratedBlock.Hash))
    
    return blocksPersistedCount, nil
}
```

## Key Configuration Parameters

### Config Template (config_templates/chainstorage/ethereum/mainnet/config.yaml)
```yaml
workflows:
  migrator:
    batch_size: 5000          # Events per activity (default: 5000)
    checkpoint_size: 50000    # Events before continue-as-new (default: 50000)
    parallelism: 8            # Concurrent workers (default: 8)
    backoff_interval: 1s      # Backoff between batches
```

### How Batching Works:
1. **Batch Size (5000)**: Each activity processes 5000 event sequences
2. **Mini-Batch Size**: batch_size / parallelism = 5000/8 = 625 events per worker
3. **Checkpoint Size (50000)**: After 50000 events (10 activities), continue-as-new
4. **Parallelism (8)**: 8 concurrent workers fetch events in parallel

## Migration Examples

### Example 1: Fresh Start
```
StartEventSequence: 0
Batch Size: 5000
Parallelism: 8

Activity 1: Events 0-5000 (8 workers, each fetches 625 events)
Activity 2: Events 5000-10000
...
Activity 10: Events 45000-50000
Checkpoint → Continue as new from 50000
```

### Example 2: With Reorgs
```
Events 1000-1010:
- Event 1000: BLOCK_ADDED height=1000
- Event 1001: BLOCK_REMOVED height=1000
- Event 1002: BLOCK_ADDED height=1000 (reorg)

1. Fetch events 1000-1010
2. Extract 2 blocks at height 1000
3. Detect reorg → use slow path
4. Persist blocks one-by-one (last wins)
5. Persist all events
```

### Example 3: Auto-Resume
```
PostgreSQL has events up to sequence 75432
1. Query latest: 75432
2. Start from: 75433
3. Continue with batch size 5000
4. Process events 75433-80433, 80433-85433, etc.
```

## Benefits of This Approach

1. **Clear Separation**: No confusion between StartHeight and StartEventSequence
2. **Configurable Batching**: Easy to tune batch_size and checkpoint_size
3. **Efficient Parallelism**: Mini-batches distribute work evenly
4. **Proper Checkpointing**: Continue-as-new based on events processed
5. **Reorg Handling**: Existing fast/slow path still works perfectly

## Implementation Checklist

- [ ] Remove StartHeight/EndHeight from MigratorRequest
- [ ] Add StartEventSequence/EndEventSequence
- [ ] Update workflow to use event sequences
- [ ] Add batch_size to config templates
- [ ] Implement fetchEventsBySequence with mini-batches
- [ ] Implement extractBlocksFromEvents
- [ ] Adapt existing block migration to use extracted blocks
- [ ] Update auto-resume to use event sequences
- [ ] Test with various batch sizes and parallelism settings