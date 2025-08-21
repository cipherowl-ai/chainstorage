package activity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/uber-go/tally/v4"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	dynamodb_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	dynamodb_model "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	postgres_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Migrator struct {
		baseActivity
		config       *config.Config
		session      *session.Session
		dynamoClient *dynamodb.DynamoDB
		blockTable   string
		metrics      tally.Scope
	}

	BlockToMigrate struct {
		Height     uint64
		Hash       string
		ParentHash string
		EventSeq   int64 // Event sequence for ordering
	}

	GetLatestBlockHeightActivity struct {
		baseActivity
		config       *config.Config
		session      *session.Session
		dynamoClient *dynamodb.DynamoDB
		blockTable   string
		metrics      tally.Scope
	}

	GetLatestBlockFromPostgresActivity struct {
		baseActivity
		config  *config.Config
		metrics tally.Scope
	}

	GetLatestEventFromPostgresActivity struct {
		baseActivity
		config  *config.Config
		metrics tally.Scope
	}

	GetMaxEventIdActivity struct {
		baseActivity
		config  *config.Config
		session *session.Session
		metrics tally.Scope
	}

	MigratorParams struct {
		fx.In
		fxparams.Params
		Runtime cadence.Runtime
		Session *session.Session
	}

	MigratorRequest struct {
		StartEventSequence int64 // Start event sequence (no more StartHeight!)
		EndEventSequence   int64 // End event sequence (exclusive)
		EventTag           uint32
		Tag                uint32
		Parallelism        int // Number of concurrent workers
	}

	MigratorResponse struct {
		BlocksMigrated int
		EventsMigrated int
		Success        bool
		Message        string
	}

	MigrationData struct {
		SourceStorage metastorage.MetaStorage
		DestStorage   metastorage.MetaStorage
		Config        *config.Config
		DynamoClient  *dynamodb.DynamoDB
		BlockTable    string
	}
)

func NewMigrator(params MigratorParams) *Migrator {
	a := &Migrator{
		baseActivity: newBaseActivity(ActivityMigrator, params.Runtime),
		config:       params.Config,
		session:      params.Session,
		dynamoClient: dynamodb.New(params.Session),
		blockTable:   params.Config.AWS.DynamoDB.BlockTable,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func NewGetLatestBlockHeightActivity(params MigratorParams) *GetLatestBlockHeightActivity {
	a := &GetLatestBlockHeightActivity{
		baseActivity: newBaseActivity(ActivityGetLatestBlockHeight, params.Runtime),
		config:       params.Config,
		session:      params.Session,
		dynamoClient: dynamodb.New(params.Session),
		blockTable:   params.Config.AWS.DynamoDB.BlockTable,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func NewGetLatestBlockFromPostgresActivity(params MigratorParams) *GetLatestBlockFromPostgresActivity {
	a := &GetLatestBlockFromPostgresActivity{
		baseActivity: newBaseActivity(ActivityGetLatestBlockFromPostgres, params.Runtime),
		config:       params.Config,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func NewGetLatestEventFromPostgresActivity(params MigratorParams) *GetLatestEventFromPostgresActivity {
	a := &GetLatestEventFromPostgresActivity{
		baseActivity: newBaseActivity(ActivityGetLatestEventFromPostgres, params.Runtime),
		config:       params.Config,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func NewGetMaxEventIdActivity(params MigratorParams) *GetMaxEventIdActivity {
	a := &GetMaxEventIdActivity{
		baseActivity: newBaseActivity(ActivityGetMaxEventId, params.Runtime),
		config:       params.Config,
		session:      params.Session,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func (a *Migrator) Execute(ctx workflow.Context, request *MigratorRequest) (*MigratorResponse, error) {
	var response MigratorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Migrator) execute(ctx context.Context, request *MigratorRequest) (*MigratorResponse, error) {
	startTime := time.Now()
	logger := a.getLogger(ctx).With(
		zap.Int64("startEventSequence", request.StartEventSequence),
		zap.Int64("endEventSequence", request.EndEventSequence),
		zap.Uint32("eventTag", request.EventTag),
		zap.Int("parallelism", request.Parallelism))

	logger.Info("Starting event-driven migration")

	// Add heartbeat mechanism - send heartbeat every 10 seconds to avoid timeout
	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()

	go func() {
		for range heartbeatTicker.C {
			select {
			case <-ctx.Done():
				return
			default:
				activity.RecordHeartbeat(ctx, fmt.Sprintf("Processing events [%d, %d), elapsed: %v",
					request.StartEventSequence, request.EndEventSequence, time.Since(startTime)))
			}
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
	// Note: Block validation is handled within migrateExtractedBlocks by querying PostgreSQL
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
		Message: fmt.Sprintf("Migrated %d blocks and %d events in %v",
			blocksMigrated, eventsMigrated, duration),
	}, nil
}

func (a *Migrator) createStorageInstances(ctx context.Context) (*MigrationData, error) {
	logger := a.getLogger(ctx)

	// Create DynamoDB storage directly
	dynamoDBParams := dynamodb_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
		Session: a.session,
	}
	sourceResult, err := dynamodb_storage.NewMetaStorage(dynamoDBParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create DynamoDB storage: %w", err)
	}

	// Create PostgreSQL storage using shared connection pool
	postgresParams := postgres_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
	}
	destResult, err := postgres_storage.NewMetaStorage(postgresParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
	}

	return &MigrationData{
		SourceStorage: sourceResult.MetaStorage,
		DestStorage:   destResult.MetaStorage,
		Config:        a.config,
		DynamoClient:  a.dynamoClient,
		BlockTable:    a.blockTable,
	}, nil
}

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

	inputChan := make(chan struct{ start, end int64 }, parallelism)
	resultChan := make(chan batchResult, parallelism)

	// Create work items
	for start := startSeq; start < endSeq; start += miniBatchSize {
		end := start + miniBatchSize
		if end > endSeq {
			end = endSeq
		}
		inputChan <- struct{ start, end int64 }{start, end}
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
					err:    err,
					start:  work.start,
					end:    work.end,
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
	processedBatches := 0
	totalBatches := (endSeq-startSeq+miniBatchSize-1)/miniBatchSize

	for result := range resultChan {
		processedBatches++
		
		// Record heartbeat with progress
		activity.RecordHeartbeat(ctx, fmt.Sprintf("Fetching events: processed %d/%d mini-batches, collected %d events so far",
			processedBatches, totalBatches, len(allEvents)))
		
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

		// Query PostgreSQL for the latest canonical block for validation
		// Since there are no reorgs, the first block in our batch should reference the latest canonical block
		var lastBlock *api.BlockMetadata
		if len(allBlocks) > 0 && allBlocks[0].Height > 0 {
			// Get the previous block from PostgreSQL (should be the latest canonical)
			prevHeight := allBlocks[0].Height - 1
			prevBlock, err := data.DestStorage.GetBlockByHeight(ctx, request.Tag, prevHeight)
			if err == nil {
				lastBlock = prevBlock
				logger.Debug("Found latest canonical block in PostgreSQL for validation",
					zap.Uint64("prevHeight", prevHeight),
					zap.String("prevHash", prevBlock.Hash))
			} else if !errors.Is(err, storage.ErrItemNotFound) {
				// Log error but continue without validation (genesis or error case)
				logger.Warn("Could not fetch previous block for validation",
					zap.Uint64("prevHeight", prevHeight),
					zap.Error(err))
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

	} else {
		// SLOW PATH: Has reorgs, persist one by one without validation
		logger.Info("Reorgs detected, persisting blocks one by one")

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

			// Log progress and send heartbeat periodically
			if blocksPersistedCount%100 == 0 {
				activity.RecordHeartbeat(ctx, fmt.Sprintf("Migrating blocks (slow path): %d/%d blocks persisted",
					blocksPersistedCount, len(allBlocksWithInfo)))
				logger.Debug("Progress update",
					zap.Int("persisted", blocksPersistedCount),
					zap.Int("total", len(allBlocksWithInfo)))
			}
		}
	}

	logger.Info("Blocks migrated successfully",
		zap.Int("blocksPersistedCount", blocksPersistedCount),
		zap.Bool("hadReorgs", hasReorgs))

	return blocksPersistedCount, nil
}

func (a *Migrator) detectReorgs(blocks []BlockToMigrate) bool {
	heightMap := make(map[uint64]int)
	for _, block := range blocks {
		heightMap[block.Height]++
		if heightMap[block.Height] > 1 {
			return true
		}
	}
	return false
}

type BlockWithInfo struct {
	*api.BlockMetadata
	Height   uint64
	Hash     string
	EventSeq int64
}

func (a *Migrator) fetchBlockData(ctx context.Context, logger *zap.Logger,
	data *MigrationData, request *MigratorRequest,
	blocksToMigrate []BlockToMigrate) ([]*BlockWithInfo, error) {

	var allBlocksWithInfo []*BlockWithInfo

	for _, block := range blocksToMigrate {
		// Always fetch by hash - this handles reorgs correctly
		blockMeta, err := data.SourceStorage.GetBlockByHash(ctx, request.Tag, block.Height, block.Hash)
		if err != nil {
			logger.Warn("Failed to fetch block by hash",
				zap.Uint64("height", block.Height),
				zap.String("hash", block.Hash),
				zap.Error(err))
			continue
		}

		allBlocksWithInfo = append(allBlocksWithInfo, &BlockWithInfo{
			BlockMetadata: blockMeta,
			Height:        block.Height,
			Hash:          block.Hash,
			EventSeq:      block.EventSeq,
		})
	}

	return allBlocksWithInfo, nil
}

func (a *Migrator) persistEvents(ctx context.Context, logger *zap.Logger,
	data *MigrationData, request *MigratorRequest,
	events []*model.EventEntry) (int, error) {

	if len(events) == 0 {
		return 0, nil
	}

	// Events are already sorted by EventId from fetchEventsBySequence
	firstId := events[0].EventId
	lastId := events[len(events)-1].EventId

	logger.Info("Persisting events batch",
		zap.Int("totalEvents", len(events)),
		zap.Int64("firstEventId", firstId),
		zap.Int64("lastEventId", lastId))

	// Check for gaps in event sequences (for debugging)
	var gaps []string
	for i := 1; i < len(events); i++ {
		if events[i].EventId != events[i-1].EventId+1 {
			gap := fmt.Sprintf("position=%d: %d->%d (gap=%d)",
				i, events[i-1].EventId, events[i].EventId,
				events[i].EventId-events[i-1].EventId-1)
			gaps = append(gaps, gap)
		}
	}
	if len(gaps) > 0 {
		logger.Warn("Found gaps in event sequences",
			zap.Strings("gaps", gaps))
	}

	// Persist all events in a single call
	persistStart := time.Now()
	if err := data.DestStorage.AddEventEntries(ctx, request.EventTag, events); err != nil {
		return 0, xerrors.Errorf("failed to persist events: %w", err)
	}

	persistDuration := time.Since(persistStart)
	logger.Info("Events persisted successfully",
		zap.Int("eventCount", len(events)),
		zap.Duration("persistDuration", persistDuration),
		zap.Float64("eventsPerSecond", float64(len(events))/persistDuration.Seconds()))

	return len(events), nil
}

type heightRange struct {
	start uint64
	end   uint64
}


// migrateBlocksBatch processes a batch of blocks with parallelism, similar to event migration approach
func (a *Migrator) migrateBlocksBatch(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest, startHeight, endHeight uint64, parallelism int) (int, error) {
	batchStart := time.Now()
	totalHeights := endHeight - startHeight + 1
	if parallelism <= 0 {
		parallelism = 1
	}

	// Create mini-batches sized to parallelism to minimize number of DB writes.
	// ceil(totalHeights / parallelism)
	miniBatchSize := (totalHeights + uint64(parallelism) - 1) / uint64(parallelism)

	type batchResult struct {
		startHeight       uint64
		endHeight         uint64
		blocks            []BlockWithCanonicalInfo
		nonCanonicalCount int
		err               error
		fetchDuration     time.Duration
	}

	logger.Info("Starting parallel block migration",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight),
		zap.Uint64("totalHeights", totalHeights),
		zap.Uint64("miniBatchSize", miniBatchSize),
		zap.Int("parallelism", parallelism))

	inputChannel := make(chan heightRange, parallelism*2)
	resultChannel := make(chan batchResult, parallelism*2)

	// Start parallel workers
	for i := 0; i < parallelism; i++ {
		go func(workerID int) {
			for heightRange := range inputChannel {
				workerStart := time.Now()
				allBlocks := []BlockWithCanonicalInfo{}
				totalNonCanonical := 0

				// Fetch all blocks in this height range (parallel DynamoDB queries)
				for height := heightRange.start; height <= heightRange.end; height++ {
					blockPid := fmt.Sprintf("%d-%d", request.Tag, height)

					// Get ALL blocks at this height (canonical + non-canonical) in one DynamoDB query
					blocksAtHeight, err := a.getAllBlocksAtHeight(ctx, data, blockPid)
					if err != nil {
						if !errors.Is(err, storage.ErrItemNotFound) {
							resultChannel <- batchResult{
								startHeight: heightRange.start,
								endHeight:   heightRange.end,
								err:         xerrors.Errorf("failed to get blocks at height %d: %w", height, err),
							}
							return
						}
						// No blocks at this height, continue
						continue
					}

					// Collect all blocks with canonical info for later sorting
					for _, blockWithInfo := range blocksAtHeight {
						allBlocks = append(allBlocks, blockWithInfo)
						if !blockWithInfo.IsCanonical {
							totalNonCanonical++
						}
					}
				}

				resultChannel <- batchResult{
					startHeight:       heightRange.start,
					endHeight:         heightRange.end,
					blocks:            allBlocks,
					nonCanonicalCount: totalNonCanonical,
					fetchDuration:     time.Since(workerStart),
				}
			}
		}(i)
	}

	// Generate work items
	for currentHeight := startHeight; currentHeight <= endHeight; {
		batchEnd := currentHeight + miniBatchSize - 1
		if batchEnd > endHeight {
			batchEnd = endHeight
		}
		inputChannel <- heightRange{start: currentHeight, end: batchEnd}
		currentHeight = batchEnd + 1
	}
	close(inputChannel)

	// Collect all results first (parallel fetch phase)
	totalBatches := int((totalHeights + miniBatchSize - 1) / miniBatchSize)
	totalNonCanonicalBlocks := 0
	processedBatches := 0
	allBlocksWithInfo := []BlockWithCanonicalInfo{}

	logger.Info("Collecting parallel fetch results", zap.Int("totalBatches", totalBatches))

	for i := 0; i < totalBatches; i++ {
		result := <-resultChannel
		processedBatches++

		if result.err != nil {
			logger.Error("Block migration batch failed",
				zap.Uint64("startHeight", result.startHeight),
				zap.Uint64("endHeight", result.endHeight),
				zap.Error(result.err))
			return 0, result.err
		}

		// Collect all blocks for sorting
		allBlocksWithInfo = append(allBlocksWithInfo, result.blocks...)
		totalNonCanonicalBlocks += result.nonCanonicalCount

		// Progress logging
		if processedBatches%5 == 0 || processedBatches == totalBatches {
			percentage := float64(processedBatches) / float64(totalBatches) * 100
			logger.Info("Fetch progress",
				zap.Int("processedBatches", processedBatches),
				zap.Int("totalBatches", totalBatches),
				zap.Float64("percentage", percentage),
				zap.Int("blocksCollected", len(allBlocksWithInfo)),
				zap.Duration("batchDuration", result.fetchDuration))
		}

		// Heartbeat for Temporal UI
		if processedBatches%10 == 0 {
			percentage := float64(processedBatches) / float64(totalBatches) * 100
			activity.RecordHeartbeat(ctx, fmt.Sprintf(
				"fetched blocks: batch=%d/%d (%.1f%%), collected=%d blocks",
				processedBatches, totalBatches, percentage, len(allBlocksWithInfo),
			))
		}
	}

	// Handle empty result case
	if len(allBlocksWithInfo) == 0 {
		logger.Info("No blocks found in range, migration complete",
			zap.Uint64("startHeight", startHeight),
			zap.Uint64("endHeight", endHeight))
		return 0, nil
	}

	logger.Info("Parallel fetch completed, starting sort and persist phase",
		zap.Int("totalBlocks", len(allBlocksWithInfo)),
		zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks))

	// Sort all blocks: first by height, then non-canonical BEFORE canonical (for "last block wins")
	if len(allBlocksWithInfo) > 0 {
		sortStart := time.Now()
		sort.Slice(allBlocksWithInfo, func(i, j int) bool {
			if allBlocksWithInfo[i].Height != allBlocksWithInfo[j].Height {
				return allBlocksWithInfo[i].Height < allBlocksWithInfo[j].Height
			}
			// CRITICAL: For same height, non-canonical blocks should come blocks must be processed FIRST
			// so canonical ones blocks win with "last block wins" behavior
			if allBlocksWithInfo[i].IsCanonical != allBlocksWithInfo[j].IsCanonical {
				return !allBlocksWithInfo[i].IsCanonical
			}
			return false // Preserve order for blocks with same height and canonical status
		})
		sortDuration := time.Since(sortStart)

		// Check if there are any reorgs in this batch (multiple blocks at same height)
		hasReorgs := false
		heightCounts := make(map[uint64]int)
		for _, block := range allBlocksWithInfo {
			heightCounts[block.Height]++
			if heightCounts[block.Height] > 1 {
				hasReorgs = true
			}
		}

		// Log reorg details if found
		if hasReorgs {
			reorgCount := 0
			for height, count := range heightCounts {
				if count > 1 {
					reorgCount++
					logger.Info("Reorg detected at height",
						zap.Uint64("height", height),
						zap.Int("blockCount", count))
				}
			}
			logger.Info("Total reorg heights detected", zap.Int("reorgCount", reorgCount))
		}

		logger.Info("Blocks sorted",
			zap.Int("totalBlocks", len(allBlocksWithInfo)),
			zap.Bool("hasReorgs", hasReorgs),
			zap.Duration("sortDuration", sortDuration))

		// Get the last block before our range for chain validation
		var lastBlock *api.BlockMetadata
		if startHeight > 0 {
			// Try to get the previous block for chain validation
			prevHeight := startHeight - 1
			prevBlock, err := data.DestStorage.GetBlockByHeight(ctx, request.Tag, prevHeight)
			if err == nil {
				lastBlock = prevBlock
				logger.Debug("Found previous block for chain validation",
					zap.Uint64("prevHeight", prevHeight),
					zap.String("prevHash", prevBlock.Hash))
			} else {
				logger.Debug("No previous block found, proceeding without chain validation",
					zap.Uint64("startHeight", startHeight))
			}
		}

		blocksPersistedCount := 0
		persistStart := time.Now()

		if !hasReorgs {
			// FAST PATH: No reorgs, can bulk persist with chain validation
			logger.Info("No reorgs detected, using fast bulk persist")
			allBlocks := make([]*api.BlockMetadata, len(allBlocksWithInfo))
			for i, blockWithInfo := range allBlocksWithInfo {
				allBlocks[i] = blockWithInfo.BlockMetadata
			}

			err := data.DestStorage.PersistBlockMetas(ctx, false, allBlocks, lastBlock)
			if err != nil {
				logger.Error("Failed to bulk persist blocks",
					zap.Error(err),
					zap.Int("blockCount", len(allBlocks)))
				return 0, xerrors.Errorf("failed to bulk persist blocks: %w", err)
			}
			blocksPersistedCount = len(allBlocks)

		} else {
			// SLOW PATH: Has reorgs, persist one by one without chain validation
			logger.Info("Reorgs detected, persisting blocks one by one")

			for _, blockWithInfo := range allBlocksWithInfo {
				err := data.DestStorage.PersistBlockMetas(ctx, false,
					[]*api.BlockMetadata{blockWithInfo.BlockMetadata}, nil)
				if err != nil {
					logger.Error("Failed to persist block",
						zap.Uint64("height", blockWithInfo.Height),
						zap.String("hash", blockWithInfo.Hash),
						zap.Bool("canonical", blockWithInfo.IsCanonical),
						zap.Error(err))
					return blocksPersistedCount, xerrors.Errorf("failed to persist block: %w", err)
				}
				blocksPersistedCount++

				// Log progress periodically
				if blocksPersistedCount%100 == 0 {
					logger.Debug("Progress update",
						zap.Int("persisted", blocksPersistedCount),
						zap.Int("total", len(allBlocksWithInfo)))
				}
			}
		}

		persistDuration := time.Since(persistStart)
		logger.Info("Bulk persistence completed",
			zap.Int("totalBlocksPersisted", blocksPersistedCount),
			zap.Duration("persistDuration", persistDuration))
	}

	totalDuration := time.Since(batchStart)
	logger.Info("Parallel block metadata migration completed",
		zap.Int("totalBlocksMigrated", len(allBlocksWithInfo)),
		zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks),
		zap.Duration("totalDuration", totalDuration),
		zap.Float64("avgSecondsPerHeight", totalDuration.Seconds()/float64(totalHeights)))

	return len(allBlocksWithInfo), nil
}

// BlockWithCanonicalInfo wraps BlockMetadata with canonical information
type BlockWithCanonicalInfo struct {
	*api.BlockMetadata
	IsCanonical bool
}

func (a *Migrator) getAllBlocksAtHeight(ctx context.Context, data *MigrationData, blockPid string) ([]BlockWithCanonicalInfo, error) {
	logger := a.getLogger(ctx)

	input := &dynamodb.QueryInput{
		TableName:              awssdk.String(data.BlockTable),
		KeyConditionExpression: awssdk.String("block_pid = :blockPid"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":blockPid": {
				S: awssdk.String(blockPid),
			},
		},
		ConsistentRead: awssdk.Bool(true),
	}

	queryStart := time.Now()
	result, err := data.DynamoClient.QueryWithContext(ctx, input)
	queryDuration := time.Since(queryStart)

	logger.Debug("DynamoDB query for all blocks at height",
		zap.String("blockPid", blockPid),
		zap.Duration("queryDuration", queryDuration),
		zap.Bool("success", err == nil))

	if err != nil {
		logger.Error("DynamoDB query failed for all blocks at height",
			zap.String("blockPid", blockPid),
			zap.Duration("queryDuration", queryDuration),
			zap.Error(err))
		return nil, xerrors.Errorf("failed to query all blocks at height: %w", err)
	}

	if len(result.Items) == 0 {
		return nil, storage.ErrItemNotFound
	}

	// Use a map to deduplicate blocks with the same hash
	// Keep the canonical entry if there are duplicates
	blockMap := make(map[string]BlockWithCanonicalInfo)

	for _, item := range result.Items {
		var blockEntry dynamodb_model.BlockMetaDataDDBEntry
		err := dynamodbattribute.UnmarshalMap(item, &blockEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal DynamoDB item: %w", err)
		}

		// Determine if this block is canonical based on block_rid
		isCanonical := blockEntry.BlockRid == "canonical"

		blockWithInfo := BlockWithCanonicalInfo{
			BlockMetadata: dynamodb_model.BlockMetadataToProto(&blockEntry),
			IsCanonical:   isCanonical,
		}

		// Deduplicate: if we already have this block hash, keep the canonical one
		existing, exists := blockMap[blockEntry.Hash]
		if !exists {
			blockMap[blockEntry.Hash] = blockWithInfo
		} else if isCanonical && !existing.IsCanonical {
			// Replace with canonical version if current is canonical and existing is not
			blockMap[blockEntry.Hash] = blockWithInfo
		}
	}

	// Convert map to slice
	allBlocks := make([]BlockWithCanonicalInfo, 0, len(blockMap))
	for _, block := range blockMap {
		allBlocks = append(allBlocks, block)
	}

	// Log if we found duplicates
	if len(result.Items) != len(allBlocks) {
		logger.Debug("Deduplicated blocks from DynamoDB",
			zap.String("blockPid", blockPid),
			zap.Int("originalCount", len(result.Items)),
			zap.Int("dedupedCount", len(allBlocks)))
	}

	return allBlocks, nil
}


// migrateEventsBatch processes a batch of events with parallelism
func (a *Migrator) migrateEventsBatch(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest, startHeight, endHeight uint64, parallelism int) (int, error) {
	if parallelism <= 0 {
		parallelism = 1
	}
	totalHeights := endHeight - startHeight + 1
	miniBatchSize := (totalHeights + uint64(parallelism) - 1) / uint64(parallelism)

	type heightRange struct {
		start, end uint64
	}

	type batchResult struct {
		events    []*model.EventEntry
		err       error
		rangeInfo heightRange
	}

	// Generate batches
	actualBatches := 0
	var heightRanges []heightRange
	for batchStart := startHeight; batchStart <= endHeight; batchStart += miniBatchSize {
		batchEnd := batchStart + miniBatchSize - 1
		if batchEnd > endHeight {
			batchEnd = endHeight
		}
		heightRanges = append(heightRanges, heightRange{start: batchStart, end: batchEnd})
		actualBatches++
	}

	logger.Info("Starting parallel event fetch",
		zap.Int("batches", actualBatches),
		zap.Int("workers", parallelism),
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight))

	inputChannel := make(chan heightRange, actualBatches)
	resultChannel := make(chan batchResult, actualBatches)

	// Add all batches to input channel
	for _, hr := range heightRanges {
		inputChannel <- hr
	}
	close(inputChannel)

	// Start parallel workers with WaitGroup to track completion
	var wg sync.WaitGroup
	wg.Add(parallelism)

	for i := 0; i < parallelism; i++ {
		go func(workerID int) {
			defer wg.Done()
			for heightRange := range inputChannel {
				evts, err := a.fetchEventsRange(ctx, data, request, heightRange.start, heightRange.end)
				resultChannel <- batchResult{
					events:    evts,
					err:       err,
					rangeInfo: heightRange,
				}
			}
		}(i)
	}

	// Wait for all workers to complete in a separate goroutine
	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	// Collect all results - MUST collect exactly actualBatches results
	var allEvents []*model.EventEntry
	collectedBatches := 0
	for i := 0; i < actualBatches; i++ {
		result := <-resultChannel
		collectedBatches++

		if result.err != nil {
			return 0, result.err
		}
		if len(result.events) > 0 {
			allEvents = append(allEvents, result.events...)
		}

		// Log and heartbeat fetch progress
		activity.RecordHeartbeat(ctx, fmt.Sprintf(
			"fetched batch %d/%d: heights [%d-%d], events=%d",
			collectedBatches, actualBatches, result.rangeInfo.start, result.rangeInfo.end, len(result.events),
		))
	}

	logger.Info("All events fetched, starting sort and persist",
		zap.Int("totalEvents", len(allEvents)))

	// Sort all collected events by EventId (ascending)
	if len(allEvents) > 0 {
		sort.Slice(allEvents, func(i, j int) bool { return allEvents[i].EventId < allEvents[j].EventId })

		firstId := allEvents[0].EventId
		lastId := allEvents[len(allEvents)-1].EventId

		logger.Info("Attempting to persist all events in single batch",
			zap.Int("totalEvents", len(allEvents)),
			zap.Int64("firstEventId", firstId),
			zap.Int64("lastEventId", lastId))

		// Log first 10 and last 10 event IDs for debugging
		var eventIdSample []int64
		if len(allEvents) <= 20 {
			for _, evt := range allEvents {
				eventIdSample = append(eventIdSample, evt.EventId)
			}
		} else {
			// First 10
			for j := 0; j < 10; j++ {
				eventIdSample = append(eventIdSample, allEvents[j].EventId)
			}
			// Last 10
			for j := len(allEvents) - 10; j < len(allEvents); j++ {
				eventIdSample = append(eventIdSample, allEvents[j].EventId)
			}
		}
		logger.Debug("Event ID sample (first 10 and last 10)",
			zap.Int64s("eventIds", eventIdSample))

		// Persist all events in a single call
		persistStart := time.Now()
		if err := data.DestStorage.AddEventEntries(ctx, request.EventTag, allEvents); err != nil {
			// On failure, check for gaps in our collected events
			var gaps []string
			for j := 1; j < len(allEvents); j++ {
				if allEvents[j].EventId != allEvents[j-1].EventId+1 {
					gap := fmt.Sprintf("position=%d: %d->%d (gap=%d)",
						j, allEvents[j-1].EventId, allEvents[j].EventId,
						allEvents[j].EventId-allEvents[j-1].EventId-1)
					gaps = append(gaps, gap)
				}
			}

			if len(gaps) > 0 {
				logger.Error("Gaps found in collected events",
					zap.Strings("gaps", gaps),
					zap.Int("totalGaps", len(gaps)))
			}

			// Log sample of event IDs around the error point mentioned in the error message
			// The error says: "prev event id: 19793231, current event id: 19793530"
			var contextEventIds []int64
			for idx, evt := range allEvents {
				if evt.EventId >= 19793220 && evt.EventId <= 19793540 {
					contextEventIds = append(contextEventIds, evt.EventId)
					if evt.EventId == 19793231 {
						logger.Error("Found event 19793231 at position",
							zap.Int("position", idx),
							zap.Int64("eventId", evt.EventId))
						if idx+1 < len(allEvents) {
							logger.Error("Next event after 19793231",
								zap.Int("position", idx+1),
								zap.Int64("eventId", allEvents[idx+1].EventId))
						}
					}
				}
			}
			if len(contextEventIds) > 0 {
				logger.Error("Events around error range",
					zap.Int64s("contextEventIds", contextEventIds))
			}

			return 0, xerrors.Errorf("failed to bulk add %d events for range [%d-%d]: %w", len(allEvents), startHeight, endHeight, err)
		}

		duration := time.Since(persistStart)
		logger.Info("Successfully persisted all events to Postgres",
			zap.Int("totalEvents", len(allEvents)),
			zap.Int64("firstEventId", firstId),
			zap.Int64("lastEventId", lastId),
			zap.Duration("duration", duration))

		activity.RecordHeartbeat(ctx, fmt.Sprintf(
			"persisted all events: count=%d, event_id=[%d-%d], took=%s",
			len(allEvents), firstId, lastId, duration,
		))
	}
	return len(allEvents), nil
}

// migrateEventsRange processes events for a specific height range efficiently
func (a *Migrator) fetchEventsRange(ctx context.Context, data *MigrationData, request *MigratorRequest, startHeight, endHeight uint64) ([]*model.EventEntry, error) {
	allEvents := make([]*model.EventEntry, 0, (endHeight-startHeight+1)*2) // Estimate 2 events per block

	// Collect all events in this range
	for h := startHeight; h <= endHeight; h++ {
		sourceEvents, err := data.SourceStorage.GetEventsByBlockHeight(ctx, request.EventTag, h)
		if err != nil {
			if errors.Is(err, storage.ErrItemNotFound) {
				continue // No events at this height; skip
			}
			return nil, xerrors.Errorf("failed to get events at height %d: %w", h, err)
		}
		if len(sourceEvents) > 0 {
			allEvents = append(allEvents, sourceEvents...)
		}
	}

	// Sort events by event_sequence to ensure proper ordering and prevent gaps
	if len(allEvents) > 0 {
		sort.Slice(allEvents, func(i, j int) bool {
			return allEvents[i].EventId < allEvents[j].EventId
		})
	}
	return allEvents, nil
}

type GetLatestBlockHeightRequest struct {
	Tag uint32
}

type GetLatestBlockHeightResponse struct {
	Height uint64
}

type GetLatestBlockFromPostgresRequest struct {
	Tag uint32
}

type GetLatestBlockFromPostgresResponse struct {
	Height uint64
	Found  bool // true if a block was found, false if no blocks exist yet
}

type GetLatestEventFromPostgresRequest struct {
	EventTag uint32
}

type GetLatestEventFromPostgresResponse struct {
	Sequence int64  // Event sequence number
	Height   uint64 // Block height (for backward compatibility)
	Found    bool   // true if events were found, false if no events exist yet
}

type GetMaxEventIdRequest struct {
	EventTag uint32
}

type GetMaxEventIdResponse struct {
	MaxEventId int64 // Maximum event ID in DynamoDB
	Found      bool  // true if events were found
}

func (a *Migrator) GetLatestBlockHeight(ctx context.Context, req *GetLatestBlockHeightRequest) (*GetLatestBlockHeightResponse, error) {
	migrationData, err := a.createStorageInstances(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to create storage instances: %w", err)
	}
	latestBlock, err := migrationData.SourceStorage.GetLatestBlock(ctx, req.Tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block from DynamoDB: %w", err)
	}
	return &GetLatestBlockHeightResponse{Height: latestBlock.Height}, nil
}

func (a *GetLatestBlockHeightActivity) Execute(ctx workflow.Context, request *GetLatestBlockHeightRequest) (*GetLatestBlockHeightResponse, error) {
	var response GetLatestBlockHeightResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *GetLatestBlockHeightActivity) execute(ctx context.Context, request *GetLatestBlockHeightRequest) (*GetLatestBlockHeightResponse, error) {
	migrationData, err := a.createStorageInstances(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to create storage instances: %w", err)
	}
	latestBlock, err := migrationData.SourceStorage.GetLatestBlock(ctx, request.Tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block from DynamoDB: %w", err)
	}
	return &GetLatestBlockHeightResponse{Height: latestBlock.Height}, nil
}

func (a *GetLatestBlockFromPostgresActivity) Execute(ctx workflow.Context, request *GetLatestBlockFromPostgresRequest) (*GetLatestBlockFromPostgresResponse, error) {
	var response GetLatestBlockFromPostgresResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *GetLatestBlockFromPostgresActivity) execute(ctx context.Context, request *GetLatestBlockFromPostgresRequest) (*GetLatestBlockFromPostgresResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))

	// Create PostgreSQL storage using shared connection pool to query destination
	postgresParams := postgres_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
	}
	destResult, err := postgres_storage.NewMetaStorage(postgresParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
	}

	latestBlock, err := destResult.MetaStorage.GetLatestBlock(ctx, request.Tag)
	if err != nil {
		// Check if it's a "not found" error, which means no blocks migrated yet
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "not found") || strings.Contains(errStr, "no rows") {
			logger.Info("No blocks found in PostgreSQL destination - starting from beginning")
			return &GetLatestBlockFromPostgresResponse{
				Height: 0,
				Found:  false,
			}, nil
		}
		return nil, xerrors.Errorf("failed to get latest block from PostgreSQL: %w", err)
	}

	logger.Info("Found latest block in PostgreSQL destination", zap.Uint64("height", latestBlock.Height))
	return &GetLatestBlockFromPostgresResponse{
		Height: latestBlock.Height,
		Found:  true,
	}, nil
}

func (a *GetLatestEventFromPostgresActivity) Execute(ctx workflow.Context, request *GetLatestEventFromPostgresRequest) (*GetLatestEventFromPostgresResponse, error) {
	var response GetLatestEventFromPostgresResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *GetLatestEventFromPostgresActivity) execute(ctx context.Context, request *GetLatestEventFromPostgresRequest) (*GetLatestEventFromPostgresResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))

	// Create PostgreSQL storage using shared connection pool to query destination
	postgresParams := postgres_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
	}

	destResult, err := postgres_storage.NewMetaStorage(postgresParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
	}

	// Get the latest event sequence from PostgreSQL
	maxEventId, err := destResult.MetaStorage.GetMaxEventId(ctx, request.EventTag)
	if err != nil {
		// Check if it's a "no event history" error, which means no events migrated yet
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "no event history") || strings.Contains(errStr, "not found") || strings.Contains(errStr, "no rows") {
			logger.Info("No events found in PostgreSQL destination - starting from beginning")
			return &GetLatestEventFromPostgresResponse{
				Sequence: 0,
				Height:   0,
				Found:    false,
			}, nil
		}
		return nil, xerrors.Errorf("failed to get latest event sequence from PostgreSQL: %w", err)
	}

	// Get the event entry to also return the height for backward compatibility
	eventEntry, err := destResult.MetaStorage.GetEventByEventId(ctx, request.EventTag, maxEventId)
	if err != nil {
		// If we can't get the event entry, just return the sequence
		logger.Warn("Failed to get event entry for max sequence, returning sequence only",
			zap.Int64("maxEventId", maxEventId), zap.Error(err))
		return &GetLatestEventFromPostgresResponse{
			Sequence: maxEventId,
			Height:   0,
			Found:    true,
		}, nil
	}

	logger.Info("Found latest event in PostgreSQL destination",
		zap.Int64("sequence", maxEventId),
		zap.Uint64("height", eventEntry.BlockHeight))
	return &GetLatestEventFromPostgresResponse{
		Sequence: maxEventId,
		Height:   eventEntry.BlockHeight,
		Found:    true,
	}, nil
}

func (a *GetLatestEventFromPostgresActivity) getLatestEventHeight(ctx context.Context, storage metastorage.MetaStorage, eventTag uint32) (uint64, error) {
	// Get the max event sequence (equivalent to max event ID)
	maxEventId, err := storage.GetMaxEventId(ctx, eventTag)
	if err != nil {
		return 0, err
	}

	// Get the event entry for that max event sequence to get its height
	eventEntry, err := storage.GetEventByEventId(ctx, eventTag, maxEventId)
	if err != nil {
		return 0, xerrors.Errorf("failed to get event entry for max event sequence %d: %w", maxEventId, err)
	}

	return eventEntry.BlockHeight, nil
}

func (a *GetMaxEventIdActivity) Execute(ctx workflow.Context, request *GetMaxEventIdRequest) (*GetMaxEventIdResponse, error) {
	var response GetMaxEventIdResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *GetMaxEventIdActivity) execute(ctx context.Context, request *GetMaxEventIdRequest) (*GetMaxEventIdResponse, error) {
	logger := a.getLogger(ctx).With(
		zap.Uint32("eventTag", request.EventTag),
	)
	logger.Info("getting max event ID from DynamoDB")

	// Create DynamoDB storage directly
	dynamoDBParams := dynamodb_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
		Session: a.session,
	}
	sourceResult, err := dynamodb_storage.NewMetaStorage(dynamoDBParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create DynamoDB storage: %w", err)
	}

	// Get max event ID from DynamoDB
	maxEventId, err := sourceResult.MetaStorage.GetMaxEventId(ctx, request.EventTag)
	if err != nil {
		if errors.Is(err, storage.ErrItemNotFound) {
			logger.Warn("no events found in DynamoDB")
			return &GetMaxEventIdResponse{
				MaxEventId: 0,
				Found:      false,
			}, nil
		}
		return nil, xerrors.Errorf("failed to get max event ID: %w", err)
	}

	logger.Info("found max event ID in DynamoDB",
		zap.Int64("maxEventId", maxEventId))

	return &GetMaxEventIdResponse{
		MaxEventId: maxEventId,
		Found:      true,
	}, nil
}

func (a *GetLatestBlockHeightActivity) createStorageInstances(ctx context.Context) (*MigrationData, error) {
	logger := a.getLogger(ctx)

	// Create DynamoDB storage directly
	dynamoDBParams := dynamodb_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
		Session: a.session,
	}
	sourceResult, err := dynamodb_storage.NewMetaStorage(dynamoDBParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create DynamoDB storage: %w", err)
	}

	// Create PostgreSQL storage using shared connection pool
	postgresParams := postgres_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
	}
	destResult, err := postgres_storage.NewMetaStorage(postgresParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
	}

	return &MigrationData{
		SourceStorage: sourceResult.MetaStorage,
		DestStorage:   destResult.MetaStorage,
		Config:        a.config,
		DynamoClient:  a.dynamoClient,
		BlockTable:    a.blockTable,
	}, nil
}

const (
	ActivityMigrator                   = "activity.migrator"
	ActivityGetLatestBlockHeight       = "activity.migrator.GetLatestBlockHeight"
	ActivityGetLatestBlockFromPostgres = "activity.migrator.GetLatestBlockFromPostgres"
	ActivityGetLatestEventFromPostgres = "activity.migrator.GetLatestEventFromPostgres"
	ActivityGetMaxEventId              = "activity.migrator.GetMaxEventId"
)
