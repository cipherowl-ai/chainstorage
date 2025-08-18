package activity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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

	MigratorParams struct {
		fx.In
		fxparams.Params
		Runtime cadence.Runtime
		Session *session.Session
	}

	MigratorRequest struct {
		StartHeight uint64
		EndHeight   uint64 // Optional. If not specified, will query latest block from DynamoDB
		EventTag    uint32
		Tag         uint32
		Parallelism int
		SkipEvents  bool
		SkipBlocks  bool
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

func (a *Migrator) Execute(ctx workflow.Context, request *MigratorRequest) (*MigratorResponse, error) {
	var response MigratorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Migrator) execute(ctx context.Context, request *MigratorRequest) (*MigratorResponse, error) {
	startTime := time.Now()
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	// Validate height range early to fail fast and avoid expensive setup.
	if request.EndHeight <= request.StartHeight {
		return nil, xerrors.Errorf("invalid request: EndHeight (%d) must be greater than StartHeight (%d)", request.EndHeight, request.StartHeight)
	}

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))
	logger.Info("Migrator activity started",
		zap.Uint64("startHeight", request.StartHeight),
		zap.Uint64("endHeight", request.EndHeight),
		zap.Uint64("totalBlocks", request.EndHeight-request.StartHeight),
		zap.Bool("skipBlocks", request.SkipBlocks),
		zap.Bool("skipEvents", request.SkipEvents))

	// Add heartbeat mechanism
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer heartbeatTicker.Stop()

	go func() {
		for range heartbeatTicker.C {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("Processing batch [%d, %d), elapsed: %v",
				request.StartHeight, request.EndHeight, time.Since(startTime)))
		}
	}()

	// No per-activity batch size; batching governed by workflow and parallelism

	// Both skip flags cannot be true
	if request.SkipEvents && request.SkipBlocks {
		return &MigratorResponse{
			Success: false,
			Message: "cannot skip both events and blocks - nothing to migrate",
		}, nil
	}

	// Create storage instances
	migrationData, err := a.createStorageInstances(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to create storage instances: %w", err)
	}

	var blocksMigrated, eventsMigrated int

	// Phase 1: Migrate block metadata FIRST (required for foreign key references)
	if !request.SkipBlocks {
		count, err := a.migrateBlocks(ctx, logger, migrationData, request)
		if err != nil {
			return nil, xerrors.Errorf("failed to migrate blocks: %w", err)
		}
		blocksMigrated = count
	}

	// Phase 2: Migrate events AFTER blocks (depends on block metadata foreign keys)
	if !request.SkipEvents {
		count, err := a.migrateEvents(ctx, logger, migrationData, request)
		if err != nil {
			return nil, xerrors.Errorf("failed to migrate events: %w", err)
		}
		eventsMigrated = count
	}

	totalDuration := time.Since(startTime)
	logger.Info("Migration completed successfully",
		zap.Int("blocksMigrated", blocksMigrated),
		zap.Int("eventsMigrated", eventsMigrated),
		zap.Duration("totalDuration", totalDuration),
		zap.Float64("blocksPerSecond", float64(blocksMigrated)/totalDuration.Seconds()))

	return &MigratorResponse{
		BlocksMigrated: blocksMigrated,
		EventsMigrated: eventsMigrated,
		Success:        true,
		Message:        "Migration completed successfully",
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

type heightRange struct {
	start uint64
	end   uint64
}

func (a *Migrator) migrateBlocks(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest) (int, error) {
	// Determine parallelism
	parallelism := request.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}

	logger.Info("Starting parallel block metadata migration with complete reorg support",
		zap.Uint64("startHeight", request.StartHeight),
		zap.Uint64("endHeight", request.EndHeight),
		zap.Uint64("totalHeights", request.EndHeight-request.StartHeight),
		zap.Int("parallelism", parallelism))

	// Use parallel batch processing similar to event migration
	return a.migrateBlocksBatch(ctx, logger, data, request, request.StartHeight, request.EndHeight-1, parallelism)
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

	// Check if there are any reorgs in this batch (multiple blocks at same height)
	hasReorgs := false
	heightCounts := make(map[uint64]int)
	for _, block := range allBlocksWithInfo {
		heightCounts[block.Height]++
		if heightCounts[block.Height] > 1 {
			hasReorgs = true
		}
	}

	if len(allBlocksWithInfo) > 0 {
		sortStart := time.Now()

		// Always sort by height first, then non-canonical before canonical
		sort.Slice(allBlocksWithInfo, func(i, j int) bool {
			if allBlocksWithInfo[i].Height != allBlocksWithInfo[j].Height {
				return allBlocksWithInfo[i].Height < allBlocksWithInfo[j].Height
			}
			// For same height, non-canonical before canonical
			return !allBlocksWithInfo[i].IsCanonical && allBlocksWithInfo[j].IsCanonical
		})

		sortDuration := time.Since(sortStart)
		logger.Info("Blocks sorted",
			zap.Int("totalBlocks", len(allBlocksWithInfo)),
			zap.Bool("hasReorgs", hasReorgs),
			zap.Duration("sortDuration", sortDuration))

		// Get the last block before our range for chain validation
		var lastBlock *api.BlockMetadata
		if startHeight > 0 {
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

		persistStart := time.Now()
		blocksPersistedCount := 0

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

			for i, blockWithInfo := range allBlocksWithInfo {
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

				// Heartbeat more frequently during slow path
				if i%50 == 0 {
					activity.RecordHeartbeat(ctx, fmt.Sprintf(
						"Persisting blocks with reorgs: %d/%d completed",
						blocksPersistedCount, len(allBlocksWithInfo)))
				}

				// Log progress periodically
				if blocksPersistedCount%100 == 0 {
					logger.Debug("Progress update",
						zap.Int("persisted", blocksPersistedCount),
						zap.Int("total", len(allBlocksWithInfo)))
				}
			}
		}

		persistDuration := time.Since(persistStart)

		logger.Info("Block persistence completed",
			zap.Int("totalBlocks", blocksPersistedCount),
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

func (a *Migrator) migrateEvents(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest) (int, error) {
	logger.Info("Starting batched event migration with parallelism",
		zap.Int("parallelism", request.Parallelism))

	// If we're skipping blocks, validate that required block metadata exists in PostgreSQL
	if request.SkipBlocks {
		logger.Info("Skip-blocks enabled, validating that block metadata exists in PostgreSQL")
		if err := a.validateBlockMetadataExists(ctx, data, request); err != nil {
			return 0, xerrors.Errorf("block metadata validation failed: %w", err)
		}
		logger.Info("Block metadata validation passed")
	}

	// Migrate events for the entire requested height range in a single write to Postgres.
	startHeight := request.StartHeight
	if request.EndHeight == 0 || startHeight >= request.EndHeight {
		return 0, nil
	}
	endHeightInclusive := request.EndHeight - 1

	// Determine parallelism
	parallelism := request.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}

	batchStartTime := time.Now()
	logger.Info("Processing event batch (single write)",
		zap.Uint64("batchStart", startHeight),
		zap.Uint64("batchEnd", endHeightInclusive),
		zap.Int("parallelism", parallelism))

	totalEvents, err := a.migrateEventsBatch(ctx, logger, data, request, startHeight, endHeightInclusive, parallelism)
	if err != nil {
		return 0, xerrors.Errorf("failed to migrate events for range [%d-%d]: %w", startHeight, endHeightInclusive, err)
	}

	logger.Info("Event migration completed (single write)",
		zap.Uint64("rangeStart", startHeight),
		zap.Uint64("rangeEnd", endHeightInclusive),
		zap.Int("eventsMigrated", totalEvents),
		zap.Duration("duration", time.Since(batchStartTime)))
	return totalEvents, nil
}

// migrateEventsBatch processes a batch of events with parallelism, similar to block migration approach
func (a *Migrator) migrateEventsBatch(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest, startHeight, endHeight uint64, parallelism int) (int, error) {
	// Create mini-batches sized to parallelism to minimize number of DB writes.
	// Each worker handles one contiguous height range when possible.
	if parallelism <= 0 {
		parallelism = 1
	}
	totalHeights := endHeight - startHeight + 1
	// ceil(totalHeights / parallelism)
	miniBatchSize := (totalHeights + uint64(parallelism) - 1) / uint64(parallelism)

	// Create channels for parallel processing
	type heightRange struct {
		start, end uint64
	}

	type batchResult struct {
		events   []*model.EventEntry
		err      error
		range_   heightRange
		duration time.Duration
	}

	inputChannel := make(chan heightRange, int(totalHeights/miniBatchSize)+1)
	resultChannel := make(chan batchResult, parallelism*2)

	// Generate mini-batches
	for batchStart := startHeight; batchStart <= endHeight; batchStart += miniBatchSize {
		batchEnd := batchStart + miniBatchSize - 1
		if batchEnd > endHeight {
			batchEnd = endHeight
		}
		inputChannel <- heightRange{start: batchStart, end: batchEnd}
	}
	close(inputChannel)

	// Start parallel workers
	for i := 0; i < parallelism; i++ {
		go func(workerID int) {
			for heightRange := range inputChannel {
				fetchStart := time.Now()
				evts, err := a.fetchEventsRange(ctx, data, request, heightRange.start, heightRange.end)
				resultChannel <- batchResult{
					events:   evts,
					err:      err,
					range_:   heightRange,
					duration: time.Since(fetchStart),
				}
			}
		}(i)
	}

	// Collect results
	totalEvents := 0
	var allEvents []*model.EventEntry
	expectedBatches := int(totalHeights / miniBatchSize)
	if totalHeights%miniBatchSize != 0 {
		expectedBatches++
	}

	for i := 0; i < expectedBatches; i++ {
		result := <-resultChannel
		if result.err != nil {
			return totalEvents, xerrors.Errorf("failed to migrate events range [%d-%d]: %w",
				result.range_.start, result.range_.end, result.err)
		}
		if len(result.events) > 0 {
			allEvents = append(allEvents, result.events...)
			totalEvents += len(result.events)
		}

		logger.Info("Fetched events from Dynamo",
			zap.Uint64("rangeStart", result.range_.start),
			zap.Uint64("rangeEnd", result.range_.end),
			zap.Int("events", len(result.events)),
			zap.Duration("duration", result.duration))

		// Heartbeat progress so it shows up in Temporal UI
		activity.RecordHeartbeat(ctx, fmt.Sprintf(
			"fetched events: heights [%d-%d], events=%d, took=%s",
			result.range_.start, result.range_.end, len(result.events), result.duration,
		))

		if i%10 == 0 || len(result.events) > 0 {
			logger.Debug("Mini-batch completed",
				zap.Uint64("rangeStart", result.range_.start),
				zap.Uint64("rangeEnd", result.range_.end),
				zap.Int("events", len(result.events)),
				zap.Int("totalSoFar", totalEvents))
		}
	}

	// Sort all collected events once and write in contiguous chunks to avoid long-running transactions
	if len(allEvents) > 0 {
		sort.Slice(allEvents, func(i, j int) bool { return allEvents[i].EventId < allEvents[j].EventId })
		// larger chunk size now that writes are bulked via COPY+JOIN
		const eventChunkSize = 1000
		for i := 0; i < len(allEvents); i += eventChunkSize {
			end := i + eventChunkSize
			if end > len(allEvents) {
				end = len(allEvents)
			}
			chunk := allEvents[i:end]
			firstId := chunk[0].EventId
			lastId := chunk[len(chunk)-1].EventId
			persistStart := time.Now()
			if err := data.DestStorage.AddEventEntries(ctx, request.EventTag, chunk); err != nil {
				return 0, xerrors.Errorf("failed to bulk add %d events for range [%d-%d]: %w", len(chunk), startHeight, endHeight, err)
			}
			logger.Info("Persisted events chunk to Postgres",
				zap.Int("chunkSize", len(chunk)),
				zap.Int64("firstEventId", firstId),
				zap.Int64("lastEventId", lastId),
				zap.Duration("duration", time.Since(persistStart)))

			activity.RecordHeartbeat(ctx, fmt.Sprintf(
				"persisted chunk: events=%d, event_id=[%d-%d], took=%s",
				len(chunk), firstId, lastId, time.Since(persistStart),
			))
		}
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

// validateBlockMetadataExists checks if block metadata exists in PostgreSQL for the height range
// This is critical when skip-blocks is enabled, as events depend on block metadata via foreign keys
func (a *Migrator) validateBlockMetadataExists(ctx context.Context, data *MigrationData, request *MigratorRequest) error {
	logger := a.getLogger(ctx)

	// Sample a few heights to check if block metadata exists
	sampleHeights := []uint64{
		request.StartHeight,
		request.StartHeight + (request.EndHeight-request.StartHeight)/2,
		request.EndHeight - 1,
	}

	missingHeights := []uint64{}

	for _, height := range sampleHeights {
		if height >= request.EndHeight {
			continue
		}

		// Check if block metadata exists at this height
		_, err := data.DestStorage.GetBlockByHeight(ctx, request.Tag, height)
		if err != nil {
			if errors.Is(err, storage.ErrItemNotFound) {
				missingHeights = append(missingHeights, height)
				logger.Warn("Block metadata missing at height", zap.Uint64("height", height))
			} else {
				return xerrors.Errorf("failed to check block metadata at height %d: %w", height, err)
			}
		}
	}

	if len(missingHeights) > 0 {
		return xerrors.Errorf("cannot migrate events with skip-blocks=true: block metadata missing at heights %v. "+
			"Block metadata must be migrated first (run migration with skip-blocks=false) before migrating events only",
			missingHeights)
	}

	// Additionally, check a few specific heights that events will reference
	// Get some events to check their referenced block heights
	sourceEvents, err := data.SourceStorage.GetEventsByBlockHeight(ctx, request.EventTag, request.StartHeight)
	if err != nil && !errors.Is(err, storage.ErrItemNotFound) {
		return xerrors.Errorf("failed to get sample events for validation: %w", err)
	}

	if len(sourceEvents) > 0 {
		// Check first few events to see if their block metadata exists
		checkCount := 3
		if len(sourceEvents) < checkCount {
			checkCount = len(sourceEvents)
		}

		for i := 0; i < checkCount; i++ {
			event := sourceEvents[i]
			_, err := data.DestStorage.GetBlockByHeight(ctx, request.Tag, event.BlockHeight)
			if err != nil {
				if errors.Is(err, storage.ErrItemNotFound) {
					return xerrors.Errorf("cannot migrate events with skip-blocks=true: block metadata missing for event at height %d. "+
						"Block metadata must be migrated first before migrating events", event.BlockHeight)
				}
				return xerrors.Errorf("failed to validate block metadata for event at height %d: %w", event.BlockHeight, err)
			}
		}
	}

	return nil
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
	Height uint64
	Found  bool // true if events were found, false if no events exist yet
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

	// Get the latest event height from PostgreSQL by querying max event_sequence and its corresponding height
	latestEventHeight, err := a.getLatestEventHeight(ctx, destResult.MetaStorage, request.EventTag)
	if err != nil {
		// Check if it's a "no event history" error, which means no events migrated yet
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "no event history") || strings.Contains(errStr, "not found") || strings.Contains(errStr, "no rows") {
			logger.Info("No events found in PostgreSQL destination - starting from beginning")
			return &GetLatestEventFromPostgresResponse{
				Height: 0,
				Found:  false,
			}, nil
		}
		return nil, xerrors.Errorf("failed to get latest event height from PostgreSQL: %w", err)
	}

	logger.Info("Found latest event in PostgreSQL destination", zap.Uint64("height", latestEventHeight))
	return &GetLatestEventFromPostgresResponse{
		Height: latestEventHeight,
		Found:  true,
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
)