package activity

import (
	"context"
	"errors"
	"fmt"
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

	MigratorParams struct {
		fx.In
		fxparams.Params
		Runtime cadence.Runtime
		Session *session.Session
	}

	MigratorRequest struct {
		StartHeight    uint64
		EndHeight      uint64 // Optional. If not specified, will query latest block from DynamoDB
		EventTag       uint32
		Tag            uint32
		BatchSize      int
		Parallelism    int
		SkipEvents     bool
		SkipBlocks     bool
		DoEventCatchUp bool // If true, perform smart event catch-up to match Postgres block height (manual start only)
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
	// Allow equal or inverted ranges when doing event-only catch-up scenarios.
	if request.EndHeight <= request.StartHeight {
		if !(request.SkipBlocks || request.DoEventCatchUp) {
			return nil, xerrors.Errorf("invalid request: EndHeight (%d) must be greater than StartHeight (%d)", request.EndHeight, request.StartHeight)
		}
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

	// Validate batch size
	if request.BatchSize <= 0 {
		request.BatchSize = 100
	}

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

func (a *Migrator) migrateBlocks(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest) (int, error) {
	migrateBlocksStart := time.Now()
	logger.Info("Starting height-by-height block metadata migration with complete reorg support",
		zap.Uint64("startHeight", request.StartHeight),
		zap.Uint64("endHeight", request.EndHeight),
		zap.Uint64("totalHeights", request.EndHeight-request.StartHeight))

	totalNonCanonicalBlocks := 0
	totalHeights := request.EndHeight - request.StartHeight

	for height := request.StartHeight; height < request.EndHeight; height++ {
		heightStartTime := time.Now()

		nonCanonicalCount, err := a.migrateBlocksAtHeight(ctx, data, request, height)
		if err != nil {
			logger.Error("Failed to migrate blocks at height",
				zap.Uint64("height", height),
				zap.Duration("heightDuration", time.Since(heightStartTime)),
				zap.Error(err))
			return 0, xerrors.Errorf("failed to migrate blocks at height %d: %w", height, err)
		}

		totalNonCanonicalBlocks += nonCanonicalCount

		// Progress logging every 10 heights for detailed monitoring
		if (height-request.StartHeight+1)%10 == 0 {
			percentage := float64(height-request.StartHeight+1) / float64(totalHeights) * 100
			logger.Info("Block migration progress",
				zap.Uint64("currentHeight", height),
				zap.Uint64("processed", height-request.StartHeight+1),
				zap.Uint64("total", totalHeights),
				zap.Float64("percentage", percentage),
				zap.Duration("avgPerHeight", time.Since(migrateBlocksStart)/time.Duration(height-request.StartHeight+1)),
				zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks))
		}
	}

	totalDuration := time.Since(migrateBlocksStart)
	logger.Info("Height-by-height block metadata migration completed",
		zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks),
		zap.Duration("totalDuration", totalDuration),
		zap.Float64("avgSecondsPerHeight", totalDuration.Seconds()/float64(totalHeights)))

	return int(totalHeights), nil
}

func (a *Migrator) migrateBlocksAtHeight(ctx context.Context, data *MigrationData, request *MigratorRequest, height uint64) (int, error) {
	blockPid := fmt.Sprintf("%d-%d", request.Tag, height)
	logger := a.getLogger(ctx)

	// Get ALL blocks at this height (canonical + non-canonical) in one DynamoDB query
	queryStart := time.Now()
	allBlocks, err := a.getAllBlocksAtHeight(ctx, data, blockPid)
	queryDuration := time.Since(queryStart)

	if err != nil {
		if errors.Is(err, storage.ErrItemNotFound) {
			logger.Debug("No blocks found at height",
				zap.Uint64("height", height),
				zap.Duration("queryDuration", queryDuration))
			return 0, nil
		}
		logger.Error("Failed to get blocks at height",
			zap.Uint64("height", height),
			zap.Duration("queryDuration", queryDuration),
			zap.Error(err))
		return 0, xerrors.Errorf("failed to get blocks at height %d: %w", height, err)
	}

	if len(allBlocks) == 0 {
		logger.Debug("No blocks found at height", zap.Uint64("height", height))
		return 0, nil
	}

	// Separate canonical and non-canonical blocks
	var canonicalBlocks []*api.BlockMetadata
	var nonCanonicalBlocks []*api.BlockMetadata

	for _, blockWithInfo := range allBlocks {
		if blockWithInfo.IsCanonical {
			canonicalBlocks = append(canonicalBlocks, blockWithInfo.BlockMetadata)
		} else {
			nonCanonicalBlocks = append(nonCanonicalBlocks, blockWithInfo.BlockMetadata)
		}
	}

	// Persist each block individually to avoid chain validation issues between different blocks at same height
	persistStart := time.Now()

	// First persist non-canonical blocks (won't become canonical in PostgreSQL)
	for _, block := range nonCanonicalBlocks {
		err = data.DestStorage.PersistBlockMetas(ctx, false, []*api.BlockMetadata{block}, nil)
		if err != nil {
			logger.Error("Failed to persist non-canonical block",
				zap.Uint64("height", height),
				zap.String("blockHash", block.Hash),
				zap.Error(err))
			return 0, xerrors.Errorf("failed to persist non-canonical block at height %d: %w", height, err)
		}
	}

	// Then persist canonical blocks (will become canonical in PostgreSQL due to "last block wins")
	for _, block := range canonicalBlocks {
		err = data.DestStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block}, nil)
		if err != nil {
			logger.Error("Failed to persist canonical block",
				zap.Uint64("height", height),
				zap.String("blockHash", block.Hash),
				zap.Error(err))
			return 0, xerrors.Errorf("failed to persist canonical block at height %d: %w", height, err)
		}
	}

	persistDuration := time.Since(persistStart)
	totalBlocks := len(allBlocks)
	nonCanonicalCount := len(nonCanonicalBlocks)

	logger.Debug("Persisted all blocks at height",
		zap.Uint64("height", height),
		zap.Int("totalBlocks", totalBlocks),
		zap.Int("canonicalBlocks", len(canonicalBlocks)),
		zap.Int("nonCanonicalBlocks", nonCanonicalCount),
		zap.Duration("persistDuration", persistDuration))

	return nonCanonicalCount, nil
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

	var allBlocks []BlockWithCanonicalInfo
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
		allBlocks = append(allBlocks, blockWithInfo)
	}

	return allBlocks, nil
}

// smartEventCatchUp performs efficient sequential event catch-up when there's a gap between event height and block height
func (a *Migrator) smartEventCatchUp(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest) (int, error) {
	// Get current PostgreSQL block height
	destLatestBlock, err := data.DestStorage.GetLatestBlock(ctx, request.Tag)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") || strings.Contains(strings.ToLower(err.Error()), "no rows") {
			// No blocks in destination yet, no catch-up needed
			return 0, nil
		}
		return 0, xerrors.Errorf("failed to get latest block from postgres: %w", err)
	}

	// Get current PostgreSQL event height  
	latestEventHeight, hasEvents, err := a.getLatestEventHeightFromPostgres(ctx, data, request.EventTag)
	if err != nil {
		return 0, xerrors.Errorf("failed to get latest event height from postgres: %w", err)
	}

	// Calculate the gap
	blockHeight := destLatestBlock.Height
	eventHeight := uint64(0)
	if hasEvents {
		eventHeight = latestEventHeight
	}

	if blockHeight <= eventHeight {
		// Events are caught up or ahead, no catch-up needed
		return 0, nil
	}

	gap := blockHeight - eventHeight
	logger.Info("Detected event gap, performing sequential catch-up",
		zap.Uint64("currentBlockHeight", blockHeight),
		zap.Uint64("currentEventHeight", eventHeight),
		zap.Uint64("gap", gap))

	// Use sequential batches to ensure event sequence continuity during catch-up
	batchSize := uint64(100) // Default batch size for catch-up
	if request.BatchSize > 0 {
		batchSize = uint64(request.BatchSize)
	}

	totalEventsCaughtUp := 0
	catchUpStart := eventHeight + 1
	catchUpEnd := blockHeight

	for batchStart := catchUpStart; batchStart <= catchUpEnd; batchStart += batchSize {
		batchEnd := batchStart + batchSize - 1
		if batchEnd > catchUpEnd {
			batchEnd = catchUpEnd
		}

		logger.Info("Catching up event batch sequentially",
			zap.Uint64("batchStart", batchStart),
			zap.Uint64("batchEnd", batchEnd))

		// Process this batch sequentially to preserve DynamoDB sequence mapping
		batchEvents, err := a.migrateEventsRangeSequential(ctx, data, request, batchStart, batchEnd)
		if err != nil {
			return totalEventsCaughtUp, xerrors.Errorf("failed to catch up events batch [%d-%d]: %w", batchStart, batchEnd, err)
		}

		totalEventsCaughtUp += batchEvents
		progress := float64(batchEnd-catchUpStart+1) / float64(gap) * 100

		logger.Info("Event catch-up batch completed",
			zap.Uint64("batchStart", batchStart),
			zap.Uint64("batchEnd", batchEnd),
			zap.Int("batchEvents", batchEvents),
			zap.Int("totalCaughtUp", totalEventsCaughtUp),
			zap.Float64("progress", progress))
	}

	return totalEventsCaughtUp, nil
}

// migrateEventsRangeSequential processes events for a height range sequentially (no mini-batch parallelism)
// Used for catch-up scenarios where event sequence continuity is critical
// Preserves original DynamoDB sequence numbers for 1:1 mapping
func (a *Migrator) migrateEventsRangeSequential(ctx context.Context, data *MigrationData, request *MigratorRequest, startHeight, endHeight uint64) (int, error) {
	allEvents := make([]*model.EventEntry, 0, (endHeight-startHeight+1)*2) // Estimate 2 events per block

	// Collect all events in this range sequentially
	for h := startHeight; h <= endHeight; h++ {
		sourceEvents, err := data.SourceStorage.GetEventsByBlockHeight(ctx, request.EventTag, h)
		if err != nil {
			if errors.Is(err, storage.ErrItemNotFound) {
				continue // No events at this height; skip
			}
			return 0, xerrors.Errorf("failed to get events at height %d: %w", h, err)
		}
		if len(sourceEvents) > 0 {
			allEvents = append(allEvents, sourceEvents...)
		}
	}

	// Insert events as-is to preserve DynamoDB sequence mapping
	if len(allEvents) > 0 {
		if err := data.DestStorage.AddEventEntries(ctx, request.EventTag, allEvents); err != nil {
			return 0, xerrors.Errorf("failed to add sequential events for range [%d-%d]: %w",
				startHeight, endHeight, err)
		}
	}

	return len(allEvents), nil
}

// getLatestEventHeightFromPostgres returns the block height of the latest event currently stored in PostgreSQL.
// If there is no event history yet, it returns hasEvents=false with height=0.
func (a *Migrator) getLatestEventHeightFromPostgres(
	ctx context.Context,
	data *MigrationData,
	eventTag uint32,
) (uint64, bool, error) {
	maxEventId, err := data.DestStorage.GetMaxEventId(ctx, eventTag)
	if err != nil {
		// No events yet in destination
		if strings.Contains(strings.ToLower(err.Error()), "no event history") || strings.Contains(strings.ToLower(err.Error()), "not found") {
			return 0, false, nil
		}
		return 0, false, xerrors.Errorf("failed to get max event id from postgres: %w", err)
	}
	event, err := data.DestStorage.GetEventByEventId(ctx, eventTag, maxEventId)
	if err != nil {
		return 0, false, xerrors.Errorf("failed to get event by id from postgres: %w", err)
	}
	return event.BlockHeight, true, nil
}

func (a *Migrator) migrateEvents(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest) (int, error) {
	logger.Info("Starting batched event migration with parallelism",
		zap.Int("parallelism", request.Parallelism),
		zap.Int("batchSize", request.BatchSize))

	// If we're skipping blocks, validate that required block metadata exists in PostgreSQL
	if request.SkipBlocks {
		logger.Info("Skip-blocks enabled, validating that block metadata exists in PostgreSQL")
		if err := a.validateBlockMetadataExists(ctx, data, request); err != nil {
			return 0, xerrors.Errorf("block metadata validation failed: %w", err)
		}
		logger.Info("Block metadata validation passed")
	}

	totalEventsMigrated := 0

	// Smart catch-up: only on manual start (DoEventCatchUp=true), not on continue-as-new
	if request.DoEventCatchUp && !request.SkipEvents {
		catchUpCount, err := a.smartEventCatchUp(ctx, logger, data, request)
		if err != nil {
			return 0, xerrors.Errorf("failed to perform smart event catch-up: %w", err)
		}
		totalEventsMigrated += catchUpCount
		if catchUpCount > 0 {
			logger.Info("Smart event catch-up completed", zap.Int("eventsCaughtUp", catchUpCount))
		}
	}

	// Resume events from the current latest event height in Postgres, but clamp to the requested range.
	latestEventHeight, hasEvents, err := a.getLatestEventHeightFromPostgres(ctx, data, request.EventTag)
	if err != nil {
		return 0, xerrors.Errorf("failed to get latest event height from postgres: %w", err)
	}

	// Determine start height: default to requested StartHeight
	startHeight := request.StartHeight
	if hasEvents {
		candidate := latestEventHeight + 1
		if candidate > startHeight {
			startHeight = candidate
		}
	}
	// Determine inclusive end height: EndHeight is exclusive in requests
	if request.EndHeight == 0 {
		return 0, nil
	}
	endHeightInclusive := request.EndHeight - 1
	if startHeight > endHeightInclusive {
		return 0, nil
	}

	// Calculate batch size - use request.BatchSize or default to reasonable size
	batchSize := uint64(request.BatchSize)
	if batchSize == 0 {
		batchSize = 100 // Default batch size for height ranges
	}

	// Determine parallelism
	parallelism := request.Parallelism
	if parallelism <= 0 {
		parallelism = 1 // Default to sequential processing
	}

	totalEvents := 0
	totalHeights := endHeightInclusive - startHeight + 1

	// Process in batches with parallelism
	for batchStart := startHeight; batchStart <= endHeightInclusive; batchStart += batchSize {
		batchEnd := batchStart + batchSize - 1
		if batchEnd > endHeightInclusive {
			batchEnd = endHeightInclusive
		}

		logger.Info("Processing event batch",
			zap.Uint64("batchStart", batchStart),
			zap.Uint64("batchEnd", batchEnd),
			zap.Uint64("batchSize", batchEnd-batchStart+1))

		batchEvents, err := a.migrateEventsBatch(ctx, logger, data, request, batchStart, batchEnd, parallelism)
		if err != nil {
			return totalEvents, xerrors.Errorf("failed to migrate events batch [%d-%d]: %w", batchStart, batchEnd, err)
		}

		totalEvents += batchEvents
		progress := float64(batchEnd-startHeight+1) / float64(totalHeights) * 100

		logger.Info("Event batch completed",
			zap.Uint64("batchStart", batchStart),
			zap.Uint64("batchEnd", batchEnd),
			zap.Int("batchEvents", batchEvents),
			zap.Int("totalEvents", totalEvents),
			zap.Float64("progress", progress))
	}

	logger.Info("Batched event migration completed",
		zap.Int("totalEventsMigrated", totalEvents),
		zap.Uint64("totalHeights", totalHeights))
	return totalEvents, nil
}

// migrateEventsBatch processes a batch of events with parallelism, similar to block migration approach
func (a *Migrator) migrateEventsBatch(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest, startHeight, endHeight uint64, parallelism int) (int, error) {
	// Create mini-batches for parallel processing
	miniBatchSize := uint64(10) // Process 10 heights per mini-batch
	totalHeights := endHeight - startHeight + 1

	// Adjust mini-batch size if total heights is small
	if totalHeights < 10 {
		miniBatchSize = totalHeights
	}

	// Create channels for parallel processing
	type heightRange struct {
		start, end uint64
	}

	type batchResult struct {
		events int
		err    error
		range_ heightRange
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
				events, err := a.migrateEventsRange(ctx, data, request, heightRange.start, heightRange.end)
				resultChannel <- batchResult{
					events: events,
					err:    err,
					range_: heightRange,
				}
			}
		}(i)
	}

	// Collect results
	totalEvents := 0
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
		totalEvents += result.events

		if i%10 == 0 || result.events > 0 {
			logger.Debug("Mini-batch completed",
				zap.Uint64("rangeStart", result.range_.start),
				zap.Uint64("rangeEnd", result.range_.end),
				zap.Int("events", result.events),
				zap.Int("totalSoFar", totalEvents))
		}
	}

	return totalEvents, nil
}

// migrateEventsRange processes events for a specific height range efficiently
func (a *Migrator) migrateEventsRange(ctx context.Context, data *MigrationData, request *MigratorRequest, startHeight, endHeight uint64) (int, error) {
	allEvents := make([]*model.EventEntry, 0, (endHeight-startHeight+1)*2) // Estimate 2 events per block

	// Collect all events in this range
	for h := startHeight; h <= endHeight; h++ {
		sourceEvents, err := data.SourceStorage.GetEventsByBlockHeight(ctx, request.EventTag, h)
		if err != nil {
			if errors.Is(err, storage.ErrItemNotFound) {
				continue // No events at this height; skip
			}
			return 0, xerrors.Errorf("failed to get events at height %d: %w", h, err)
		}
		if len(sourceEvents) > 0 {
			allEvents = append(allEvents, sourceEvents...)
		}
	}

	// Bulk insert all events at once if we have any
	if len(allEvents) > 0 {
		if err := data.DestStorage.AddEventEntries(ctx, request.EventTag, allEvents); err != nil {
			return 0, xerrors.Errorf("failed to bulk add %d events for range [%d-%d]: %w",
				len(allEvents), startHeight, endHeight, err)
		}
	}

	return len(allEvents), nil
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
)
