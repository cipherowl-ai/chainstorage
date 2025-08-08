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
		DoEventCatchUp bool // If true, perform one-time event catch-up to Postgres block height
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

	// NEW: Catch up events to the current PostgreSQL block height BEFORE migrating new blocks/events
	if request.DoEventCatchUp && !request.SkipEvents {
		catchUpCount, err := a.catchUpEventsToPostgresBlockHeight(ctx, logger, migrationData, request)
		if err != nil {
			return nil, xerrors.Errorf("failed to catch up events to postgres block height: %w", err)
		}
		if catchUpCount > 0 {
			logger.Info("Event catch-up completed", zap.Int("eventsCaughtUp", catchUpCount))
		}
	}

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

	// Phase 1: Get and persist non-canonical blocks first
	nonCanonicalStart := time.Now()
	nonCanonicalBlocks, err := a.getNonCanonicalBlocksAtHeight(ctx, data, blockPid)
	nonCanonicalQueryDuration := time.Since(nonCanonicalStart)

	if err != nil && !errors.Is(err, storage.ErrItemNotFound) {
		logger.Error("Failed to get non-canonical blocks",
			zap.Uint64("height", height),
			zap.Duration("queryDuration", nonCanonicalQueryDuration),
			zap.Error(err))
		return 0, xerrors.Errorf("failed to get non-canonical blocks at height %d: %w", height, err)
	}

	nonCanonicalCount := len(nonCanonicalBlocks)
	if nonCanonicalCount > 0 {
		// Persist non-canonical blocks FIRST
		persistStart := time.Now()
		err = data.DestStorage.PersistBlockMetas(ctx, false, nonCanonicalBlocks, nil)
		persistDuration := time.Since(persistStart)

		if err != nil {
			logger.Error("Failed to persist non-canonical blocks",
				zap.Uint64("height", height),
				zap.Int("blockCount", nonCanonicalCount),
				zap.Duration("persistDuration", persistDuration),
				zap.Error(err))
			return 0, xerrors.Errorf("failed to persist non-canonical blocks at height %d: %w", height, err)
		}

		logger.Debug("Persisted non-canonical blocks",
			zap.Uint64("height", height),
			zap.Int("blockCount", nonCanonicalCount),
			zap.Duration("persistDuration", persistDuration))
	}

	// Phase 2: Get and persist canonical block LAST
	canonicalStart := time.Now()
	canonicalBlock, err := a.getCanonicalBlockAtHeight(ctx, data, blockPid)
	canonicalQueryDuration := time.Since(canonicalStart)

	if err != nil {
		if errors.Is(err, storage.ErrItemNotFound) {
			logger.Debug("No canonical block found at height",
				zap.Uint64("height", height),
				zap.Duration("queryDuration", canonicalQueryDuration))
			return nonCanonicalCount, nil
		}
		logger.Error("Failed to get canonical block",
			zap.Uint64("height", height),
			zap.Duration("queryDuration", canonicalQueryDuration),
			zap.Error(err))
		return 0, xerrors.Errorf("failed to get canonical block at height %d: %w", height, err)
	}

	// Persist canonical block LAST - this ensures it becomes canonical in PostgreSQL
	persistCanonicalStart := time.Now()
	err = data.DestStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{canonicalBlock}, nil)
	persistCanonicalDuration := time.Since(persistCanonicalStart)

	if err != nil {
		logger.Error("Failed to persist canonical block",
			zap.Uint64("height", height),
			zap.Duration("persistDuration", persistCanonicalDuration),
			zap.Error(err))
		return 0, xerrors.Errorf("failed to persist canonical block at height %d: %w", height, err)
	}

	logger.Debug("Persisted canonical block",
		zap.Uint64("height", height),
		zap.Duration("persistDuration", persistCanonicalDuration))

	return nonCanonicalCount, nil
}

func (a *Migrator) getNonCanonicalBlocksAtHeight(ctx context.Context, data *MigrationData, blockPid string) ([]*api.BlockMetadata, error) {
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

	logger.Debug("DynamoDB query for non-canonical blocks",
		zap.String("blockPid", blockPid),
		zap.Duration("queryDuration", queryDuration),
		zap.Bool("success", err == nil))

	if err != nil {
		logger.Error("DynamoDB query failed for non-canonical blocks",
			zap.String("blockPid", blockPid),
			zap.Duration("queryDuration", queryDuration),
			zap.Error(err))
		return nil, xerrors.Errorf("failed to query blocks at height: %w", err)
	}

	var nonCanonicalBlocks []*api.BlockMetadata
	for _, item := range result.Items {
		var blockEntry dynamodb_model.BlockMetaDataDDBEntry
		err := dynamodbattribute.UnmarshalMap(item, &blockEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal DynamoDB item: %w", err)
		}

		// Skip canonical blocks (BlockRid = "canonical")
		if blockEntry.BlockRid == "canonical" {
			continue
		}

		nonCanonicalBlocks = append(nonCanonicalBlocks, dynamodb_model.BlockMetadataToProto(&blockEntry))
	}

	return nonCanonicalBlocks, nil
}

func (a *Migrator) getCanonicalBlockAtHeight(ctx context.Context, data *MigrationData, blockPid string) (*api.BlockMetadata, error) {
	logger := a.getLogger(ctx)

	input := &dynamodb.QueryInput{
		TableName:              awssdk.String(data.BlockTable),
		KeyConditionExpression: awssdk.String("block_pid = :blockPid AND block_rid = :canonical"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":blockPid": {
				S: awssdk.String(blockPid),
			},
			":canonical": {
				S: awssdk.String("canonical"),
			},
		},
		ConsistentRead: awssdk.Bool(true),
	}

	queryStart := time.Now()
	result, err := data.DynamoClient.QueryWithContext(ctx, input)
	queryDuration := time.Since(queryStart)

	logger.Debug("DynamoDB query for canonical block",
		zap.String("blockPid", blockPid),
		zap.Duration("queryDuration", queryDuration),
		zap.Bool("success", err == nil))

	if err != nil {
		logger.Error("DynamoDB query failed for canonical block",
			zap.String("blockPid", blockPid),
			zap.Duration("queryDuration", queryDuration),
			zap.Error(err))
		return nil, xerrors.Errorf("failed to query canonical block: %w", err)
	}

	if len(result.Items) == 0 {
		return nil, storage.ErrItemNotFound
	}

	if len(result.Items) > 1 {
		return nil, xerrors.Errorf("multiple canonical blocks found for %s", blockPid)
	}

	var blockEntry dynamodb_model.BlockMetaDataDDBEntry
	err = dynamodbattribute.UnmarshalMap(result.Items[0], &blockEntry)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal canonical block: %w", err)
	}

	return dynamodb_model.BlockMetadataToProto(&blockEntry), nil
}

func (a *Migrator) catchUpEventsToPostgresBlockHeight(
	ctx context.Context,
	logger *zap.Logger,
	data *MigrationData,
	request *MigratorRequest,
) (int, error) {
	// Determine the latest block height already migrated to PostgreSQL.
	destLatestBlock, err := data.DestStorage.GetLatestBlock(ctx, request.Tag)
	if err != nil {
		// If no blocks are present in destination yet, there is nothing to catch up.
		if strings.Contains(strings.ToLower(err.Error()), "not found") || strings.Contains(strings.ToLower(err.Error()), "no rows") {
			return 0, nil
		}
		return 0, xerrors.Errorf("failed to get latest block from postgres: %w", err)
	}

	// Determine the latest event height already in PostgreSQL (if any).
	latestEventHeight, hasEvents, err := a.getLatestEventHeightFromPostgres(ctx, data, request.EventTag)
	if err != nil {
		return 0, xerrors.Errorf("failed to get latest event height from postgres: %w", err)
	}

	// Compute the catch-up target height: always catch up events to the current PostgreSQL block height.
	var targetHeight uint64
	targetHeight = destLatestBlock.Height

	// Nothing to do if we already have events at or beyond the target.
	if hasEvents && latestEventHeight >= targetHeight {
		return 0, nil
	}

	// Start catch-up from the next height after the last event height (or from 0 if there was no event history).
	startHeight := uint64(0)
	if hasEvents {
		startHeight = latestEventHeight + 1
	}
	if startHeight > targetHeight {
		return 0, nil
	}

	logger.Info("Catching up events up to postgres block height",
		zap.Uint64("fromHeight", startHeight),
		zap.Uint64("toHeight", targetHeight))

	eventsCaught := 0
	// Iterate height by height to ensure completeness.
	for h := startHeight; h <= targetHeight; h++ {
		sourceEvents, err := data.SourceStorage.GetEventsByBlockHeight(ctx, request.EventTag, h)
		if err != nil {
			if errors.Is(err, storage.ErrItemNotFound) {
				continue // No events at this height in source; skip
			}
			return eventsCaught, xerrors.Errorf("failed to get events at height %d from source: %w", h, err)
		}
		if len(sourceEvents) == 0 {
			continue
		}

		if err := data.DestStorage.AddEventEntries(ctx, request.EventTag, sourceEvents); err != nil {
			return eventsCaught, xerrors.Errorf("failed to add events for height %d to postgres: %w", h, err)
		}
		eventsCaught += len(sourceEvents)
	}

	return eventsCaught, nil
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
