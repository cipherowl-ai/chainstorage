package activity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
		Skipped    bool  // Whether this is a skipped block
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
	blocksToMigrate, err := a.extractBlocksFromEvents(logger, events)
	if err != nil {
		return nil, xerrors.Errorf("failed to extract blocks from events: %w", err)
	}

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

	logger.Info("Fetching events by sequence range",
		zap.Int64("startSeq", startSeq),
		zap.Int64("endSeq", endSeq),
		zap.Int64("totalSequences", totalSequences))

	// Record heartbeat before starting the fetch
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Starting to fetch events [%d, %d)", startSeq, endSeq))

	// GetEventsByEventIdRange already handles the fetching efficiently internally
	// No need for additional batching/parallelism as DynamoDB Query operation
	// is already optimized for sequential range scans
	events, err := data.SourceStorage.GetEventsByEventIdRange(
		ctx, request.EventTag, startSeq, endSeq)

	if err != nil {
		// Events must be continuous - any missing events indicate data integrity issues
		return nil, xerrors.Errorf("failed to fetch events [%d,%d): %w",
			startSeq, endSeq, err)
	}

	// Record heartbeat after fetch completes
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Fetched %d events from range [%d, %d)",
		len(events), startSeq, endSeq))

	// Validate we got the expected number of events
	expectedCount := int(totalSequences)
	if len(events) != expectedCount {
		return nil, xerrors.Errorf("missing events: expected %d events but got %d for range [%d,%d)",
			expectedCount, len(events), startSeq, endSeq)
	}

	// Sort events to ensure proper ordering for validation
	sort.Slice(events, func(i, j int) bool {
		return events[i].EventId < events[j].EventId
	})

	// Validate event IDs are continuous (no gaps)
	for i, event := range events {
		expectedEventId := startSeq + int64(i)
		if event.EventId != expectedEventId {
			return nil, xerrors.Errorf("gap in event sequence: expected event ID %d but got %d at index %d",
				expectedEventId, event.EventId, i)
		}
	}

	logger.Info("Fetched events successfully",
		zap.Int("totalEvents", len(events)))

	return events, nil
}

func (a *Migrator) extractBlocksFromEvents(logger *zap.Logger,
	events []*model.EventEntry) ([]BlockToMigrate, error) {

	var blocks []BlockToMigrate
	blockAddedCount := 0
	blockRemovedCount := 0
	skippedBlockCount := 0

	for _, event := range events {
		switch event.EventType {
		case api.BlockchainEvent_BLOCK_ADDED:
			blocks = append(blocks, BlockToMigrate{
				Height:     event.BlockHeight,
				Hash:       event.BlockHash,
				ParentHash: event.ParentHash,
				EventSeq:   event.EventId,
				Skipped:    event.BlockSkipped,
			})
			blockAddedCount++
			if event.BlockSkipped {
				skippedBlockCount++
			}
		case api.BlockchainEvent_BLOCK_REMOVED:
			blockRemovedCount++
		case api.BlockchainEvent_UNKNOWN:
			return nil, xerrors.Errorf("encountered UNKNOWN event type at eventId=%d, height=%d", 
				event.EventId, event.BlockHeight)
		default:
			return nil, xerrors.Errorf("unexpected event type %v at eventId=%d, height=%d", 
				event.EventType, event.EventId, event.BlockHeight)
		}
	}

	logger.Info("Extracted blocks from events",
		zap.Int("totalEvents", len(events)),
		zap.Int("blockAddedEvents", blockAddedCount),
		zap.Int("blockRemovedEvents", blockRemovedCount),
		zap.Int("skippedBlocks", skippedBlockCount),
		zap.Int("blocksToMigrate", len(blocks)))

	return blocks, nil
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

	if len(blocksToMigrate) == 0 {
		return nil, nil
	}

	// Determine parallelism - use request parallelism or default to 1
	parallelism := request.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	// Don't use more workers than blocks
	if parallelism > len(blocksToMigrate) {
		parallelism = 1
	}

	logger.Debug("Fetching block data in parallel",
		zap.Int("totalBlocks", len(blocksToMigrate)),
		zap.Int("parallelism", parallelism))

	// Channel for work distribution
	workChan := make(chan BlockToMigrate, len(blocksToMigrate))
	resultChan := make(chan *BlockWithInfo, len(blocksToMigrate))

	// Send work items directly
	for _, block := range blocksToMigrate {
		workChan <- block
	}
	close(workChan)

	// Worker function
	worker := func() {
		for block := range workChan {
			var blockMeta *api.BlockMetadata
			
			// For skipped blocks, create metadata directly without fetching
			if block.Skipped {
				blockMeta = &api.BlockMetadata{
					Tag:          request.Tag,
					Height:       block.Height,
					Skipped:      true,
					// Hash and ParentHash remain empty for skipped blocks
					// These will be stored as NULL in PostgreSQL
					Hash:         "",
					ParentHash:   "",
					ParentHeight: 0,
					Timestamp:    nil,
				}
			} else {
				// For regular blocks, fetch from source storage
				var err error
				blockMeta, err = data.SourceStorage.GetBlockByHash(ctx, request.Tag, block.Height, block.Hash)
				if err != nil {
					logger.Warn("Failed to fetch block by hash",
						zap.Uint64("height", block.Height),
						zap.String("hash", block.Hash),
						zap.Error(err))
					continue
				}
			}

			resultChan <- &BlockWithInfo{
				BlockMetadata: blockMeta,
				Height:        block.Height,
				Hash:          block.Hash,
				EventSeq:      block.EventSeq,
			}
		}
	}

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker()
		}()
	}

	// Close result channel when all workers complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results - order doesn't matter since they'll be sorted later
	var allBlocksWithInfo []*BlockWithInfo
	skippedCount := 0
	for blockInfo := range resultChan {
		allBlocksWithInfo = append(allBlocksWithInfo, blockInfo)
		if blockInfo.BlockMetadata != nil && blockInfo.BlockMetadata.Skipped {
			skippedCount++
		}
	}

	logger.Debug("Completed parallel block data fetch",
		zap.Int("requested", len(blocksToMigrate)),
		zap.Int("fetched", len(allBlocksWithInfo)),
		zap.Int("skippedBlocks", skippedCount))

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
