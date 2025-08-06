package activity

import (
	"context"
	"fmt"
	"strings"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/uber-go/tally/v4"
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
		StartHeight uint64
		EndHeight   uint64 // Optional. If not specified, will query latest block from DynamoDB
		EventTag    uint32
		Tag         uint32
		BatchSize   int
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

func (a *Migrator) Execute(ctx workflow.Context, request *MigratorRequest) (*MigratorResponse, error) {
	var response MigratorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Migrator) execute(ctx context.Context, request *MigratorRequest) (*MigratorResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))

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

	logger.Info("Migration completed successfully",
		zap.Int("blocksMigrated", blocksMigrated),
		zap.Int("eventsMigrated", eventsMigrated))

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
	logger.Info("Starting height-by-height block metadata migration with complete reorg support")

	totalNonCanonicalBlocks := 0
	totalHeights := request.EndHeight - request.StartHeight

	for height := request.StartHeight; height < request.EndHeight; height++ {
		nonCanonicalCount, err := a.migrateBlocksAtHeight(ctx, data, request, height)
		if err != nil {
			return 0, xerrors.Errorf("failed to migrate blocks at height %d: %w", height, err)
		}

		totalNonCanonicalBlocks += nonCanonicalCount

		// Progress logging every 100 heights
		if (height-request.StartHeight+1)%100 == 0 {
			percentage := float64(height-request.StartHeight+1) / float64(totalHeights) * 100
			logger.Info("Block migration progress",
				zap.Uint64("processed", height-request.StartHeight+1),
				zap.Uint64("total", totalHeights),
				zap.Float64("percentage", percentage),
				zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks))
		}
	}

	logger.Info("Height-by-height block metadata migration completed",
		zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks))

	return int(totalHeights), nil
}

func (a *Migrator) migrateBlocksAtHeight(ctx context.Context, data *MigrationData, request *MigratorRequest, height uint64) (int, error) {
	blockPid := fmt.Sprintf("%d-%d", request.Tag, height)

	// Phase 1: Get and persist non-canonical blocks first
	nonCanonicalBlocks, err := a.getNonCanonicalBlocksAtHeight(ctx, data, blockPid)
	if err != nil && !xerrors.Is(err, storage.ErrItemNotFound) {
		return 0, xerrors.Errorf("failed to get non-canonical blocks at height %d: %w", height, err)
	}

	nonCanonicalCount := len(nonCanonicalBlocks)
	if nonCanonicalCount > 0 {
		// Persist non-canonical blocks FIRST
		err = data.DestStorage.PersistBlockMetas(ctx, false, nonCanonicalBlocks, nil)
		if err != nil {
			return 0, xerrors.Errorf("failed to persist non-canonical blocks at height %d: %w", height, err)
		}
	}

	// Phase 2: Get and persist canonical block LAST
	canonicalBlock, err := a.getCanonicalBlockAtHeight(ctx, data, blockPid)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return nonCanonicalCount, nil
		}
		return 0, xerrors.Errorf("failed to get canonical block at height %d: %w", height, err)
	}

	// Persist canonical block LAST - this ensures it becomes canonical in PostgreSQL
	err = data.DestStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{canonicalBlock}, nil)
	if err != nil {
		return 0, xerrors.Errorf("failed to persist canonical block at height %d: %w", height, err)
	}

	return nonCanonicalCount, nil
}

func (a *Migrator) getNonCanonicalBlocksAtHeight(ctx context.Context, data *MigrationData, blockPid string) ([]*api.BlockMetadata, error) {
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

	result, err := data.DynamoClient.QueryWithContext(ctx, input)
	if err != nil {
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

	result, err := data.DynamoClient.QueryWithContext(ctx, input)
	if err != nil {
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

func (a *Migrator) migrateEvents(ctx context.Context, logger *zap.Logger, data *MigrationData, request *MigratorRequest) (int, error) {
	logger.Info("Starting event ID-based migration")

	// If we're skipping blocks, validate that required block metadata exists in PostgreSQL
	if request.SkipBlocks {
		logger.Info("Skip-blocks enabled, validating that block metadata exists in PostgreSQL")
		if err := a.validateBlockMetadataExists(ctx, data, request); err != nil {
			return 0, xerrors.Errorf("block metadata validation failed: %w", err)
		}
		logger.Info("Block metadata validation passed")
	}

	// Step 1: Get the first event ID at start height
	startEventId, err := data.SourceStorage.GetFirstEventIdByBlockHeight(ctx, request.EventTag, request.StartHeight)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			logger.Info("No events found at start height", zap.Uint64("startHeight", request.StartHeight))
			return 0, nil
		}
		return 0, xerrors.Errorf("failed to get first event ID at start height %d: %w", request.StartHeight, err)
	}

	// Step 2: Find the last event ID within the height range
	var endEventId int64
	if request.EndHeight > request.StartHeight {
		endEventId, err = a.findLastEventIdInRange(ctx, data.SourceStorage, request.EventTag, request.StartHeight, request.EndHeight)
		if err != nil {
			return 0, xerrors.Errorf("failed to find last event ID in range [%d, %d): %w", request.StartHeight, request.EndHeight, err)
		}

		if endEventId < startEventId {
			endEventId = startEventId
		}
	} else {
		endEventId = startEventId
	}

	totalEvents := endEventId - startEventId + 1
	if totalEvents <= 0 {
		logger.Info("No events to migrate")
		return 0, nil
	}

	logger.Info("Event ID range determined",
		zap.Int64("startEventId", startEventId),
		zap.Int64("endEventId", endEventId),
		zap.Int64("totalEvents", totalEvents))

	// Step 3: Migrate events by event ID range in batches
	processedEvents := int64(0)
	batchSize := int64(request.BatchSize)

	for currentEventId := startEventId; currentEventId <= endEventId; currentEventId += batchSize {
		batchEndEventId := currentEventId + batchSize - 1
		if batchEndEventId > endEventId {
			batchEndEventId = endEventId
		}

		sourceEvents, err := data.SourceStorage.GetEventsByEventIdRange(ctx, request.EventTag, currentEventId, batchEndEventId+1)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				processedEvents += batchEndEventId - currentEventId + 1
				continue
			}
			return 0, xerrors.Errorf("failed to get events in range [%d, %d]: %w", currentEventId, batchEndEventId, err)
		}

		if len(sourceEvents) == 0 {
			processedEvents += batchEndEventId - currentEventId + 1
			continue
		}

		err = data.DestStorage.AddEventEntries(ctx, request.EventTag, sourceEvents)
		if err != nil {
			return 0, xerrors.Errorf("failed to add events batch [%d, %d] to PostgreSQL: %w", currentEventId, batchEndEventId, err)
		}

		processedEvents += int64(len(sourceEvents))

		// Progress logging every 1000 events
		if processedEvents%1000 == 0 || processedEvents == totalEvents {
			percentage := float64(processedEvents) / float64(totalEvents) * 100
			logger.Info("Event migration progress",
				zap.Int64("processed", processedEvents),
				zap.Int64("total", totalEvents),
				zap.Float64("percentage", percentage))
		}
	}

	logger.Info("Event ID-based migration completed",
		zap.Int64("totalEventsMigrated", processedEvents))

	return int(processedEvents), nil
}

func (a *Migrator) findLastEventIdInRange(ctx context.Context, sourceStorage metastorage.MetaStorage, eventTag uint32, startHeight, endHeight uint64) (int64, error) {
	// Search backwards from endHeight-1 to startHeight to find the last event
	for height := endHeight - 1; height >= startHeight; height-- {
		events, err := sourceStorage.GetEventsByBlockHeight(ctx, eventTag, height)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				continue
			}
			return 0, xerrors.Errorf("failed to get events at height %d: %w", height, err)
		}

		// Find the maximum event ID at this height
		var maxEventId int64 = -1
		for _, event := range events {
			if event.EventId > maxEventId {
				maxEventId = event.EventId
			}
		}

		if maxEventId >= 0 {
			return maxEventId, nil
		}
	}

	return -1, storage.ErrItemNotFound
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
			if xerrors.Is(err, storage.ErrItemNotFound) {
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
	if err != nil && !xerrors.Is(err, storage.ErrItemNotFound) {
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
				if xerrors.Is(err, storage.ErrItemNotFound) {
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
