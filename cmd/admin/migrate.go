package main

import (
	"context"
	"fmt"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	dynamodb_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	postgres_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var (
	migrateFlags struct {
		startHeight uint64
		endHeight   uint64
		eventTag    uint32
		tag         uint32
		batchSize   int
		skipEvents  bool
		skipBlocks  bool
	}
)

var (
	migrateCmd = &cobra.Command{
		Use:   "migrate",
		Short: "Migrate data from DynamoDB to PostgreSQL",
		Long: `Migrate block metadata and events from DynamoDB to PostgreSQL.

Block Migration:
- Handles reorgs by migrating non-canonical blocks first, then canonical blocks last
- Captures complete reorg data by querying DynamoDB directly for all blocks at each height
- Maintains canonical block identification in PostgreSQL through migration order

Event Migration:
- Uses event ID-based iteration for efficient migration
- Gets first event ID from start height, last event ID from end height
- Migrates events sequentially by event ID range in batches
- Event IDs in DynamoDB correspond directly to event sequences in PostgreSQL

Note: Block metadata must be migrated before events since events reference blocks via foreign keys.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var deps struct {
				fx.In
				Config  *config.Config
				Session *session.Session
				Params  fxparams.Params
			}

			app := startApp(
				aws.Module,
				storage.Module,
				fx.Populate(&deps),
			)
			defer app.Close()

			// Create DynamoDB storage directly
			dynamoDBParams := dynamodb_storage.Params{
				Params:  deps.Params,
				Session: deps.Session,
			}
			sourceResult, err := dynamodb_storage.NewMetaStorage(dynamoDBParams)
			if err != nil {
				return xerrors.Errorf("failed to create DynamoDB storage: %w", err)
			}

			// Create PostgreSQL storage directly
			postgresParams := postgres_storage.Params{
				Params: deps.Params,
			}
			destResult, err := postgres_storage.NewMetaStorage(postgresParams)
			if err != nil {
				return xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
			}

			// Validate flags
			if migrateFlags.startHeight >= migrateFlags.endHeight {
				return xerrors.New("startHeight must be less than endHeight")
			}

			if migrateFlags.batchSize <= 0 {
				migrateFlags.batchSize = 100
			}

			// Both skip flags cannot be true
			if migrateFlags.skipEvents && migrateFlags.skipBlocks {
				return xerrors.New("cannot skip both events and blocks - nothing to migrate")
			}

			ctx := context.Background()

			// Create DynamoDB client for direct queries
			dynamoClient := dynamodb.New(deps.Session)
			blockTable := deps.Config.AWS.DynamoDB.BlockTable

			migrator := &DataMigrator{
				sourceStorage: sourceResult.MetaStorage,
				destStorage:   destResult.MetaStorage,
				config:        deps.Config,
				logger:        logger,
				dynamoClient:  dynamoClient,
				blockTable:    blockTable,
			}

			// Confirmation prompt
			prompt := fmt.Sprintf("This will migrate data from height %d to %d. Continue? (y/N): ",
				migrateFlags.startHeight, migrateFlags.endHeight)
			if !confirm(prompt) {
				logger.Info("Migration cancelled")
				return nil
			}

			migrateParams := MigrationParams{
				StartHeight: migrateFlags.startHeight,
				EndHeight:   migrateFlags.endHeight,
				EventTag:    migrateFlags.eventTag,
				Tag:         migrateFlags.tag,
				BatchSize:   migrateFlags.batchSize,
				SkipEvents:  migrateFlags.skipEvents,
				SkipBlocks:  migrateFlags.skipBlocks,
			}

			return migrator.Migrate(ctx, migrateParams)
		},
	}
)

type MigrationParams struct {
	StartHeight uint64
	EndHeight   uint64
	EventTag    uint32
	Tag         uint32
	BatchSize   int
	SkipEvents  bool
	SkipBlocks  bool
}

type DataMigrator struct {
	sourceStorage metastorage.MetaStorage
	destStorage   metastorage.MetaStorage
	config        *config.Config
	logger        *zap.Logger
	// Direct DynamoDB access for querying all blocks
	dynamoClient *dynamodb.DynamoDB
	blockTable   string
}

func (m *DataMigrator) Migrate(ctx context.Context, params MigrationParams) error {
	m.logger.Info("Starting migration",
		zap.Uint64("startHeight", params.StartHeight),
		zap.Uint64("endHeight", params.EndHeight),
		zap.Bool("skipBlocks", params.SkipBlocks),
		zap.Bool("skipEvents", params.SkipEvents))

	startTime := time.Now()

	// Phase 1: Migrate block metadata FIRST (required for foreign key references)
	if !params.SkipBlocks {
		if err := m.migrateBlocksPerHeight(ctx, params); err != nil {
			return xerrors.Errorf("failed to migrate blocks: %w", err)
		}
	}

	// Phase 2: Migrate events AFTER blocks (depends on block metadata foreign keys)
	if !params.SkipEvents {
		if err := m.migrateEvents(ctx, params); err != nil {
			return xerrors.Errorf("failed to migrate events: %w", err)
		}
	}

	duration := time.Since(startTime)
	m.logger.Info("Migration completed successfully",
		zap.Duration("duration", duration),
		zap.Uint64("heightRange", params.EndHeight-params.StartHeight))

	return nil
}

func (m *DataMigrator) migrateBlocksPerHeight(ctx context.Context, params MigrationParams) error {
	m.logger.Info("Starting height-by-height block metadata migration with complete reorg support")

	totalHeights := params.EndHeight - params.StartHeight
	processedHeights := uint64(0)
	totalNonCanonicalBlocks := 0

	for height := params.StartHeight; height < params.EndHeight; height++ {
		nonCanonicalCount, err := m.migrateBlocksAtHeight(ctx, params, height)
		if err != nil {
			return xerrors.Errorf("failed to migrate blocks at height %d: %w", height, err)
		}

		totalNonCanonicalBlocks += nonCanonicalCount
		processedHeights++

		// Progress logging every 100 heights
		if processedHeights%100 == 0 {
			percentage := float64(processedHeights) / float64(totalHeights) * 100
			m.logger.Info("Block migration progress",
				zap.Uint64("processed", processedHeights),
				zap.Uint64("total", totalHeights),
				zap.Float64("percentage", percentage),
				zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks))
		}
	}

	m.logger.Info("Height-by-height block metadata migration completed",
		zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks))
	return nil
}

func (m *DataMigrator) migrateBlocksAtHeight(ctx context.Context, params MigrationParams, height uint64) (int, error) {
	blockPid := fmt.Sprintf("%d-%d", params.Tag, height)

	// Phase 1: Get and persist non-canonical blocks first
	// Query: BlockPid = "{tag}-{height}" AND BlockRid != "canonical"
	nonCanonicalBlocks, err := m.getNonCanonicalBlocksAtHeight(ctx, blockPid)
	if err != nil && !xerrors.Is(err, storage.ErrItemNotFound) {
		return 0, xerrors.Errorf("failed to get non-canonical blocks at height %d: %w", height, err)
	}

	nonCanonicalCount := len(nonCanonicalBlocks)
	if nonCanonicalCount > 0 {
		m.logger.Debug("Found non-canonical (reorg) blocks at height",
			zap.Uint64("height", height),
			zap.Int("count", nonCanonicalCount))

		// Persist non-canonical blocks FIRST
		err = m.destStorage.PersistBlockMetas(ctx, false, nonCanonicalBlocks, nil)
		if err != nil {
			return 0, xerrors.Errorf("failed to persist non-canonical blocks at height %d: %w", height, err)
		}
	}

	// Phase 2: Get and persist canonical block LAST
	// Query: BlockPid = "{tag}-{height}" AND BlockRid = "canonical"
	canonicalBlock, err := m.getCanonicalBlockAtHeight(ctx, blockPid)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			m.logger.Debug("No canonical block found at height", zap.Uint64("height", height))
			return nonCanonicalCount, nil
		}
		return 0, xerrors.Errorf("failed to get canonical block at height %d: %w", height, err)
	}

	m.logger.Debug("Found canonical block at height",
		zap.Uint64("height", height),
		zap.String("hash", canonicalBlock.Hash),
		zap.Int("reorgBlockCount", nonCanonicalCount))

	// Persist canonical block LAST - this ensures it becomes canonical in PostgreSQL
	err = m.destStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{canonicalBlock}, nil)
	if err != nil {
		return 0, xerrors.Errorf("failed to persist canonical block at height %d: %w", height, err)
	}

	return nonCanonicalCount, nil
}

func (m *DataMigrator) getNonCanonicalBlocksAtHeight(ctx context.Context, blockPid string) ([]*api.BlockMetadata, error) {
	// Query DynamoDB for ALL blocks at this height: BlockPid = blockPid
	// Then filter out the canonical one client-side
	input := &dynamodb.QueryInput{
		TableName:              awssdk.String(m.blockTable),
		KeyConditionExpression: awssdk.String("block_pid = :blockPid"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":blockPid": {
				S: awssdk.String(blockPid),
			},
		},
		ConsistentRead: awssdk.Bool(true),
	}

	result, err := m.dynamoClient.QueryWithContext(ctx, input)
	if err != nil {
		return nil, xerrors.Errorf("failed to query blocks at height: %w", err)
	}

	// Filter out canonical blocks client-side
	var nonCanonicalBlocks []*api.BlockMetadata
	for _, item := range result.Items {
		var blockEntry model.BlockMetaDataDDBEntry
		err := dynamodbattribute.UnmarshalMap(item, &blockEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal DynamoDB item: %w", err)
		}

		// Skip canonical blocks (BlockRid = "canonical")
		if blockEntry.BlockRid == "canonical" {
			continue
		}

		nonCanonicalBlocks = append(nonCanonicalBlocks, model.BlockMetadataToProto(&blockEntry))
	}

	return nonCanonicalBlocks, nil
}

func (m *DataMigrator) getCanonicalBlockAtHeight(ctx context.Context, blockPid string) (*api.BlockMetadata, error) {
	// Query DynamoDB directly: BlockPid = blockPid AND BlockRid = "canonical"
	input := &dynamodb.QueryInput{
		TableName:              awssdk.String(m.blockTable),
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

	result, err := m.dynamoClient.QueryWithContext(ctx, input)
	if err != nil {
		return nil, xerrors.Errorf("failed to query canonical block: %w", err)
	}

	if len(result.Items) == 0 {
		return nil, storage.ErrItemNotFound
	}

	if len(result.Items) > 1 {
		return nil, xerrors.Errorf("multiple canonical blocks found for %s", blockPid)
	}

	var blockEntry model.BlockMetaDataDDBEntry
	err = dynamodbattribute.UnmarshalMap(result.Items[0], &blockEntry)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal canonical block: %w", err)
	}

	return model.BlockMetadataToProto(&blockEntry), nil
}

func (m *DataMigrator) migrateEvents(ctx context.Context, params MigrationParams) error {
	m.logger.Info("Starting event ID-based migration")

	// Step 1: Get the first event ID at start height
	startEventId, err := m.sourceStorage.GetFirstEventIdByBlockHeight(ctx, params.EventTag, params.StartHeight)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			m.logger.Info("No events found at start height", zap.Uint64("startHeight", params.StartHeight))
			return nil
		}
		return xerrors.Errorf("failed to get first event ID at start height %d: %w", params.StartHeight, err)
	}

	// Step 2: Get events at end height - 1 to find the last event ID
	var endEventId int64
	if params.EndHeight > 0 {
		endHeightForEvents := params.EndHeight - 1
		eventsAtEndHeight, err := m.sourceStorage.GetEventsByBlockHeight(ctx, params.EventTag, endHeightForEvents)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				m.logger.Info("No events found at end height", zap.Uint64("endHeight", endHeightForEvents))
				endEventId = startEventId // Just migrate starting from start event ID
			} else {
				return xerrors.Errorf("failed to get events at end height %d: %w", endHeightForEvents, err)
			}
		} else {
			// Find the maximum event ID at end height
			endEventId = startEventId // fallback
			for _, event := range eventsAtEndHeight {
				if event.EventId > endEventId {
					endEventId = event.EventId
				}
			}
		}
	} else {
		endEventId = startEventId
	}

	m.logger.Info("Event ID range determined",
		zap.Int64("startEventId", startEventId),
		zap.Int64("endEventId", endEventId),
		zap.Int64("totalEvents", endEventId-startEventId+1))

	if endEventId < startEventId {
		m.logger.Info("No events to migrate (end event ID < start event ID)")
		return nil
	}

	// Step 3: Migrate events by event ID range in batches
	totalEvents := endEventId - startEventId + 1
	processedEvents := int64(0)
	batchSize := int64(params.BatchSize)

	for currentEventId := startEventId; currentEventId <= endEventId; currentEventId += batchSize {
		// Calculate the end of this batch
		batchEndEventId := currentEventId + batchSize - 1
		if batchEndEventId > endEventId {
			batchEndEventId = endEventId
		}

		// Get events in this range from DynamoDB
		sourceEvents, err := m.sourceStorage.GetEventsByEventIdRange(ctx, params.EventTag, currentEventId, batchEndEventId+1)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				m.logger.Debug("No events found in event ID range",
					zap.Int64("startEventId", currentEventId),
					zap.Int64("endEventId", batchEndEventId))
				processedEvents += batchEndEventId - currentEventId + 1
				continue
			}
			return xerrors.Errorf("failed to get events in range [%d, %d]: %w", currentEventId, batchEndEventId, err)
		}

		if len(sourceEvents) == 0 {
			processedEvents += batchEndEventId - currentEventId + 1
			continue
		}

		m.logger.Debug("Migrating event batch",
			zap.Int("count", len(sourceEvents)),
			zap.Int64("startEventId", currentEventId),
			zap.Int64("endEventId", batchEndEventId))

		// Migrate this batch to PostgreSQL
		err = m.destStorage.AddEventEntries(ctx, params.EventTag, sourceEvents)
		if err != nil {
			return xerrors.Errorf("failed to add events batch [%d, %d] to PostgreSQL: %w", currentEventId, batchEndEventId, err)
		}

		processedEvents += int64(len(sourceEvents))

		// Progress logging every 1000 events
		if processedEvents%1000 == 0 || processedEvents == totalEvents {
			percentage := float64(processedEvents) / float64(totalEvents) * 100
			m.logger.Info("Event migration progress",
				zap.Int64("processed", processedEvents),
				zap.Int64("total", totalEvents),
				zap.Float64("percentage", percentage))
		}
	}

	m.logger.Info("Event ID-based migration completed",
		zap.Int64("totalEventsMigrated", processedEvents))
	return nil
}

func init() {
	migrateCmd.Flags().Uint64Var(&migrateFlags.startHeight, "start-height", 0, "start block height (inclusive)")
	migrateCmd.Flags().Uint64Var(&migrateFlags.endHeight, "end-height", 0, "end block height (exclusive)")
	migrateCmd.Flags().Uint32Var(&migrateFlags.eventTag, "event-tag", 0, "event tag for migration")
	migrateCmd.Flags().Uint32Var(&migrateFlags.tag, "tag", 1, "block tag for migration")
	migrateCmd.Flags().IntVar(&migrateFlags.batchSize, "batch-size", 100, "number of blocks to process in each batch")
	migrateCmd.Flags().BoolVar(&migrateFlags.skipEvents, "skip-events", false, "skip event migration (blocks only)")
	migrateCmd.Flags().BoolVar(&migrateFlags.skipBlocks, "skip-blocks", false, "skip block migration (events only)")

	_ = migrateCmd.MarkFlagRequired("start-height")
	_ = migrateCmd.MarkFlagRequired("end-height")

	rootCmd.AddCommand(migrateCmd)
}
