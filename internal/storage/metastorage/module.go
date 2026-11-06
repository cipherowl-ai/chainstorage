package metastorage

import (
	"context"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/firestore"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
)

type (
	MetaStorage                   = internal.MetaStorage
	BlockStorage                  = internal.BlockStorage
	EventStorage                  = internal.EventStorage
	TransactionStorage            = internal.TransactionStorage
	EventsToChainAdaptor          = internal.EventsToChainAdaptor
	BlockMetadataRecord           = internal.BlockMetadataRecord
	ConsolidationShadowPlacement  = internal.ConsolidationShadowPlacement
	ConsolidationShadowStats      = internal.ConsolidationShadowStats
	ConsolidationPromotionResult  = internal.ConsolidationPromotionResult
	SingleBlockUploadGuard        = internal.SingleBlockUploadGuard
	SingleBlockUploadGuardStorage = internal.SingleBlockUploadGuardStorage
)

const (
	EventIdStartValue = model.EventIdStartValue
	EventIdDeleted    = model.EventIdDeleted

	BatchConsolidatorAutoConsolidateCursor   = internal.BatchConsolidatorAutoConsolidateCursor
	SingleBlockRetentionFloorWatermarkCursor = internal.SingleBlockRetentionFloorWatermarkCursor
)

func NewUnfencedSingleBlockUploadGuard() SingleBlockUploadGuard {
	return internal.NewUnfencedSingleBlockUploadGuard()
}

func NewEventsToChainAdaptor() *EventsToChainAdaptor {
	return internal.NewEventsToChainAdaptor()
}

func GetTipEvent(ctx context.Context, storage EventStorage, eventTag uint32, maxEventId int64) (*model.EventEntry, error) {
	return internal.GetTipEvent(ctx, storage, eventTag, maxEventId)
}

func ValidateEvents(events []*model.EventEntry) error {
	return internal.ValidateEvents(events)
}

var Module = fx.Options(
	dynamodb.Module,
	firestore.Module,
	postgres.Module,
	fx.Provide(internal.WithMetaStorageFactory),
)
