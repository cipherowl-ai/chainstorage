package activity

import (
	"context"
	"os"
	"path/filepath"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BatchConsolidator struct {
		baseActivity
		statsActivity       baseActivity
		latestBlockActivity baseActivity
		planActivity        baseActivity
		config              *config.Config
		metaStorage         metastorage.MetaStorage
		blobStorage         blobstorage.BlobStorage
	}

	BatchConsolidatorParams struct {
		fx.In
		fxparams.Params
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
		BlobStorage blobstorage.BlobStorage
	}

	BatchConsolidatorRequest struct {
		Mode        config.ConsolidationMode `validate:"omitempty,oneof=shadow_dual_write historical_backfill"`
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64 `validate:"gtfield=StartHeight"`
		MaxBlocks   uint64 `validate:"required"`
	}

	BatchConsolidatorResponse struct {
		StartHeight        uint64
		EndHeight          uint64
		ScannedBlocks      uint64
		ConsolidatedBlocks uint64
		ObjectKey          string
	}

	BatchConsolidatorStatsRequest struct {
		Mode        config.ConsolidationMode `validate:"omitempty,oneof=shadow_dual_write historical_backfill"`
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64 `validate:"gtfield=StartHeight"`
	}

	BatchConsolidatorStatsResponse struct {
		StartHeight   uint64
		EndHeight     uint64
		ShadowObjects uint64
		ShadowBlocks  uint64
	}

	BatchConsolidatorLatestBlockRequest struct {
		Tag uint32
	}

	BatchConsolidatorLatestBlockResponse struct {
		Tag    uint32
		Height uint64
	}

	BatchConsolidatorPlanRequest struct {
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64 `validate:"gtfield=StartHeight"`
	}

	BatchConsolidatorPlanResponse struct {
		StartHeight         uint64
		EndHeight           uint64
		LatestHeight        uint64
		SafePromotionHeight uint64
		PromotionGateHeight uint64
	}
)

func NewBatchConsolidator(params BatchConsolidatorParams) *BatchConsolidator {
	a := &BatchConsolidator{
		baseActivity:        newBaseActivity(ActivityBatchConsolidator, params.Runtime),
		statsActivity:       newBaseActivity(ActivityBatchConsolidatorStats, params.Runtime),
		latestBlockActivity: newBaseActivity(ActivityBatchConsolidatorLatestBlock, params.Runtime),
		planActivity:        newBaseActivity(ActivityBatchConsolidatorPlan, params.Runtime),
		config:              params.Config,
		metaStorage:         params.MetaStorage,
		blobStorage:         params.BlobStorage,
	}
	a.register(a.execute)
	a.statsActivity.register(a.executeStats)
	a.latestBlockActivity.register(a.executeLatestBlock)
	a.planActivity.register(a.executePlan)
	return a
}

func (a *BatchConsolidator) Execute(ctx workflow.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	var response BatchConsolidatorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *BatchConsolidator) GetShadowStats(ctx workflow.Context, request *BatchConsolidatorStatsRequest) (*BatchConsolidatorStatsResponse, error) {
	var response BatchConsolidatorStatsResponse
	err := a.statsActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *BatchConsolidator) GetLatestBlock(ctx workflow.Context, request *BatchConsolidatorLatestBlockRequest) (*BatchConsolidatorLatestBlockResponse, error) {
	var response BatchConsolidatorLatestBlockResponse
	err := a.latestBlockActivity.executeActivity(ctx, request, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (a *BatchConsolidator) GetPromotionPlan(ctx workflow.Context, request *BatchConsolidatorPlanRequest) (*BatchConsolidatorPlanResponse, error) {
	var response BatchConsolidatorPlanResponse
	err := a.planActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *BatchConsolidator) executeStats(ctx context.Context, request *BatchConsolidatorStatsRequest) (*BatchConsolidatorStatsResponse, error) {
	if err := a.statsActivity.validateRequest(request); err != nil {
		return nil, err
	}
	if err := a.validateShadowStatsMode(request.Mode); err != nil {
		return nil, err
	}
	stats, err := a.getConsolidationShadowStats(ctx, request.Tag, request.StartHeight, request.EndHeight)
	if err != nil {
		return nil, err
	}
	return &BatchConsolidatorStatsResponse{
		StartHeight:   request.StartHeight,
		EndHeight:     request.EndHeight,
		ShadowObjects: stats.Objects,
		ShadowBlocks:  stats.Blocks,
	}, nil
}

func (a *BatchConsolidator) executeLatestBlock(ctx context.Context, request *BatchConsolidatorLatestBlockRequest) (*BatchConsolidatorLatestBlockResponse, error) {
	if err := a.latestBlockActivity.validateRequest(request); err != nil {
		return nil, err
	}
	latestBlock, err := a.metaStorage.GetLatestBlock(ctx, request.Tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block for historical backfill safety check: %w", err)
	}
	if latestBlock == nil {
		return nil, xerrors.New("latest block not found for historical backfill safety check")
	}
	return &BatchConsolidatorLatestBlockResponse{
		Tag:    request.Tag,
		Height: latestBlock.GetHeight(),
	}, nil
}

func (a *BatchConsolidator) executePlan(ctx context.Context, request *BatchConsolidatorPlanRequest) (*BatchConsolidatorPlanResponse, error) {
	if err := a.planActivity.validateRequest(request); err != nil {
		return nil, err
	}
	return a.planPromoteFinalized(ctx, request.Tag, request.StartHeight, request.EndHeight)
}

func (a *BatchConsolidator) execute(ctx context.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}
	mode := request.Mode
	if mode == "" {
		mode = a.config.AWS.Storage.Consolidation.Mode
	}
	switch mode {
	case config.ConsolidationModeShadowDualWrite, config.ConsolidationModeHistoricalBackfill:
		return a.executeShadowDualWrite(ctx, request)
	case config.ConsolidationModePromoteFinalized:
		return a.executePromoteFinalized(ctx, request)
	default:
		if err := a.validateConsolidationEnabled(); err != nil {
			return nil, err
		}
		return nil, xerrors.Errorf(
			"batch consolidator requires consolidation mode %q or %q, got %q",
			config.ConsolidationModeShadowDualWrite,
			config.ConsolidationModePromoteFinalized,
			mode,
		)
	}
}

func (a *BatchConsolidator) executeShadowDualWrite(ctx context.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	if err := a.validateShadowWriteMode(request.Mode); err != nil {
		return nil, err
	}
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.started")

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))
	records, err := a.metaStorage.GetBlocksMissingConsolidationShadow(ctx, request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks)
	if err != nil {
		return nil, xerrors.Errorf("failed to scan missing consolidation shadows: %w", err)
	}
	if len(records) == 0 {
		return &BatchConsolidatorResponse{
			StartHeight: request.StartHeight,
			EndHeight:   request.EndHeight,
		}, nil
	}

	payloads, recordsByID, cleanup, err := a.buildPayloads(ctx, records)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.payloads_built", len(records))

	uploadCtx := blobstorage.WithConsolidatedUploadProgress(ctx, func(stage string, details ...any) {
		heartbeatDetails := append([]any{"batch_consolidator." + stage}, details...)
		sdkactivity.RecordHeartbeat(ctx, heartbeatDetails...)
	})
	objectKey, placements, err := a.blobStorage.UploadConsolidated(uploadCtx, payloads)
	if err != nil {
		return nil, xerrors.Errorf("failed to upload consolidated block object: %w", err)
	}
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.object_uploaded", objectKey, len(placements))
	shadowPlacements, err := makeShadowPlacements(recordsByID, objectKey, placements)
	if err != nil {
		return nil, err
	}
	if err := a.metaStorage.PersistBlockConsolidationShadows(ctx, shadowPlacements); err != nil {
		return nil, xerrors.Errorf("failed to persist consolidation shadow placements: %w", err)
	}
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.shadows_persisted", objectKey, len(shadowPlacements))

	response := &BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          request.EndHeight,
		ScannedBlocks:      uint64(len(records)),
		ConsolidatedBlocks: uint64(len(shadowPlacements)),
		ObjectKey:          objectKey,
	}
	logger.Info(
		"consolidated shadow blocks",
		zap.Int("scanned_blocks", len(records)),
		zap.Int("consolidated_blocks", len(shadowPlacements)),
		zap.String("object_key", objectKey),
	)
	return response, nil
}

func (a *BatchConsolidator) executePromoteFinalized(ctx context.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	if err := a.validatePromoteFinalizedMode(); err != nil {
		return nil, err
	}
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.promote_finalized.started")

	plan, err := a.planPromoteFinalized(ctx, request.Tag, request.StartHeight, request.EndHeight)
	if err != nil {
		return nil, err
	}
	if plan.EndHeight <= request.StartHeight {
		return &BatchConsolidatorResponse{
			StartHeight: request.StartHeight,
			EndHeight:   request.StartHeight,
		}, nil
	}
	result, err := a.metaStorage.PromoteBlockConsolidationShadows(
		ctx,
		request.Tag,
		request.StartHeight,
		plan.EndHeight,
		request.MaxBlocks,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to promote consolidation shadows: %w", err)
	}
	promotedBlocks := uint64(0)
	if result != nil {
		promotedBlocks = result.Blocks
	}
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.promote_finalized.promoted", promotedBlocks)
	a.getLogger(ctx).Info(
		"promoted finalized consolidation shadows",
		zap.Reflect("request", request),
		zap.Uint64("promoted_blocks", promotedBlocks),
	)
	return &BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          plan.EndHeight,
		ScannedBlocks:      promotedBlocks,
		ConsolidatedBlocks: promotedBlocks,
	}, nil
}

func (a *BatchConsolidator) planPromoteFinalized(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) (*BatchConsolidatorPlanResponse, error) {
	if err := a.validatePromoteFinalizedMode(); err != nil {
		return nil, err
	}
	latest, err := a.metaStorage.GetLatestBlock(ctx, tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block for consolidation promotion: %w", err)
	}

	consolidation := a.config.AWS.Storage.Consolidation
	safeLag := *consolidation.SafePromotionLag
	gateHeight := *consolidation.PromotionGateHeight
	safeEnd, safeHeight, ok := promotionSafeEndHeight(latest.GetHeight(), safeLag)
	if !ok {
		return &BatchConsolidatorPlanResponse{
			StartHeight:         startHeight,
			EndHeight:           startHeight,
			LatestHeight:        latest.GetHeight(),
			SafePromotionHeight: 0,
			PromotionGateHeight: gateHeight,
		}, nil
	}

	effectiveEndHeight := endHeight
	if gateHeight < effectiveEndHeight {
		effectiveEndHeight = gateHeight
	}
	if safeEnd < effectiveEndHeight {
		effectiveEndHeight = safeEnd
	}
	if effectiveEndHeight < startHeight {
		effectiveEndHeight = startHeight
	}

	return &BatchConsolidatorPlanResponse{
		StartHeight:         startHeight,
		EndHeight:           effectiveEndHeight,
		LatestHeight:        latest.GetHeight(),
		SafePromotionHeight: safeHeight,
		PromotionGateHeight: gateHeight,
	}, nil
}

func (a *BatchConsolidator) getConsolidationShadowStats(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
) (*metastorage.ConsolidationShadowStats, error) {
	stats, err := a.metaStorage.GetBlockConsolidationShadowStats(ctx, tag, startHeight, endHeight)
	if err != nil {
		return nil, xerrors.Errorf("failed to get consolidation shadow stats: %w", err)
	}
	if stats == nil {
		return &metastorage.ConsolidationShadowStats{}, nil
	}
	return stats, nil
}

func (a *BatchConsolidator) validateConsolidationEnabled() error {
	consolidation := a.config.AWS.Storage.Consolidation
	if !consolidation.Enabled {
		return xerrors.New("batch consolidator requires aws.storage.consolidation.enabled=true")
	}
	return nil
}

func (a *BatchConsolidator) validateShadowWriteMode(mode config.ConsolidationMode) error {
	if err := a.validateConsolidationEnabled(); err != nil {
		return err
	}
	if mode == "" {
		mode = a.config.AWS.Storage.Consolidation.Mode
	}
	switch mode {
	case config.ConsolidationModeShadowDualWrite, config.ConsolidationModeHistoricalBackfill:
		return nil
	default:
		return xerrors.Errorf(
			"batch consolidator requires consolidation mode %q or %q, got %q",
			config.ConsolidationModeShadowDualWrite,
			config.ConsolidationModeHistoricalBackfill,
			mode,
		)
	}
}

func (a *BatchConsolidator) validateShadowStatsMode(mode config.ConsolidationMode) error {
	return a.validateShadowWriteMode(mode)
}

func (a *BatchConsolidator) validatePromoteFinalizedMode() error {
	if err := a.validateConsolidationEnabled(); err != nil {
		return err
	}
	consolidation := a.config.AWS.Storage.Consolidation
	if consolidation.Mode != config.ConsolidationModePromoteFinalized {
		return xerrors.Errorf("batch consolidator requires consolidation mode %q, got %q", config.ConsolidationModePromoteFinalized, consolidation.Mode)
	}
	if consolidation.PromotionGateHeight == nil {
		return xerrors.New("batch consolidator promote_finalized requires promotion_gate_height")
	}
	if consolidation.SafePromotionLag == nil {
		return xerrors.New("batch consolidator promote_finalized requires safe_promotion_lag")
	}
	return nil
}

func promotionSafeEndHeight(latestHeight uint64, safePromotionLag uint64) (uint64, uint64, bool) {
	if latestHeight < safePromotionLag {
		return 0, 0, false
	}
	safeHeight := latestHeight - safePromotionLag
	if safeHeight == ^uint64(0) {
		return safeHeight, safeHeight, true
	}
	return safeHeight + 1, safeHeight, true
}

func (a *BatchConsolidator) buildPayloads(
	ctx context.Context,
	records []*metastorage.BlockMetadataRecord,
) ([]blobstorage.ConsolidatedBlockPayload, map[int64]*api.BlockMetadata, func(), error) {
	payloads := make([]blobstorage.ConsolidatedBlockPayload, len(records))
	recordsByID := make(map[int64]*api.BlockMetadata, len(records))
	for i, record := range records {
		if record == nil || record.Metadata == nil {
			return nil, nil, nil, xerrors.New("missing block metadata record")
		}
		recordsByID[record.ID] = record.Metadata
		payloads[i].MetadataID = record.ID
		payloads[i].Metadata = record.Metadata
	}

	tempFiles := make([]string, len(records))
	cleanup := func() {
		for _, path := range tempFiles {
			if path != "" {
				_ = os.Remove(path)
			}
		}
	}

	parallelism := int(a.config.AWS.Storage.Consolidation.MaxInflightRawBlocks)
	if parallelism <= 0 {
		parallelism = 1
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(parallelism)
	for i := range records {
		i := i
		group.Go(func() error {
			record := records[i]
			metadata := record.Metadata
			block, err := a.blobStorage.Download(groupCtx, metadata)
			if err != nil {
				return xerrors.Errorf("failed to download legacy block (height=%d, hash=%s): %w", metadata.GetHeight(), metadata.GetHash(), err)
			}
			if err := validateDownloadedBlock(metadata, block); err != nil {
				return err
			}
			rawBlockPayload, err := proto.Marshal(block)
			if err != nil {
				return xerrors.Errorf("failed to marshal block (height=%d, hash=%s): %w", metadata.GetHeight(), metadata.GetHash(), err)
			}
			source, path, length, err := writeRawBlockPayloadTempFile(rawBlockPayload, a.config.AWS.Storage.Consolidation.LocalSpillDir)
			if err != nil {
				return xerrors.Errorf("failed to stage raw block payload (height=%d, hash=%s): %w", metadata.GetHeight(), metadata.GetHash(), err)
			}
			tempFiles[i] = path
			payloads[i].RawBlockPayload = source
			payloads[i].UncompressedLength = length
			sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.block_downloaded", metadata.GetHeight())
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		cleanup()
		return nil, nil, nil, err
	}
	for _, payload := range payloads {
		if payload.RawBlockPayload == nil {
			cleanup()
			return nil, nil, nil, xerrors.Errorf("missing consolidated payload for metadata_id=%d", payload.MetadataID)
		}
	}
	return payloads, recordsByID, cleanup, nil
}

func writeRawBlockPayloadTempFile(raw []byte, dir string) (blobstorage.PayloadSource, string, uint64, error) {
	if len(raw) == 0 {
		return nil, "", 0, xerrors.New("raw block payload cannot be empty")
	}
	spillDir := os.TempDir()
	if dir != "" {
		spillDir = filepath.Clean(dir)
	}
	if err := os.MkdirAll(spillDir, 0o700); err != nil {
		return nil, "", 0, err
	}
	file, err := os.CreateTemp(spillDir, "chainstorage-cscb-raw-block-*.pb")
	if err != nil {
		return nil, "", 0, err
	}
	path := file.Name()
	success := false
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
		if !success {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(raw); err != nil {
		return nil, "", 0, err
	}
	if err := file.Close(); err != nil {
		return nil, "", 0, err
	}
	closed = true
	success = true
	length := uint64(len(raw))
	return blobstorage.NewFilePayloadSource(path, length), path, length, nil
}

func validateDownloadedBlock(expected *api.BlockMetadata, block *api.Block) error {
	if block == nil || block.Metadata == nil {
		return xerrors.Errorf("downloaded block is missing metadata (height=%d, hash=%s)", expected.GetHeight(), expected.GetHash())
	}
	actual := block.Metadata
	if actual.GetTag() != expected.GetTag() || actual.GetHeight() != expected.GetHeight() || actual.GetHash() != expected.GetHash() {
		return xerrors.Errorf(
			"downloaded block metadata mismatch (expected tag=%d height=%d hash=%s, got tag=%d height=%d hash=%s)",
			expected.GetTag(),
			expected.GetHeight(),
			expected.GetHash(),
			actual.GetTag(),
			actual.GetHeight(),
			actual.GetHash(),
		)
	}
	return nil
}

func makeShadowPlacements(
	recordsByID map[int64]*api.BlockMetadata,
	objectKey string,
	placements []blobstorage.BlockPlacement,
) ([]*metastorage.ConsolidationShadowPlacement, error) {
	shadowPlacements := make([]*metastorage.ConsolidationShadowPlacement, 0, len(placements))
	for _, placement := range placements {
		metadata, ok := recordsByID[placement.MetadataID]
		if !ok {
			return nil, xerrors.Errorf("missing source metadata for placement metadata_id=%d", placement.MetadataID)
		}
		if placement.Height != metadata.GetHeight() || placement.Hash != metadata.GetHash() {
			return nil, xerrors.Errorf(
				"placement metadata mismatch (metadata_id=%d expected height=%d hash=%s, got height=%d hash=%s)",
				placement.MetadataID,
				metadata.GetHeight(),
				metadata.GetHash(),
				placement.Height,
				placement.Hash,
			)
		}
		shadowPlacements = append(shadowPlacements, &metastorage.ConsolidationShadowPlacement{
			BlockMetadataID:           placement.MetadataID,
			Tag:                       metadata.GetTag(),
			Height:                    metadata.GetHeight(),
			Hash:                      metadata.GetHash(),
			LegacyObjectKeyMain:       metadata.GetObjectKeyMain(),
			ConsolidatedObjectKeyMain: objectKey,
			ObjectFormat:              placement.ObjectFormat,
			ByteOffset:                placement.ByteOffset,
			ByteLength:                placement.ByteLength,
			UncompressedLength:        placement.UncompressedLength,
		})
	}
	return shadowPlacements, nil
}
