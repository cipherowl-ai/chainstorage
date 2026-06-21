package activity

import (
	"context"
	"os"
	"path/filepath"
	"time"

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

const batchConsolidatorHeartbeatInterval = 10 * time.Second

type (
	BatchConsolidator struct {
		baseActivity
		config      *config.Config
		metaStorage metastorage.MetaStorage
		blobStorage blobstorage.BlobStorage
	}

	BatchConsolidatorParams struct {
		fx.In
		fxparams.Params
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
		BlobStorage blobstorage.BlobStorage
	}

	BatchConsolidatorRequest struct {
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
)

func NewBatchConsolidator(params BatchConsolidatorParams) *BatchConsolidator {
	a := &BatchConsolidator{
		baseActivity: newBaseActivity(ActivityBatchConsolidator, params.Runtime),
		config:       params.Config,
		metaStorage:  params.MetaStorage,
		blobStorage:  params.BlobStorage,
	}
	a.register(a.execute)
	return a
}

func (a *BatchConsolidator) Execute(ctx workflow.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	var response BatchConsolidatorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *BatchConsolidator) execute(ctx context.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}
	if err := a.validateConsolidationMode(); err != nil {
		return nil, err
	}
	stopHeartbeat := startBatchConsolidatorHeartbeat(ctx)
	defer stopHeartbeat()

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

	objectKey, placements, err := a.blobStorage.UploadConsolidated(ctx, payloads)
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

func startBatchConsolidatorHeartbeat(ctx context.Context) func() {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	sdkactivity.RecordHeartbeat(heartbeatCtx, "batch_consolidator.started")
	go func() {
		defer close(done)
		ticker := time.NewTicker(batchConsolidatorHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				sdkactivity.RecordHeartbeat(heartbeatCtx, "batch_consolidator.alive")
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (a *BatchConsolidator) validateConsolidationMode() error {
	consolidation := a.config.AWS.Storage.Consolidation
	if !consolidation.Enabled {
		return xerrors.New("batch consolidator requires aws.storage.consolidation.enabled=true")
	}
	if consolidation.Mode != config.ConsolidationModeShadowDualWrite {
		return xerrors.Errorf("batch consolidator requires consolidation mode %q, got %q", config.ConsolidationModeShadowDualWrite, consolidation.Mode)
	}
	return nil
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
