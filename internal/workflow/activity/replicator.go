package activity

import (
	"context"
	"io"
	"net/http"
	"time"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	tracehttp "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/finalizer"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	timeout = time.Second * 30
)

var (
	ErrDownloadFailure = xerrors.New("download failure")
)

type (
	Replicator struct {
		baseActivity
		config          *config.Config
		logger          *zap.Logger
		client          gateway.Client
		blockDownloader downloader.BlockDownloader
		metaStorage     metastorage.MetaStorage
		blobStorage     blobstorage.BlobStorage
		retry           retry.RetryWithResult[[]byte]
		httpClient      *http.Client
	}

	ReplicatorParams struct {
		fx.In
		fxparams.Params
		Runtime         cadence.Runtime
		Client          gateway.Client
		BlockDownloader downloader.BlockDownloader
		MetaStorage     metastorage.MetaStorage
		BlobStorage     blobstorage.BlobStorage
	}

	ReplicatorRequest struct {
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64
		Parallelism int
		Compression api.Compression
	}

	ReplicatorResponse struct {
		StartHeight uint64
		EndHeight   uint64
	}
)

func NewReplicator(params ReplicatorParams) *Replicator {
	httpClient := &http.Client{
		Timeout: timeout,
	}
	httpClient = tracehttp.WrapClient(httpClient, tracehttp.RTWithResourceNamer(func(req *http.Request) string {
		return "/workflow/activity/replicator"
	}))
	a := &Replicator{
		baseActivity:    newBaseActivity(ActivityReplicator, params.Runtime),
		config:          params.Config,
		logger:          params.Logger,
		client:          params.Client,
		blockDownloader: params.BlockDownloader,
		metaStorage:     params.MetaStorage,
		blobStorage:     params.BlobStorage,
		httpClient:      httpClient,
		retry:           retry.NewWithResult[[]byte](retry.WithLogger(params.Logger)),
	}
	a.register(a.execute)
	return a
}

func (a *Replicator) Execute(ctx workflow.Context, request *ReplicatorRequest) (*ReplicatorResponse, error) {
	var response ReplicatorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Replicator) downloadBlockData(ctx context.Context, url string) ([]byte, error) {
	return a.retry.Retry(ctx, func(ctx context.Context) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to create download request: %w", err)
		}

		httpResp, err := a.httpClient.Do(req)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("failed to download block file: %w", err))
		}

		finalizer := finalizer.WithCloser(httpResp.Body)
		defer finalizer.Finalize()

		if statusCode := httpResp.StatusCode; statusCode != http.StatusOK {
			if statusCode == http.StatusRequestTimeout ||
				statusCode == http.StatusTooManyRequests ||
				statusCode >= http.StatusInternalServerError {
				return nil, retry.Retryable(xerrors.Errorf("received %d status code: %w", statusCode, ErrDownloadFailure))
			} else {
				return nil, xerrors.Errorf("received non-retryable %d status code: %w", statusCode, ErrDownloadFailure)
			}
		}

		bodyBytes, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("failed to read body: %w", err))
		}
		return bodyBytes, finalizer.Close()
	})
}
func (a *Replicator) prepareRawBlockData(ctx context.Context, blockFile *api.BlockFile, compression api.Compression) (*blobstorage.RawBlockData, error) {
	bodyBytes, err := a.downloadBlockData(ctx, blockFile.FileUrl)
	if err != nil {
		return nil, err
	}
	var rawBytes []byte
	var compressedBytes []byte
	switch blockFile.Compression {
	case api.Compression_NONE:
		rawBytes = bodyBytes
		if compression == api.Compression_GZIP {
			compressedBytes, err = storage_utils.Compress(rawBytes, compression)
			if err != nil {
				return nil, xerrors.Errorf("failed to compress block data with type %v: %w", compression.String(), err)
			}
		}
	case api.Compression_GZIP:
		compressedBytes = bodyBytes
		if compression == api.Compression_NONE {
			rawBytes, err = storage_utils.Decompress(rawBytes, blockFile.Compression)
			if err != nil {
				return nil, xerrors.Errorf("failed to decompress block data with type %v: %w", blockFile.Compression.String(), err)
			}
		}
	default:
		return nil, xerrors.Errorf("unknown block file compression type %v", blockFile.Compression.String())
	}
	metadata := &api.BlockMetadata{
		Tag:          blockFile.Tag,
		Hash:         blockFile.Hash,
		ParentHash:   blockFile.ParentHash,
		Height:       blockFile.Height,
		ParentHeight: blockFile.ParentHeight,
		Skipped:      blockFile.Skipped,
		Timestamp:    blockFile.BlockTimestamp,
	}
	// if block file is coming from old chainstorage api, the block timestamp is not set
	// we need to extract it from the block data
	// TODO remove this after the api upgrade
	if metadata.Timestamp == nil || (metadata.Timestamp.Nanos == 0 && metadata.Timestamp.Seconds == 0) {
		block := new(api.Block)
		rawBytes := rawBytes
		if len(rawBytes) == 0 {
			rawBytes, err = storage_utils.Decompress(bodyBytes, blockFile.Compression)
			if err != nil {
				return nil, xerrors.Errorf("failed to decompress block data with type %v: %w", blockFile.Compression.String(), err)
			}
		}

		if err := proto.Unmarshal(rawBytes, block); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal file contents: %w", err)
		}
		blockFile.BlockTimestamp = block.Metadata.Timestamp
	}
	rawBlockData := &blobstorage.RawBlockData{
		Blockchain:           a.config.Chain.Blockchain,
		Network:              a.config.Chain.Network,
		SideChain:            a.config.Chain.Sidechain,
		BlockMetadata:        metadata,
		BlockDataCompression: compression,
	}
	switch compression {
	case api.Compression_NONE:
		rawBlockData.BlockData = rawBytes
		return rawBlockData, nil
	case api.Compression_GZIP:
		rawBlockData.BlockData = compressedBytes
		return rawBlockData, nil
	default:
		return nil, xerrors.Errorf("unknown compression type %v", compression.String())
	}
}

func (a *Replicator) execute(ctx context.Context, request *ReplicatorRequest) (*ReplicatorResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}
	logger := a.getLogger(ctx).With(zap.Reflect("request", request))
	logger.Info("Fetching block range",
		zap.Uint64("startHeight", request.StartHeight),
		zap.Uint64("endHeight", request.EndHeight))
	blocks, err := a.client.GetBlockFilesByRange(ctx, &api.GetBlockFilesByRangeRequest{
		Tag:         request.Tag,
		StartHeight: request.StartHeight,
		EndHeight:   request.EndHeight,
	})
	if err != nil {
		return nil, err
	}
	blockMetas := make([]*api.BlockMetadata, len(blocks.Files))
	logger.Info("Replicating block data")
	group, errgroupCtx := errgroup.WithContext(ctx)
	group.SetLimit(request.Parallelism)
	for i := range blocks.Files {
		i := i
		group.Go(func() error {
			blockFile := blocks.Files[i]
			logger.Debug(
				"downloading block",
				zap.Uint32("tag", blockFile.Tag),
				zap.Uint64("height", blockFile.Height),
				zap.String("hash", blockFile.Hash),
			)
			rawBlockData, err := a.prepareRawBlockData(errgroupCtx, blockFile, request.Compression)
			if err != nil {
				return xerrors.Errorf("failed to prepare raw block data: %w", err)
			}
			objectKeyMain, err := a.blobStorage.UploadRaw(errgroupCtx, rawBlockData)
			if err != nil {
				return xerrors.Errorf("failed to upload raw block file: %w", err)
			}
			blockMetas[i] = rawBlockData.BlockMetadata
			blockMetas[i].ObjectKeyMain = objectKeyMain
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, xerrors.Errorf("failed to replicate block files: %w", err)
	}
	logger.Info("Persisting block metadata")
	err = a.metaStorage.PersistBlockMetas(ctx, false, blockMetas, nil)
	if err != nil {
		return nil, err
	}

	return &ReplicatorResponse{
		StartHeight: request.StartHeight,
		EndHeight:   request.EndHeight,
	}, nil
}
