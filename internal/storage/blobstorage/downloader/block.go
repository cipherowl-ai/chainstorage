package downloader

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	tracehttp "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/finalizer"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// StreamConsumer is invoked by DownloadStream after the compressed
// block has been spooled locally, decompressed, and unmarshaled into
// an api.Block. The consumer may read any chain-specific fields off
// the block (e.g. bitcoin blobdata) and pass them to a streaming
// parser.
//
// The download lifecycle (temp file, HTTP body) is released before
// DownloadStream returns. The consumer MUST NOT retain the *api.Block
// past return: downstream spool optimizations may replace the block
// with a lazily-populated view in a later phase.
type StreamConsumer func(ctx context.Context, block *api.Block) error

type (
	BlockDownloader interface {
		Download(ctx context.Context, blockFile *api.BlockFile) (*api.Block, error)

		// DownloadStream spools the compressed block to a local temp
		// file, decompresses + unmarshals into *api.Block, then
		// invokes consumer. The temp file is removed before return.
		//
		// This is chain-agnostic: consumer receives the standard
		// api.Block and picks out whatever chain-specific fields it
		// needs. Bitcoin-family callers typically extract
		// block.GetBitcoin().GetHeader() and hand it to
		// bitcoin.StreamingNativeParser.
		//
		// HTTP GET is retried under the same policy as Download.
		// Once consumer starts running, failures propagate; callers
		// wishing to retry retry the whole call.
		//
		// NOTE: This is Phase 1. The envelope is still fully
		// proto.Unmarshal'd into memory — resident peak is unchanged
		// from Download. The Phase 1 win is removing the separate
		// compressed-bytes in-memory buffer (disk-spool via io.Copy)
		// and giving callers a hook point for streaming parsers.
		// Phase 2 will replace proto.Unmarshal with a wire-format
		// walker that streams selected proto fields directly off disk
		// without materializing the full api.Block.
		DownloadStream(ctx context.Context, blockFile *api.BlockFile, consumer StreamConsumer) error
	}

	BlockDownloaderParams struct {
		fx.In
		fxparams.Params
		HttpClient HTTPClient
	}

	HTTPClient interface {
		Do(req *http.Request) (*http.Response, error)
	}

	blockDownloaderImpl struct {
		config     *config.Config
		logger     *zap.Logger
		httpClient HTTPClient
		retry      retry.RetryWithResult[*api.Block]
	}
)

const (
	timeout = time.Second * 30
)

func NewBlockDownloader(params BlockDownloaderParams) BlockDownloader {
	logger := log.WithPackage(params.Logger)
	return &blockDownloaderImpl{
		config:     params.Config,
		logger:     logger,
		httpClient: params.HttpClient,
		retry:      retry.NewWithResult[*api.Block](retry.WithLogger(logger)),
	}
}

func NewHTTPClient() HTTPClient {
	httpClient := &http.Client{
		Timeout: timeout,
	}
	httpClient = tracehttp.WrapClient(httpClient, tracehttp.RTWithResourceNamer(func(req *http.Request) string {
		return "/chainstorage/blobstorage/downloader"
	}))
	return httpClient
}

func (d *blockDownloaderImpl) Download(ctx context.Context, blockFile *api.BlockFile) (*api.Block, error) {
	if blockFile.Skipped {
		// No blob data is available when the block is skipped.
		return &api.Block{
			Blockchain: d.config.Chain.Blockchain,
			Network:    d.config.Chain.Network,
			Metadata: &api.BlockMetadata{
				Tag:     blockFile.Tag,
				Height:  blockFile.Height,
				Skipped: true,
			},
			Blobdata: nil,
		}, nil
	}

	defer d.logDuration(time.Now())
	return d.retry.Retry(ctx, func(ctx context.Context) (*api.Block, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, blockFile.FileUrl, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to create download request: %w", err)
		}

		httpResp, err := d.httpClient.Do(req)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("failed to download block file: %w", err))
		}

		finalizer := finalizer.WithCloser(httpResp.Body)
		defer finalizer.Finalize()

		if statusCode := httpResp.StatusCode; statusCode != http.StatusOK {
			if statusCode == http.StatusRequestTimeout ||
				statusCode == http.StatusTooManyRequests ||
				statusCode >= http.StatusInternalServerError {
				return nil, retry.Retryable(xerrors.Errorf("received %d status code: %w", statusCode, errors.ErrDownloadFailure))
			} else {
				return nil, xerrors.Errorf("received non-retryable %d status code: %w", statusCode, errors.ErrDownloadFailure)
			}
		}

		bodyBytes, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("failed to read body: %w", err))
		}

		block := new(api.Block)
		blockData, err := storage_utils.Decompress(bodyBytes, blockFile.Compression)
		if err != nil {
			return nil, xerrors.Errorf("failed to decompress block data with type %v: %w", blockFile.Compression.String(), err)
		}

		if err := proto.Unmarshal(blockData, block); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal file contents: %w", err)
		}

		return block, finalizer.Close()
	})
}

func (c *blockDownloaderImpl) logDuration(start time.Time) {
	c.logger.Debug(
		"downloader.request",
		zap.Duration("duration", time.Since(start)),
	)
}

// DownloadStream spools the compressed HTTP body to a local temp file,
// decompresses + unmarshals into *api.Block, then invokes consumer.
// The temp file is removed before DownloadStream returns.
func (d *blockDownloaderImpl) DownloadStream(ctx context.Context, blockFile *api.BlockFile, consumer StreamConsumer) error {
	if blockFile.Skipped {
		// Consistent with Download: no blob data is present.
		return consumer(ctx, &api.Block{
			Blockchain: d.config.Chain.Blockchain,
			Network:    d.config.Chain.Network,
			Metadata: &api.BlockMetadata{
				Tag:     blockFile.Tag,
				Height:  blockFile.Height,
				Skipped: true,
			},
			Blobdata: nil,
		})
	}

	defer d.logDuration(time.Now())

	tmp, err := os.CreateTemp("", "chainstorage-block-*.bin")
	if err != nil {
		return xerrors.Errorf("create temp spool: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if err := d.spoolToFile(ctx, blockFile, tmp); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return xerrors.Errorf("close temp spool: %w", err)
	}

	// Phase 1: decompress the full file, proto.Unmarshal. This still
	// materializes the api.Block in memory. See NOTE on the interface.
	rf, err := os.Open(tmpPath)
	if err != nil {
		return xerrors.Errorf("reopen temp spool: %w", err)
	}
	defer rf.Close()

	dec, err := storage_utils.DecompressReader(rf, blockFile.Compression)
	if err != nil {
		return xerrors.Errorf("wrap decompressor: %w", err)
	}
	defer dec.Close()

	blockData, err := ioutil.ReadAll(dec)
	if err != nil {
		return xerrors.Errorf("read decompressed: %w", err)
	}

	block := new(api.Block)
	if err := proto.Unmarshal(blockData, block); err != nil {
		return xerrors.Errorf("proto.Unmarshal: %v: %w", err, errors.ErrDownloadFailure)
	}
	// Release our local handle on the big decompressed buffer; api.Block
	// retains the slices it needs.
	blockData = nil

	return consumer(ctx, block)
}

// spoolToFile performs the (retryable) HTTP GET and streams the body to
// dst. The file is truncated + rewound on every retry attempt.
func (d *blockDownloaderImpl) spoolToFile(ctx context.Context, blockFile *api.BlockFile, dst *os.File) error {
	_, err := d.retry.Retry(ctx, func(ctx context.Context) (*api.Block, error) {
		if _, err := dst.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		if err := dst.Truncate(0); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, blockFile.FileUrl, nil)
		if err != nil {
			return nil, xerrors.Errorf("create request: %w", err)
		}

		resp, err := d.httpClient.Do(req)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("http do: %w", err))
		}
		closer := finalizer.WithCloser(resp.Body)
		defer closer.Finalize()

		if sc := resp.StatusCode; sc != http.StatusOK {
			if sc == http.StatusRequestTimeout || sc == http.StatusTooManyRequests || sc >= http.StatusInternalServerError {
				return nil, retry.Retryable(xerrors.Errorf("status %d: %w", sc, errors.ErrDownloadFailure))
			}
			return nil, xerrors.Errorf("status %d: %w", sc, errors.ErrDownloadFailure)
		}

		if _, err := io.Copy(dst, resp.Body); err != nil {
			return nil, retry.Retryable(xerrors.Errorf("spool body: %w", err))
		}
		return nil, closer.Close()
	})
	return err
}
