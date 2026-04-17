package downloader

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"sync"
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

type (
	BlockDownloader interface {
		Download(ctx context.Context, blockFile *api.BlockFile) (*api.Block, error)

		// DownloadStream spools the compressed block to a local temp
		// file, decompresses to a second temp file, and returns a
		// chain-agnostic SpooledBlock handle. The decompressed spool
		// is held open until SpooledBlock.Close() is called — callers
		// MUST close it to release disk.
		//
		// This is plumbing only: no proto parsing happens at the
		// downloader layer. Chain-specific walkers live in the
		// parser package, which consumes the SpooledBlock to produce
		// a StreamedBlock with chain-aware lazy accessors.
		//
		// A runtime cleanup is wired as a safety net for leaks but
		// runs non-deterministically; do not rely on it.
		DownloadStream(ctx context.Context, blockFile *api.BlockFile) (*SpooledBlock, error)
	}

	// SpooledBlock is a chain-agnostic handle over the decompressed
	// bytes of a block, backed by a local temp file. The parser
	// package consumes it and produces a chain-specific
	// StreamedBlock with lazy accessors.
	//
	// Open may be called multiple times; each call returns an
	// independent io.ReadCloser positioned at byte 0 of the
	// decompressed stream. The caller is responsible for closing
	// every returned reader.
	//
	// Close removes the backing temp file and is safe to call
	// multiple times.
	SpooledBlock struct {
		// BlockFile is the source descriptor (chain, tag, height,
		// hash, compression). Parser implementations branch on
		// BlockFile.Skipped, among other fields.
		BlockFile *api.BlockFile

		// Open returns a fresh read-only handle on the decompressed
		// spool file. Never nil for non-skipped blocks; for skipped
		// blocks Open returns an empty reader so callers can treat
		// the API uniformly.
		Open func() (io.ReadCloser, error)

		closeOnce sync.Once
		closeFn   func() error
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

// Close releases the locally-spooled decompressed file. Safe to call
// multiple times; after Close, Open returns a file-not-found error.
func (s *SpooledBlock) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		if s.closeFn != nil {
			err = s.closeFn()
		}
	})
	return err
}

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

// DownloadStream spools the compressed blob to disk, decompresses to a
// second spool file, and returns a chain-agnostic SpooledBlock handle.
// The decompressed spool persists until SpooledBlock.Close() is called —
// callers MUST close it.
//
// For skipped blocks, the returned SpooledBlock has an Open() that
// returns an empty reader and a no-op Close; the parser detects
// skipped blocks via BlockFile.Skipped.
func (d *blockDownloaderImpl) DownloadStream(ctx context.Context, blockFile *api.BlockFile) (*SpooledBlock, error) {
	if blockFile.Skipped {
		return &SpooledBlock{
			BlockFile: blockFile,
			Open: func() (io.ReadCloser, error) {
				return io.NopCloser(io.LimitReader(nil, 0)), nil
			},
			closeFn: func() error { return nil },
		}, nil
	}

	defer d.logDuration(time.Now())

	// Spool #1: compressed body → tempfile1.
	tmpCompressed, err := os.CreateTemp("", "chainstorage-block-compressed-*.bin")
	if err != nil {
		return nil, xerrors.Errorf("create compressed spool: %w", err)
	}
	compressedPath := tmpCompressed.Name()
	// Compressed spool is scoped to this function — remove no matter what.
	defer os.Remove(compressedPath)

	if err := d.spoolToFile(ctx, blockFile, tmpCompressed); err != nil {
		tmpCompressed.Close()
		return nil, err
	}
	if err := tmpCompressed.Close(); err != nil {
		return nil, xerrors.Errorf("close compressed spool: %w", err)
	}

	// Spool #2: decompressed bytes → tempfile2. This spool outlives
	// the call — ownership passes to the returned SpooledBlock.
	tmpDecompressed, err := os.CreateTemp("", "chainstorage-block-decompressed-*.bin")
	if err != nil {
		return nil, xerrors.Errorf("create decompressed spool: %w", err)
	}
	decompressedPath := tmpDecompressed.Name()

	// If we fail before handing the spool over to the caller, remove it here.
	transferred := false
	defer func() {
		if !transferred {
			os.Remove(decompressedPath)
		}
	}()

	compressedFile, err := os.Open(compressedPath)
	if err != nil {
		tmpDecompressed.Close()
		return nil, xerrors.Errorf("reopen compressed spool: %w", err)
	}

	dec, err := storage_utils.DecompressReader(compressedFile, blockFile.Compression)
	if err != nil {
		compressedFile.Close()
		tmpDecompressed.Close()
		return nil, xerrors.Errorf("wrap decompressor: %w", err)
	}

	if _, err := io.Copy(tmpDecompressed, dec); err != nil {
		dec.Close()
		compressedFile.Close()
		tmpDecompressed.Close()
		return nil, xerrors.Errorf("decompress to spool: %v: %w", err, errors.ErrDownloadFailure)
	}
	dec.Close()
	compressedFile.Close()
	if err := tmpDecompressed.Sync(); err != nil {
		tmpDecompressed.Close()
		return nil, xerrors.Errorf("sync decompressed spool: %w", err)
	}
	if err := tmpDecompressed.Close(); err != nil {
		return nil, xerrors.Errorf("close decompressed spool: %w", err)
	}

	stream := &SpooledBlock{
		BlockFile: blockFile,
		Open: func() (io.ReadCloser, error) {
			f, err := os.Open(decompressedPath)
			if err != nil {
				return nil, xerrors.Errorf("open decompressed spool: %w", err)
			}
			return f, nil
		},
		closeFn: func() error {
			if err := os.Remove(decompressedPath); err != nil && !os.IsNotExist(err) {
				return xerrors.Errorf("remove decompressed spool: %w", err)
			}
			return nil
		},
	}

	// Safety net: if the caller leaks the stream, GC will eventually
	// clean up the spool file. Non-deterministic — callers must still
	// explicitly Close().
	runtime.AddCleanup(stream, func(path string) {
		_ = os.Remove(path)
	}, decompressedPath)

	transferred = true
	return stream, nil
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
