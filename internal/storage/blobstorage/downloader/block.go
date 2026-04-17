package downloader

import (
	"bytes"
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
		// file, decompresses + unmarshals into *api.Block via the
		// proto wire-walker, and returns the fully-materialized block.
		// The temp file is removed before return — caller has no
		// cleanup responsibility.
		//
		// Memory profile: roughly 1× block size resident during
		// decode, vs ~2× for Download (which does
		// ioutil.ReadAll + proto.Unmarshal). The returned block has
		// identical shape to what Download returns.
		//
		// For bitcoin-family blocks with multi-GB InputTransactions,
		// use DownloadStreamBitcoin instead — it leaves Header and
		// InputTransactions on disk and delivers them via a
		// seek-based loader.
		DownloadStream(ctx context.Context, blockFile *api.BlockFile) (*api.Block, error)

		// DownloadStreamBitcoin is the low-peak-RAM path for
		// bitcoin-family blocks. It spools compressed bytes to disk,
		// streams the decompressed bytes through a proto wire-walker
		// that materializes everything except BitcoinBlobdata.Header
		// and BitcoinBlobdata.InputTransactions, and returns a
		// BitcoinStream handle exposing those fields via seek-based
		// loaders.
		//
		// The returned *BitcoinStream owns a decompressed spool file
		// on disk. Callers MUST call Close() when done to remove it.
		// A runtime cleanup is wired as a safety net for leaks, but
		// it runs non-deterministically and should not be relied on
		// in production.
		//
		// Peak RAM drops from ~2× block size (DownloadStream) to
		// ~metadata + one-tx-group scratch, paid for by ~1× block
		// size on disk.
		DownloadStreamBitcoin(ctx context.Context, blockFile *api.BlockFile) (*BitcoinStream, error)
	}

	// BitcoinStream is a pull-based handle over a bitcoin-family
	// block whose Header and InputTransactions bytes live in a
	// locally-spooled decompressed file. Callers stream fields on
	// demand via OpenHeaderReader / LoadInputTxGroup and release the
	// spool by calling Close.
	//
	// Zero-alloc fields (Block metadata, non-bitcoin fields) are
	// populated eagerly; the lazy fields are BitcoinBlobdata.Header
	// (nil — open via OpenHeaderReader) and
	// BitcoinBlobdata.InputTransactions (nil — load i-th group via
	// LoadInputTxGroup).
	//
	// Not safe for concurrent use across goroutines.
	BitcoinStream struct {
		// Block is a partial api.Block: Header and InputTransactions
		// on its BitcoinBlobdata are left nil. All other fields are
		// populated normally.
		Block *api.Block

		// OpenHeaderReader returns a fresh io.ReadCloser over the
		// BitcoinBlobdata.Header JSON bytes. May be called multiple
		// times; each call returns an independent handle on the
		// spool file.
		OpenHeaderReader func() (io.ReadCloser, error)

		// LoadInputTxGroup returns the i-th entry of
		// BitcoinBlobdata.InputTransactions on demand. Returns (nil,
		// nil) for out-of-range i or for skipped blocks.
		LoadInputTxGroup func(i int) (*api.RepeatedBytes, error)

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

// Close releases the locally-spooled decompressed file that backs
// OpenHeaderReader and LoadInputTxGroup. Safe to call multiple times.
// After Close, further calls to the loaders may fail with a
// file-not-found error.
func (s *BitcoinStream) Close() error {
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

// DownloadStream spools the compressed HTTP body to a local temp file,
// wire-walks the decompressed stream into *api.Block, removes the temp
// file, and returns the fully-materialized block. No caller cleanup.
func (d *blockDownloaderImpl) DownloadStream(ctx context.Context, blockFile *api.BlockFile) (*api.Block, error) {
	if blockFile.Skipped {
		// Consistent with Download: no blob data is present.
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

	tmp, err := os.CreateTemp("", "chainstorage-block-*.bin")
	if err != nil {
		return nil, xerrors.Errorf("create temp spool: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if err := d.spoolToFile(ctx, blockFile, tmp); err != nil {
		tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, xerrors.Errorf("close temp spool: %w", err)
	}

	rf, err := os.Open(tmpPath)
	if err != nil {
		return nil, xerrors.Errorf("reopen temp spool: %w", err)
	}
	defer rf.Close()

	dec, err := storage_utils.DecompressReader(rf, blockFile.Compression)
	if err != nil {
		return nil, xerrors.Errorf("wrap decompressor: %w", err)
	}
	defer dec.Close()

	// Wire-walk the decompressed stream directly into *api.Block. This
	// avoids the `ioutil.ReadAll + proto.Unmarshal` pair's ~2× peak
	// memory, dropping resident RAM during parse to ~1× block size for
	// all chains (not just bitcoin).
	block, err := api.WalkBlockEnvelope(dec)
	if err != nil {
		return nil, xerrors.Errorf("walk block envelope: %v: %w", err, errors.ErrDownloadFailure)
	}

	return block, nil
}

// DownloadStreamBitcoin spools the compressed blob to disk, decompresses
// to a second spool file while simultaneously walking the proto
// envelope, and returns a BitcoinStream handle whose OpenHeaderReader
// and LoadInputTxGroup closures seek into that decompressed spool.
//
// The returned stream owns the decompressed spool file; caller MUST
// call Close() to remove it. A runtime cleanup is wired as a safety
// net for leaks but should not be relied on.
func (d *blockDownloaderImpl) DownloadStreamBitcoin(ctx context.Context, blockFile *api.BlockFile) (*BitcoinStream, error) {
	if blockFile.Skipped {
		stream := &BitcoinStream{
			Block: &api.Block{
				Blockchain: d.config.Chain.Blockchain,
				Network:    d.config.Chain.Network,
				Metadata: &api.BlockMetadata{
					Tag: blockFile.Tag, Height: blockFile.Height, Skipped: true,
				},
				Blobdata: nil,
			},
			OpenHeaderReader: func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(nil)), nil
			},
			LoadInputTxGroup: func(i int) (*api.RepeatedBytes, error) { return nil, nil },
			closeFn:          func() error { return nil },
		}
		return stream, nil
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

	// Spool #2: decompressed bytes → tempfile2. We write via a tee so
	// the walker can read them inline. This spool outlives the call —
	// ownership passes to the returned BitcoinStream.
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

	tee := io.TeeReader(dec, tmpDecompressed)
	block, chunks, walkErr := api.WalkBitcoinEnvelope(tee)

	// Fully drain the tee so the remaining bytes (if any) land in the
	// decompressed spool. WalkBitcoinEnvelope consumes exactly up to
	// the end of the Block message; drain anything past that.
	if walkErr == nil {
		_, _ = io.Copy(io.Discard, tee)
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

	if walkErr != nil {
		return nil, xerrors.Errorf("walk bitcoin envelope: %w", walkErr)
	}
	if chunks.Header.Length == 0 && chunks.Header.Offset == 0 {
		return nil, xerrors.Errorf("block at (tag=%v, height=%v) has no bitcoin header", blockFile.Tag, blockFile.Height)
	}

	openHeaderReader := func() (io.ReadCloser, error) {
		f, err := os.Open(decompressedPath)
		if err != nil {
			return nil, xerrors.Errorf("open decompressed spool for header read: %w", err)
		}
		if _, err := f.Seek(chunks.Header.Offset, io.SeekStart); err != nil {
			f.Close()
			return nil, xerrors.Errorf("seek to header: %w", err)
		}
		return &seekedHeaderReader{f: f, limit: io.LimitReader(f, chunks.Header.Length)}, nil
	}

	// Lazy per-tx input-transaction loader: seek + read + Unmarshal
	// one RepeatedBytes group from the decompressed spool per call.
	// The returned *api.RepeatedBytes is expected to be discarded by
	// the caller after the corresponding tx is parsed, so each call's
	// allocation scope stays bounded.
	groups := chunks.InputTransactionsGroups
	loadGroup := func(i int) (*api.RepeatedBytes, error) {
		if i < 0 || i >= len(groups) {
			return nil, nil
		}
		ref := groups[i]
		f, err := os.Open(decompressedPath)
		if err != nil {
			return nil, xerrors.Errorf("open decompressed spool for input tx group [%d]: %w", i, err)
		}
		defer f.Close()
		if _, err := f.Seek(ref.Offset, io.SeekStart); err != nil {
			return nil, xerrors.Errorf("seek to input tx group [%d]: %w", i, err)
		}
		b := make([]byte, ref.Length)
		if _, err := io.ReadFull(f, b); err != nil {
			return nil, xerrors.Errorf("read input tx group [%d]: %w", i, err)
		}
		rb := &api.RepeatedBytes{}
		if err := proto.Unmarshal(b, rb); err != nil {
			return nil, xerrors.Errorf("unmarshal input tx group [%d]: %w", i, err)
		}
		return rb, nil
	}

	stream := &BitcoinStream{
		Block:            block,
		OpenHeaderReader: openHeaderReader,
		LoadInputTxGroup: loadGroup,
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

// seekedHeaderReader is an io.ReadCloser that reads up to `limit`
// bytes from `f` and closes `f` on Close.
type seekedHeaderReader struct {
	f     *os.File
	limit io.Reader
}

func (r *seekedHeaderReader) Read(p []byte) (int, error) { return r.limit.Read(p) }
func (r *seekedHeaderReader) Close() error               { return r.f.Close() }

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
