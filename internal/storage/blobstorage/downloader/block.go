package downloader

import (
	"bytes"
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
		// proto.Unmarshal'd into memory — resident peak is
		// decompressed_bytes + api.Block (roughly 2× block size).
		// For the Phase 2 reduced-peak path, use
		// DownloadStreamBitcoin which streams the bitcoin
		// BitcoinBlobdata.Header off disk.
		DownloadStream(ctx context.Context, blockFile *api.BlockFile, consumer StreamConsumer) error

		// DownloadStreamBitcoin is the Phase 2 path for
		// bitcoin-family blocks. It spools compressed bytes to disk,
		// streams the decompressed bytes through a proto wire-walker
		// that materializes everything except BitcoinBlobdata.Header,
		// and invokes consumer with that partial *api.Block plus an
		// OpenHeaderReader factory that seeks into the spooled
		// decompressed file to deliver the header JSON bytes.
		//
		// Peak RAM drops from ~2× block size (DownloadStream) to
		// ~1× block size (the input_transactions portion) plus the
		// decompressed bytes as a file on disk.
		//
		// consumer sees block.Bitcoin.Header = nil. OpenHeaderReader
		// may be called multiple times during the consumer's work
		// (e.g. once for bitcoin.BlockStream.Transactions() and, if
		// Header() is invoked before iteration, a second time for the
		// header-only scan). Each returned ReadCloser is an
		// independent handle on the spool file.
		//
		// Returns an error if the block is not bitcoin-shaped.
		DownloadStreamBitcoin(ctx context.Context, blockFile *api.BlockFile, consumer BitcoinStreamConsumer) error
	}

	// BitcoinStreamConsumer receives a partial api.Block (with both
	// BitcoinBlobdata.Header and BitcoinBlobdata.InputTransactions
	// left nil) plus (a) a factory for opening a reader over the
	// Header bytes and (b) a loader that returns prev-output
	// transactions for the i-th block tx on demand.
	//
	// Both factories read from a locally-spooled copy of the
	// decompressed block, so memory use is bounded by whatever the
	// consumer actively holds (one header reader, one tx group at a
	// time) rather than by the full InputTransactions set.
	BitcoinStreamConsumer func(
		ctx context.Context,
		block *api.Block,
		openHeaderReader func() (io.ReadCloser, error),
		loadInputTxGroup func(i int) (*api.RepeatedBytes, error),
	) error

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

	// Wire-walk the decompressed stream directly into *api.Block. This
	// avoids the `ioutil.ReadAll + proto.Unmarshal` pair's ~2× peak
	// memory, dropping resident RAM during parse to ~1× block size for
	// all chains (not just bitcoin).
	block, err := api.WalkBlockEnvelope(dec)
	if err != nil {
		return xerrors.Errorf("walk block envelope: %v: %w", err, errors.ErrDownloadFailure)
	}

	return consumer(ctx, block)
}

// DownloadStreamBitcoin is the Phase 2 streaming path. It spools the
// compressed blob to disk, decompresses to a second temp file while
// simultaneously walking the proto envelope, and hands the consumer a
// partially-populated *api.Block plus a header reader factory that
// seeks into the decompressed spool on demand.
//
// Peak RAM during this call is dominated by any materialized proto
// fields (InputTransactions in particular), not by a duplicate []byte
// copy of the decompressed block.
func (d *blockDownloaderImpl) DownloadStreamBitcoin(ctx context.Context, blockFile *api.BlockFile, consumer BitcoinStreamConsumer) error {
	if blockFile.Skipped {
		emptyHeader := func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		noGroups := func(i int) (*api.RepeatedBytes, error) { return nil, nil }
		return consumer(ctx, &api.Block{
			Blockchain: d.config.Chain.Blockchain,
			Network:    d.config.Chain.Network,
			Metadata: &api.BlockMetadata{
				Tag: blockFile.Tag, Height: blockFile.Height, Skipped: true,
			},
			Blobdata: nil,
		}, emptyHeader, noGroups)
	}

	defer d.logDuration(time.Now())

	// Spool #1: compressed body → tempfile1.
	tmpCompressed, err := os.CreateTemp("", "chainstorage-block-compressed-*.bin")
	if err != nil {
		return xerrors.Errorf("create compressed spool: %w", err)
	}
	compressedPath := tmpCompressed.Name()
	defer os.Remove(compressedPath)
	if err := d.spoolToFile(ctx, blockFile, tmpCompressed); err != nil {
		tmpCompressed.Close()
		return err
	}
	if err := tmpCompressed.Close(); err != nil {
		return xerrors.Errorf("close compressed spool: %w", err)
	}

	// Spool #2: decompressed bytes → tempfile2. We write via a tee so
	// the walker can read them inline.
	tmpDecompressed, err := os.CreateTemp("", "chainstorage-block-decompressed-*.bin")
	if err != nil {
		return xerrors.Errorf("create decompressed spool: %w", err)
	}
	decompressedPath := tmpDecompressed.Name()
	defer os.Remove(decompressedPath)

	compressedFile, err := os.Open(compressedPath)
	if err != nil {
		tmpDecompressed.Close()
		return xerrors.Errorf("reopen compressed spool: %w", err)
	}

	dec, err := storage_utils.DecompressReader(compressedFile, blockFile.Compression)
	if err != nil {
		compressedFile.Close()
		tmpDecompressed.Close()
		return xerrors.Errorf("wrap decompressor: %w", err)
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
		return xerrors.Errorf("sync decompressed spool: %w", err)
	}
	if err := tmpDecompressed.Close(); err != nil {
		return xerrors.Errorf("close decompressed spool: %w", err)
	}

	if walkErr != nil {
		return xerrors.Errorf("walk bitcoin envelope: %w", walkErr)
	}
	if chunks.Header.Length == 0 && chunks.Header.Offset == 0 {
		return xerrors.Errorf("block at (tag=%v, height=%v) has no bitcoin header", blockFile.Tag, blockFile.Height)
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

	return consumer(ctx, block, openHeaderReader, loadGroup)
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
