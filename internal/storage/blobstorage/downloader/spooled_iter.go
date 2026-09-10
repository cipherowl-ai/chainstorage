package downloader

import (
	"bytes"
	"context"
	"io"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	// SpooledBlockIterator yields one SpooledBlock per input block file, in
	// input order, and returns io.EOF when exhausted. Consecutive CSCB
	// block files that live in the same chunk are served from a single
	// HTTP range request and a single decompressor pass, so a range walk
	// over consolidated history decompresses each chunk once instead of
	// once per block (DownloadStream re-reads the chunk for every block).
	//
	// Only the block being handed out is resident: callers that Close
	// each SpooledBlock before calling Next keep memory bounded to one
	// block payload, not one chunk. Callers MUST close every returned
	// SpooledBlock and the iterator.
	SpooledBlockIterator interface {
		Next(ctx context.Context) (*SpooledBlock, error)
		Close() error
	}

	spooledBlockIteratorImpl struct {
		downloader *blockDownloaderImpl
		blockFiles []*api.BlockFile
		current    int
		group      *cscbChunkGroup
		indexByURL map[string]*cscb.Index
		closed     bool
	}

	// cscbChunkGroup is the in-flight state for one run of block files
	// that share a CSCB chunk. reader is nil between attempts: a failed
	// read closes it, and the next attempt reopens the chunk range and
	// skips straight to the first block still owed.
	cscbChunkGroup struct {
		fileURL   string
		codec     api.Compression
		chunk     *cscb.ChunkDescriptor
		downloads []cscbBlockDownload
		pos       int
		reader    *cscb.ChunkBlockReader
	}

	// bytesReadCloser is an in-memory SpooledBlock reader. It keeps
	// bytes.Reader's ReadAt/Seek so the parser's spool handle contract
	// (io.ReaderAt for header section readers) holds for memory-backed
	// blocks exactly as it does for file-backed ones.
	bytesReadCloser struct {
		*bytes.Reader
	}
)

func (bytesReadCloser) Close() error { return nil }

// OpenSpooledBlocks opens a SpooledBlockIterator over blockFiles. See
// SpooledBlockIterator for the chunk-once contract.
func (d *blockDownloaderImpl) OpenSpooledBlocks(ctx context.Context, blockFiles []*api.BlockFile) (SpooledBlockIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &spooledBlockIteratorImpl{
		downloader: d,
		blockFiles: blockFiles,
		indexByURL: make(map[string]*cscb.Index),
	}, nil
}

func (i *spooledBlockIteratorImpl) Next(ctx context.Context) (*SpooledBlock, error) {
	if i.closed {
		return nil, xerrors.New("spooled block iterator is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i.group != nil {
		if i.group.pos < len(i.group.downloads) {
			return i.group.next(ctx, i.downloader)
		}
		i.closeGroup()
	}
	if i.current >= len(i.blockFiles) {
		return nil, io.EOF
	}

	blockFile := i.blockFiles[i.current]
	if blockFile.GetSkipped() || !isCSCBBlockFile(blockFile) {
		i.current++
		return i.downloader.DownloadStream(ctx, blockFile)
	}
	if err := i.openNextCSCBGroup(ctx); err != nil {
		return nil, err
	}
	return i.group.next(ctx, i.downloader)
}

func (i *spooledBlockIteratorImpl) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true
	i.closeGroup()
	return nil
}

func (i *spooledBlockIteratorImpl) closeGroup() {
	if i.group != nil {
		i.group.close()
		i.group = nil
	}
}

// openNextCSCBGroup collects the run of block files starting at
// i.current that share the current file's chunk and appear in ascending
// chunk-offset order (the order GetBlockFilesByRange returns them), so
// the run can be served by one sequential pass without reordering.
func (i *spooledBlockIteratorImpl) openNextCSCBGroup(ctx context.Context) error {
	firstFile := i.blockFiles[i.current]
	fileURL := firstFile.GetFileUrl()
	if fileURL == "" {
		return xerrors.Errorf("missing CSCB file url for height %d", firstFile.GetHeight())
	}
	index, err := i.getCSCBIndex(ctx, fileURL)
	if err != nil {
		return err
	}
	firstBlock, firstChunk, err := index.LookupBlock(blockFileToMetadata(firstFile))
	if err != nil {
		return err
	}

	downloads := []cscbBlockDownload{{
		ref:   downloadRef{index: i.current, blockFile: firstFile},
		block: firstBlock,
		chunk: firstChunk,
	}}
	previousEnd := firstBlock.ChunkRelativeOffset + firstBlock.PayloadLength
	nextIndex := i.current + 1
	for nextIndex < len(i.blockFiles) {
		nextFile := i.blockFiles[nextIndex]
		if nextFile.GetSkipped() || !isCSCBBlockFile(nextFile) || nextFile.GetFileUrl() != fileURL {
			break
		}
		nextBlock, nextChunk, err := index.LookupBlock(blockFileToMetadata(nextFile))
		if err != nil {
			return err
		}
		if nextChunk.Index != firstChunk.Index || nextBlock.ChunkRelativeOffset < previousEnd {
			break
		}
		downloads = append(downloads, cscbBlockDownload{
			ref:   downloadRef{index: nextIndex, blockFile: nextFile},
			block: nextBlock,
			chunk: nextChunk,
		})
		previousEnd = nextBlock.ChunkRelativeOffset + nextBlock.PayloadLength
		nextIndex++
	}

	i.group = &cscbChunkGroup{
		fileURL:   fileURL,
		codec:     index.Header.Codec,
		chunk:     firstChunk,
		downloads: downloads,
	}
	i.current = nextIndex
	return nil
}

func (i *spooledBlockIteratorImpl) getCSCBIndex(ctx context.Context, fileURL string) (*cscb.Index, error) {
	if index, ok := i.indexByURL[fileURL]; ok {
		return index, nil
	}
	index, err := i.downloader.readCSCBIndex(ctx, nil, fileURL)
	if err != nil {
		return nil, err
	}
	i.indexByURL[fileURL] = index
	return index, nil
}

// next hands out the group's next block. The chunk range is opened
// lazily on the first call and reopened after a failed read, in which
// case the retry resumes at the block that failed rather than at the
// start of the group; blocks already handed out are never re-yielded.
func (g *cscbChunkGroup) next(ctx context.Context, d *blockDownloaderImpl) (*SpooledBlock, error) {
	download := g.downloads[g.pos]
	payload, err := d.retryBytes.Retry(ctx, func(ctx context.Context) ([]byte, error) {
		if g.reader == nil {
			reader, err := g.open(ctx, d)
			if err != nil {
				return nil, err
			}
			g.reader = reader
		}
		_, payload, err := g.reader.Next()
		if err != nil {
			_ = g.reader.Close()
			g.reader = nil
			if xerrors.Is(err, io.EOF) || xerrors.Is(err, io.ErrUnexpectedEOF) {
				return nil, retry.Retryable(err)
			}
			return nil, err
		}
		return payload, nil
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to read CSCB block at height %d: %w", download.ref.blockFile.GetHeight(), err)
	}
	g.pos++
	return newBytesSpooledBlock(download.ref.blockFile, payload), nil
}

// open issues the chunk's range request and positions a ChunkBlockReader
// at the first block still owed (g.downloads[g.pos:]).
func (g *cscbChunkGroup) open(ctx context.Context, d *blockDownloaderImpl) (*cscb.ChunkBlockReader, error) {
	body, err := d.openHTTPRangeByLengthUnthrottled(ctx, g.fileURL, g.chunk.CompressedPayloadOffset, g.chunk.CompressedLength)
	if err != nil {
		return nil, err
	}
	limitedBody, err := limitReaderByLength(body, g.chunk.CompressedLength)
	if err != nil {
		_ = body.Close()
		return nil, err
	}
	remaining := g.downloads[g.pos:]
	blocks := make([]*cscb.BlockDescriptor, len(remaining))
	for j, download := range remaining {
		blocks[j] = download.block
	}
	return cscb.NewChunkBlockReader(&limitedReadCloser{Reader: limitedBody, closer: body}, g.codec, g.chunk, blocks)
}

func (g *cscbChunkGroup) close() {
	if g.reader != nil {
		_ = g.reader.Close()
		g.reader = nil
	}
}

// newBytesSpooledBlock wraps an already-validated block payload as a
// SpooledBlock. Open returns a fresh reader over the same bytes each
// time; Close drops the reference so the payload is collectable.
func newBytesSpooledBlock(blockFile *api.BlockFile, payload []byte) *SpooledBlock {
	spooled := &SpooledBlock{BlockFile: blockFile}
	spooled.Open = func() (io.ReadCloser, error) {
		if payload == nil {
			return nil, xerrors.Errorf("spooled block at height %d is closed", blockFile.GetHeight())
		}
		return bytesReadCloser{bytes.NewReader(payload)}, nil
	}
	spooled.closeFn = func() error {
		payload = nil
		return nil
	}
	return spooled
}
