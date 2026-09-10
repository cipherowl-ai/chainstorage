package sdk

import (
	"context"
	"io"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
)

type (
	// NativeStreamedBlockIterator yields one NativeStreamedBlock per
	// block in [startHeight, endHeight) order and returns io.EOF when
	// exhausted. It is the chunk-once counterpart of calling
	// StreamNativeBlock per height: consecutive blocks that share a
	// consolidated (CSCB) chunk are decompressed in one pass.
	//
	// Only the block most recently returned is resident. Close each
	// NativeStreamedBlock before calling Next again to keep memory
	// bounded to one block; callers MUST close every returned block and
	// the iterator.
	NativeStreamedBlockIterator interface {
		Next(ctx context.Context) (NativeStreamedBlock, error)
		Close() error
	}

	nativeStreamedBlockIterator struct {
		inner  downloader.SpooledBlockIterator
		parser streamingParser
		opts   []ParseOption
	}
)

func (i *nativeStreamedBlockIterator) Next(ctx context.Context) (NativeStreamedBlock, error) {
	spooled, err := i.inner.Next(ctx)
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	stream, err := i.parser.ParseStreamNative(ctx, spooled, i.opts...)
	if err != nil {
		_ = spooled.Close()
		return nil, xerrors.Errorf("failed to create native stream (height=%d): %w", spooled.BlockFile.GetHeight(), err)
	}
	return stream, nil
}

func (i *nativeStreamedBlockIterator) Close() error {
	if i == nil || i.inner == nil {
		return nil
	}
	return i.inner.Close()
}
