package internal

import (
	"context"
	"io"
	"iter"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// BitcoinBlockStream is an iterator view over a bitcoin-family block.
// See the bitcoin parser package's StreamBlockIter for the full contract
// (free-vs-paid Header() semantics, lifecycle bound to iteration).
type BitcoinBlockStream interface {
	// Transactions yields each decoded transaction in the block.
	Transactions() iter.Seq2[*api.BitcoinTransaction, error]
	// Header returns the block header, lazily populated. See the
	// bitcoin parser package for the ordering contract.
	Header() (*api.BitcoinHeader, error)
}

// BitcoinStreamer identifies bitcoin-family NativeParser implementations
// that support streaming via StreamBlockIter.
//
// The generic parser.Parser impl type-asserts its underlying
// NativeParser to BitcoinStreamer; non-bitcoin chains don't implement
// this and see an error from Parser.StreamBitcoinBlock.
type BitcoinStreamer interface {
	StreamBlockIter(
		ctx context.Context,
		openReader func() (io.ReadCloser, error),
		blobdata *api.BitcoinBlobdata,
		opts ...ParseOption,
	) BitcoinBlockStream
}
