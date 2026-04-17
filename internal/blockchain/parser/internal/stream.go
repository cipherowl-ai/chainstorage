package internal

import (
	"context"
	"io"
	"iter"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// StreamedBlock is the chain-agnostic interface returned by
// Parser.StreamBlock. It exposes block metadata plus a Close method
// that releases backing resources (most notably the decompressed spool
// file in the downloader layer).
//
// Callers type-assert to a chain-specific extension
// (e.g. BitcoinStreamedBlock) to access streaming iterators or
// chain-specific lazy accessors.
type StreamedBlock interface {
	// GetMetadata returns the block metadata from the downloaded envelope.
	GetMetadata() *api.BlockMetadata
	// Close releases backing resources. Safe to call multiple times.
	Close() error
}

// BitcoinBlockIter is the iterator-only view produced by
// BitcoinStreamer.StreamBlockIter. It has no resource ownership; a
// wrapper (see Parser.StreamBlock) adds the StreamedBlock contract by
// composing this with metadata + Close.
//
// See the bitcoin parser package for the full contract (free-vs-paid
// Header() semantics, lifecycle bound to iteration).
type BitcoinBlockIter interface {
	// Transactions yields each decoded transaction in the block.
	Transactions() iter.Seq2[*api.BitcoinTransaction, error]
	// Header returns the block header, lazily populated. See the
	// bitcoin parser package for the ordering contract.
	Header() (*api.BitcoinHeader, error)
}

// BitcoinStreamedBlock is the bitcoin-family extension of
// StreamedBlock. Returned by Parser.StreamBlock when the configured
// chain is bitcoin-family.
type BitcoinStreamedBlock interface {
	StreamedBlock
	BitcoinBlockIter
}

// BitcoinInputTxGroupLoader returns the prev-output transactions for
// the i-th block transaction. See bitcoin.InputTxGroupLoader for
// details.
type BitcoinInputTxGroupLoader func(i int) (*api.RepeatedBytes, error)

// BitcoinStreamer identifies bitcoin-family NativeParser implementations
// that support streaming via StreamBlockIter. The generic Parser impl
// type-asserts its underlying NativeParser to BitcoinStreamer; non-
// bitcoin chains don't implement this and fall back to the generic
// walker path.
type BitcoinStreamer interface {
	StreamBlockIter(
		ctx context.Context,
		openReader func() (io.ReadCloser, error),
		loadGroup BitcoinInputTxGroupLoader,
		opts ...ParseOption,
	) BitcoinBlockIter
}

