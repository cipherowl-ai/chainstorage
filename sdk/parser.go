package sdk

import (
	"context"
	"io"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// ParseOption is re-exported from the parser package so SDK consumers
// can skip fields they will not consume (e.g. bitcoin scripts,
// witnesses, zcash shielded data). See parser.WithSkip* helpers.
type ParseOption = parser.ParseOption

// BitcoinBlockStream is re-exported from the parser package. See the
// bitcoin parser's StreamBlockIter for the contract (free-vs-paid
// Header() semantics, lifecycle bound to iteration).
type BitcoinBlockStream = parser.BitcoinBlockStream

// BitcoinInputTxGroupLoader resolves prev-output transactions for the
// i-th block tx on demand. Implementations typically seek into a
// locally-spooled copy of the decompressed proto.
type BitcoinInputTxGroupLoader = parser.BitcoinInputTxGroupLoader

// StreamedBlock is the chain-agnostic view returned by Client.StreamBlock.
// It mirrors the shape of *api.Block: a metadata field plus one
// chain-specific field populated based on the block's underlying
// blobdata. All non-matching chain fields are nil.
//
// A StreamedBlock may own backing resources (a disk spool for bitcoin
// chains); callers MUST call Close() when done. A runtime cleanup is
// wired as a safety net but should not be relied on.
//
// Consumer pattern parallels GetBlock:
//
//	view, err := client.StreamBlock(ctx, tag, height, hash)
//	if err != nil { return err }
//	defer view.Close()
//
//	if bs := view.GetBitcoin(); bs != nil {
//	    for tx, err := range bs.Transactions() {
//	        ...
//	    }
//	}
type StreamedBlock struct {
	// Metadata mirrors api.Block.Metadata from the downloaded envelope.
	Metadata *api.BlockMetadata
	// bitcoin is populated for bitcoin-family blocks; nil otherwise.
	bitcoin BitcoinBlockStream
	// closeFn releases backing resources (disk spool, etc.); may be nil.
	closeFn func() error
}

// GetMetadata returns the block metadata.
func (s *StreamedBlock) GetMetadata() *api.BlockMetadata {
	if s == nil {
		return nil
	}
	return s.Metadata
}

// GetBitcoin returns the bitcoin-family block stream, or nil for
// non-bitcoin chains. Callers should nil-check this the same way they
// would check api.Block.GetBitcoin().
func (s *StreamedBlock) GetBitcoin() BitcoinBlockStream {
	if s == nil {
		return nil
	}
	return s.bitcoin
}

// Close releases any backing resources held by the StreamedBlock (most
// notably the disk-spool file that backs bitcoin-family streams). Safe
// to call multiple times; no-op on non-bitcoin chains.
func (s *StreamedBlock) Close() error {
	if s == nil || s.closeFn == nil {
		return nil
	}
	return s.closeFn()
}

var (
	WithSkipScripts   = parser.WithSkipScripts
	WithSkipWitnesses = parser.WithSkipWitnesses
	WithSkipShielded  = parser.WithSkipShielded
)

type Parser interface {
	ParseNativeBlock(ctx context.Context, rawBlock *api.Block, opts ...ParseOption) (*api.NativeBlock, error)
	GetNativeTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error)
	ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error)
	ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error
	// StreamBitcoinBlock returns a bitcoin-family BlockStream for iterator
	// traversal. Non-bitcoin chains return an error.
	StreamBitcoinBlock(ctx context.Context, openReader func() (io.ReadCloser, error), loadGroup BitcoinInputTxGroupLoader, opts ...ParseOption) (BitcoinBlockStream, error)
}
