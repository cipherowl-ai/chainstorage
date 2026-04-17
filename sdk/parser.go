package sdk

import (
	"context"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// ParseOption is re-exported from the parser package so SDK consumers
// can skip fields they will not consume (e.g. bitcoin scripts,
// witnesses, zcash shielded data). See parser.WithSkip* helpers.
type ParseOption = parser.ParseOption

// StreamedBlock is the chain-agnostic view returned by Client.StreamBlock.
// It exposes block metadata plus a Close method that releases backing
// resources (disk spool, etc.). Callers type-assert to a chain-specific
// extension (BitcoinStreamedBlock) to access chain-specific iterators.
//
// Consumer pattern parallels GetBlock:
//
//	view, err := client.StreamBlock(ctx, tag, height, hash)
//	if err != nil { return err }
//	defer view.Close()
//
//	if bs, ok := view.(sdk.BitcoinStreamedBlock); ok {
//	    for tx, err := range bs.Transactions() { ... }
//	    header, _ := bs.Header()
//	}
type StreamedBlock = parser.StreamedBlock

// BitcoinStreamedBlock is the bitcoin-family extension of
// StreamedBlock. Callers obtain it by type-asserting on the result of
// Client.StreamBlock when the configured chain is bitcoin-family. See
// the bitcoin parser package for the free-vs-paid Header() contract.
type BitcoinStreamedBlock = parser.BitcoinStreamedBlock

// BitcoinInputTxGroupLoader resolves prev-output transactions for the
// i-th block tx on demand. Implementations typically seek into a
// locally-spooled copy of the decompressed proto.
type BitcoinInputTxGroupLoader = parser.BitcoinInputTxGroupLoader

// SpooledBlock is re-exported from the downloader package so the SDK
// Parser interface can describe StreamBlock without leaking internal
// package paths.
type SpooledBlock = downloader.SpooledBlock

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
	// StreamBlock returns a chain-specific StreamedBlock view over a
	// spooled block. Callers type-assert to BitcoinStreamedBlock for
	// bitcoin-family chains.
	StreamBlock(ctx context.Context, spooled *SpooledBlock, opts ...ParseOption) (StreamedBlock, error)
}
