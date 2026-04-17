package bitcoin

import (
	"context"
	"encoding/json"
	"io"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// BitcoinBlockVisitor receives transactions one at a time as the block's
// JSON is streamed in. Returning a non-nil error aborts the stream.
type BitcoinBlockVisitor interface {
	VisitTransaction(tx *api.BitcoinTransaction) error
}

// InputTxGroupLoader returns the prev-output transactions
// corresponding to the inputs of the i-th block transaction. The
// returned RepeatedBytes may be nil for txs with no inputs (e.g.
// coinbase) or when no input_transactions are available for the
// block.
//
// Callers may implement this against in-memory data (adapting
// *api.BitcoinBlobdata.InputTransactions) or against a spooled copy
// of the proto bytes (per-group seek+proto.Unmarshal). The lazy
// spool-based implementation is what drops parser peak RAM from
// ~O(block) to ~O(largest tx's input_transactions) on zcash.
//
// Alias of internal.BitcoinInputTxGroupLoader so the streamer
// interface and its bitcoin-family implementation share the same
// underlying type.
type InputTxGroupLoader = internal.BitcoinInputTxGroupLoader

// NewInMemoryInputTxGroupLoader adapts the legacy in-memory
// []*api.RepeatedBytes layout to the loader interface. Memory
// characteristics are unchanged vs directly passing the blobdata.
func NewInMemoryInputTxGroupLoader(groups []*api.RepeatedBytes) InputTxGroupLoader {
	return func(i int) (*api.RepeatedBytes, error) {
		if i < 0 || i >= len(groups) {
			return nil, nil
		}
		return groups[i], nil
	}
}

// StreamingNativeParser is implemented by bitcoin-family native parsers
// that support streaming decoding via StreamBlock.
type StreamingNativeParser interface {
	StreamBlock(
		ctx context.Context,
		r io.Reader,
		loadGroup InputTxGroupLoader,
		visitor BitcoinBlockVisitor,
		opts ...internal.ParseOption,
	) (*api.BitcoinHeader, error)
}

// BitcoinBlockVisitorFunc is a function adapter for BitcoinBlockVisitor.
type BitcoinBlockVisitorFunc func(tx *api.BitcoinTransaction) error

func (f BitcoinBlockVisitorFunc) VisitTransaction(tx *api.BitcoinTransaction) error {
	return f(tx)
}

// StreamBlock decodes a bitcoin-family block JSON from r, invoking visitor
// once per transaction as the stream progresses. Only a single
// BitcoinTransaction is held in memory at a time (plus a small buffer of
// the non-"tx" header fields), so peak parser memory is
// O(largest_tx + header) rather than O(block).
//
// Peak memory for a 4 GB zcash-heavy block is typically < 100 MB, versus
// ~2-3x the block size for ParseBlock.
//
// The block's raw header bytes (pre-"tx" fields marshaled back into JSON)
// plus the InputTransactions are still passed via blobdata; those are
// small relative to the transaction payload. When the underlying blob
// store exposes an io.Reader (Item 5's follow-up), raw_json never
// materializes in memory.
//
// The returned header is available only after all transactions are
// visited, because tx may precede or follow header fields in JSON.
func (b *bitcoinNativeParserImpl) StreamBlock(
	ctx context.Context,
	r io.Reader,
	loadGroup InputTxGroupLoader,
	visitor BitcoinBlockVisitor,
	opts ...internal.ParseOption,
) (*api.BitcoinHeader, error) {
	if visitor == nil {
		return nil, xerrors.New("nil visitor")
	}

	optView := internal.ResolveParseOptions(opts)

	dec := json.NewDecoder(r)
	openTok, err := dec.Token()
	if err != nil {
		return nil, xerrors.Errorf("failed to read opening token: %w", err)
	}
	if d, ok := openTok.(json.Delim); !ok || d != '{' {
		return nil, xerrors.Errorf("expected '{' at block start, got %v", openTok)
	}

	headerFields := make(map[string]json.RawMessage)
	txIdx := 0

	for dec.More() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keyTok, err := dec.Token()
		if err != nil {
			return nil, xerrors.Errorf("failed to read key token: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, xerrors.Errorf("expected string key, got %v", keyTok)
		}

		if key == "tx" {
			openArr, err := dec.Token()
			if err != nil {
				return nil, xerrors.Errorf("failed to read tx opening: %w", err)
			}
			if d, ok := openArr.(json.Delim); !ok || d != '[' {
				return nil, xerrors.Errorf("expected '[' for tx, got %v", openArr)
			}
			for dec.More() {
				var rawTx BitcoinTransaction
				if err := dec.Decode(&rawTx); err != nil {
					return nil, xerrors.Errorf("failed to decode tx[%d]: %w", txIdx, err)
				}

				// Lazy: load only this tx's prev-output group,
				// build a per-tx metadata map, and let it go
				// out of scope immediately after parsing.
				var metadataMap map[string][]*api.BitcoinTransactionOutput
				if loadGroup != nil {
					group, err := loadGroup(txIdx)
					if err != nil {
						return nil, xerrors.Errorf("load input tx group [%d]: %w", txIdx, err)
					}
					metadataMap, err = b.buildMetadataForGroup(group, optView)
					if err != nil {
						return nil, xerrors.Errorf("build metadata for tx[%d]: %w", txIdx, err)
					}
				}

				apiTx, err := rawTx.ToApiBitcoinTransaction(txIdx, metadataMap, b.p2pkhVersionByte, optView)
				if err != nil {
					return nil, xerrors.Errorf("failed to convert tx[%d]: %w", txIdx, err)
				}
				if err := visitor.VisitTransaction(apiTx); err != nil {
					return nil, xerrors.Errorf("visitor returned error at tx[%d]: %w", txIdx, err)
				}
				txIdx++
			}
			if _, err := dec.Token(); err != nil {
				return nil, xerrors.Errorf("failed to read tx closing: %w", err)
			}
			continue
		}

		// Header fields are small; buffer as json.RawMessage and reassemble
		// into a BitcoinBlock at the end. This is well under 1 KB total.
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, xerrors.Errorf("failed to decode header field %q: %w", key, err)
		}
		headerFields[key] = raw
	}
	if _, err := dec.Token(); err != nil {
		return nil, xerrors.Errorf("failed to read block closing: %w", err)
	}

	// Reconstitute the header. Remove "tx" so validation does not require it
	// to be present here; transactions were already streamed.
	delete(headerFields, "tx")
	headerJSON, err := json.Marshal(headerFields)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal header fields: %w", err)
	}
	var block BitcoinBlock
	if err := json.Unmarshal(headerJSON, &block); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal header: %w", err)
	}
	return block.GetApiBitcoinHeader(), nil
}
