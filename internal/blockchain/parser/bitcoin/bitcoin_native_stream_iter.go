package bitcoin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"sync"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// BlockStream is a bitcoin-package alias for internal.BitcoinBlockStream.
// The stream is an iterator-based view over a bitcoin-family block.
//
// The stream owns the decoder lifecycle: resources acquired by the
// underlying reader factory (file handles, decompressors) are released
// when Transactions() iteration completes, breaks, returns, or panics.
// There is no explicit Close method.
//
// # Header() and the "free after iteration" optimization
//
// Header() can be called before, during, or after Transactions()
// iteration. The cost depends on the order of operations:
//
//   - Call Header() AFTER Transactions() has been fully consumed: free.
//     The header is captured as a side effect of the streaming parse
//     (we already had to reconstitute it to return from the underlying
//     StreamBlock call). Subsequent Header() calls return the cached
//     value.
//
//   - Call Header() BEFORE (or WITHOUT) iterating: a dedicated
//     header-only pass over the block's JSON is performed once. The
//     pass walks top-level fields and skips the "tx" array via
//     token-depth tracking without decoding its elements, then
//     reconstitutes a BitcoinHeader from the remaining fields. The
//     result is cached.
//
//   - Call Header() DURING iteration (e.g. mid-range inside the loop):
//     treated as "before" — the dedicated pass runs. Avoid this if
//     possible.
//
// Callers that need both transactions and header should iterate first
// and read the header after the range loop to avoid the extra pass.
//
// BlockStream is not safe for concurrent use across goroutines. The
// typical single-goroutine range-loop pattern is always safe.
type BlockStream = internal.BitcoinBlockStream

// errIterStopped is an internal sentinel used to unwind StreamBlock
// when the iterator consumer breaks. It is not exposed to callers.
var errIterStopped = errors.New("bitcoin: block stream iteration stopped by consumer")

type bitcoinBlockStream struct {
	ctx        context.Context
	parser     *bitcoinNativeParserImpl
	blob       *api.BitcoinBlobdata
	opts       []internal.ParseOption
	openReader func() (io.ReadCloser, error)

	mu        sync.Mutex
	header    *api.BitcoinHeader
	headerErr error
	headerSet bool
}

// StreamBlockIter returns a BlockStream that consumes block JSON via the
// supplied reader factory. The factory may be called more than once —
// once for each Transactions() iteration and at most once more for an
// early Header() call. It must return a freshly-positioned reader on
// each call.
//
// The InputTransactions in blobdata must be available up front; they
// are used to build the prev-output metadata map required for each
// transaction.
func (b *bitcoinNativeParserImpl) StreamBlockIter(
	ctx context.Context,
	openReader func() (io.ReadCloser, error),
	blobdata *api.BitcoinBlobdata,
	opts ...internal.ParseOption,
) BlockStream {
	return &bitcoinBlockStream{
		ctx:        ctx,
		parser:     b,
		blob:       blobdata,
		opts:       opts,
		openReader: openReader,
	}
}

func (s *bitcoinBlockStream) Transactions() iter.Seq2[*api.BitcoinTransaction, error] {
	return func(yield func(*api.BitcoinTransaction, error) bool) {
		if s.openReader == nil {
			yield(nil, xerrors.New("nil openReader"))
			return
		}

		r, err := s.openReader()
		if err != nil {
			yield(nil, xerrors.Errorf("failed to open block reader: %w", err))
			return
		}
		defer r.Close()

		var consumerStopped bool
		visitor := BitcoinBlockVisitorFunc(func(tx *api.BitcoinTransaction) error {
			if !yield(tx, nil) {
				consumerStopped = true
				return errIterStopped
			}
			return nil
		})

		hdr, err := s.parser.StreamBlock(s.ctx, r, s.blob, visitor, s.opts...)
		switch {
		case errors.Is(err, errIterStopped):
			// Consumer broke the range. Nothing to report; defers clean up.
		case err != nil:
			yield(nil, err)
		default:
			// Full consumption: cache header for a free subsequent Header() call.
			s.mu.Lock()
			if !s.headerSet {
				s.header = hdr
				s.headerSet = true
			}
			s.mu.Unlock()
			_ = consumerStopped
		}
	}
}

func (s *bitcoinBlockStream) Header() (*api.BitcoinHeader, error) {
	s.mu.Lock()
	if s.headerSet {
		defer s.mu.Unlock()
		return s.header, s.headerErr
	}
	s.mu.Unlock()

	// Slow path: dedicated header-only scan.
	r, err := s.openReader()
	if err != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.headerSet = true
		s.headerErr = xerrors.Errorf("failed to open block reader for header scan: %w", err)
		return nil, s.headerErr
	}
	defer r.Close()

	hdr, err := parseBitcoinHeaderSkippingTx(r)

	s.mu.Lock()
	defer s.mu.Unlock()
	// Another caller may have raced to populate the header via iteration.
	// In that case keep the iteration-derived value (it is equivalent).
	if !s.headerSet {
		s.header = hdr
		s.headerErr = err
		s.headerSet = true
	}
	return s.header, s.headerErr
}

// parseBitcoinHeaderSkippingTx performs a one-pass JSON scan over r,
// buffering every top-level field except "tx" (which is skipped via
// token-depth tracking — no transaction values are allocated). The
// remaining fields are reassembled into a BitcoinBlock struct and the
// derived header is returned.
func parseBitcoinHeaderSkippingTx(r io.Reader) (*api.BitcoinHeader, error) {
	dec := json.NewDecoder(r)
	openTok, err := dec.Token()
	if err != nil {
		return nil, xerrors.Errorf("read opening token: %w", err)
	}
	if d, ok := openTok.(json.Delim); !ok || d != '{' {
		return nil, xerrors.Errorf("expected '{' at block start, got %v", openTok)
	}

	headerFields := make(map[string]json.RawMessage)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, xerrors.Errorf("read key token: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, xerrors.Errorf("expected string key, got %v", keyTok)
		}
		if key == "tx" {
			if err := skipJSONValue(dec); err != nil {
				return nil, xerrors.Errorf("skip tx array: %w", err)
			}
			continue
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, xerrors.Errorf("decode header field %q: %w", key, err)
		}
		headerFields[key] = raw
	}
	if _, err := dec.Token(); err != nil {
		return nil, xerrors.Errorf("read closing token: %w", err)
	}

	headerJSON, err := json.Marshal(headerFields)
	if err != nil {
		return nil, xerrors.Errorf("marshal header fields: %w", err)
	}
	var block BitcoinBlock
	if err := json.Unmarshal(headerJSON, &block); err != nil {
		return nil, xerrors.Errorf("unmarshal header: %w", err)
	}
	return block.GetApiBitcoinHeader(), nil
}

// skipJSONValue consumes exactly one JSON value (scalar or container)
// from dec's current position, without allocating any sub-values.
func skipJSONValue(dec *json.Decoder) error {
	depth := 0
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		d, ok := tok.(json.Delim)
		if !ok {
			if depth == 0 {
				// Scalar at the top level — we're done.
				return nil
			}
			continue
		}
		switch d {
		case '[', '{':
			depth++
		case ']', '}':
			depth--
			if depth == 0 {
				return nil
			}
		}
	}
}
