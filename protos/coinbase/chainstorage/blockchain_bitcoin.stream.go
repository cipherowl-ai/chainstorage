package chainstorage

// Bitcoin-specific variant of the Block wire walker. Shares the
// generic walker infrastructure from block_stream.go, but when the
// BitcoinBlobdata.Header field is encountered, the bytes are skipped
// and only their (offset, length) within the walked stream is
// recorded. Callers layer a spool-file reader over that offset to
// stream the JSON header directly into the parser without
// materializing it as a []byte in RAM.
//
// Everything else (Block outer fields, BitcoinBlobdata fields other
// than Header, sub-messages, repeated bytes, map entries) delegates
// to the generic walker's reflection-driven decoder — so peak RAM on
// InputTransactions-heavy zcash blocks matches the generic walker's
// direct-read efficiency.

import (
	"bufio"
	"io"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Proto wire types (see https://protobuf.dev/programming-guides/encoding/).
// Defined here and shared with the generic walker in block_stream.go.
const (
	wireVarint     = 0
	wireFixed64    = 1
	wireBytes      = 2 // length-delimited
	wireStartGroup = 3 // deprecated
	wireEndGroup   = 4 // deprecated
	wireFixed32    = 5
)

// Block field numbers.
const (
	blockFieldBlockchain          = 1
	blockFieldNetwork             = 2
	blockFieldMetadata            = 3
	blockFieldTransactionMetadata = 4
	blockFieldSideChain           = 5
	// blobdata oneof:
	blockFieldEthereum       = 100
	blockFieldBitcoin        = 101
	blockFieldRosetta        = 102
	blockFieldSolana         = 103
	blockFieldAptos          = 104
	blockFieldEthereumBeacon = 105
)

// BitcoinBlobdata field numbers.
const (
	bitcoinBlobFieldHeader               = 1
	bitcoinBlobFieldInputTransactions    = 2
	bitcoinBlobFieldRawInputTransactions = 3
)

// WireWalkerKnownField describes one field the bitcoin wire walker
// handles. The contract test in blockchain_bitcoin.stream_test.go
// asserts that KnownBlockFields and KnownBitcoinBlobFields match the
// proto descriptors exactly, in both directions.
type WireWalkerKnownField struct {
	Kind     protoreflect.Kind // expected proto kind
	Repeated bool              // repeated (includes map)
	Map      bool              // map<K, V>
	Oneof    string            // containing oneof name, empty if none
	Process  bool              // walker decodes this field into the result
}

// KnownBlockFields enumerates every field the walker recognizes on
// Block. See the contract test + switch-coverage test for the
// drift-defense story.
var KnownBlockFields = map[protoreflect.FieldNumber]WireWalkerKnownField{
	blockFieldBlockchain:          {Kind: protoreflect.EnumKind, Process: true},
	blockFieldNetwork:             {Kind: protoreflect.EnumKind, Process: true},
	blockFieldMetadata:            {Kind: protoreflect.MessageKind, Process: true},
	blockFieldTransactionMetadata: {Kind: protoreflect.MessageKind, Process: true},
	blockFieldSideChain:           {Kind: protoreflect.EnumKind, Process: true},
	blockFieldEthereum:            {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldBitcoin:             {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldRosetta:             {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldSolana:              {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldAptos:               {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldEthereumBeacon:      {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
}

// KnownBitcoinBlobFields enumerates every field the walker recognizes
// on BitcoinBlobdata.
var KnownBitcoinBlobFields = map[protoreflect.FieldNumber]WireWalkerKnownField{
	bitcoinBlobFieldHeader:               {Kind: protoreflect.BytesKind, Process: true},
	bitcoinBlobFieldInputTransactions:    {Kind: protoreflect.MessageKind, Repeated: true, Process: true},
	bitcoinBlobFieldRawInputTransactions: {Kind: protoreflect.MessageKind, Repeated: true, Map: true, Process: true},
}

// WalkBitcoinEnvelope reads a proto-encoded Block from r and returns
// a partially-populated *Block where BitcoinBlobdata.Header is left
// nil. The offset and length of the Header field within the walked
// stream are returned so the caller can seek into a spooled copy of
// r and read the header JSON without ever materializing it as a
// []byte in memory.
//
// Every other field (including BitcoinBlobdata.InputTransactions,
// other blobdata variants, Block metadata) is decoded normally via
// the generic walker's reflection-driven path — so peak RAM on
// InputTransactions-heavy zcash blocks is bounded by the materialized
// api.Block, not by an intermediate []byte copy of the decompressed
// proto.
//
// Unknown field numbers on the wire return an error. The contract +
// switch-coverage tests in protos/coinbase/chainstorage/ ensure any
// proto edit that adds a field either gets registered here or fails
// CI.
func WalkBitcoinEnvelope(r io.Reader) (*Block, int64, int64, error) {
	block := &Block{}
	w := &walker{br: bufio.NewReaderSize(r, 64*1024)}
	blockMsg := block.ProtoReflect()
	blockDesc := blockMsg.Descriptor()

	var headerOffset, headerLength int64 = -1, 0

	for {
		if _, err := w.br.Peek(1); err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, 0, xerrors.Errorf("peek at top-level: %w", err)
		}
		tag, err := w.readVarint()
		if err != nil {
			return nil, 0, 0, xerrors.Errorf("read top-level tag: %w", err)
		}
		fieldNum := protoreflect.FieldNumber(tag >> 3)
		wireType := int(tag & 0x7)

		fd := blockDesc.Fields().ByNumber(fieldNum)
		if fd == nil {
			return nil, 0, 0, xerrors.Errorf("unknown Block field %d (wt=%d): proto changed; update KnownBlockFields + switch in blockchain_bitcoin.stream.go", fieldNum, wireType)
		}

		// Specialized: when descending into the bitcoin blob, track
		// Header offset instead of materializing it.
		if fieldNum == blockFieldBitcoin {
			length, err := w.readVarint()
			if err != nil {
				return nil, 0, 0, xerrors.Errorf("read bitcoin length: %w", err)
			}
			blob := &BitcoinBlobdata{}
			block.Blobdata = &Block_Bitcoin{Bitcoin: blob}
			endPos := w.pos + int64(length)
			hoff, hlen, err := w.walkBitcoinBlob(blob, endPos)
			if err != nil {
				return nil, 0, 0, xerrors.Errorf("walk bitcoin blob: %w", err)
			}
			if hoff >= 0 {
				headerOffset = hoff
				headerLength = hlen
			}
			continue
		}

		// All other fields: delegate to the generic walker's
		// reflection-driven decoder. This gives us direct-read
		// efficiency for repeated bytes inside sub-messages without
		// proto.Unmarshal transient buffers.
		if err := w.decodeField(blockMsg, fd, wireType); err != nil {
			return nil, 0, 0, xerrors.Errorf("decode Block.%s: %w", fd.Name(), err)
		}
	}

	return block, headerOffset, headerLength, nil
}

// walkBitcoinBlob walks a BitcoinBlobdata sub-message. The Header
// field (bitcoinBlobFieldHeader) is skipped — its offset + length are
// recorded so the caller can seek back into the spool file and stream
// the bytes later. All other fields delegate to the generic walker.
func (w *walker) walkBitcoinBlob(blob *BitcoinBlobdata, endPos int64) (int64, int64, error) {
	var headerOffset, headerLength int64 = -1, 0
	blobMsg := blob.ProtoReflect()
	blobDesc := blobMsg.Descriptor()

	for w.pos < endPos {
		tag, err := w.readVarint()
		if err != nil {
			return 0, 0, xerrors.Errorf("read bitcoin tag: %w", err)
		}
		fieldNum := protoreflect.FieldNumber(tag >> 3)
		wireType := int(tag & 0x7)

		if fieldNum == bitcoinBlobFieldHeader {
			length, err := w.readVarint()
			if err != nil {
				return 0, 0, xerrors.Errorf("read header length: %w", err)
			}
			headerOffset = w.pos
			headerLength = int64(length)
			n, err := io.CopyN(io.Discard, w.br, int64(length))
			w.pos += n
			if err != nil {
				return 0, 0, xerrors.Errorf("discard header bytes: %w", err)
			}
			continue
		}

		fd := blobDesc.Fields().ByNumber(fieldNum)
		if fd == nil {
			return 0, 0, xerrors.Errorf("unknown BitcoinBlobdata field %d (wt=%d): proto changed; update KnownBitcoinBlobFields + switch in blockchain_bitcoin.stream.go", fieldNum, wireType)
		}
		if err := w.decodeField(blobMsg, fd, wireType); err != nil {
			return 0, 0, xerrors.Errorf("decode BitcoinBlobdata.%s: %w", fd.Name(), err)
		}
	}

	if w.pos != endPos {
		return 0, 0, xerrors.Errorf("bitcoin blob consumed %d bytes, expected %d", w.pos, endPos)
	}
	return headerOffset, headerLength, nil
}
