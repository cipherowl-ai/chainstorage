package chainstorage

// Streaming wire walker for Block with a BitcoinBlobdata payload.
// Lives next to the generated proto so any edit to blockchain.proto /
// blockchain_bitcoin.proto lands in the same directory and naturally
// surfaces the contract test in the diff.
//
// The walker hand-reads the proto wire format so that
// BitcoinBlobdata.Header (the JSON header bytes, potentially multi-GB
// on bitcoin mainnet) is never materialized as a []byte in RAM.
// Instead, the caller receives:
//   - a partially-populated *Block (everything except Header decoded).
//   - the offset and length of Header within the walked stream, so
//     the caller can seek into a spooled copy of the decompressed
//     bytes and stream header JSON directly into the parser.

import (
	"bufio"
	"io"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

// Proto wire types (see https://protobuf.dev/programming-guides/encoding/).
const (
	wireVarint     = 0
	wireFixed64    = 1
	wireBytes      = 2 // length-delimited: strings, bytes, embedded messages, packed repeated
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

// WireWalkerKnownField describes one field the wire walker handles.
// Fields with Process == true are decoded into the returned *Block.
// Fields with Process == false are recognized but their values are
// discarded intact. An on-wire field number not registered here
// causes WalkBitcoinEnvelope to return an error.
type WireWalkerKnownField struct {
	Kind     protoreflect.Kind // expected proto kind for drift detection
	Repeated bool              // repeated (includes map)
	Map      bool              // map<K, V>
	Oneof    string            // containing oneof name, empty if none
	Process  bool              // walker decodes this field into the result
}

// KnownBlockFields enumerates every field the walker recognizes on
// Block. The set MUST exactly match the proto descriptor — the
// contract test in blockchain_bitcoin.stream_test.go fails in either
// direction if it doesn't.
//
// When you add a field to Block in blockchain.proto:
//  1. Add an entry here describing its kind/cardinality/oneof.
//  2. If Process == true, add a switch case in WalkBitcoinEnvelope
//     to decode it; otherwise add its field number to the
//     known-but-unused skip group in the same switch.
var KnownBlockFields = map[protoreflect.FieldNumber]WireWalkerKnownField{
	blockFieldBlockchain:          {Kind: protoreflect.EnumKind, Process: true},
	blockFieldNetwork:             {Kind: protoreflect.EnumKind, Process: true},
	blockFieldMetadata:            {Kind: protoreflect.MessageKind, Process: true},
	blockFieldTransactionMetadata: {Kind: protoreflect.MessageKind, Process: false},
	blockFieldSideChain:           {Kind: protoreflect.EnumKind, Process: true},
	blockFieldEthereum:            {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: false},
	blockFieldBitcoin:             {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldRosetta:             {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: false},
	blockFieldSolana:              {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: false},
	blockFieldAptos:               {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: false},
	blockFieldEthereumBeacon:      {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: false},
}

// KnownBitcoinBlobFields enumerates every field the walker recognizes
// on BitcoinBlobdata. Same contract as KnownBlockFields.
var KnownBitcoinBlobFields = map[protoreflect.FieldNumber]WireWalkerKnownField{
	bitcoinBlobFieldHeader:               {Kind: protoreflect.BytesKind, Process: true},
	bitcoinBlobFieldInputTransactions:    {Kind: protoreflect.MessageKind, Repeated: true, Process: true},
	bitcoinBlobFieldRawInputTransactions: {Kind: protoreflect.MessageKind, Repeated: true, Map: true, Process: true},
}

// WalkBitcoinEnvelope reads a proto-encoded Block from r and returns
// a partially-populated *Block where BitcoinBlobdata.Header is left
// nil. The offset (in bytes read from r) and length of the Header
// field are returned so the caller can seek into a spooled copy of r
// and read the header JSON without ever materializing it as a []byte
// in memory.
//
// Compared to proto.Unmarshal on the full decompressed []byte,
// WalkBitcoinEnvelope avoids duplicating the block bytes in memory.
// For zcash-family blocks dominated by InputTransactions, peak RAM
// drops from ~2× block size to ~1× block size.
//
// Unknown field numbers — fields present on the wire but not in
// KnownBlockFields or KnownBitcoinBlobFields — cause an error rather
// than a silent skip. The contract test asserts the Known* maps match
// the proto descriptor exactly, so any proto edit that adds a field
// either gets registered in the Known* map and walker switch, or
// fails the contract test / runtime walker.
func WalkBitcoinEnvelope(r io.Reader) (*Block, int64, int64, error) {
	w := &wireWalker{br: bufio.NewReaderSize(r, 64*1024)}
	block := &Block{}
	var headerOffset, headerLength int64 = -1, 0

	for {
		if _, err := w.br.Peek(1); err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, 0, xerrors.Errorf("peek at top-level: %w", err)
		}
		fieldNum, wireType, err := w.readTag()
		if err != nil {
			return nil, 0, 0, xerrors.Errorf("read top-level tag: %w", err)
		}

		switch fieldNum {
		case blockFieldBlockchain:
			v, err := w.readVarint()
			if err != nil {
				return nil, 0, 0, xerrors.Errorf("read blockchain: %w", err)
			}
			block.Blockchain = common.Blockchain(v)
		case blockFieldNetwork:
			v, err := w.readVarint()
			if err != nil {
				return nil, 0, 0, xerrors.Errorf("read network: %w", err)
			}
			block.Network = common.Network(v)
		case blockFieldMetadata:
			b, err := w.readLengthDelimited()
			if err != nil {
				return nil, 0, 0, xerrors.Errorf("read metadata: %w", err)
			}
			block.Metadata = &BlockMetadata{}
			if err := proto.Unmarshal(b, block.Metadata); err != nil {
				return nil, 0, 0, xerrors.Errorf("unmarshal metadata: %w", err)
			}
		case blockFieldSideChain:
			v, err := w.readVarint()
			if err != nil {
				return nil, 0, 0, xerrors.Errorf("read side_chain: %w", err)
			}
			block.SideChain = SideChain(v)
		case blockFieldBitcoin:
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
		case blockFieldTransactionMetadata,
			blockFieldEthereum,
			blockFieldRosetta,
			blockFieldSolana,
			blockFieldAptos,
			blockFieldEthereumBeacon:
			// Known fields we do not process. Skip the value but do
			// NOT fall into the unknown-field branch below.
			if err := w.skipValue(wireType); err != nil {
				return nil, 0, 0, xerrors.Errorf("skip Block field %d (wt=%d): %w", fieldNum, wireType, err)
			}
		default:
			return nil, 0, 0, xerrors.Errorf("unknown Block field %d (wt=%d): proto changed; update KnownBlockFields + switch in blockchain_bitcoin.stream.go", fieldNum, wireType)
		}
	}

	return block, headerOffset, headerLength, nil
}

func (w *wireWalker) walkBitcoinBlob(blob *BitcoinBlobdata, endPos int64) (int64, int64, error) {
	var headerOffset, headerLength int64 = -1, 0

	for w.pos < endPos {
		fieldNum, wireType, err := w.readTag()
		if err != nil {
			return 0, 0, xerrors.Errorf("read bitcoin tag: %w", err)
		}

		switch fieldNum {
		case bitcoinBlobFieldHeader:
			length, err := w.readVarint()
			if err != nil {
				return 0, 0, xerrors.Errorf("read header length: %w", err)
			}
			// Record position of header bytes in the underlying stream.
			headerOffset = w.pos
			headerLength = int64(length)
			// Advance past the header bytes without materializing them.
			// The teed underlying reader still writes them to the spool
			// file on the way through.
			if err := w.discardN(int64(length)); err != nil {
				return 0, 0, xerrors.Errorf("discard header bytes: %w", err)
			}
		case bitcoinBlobFieldInputTransactions:
			b, err := w.readLengthDelimited()
			if err != nil {
				return 0, 0, xerrors.Errorf("read input_transactions: %w", err)
			}
			rb := &RepeatedBytes{}
			if err := proto.Unmarshal(b, rb); err != nil {
				return 0, 0, xerrors.Errorf("unmarshal RepeatedBytes: %w", err)
			}
			blob.InputTransactions = append(blob.InputTransactions, rb)
		case bitcoinBlobFieldRawInputTransactions:
			// Map entry: one key/value pair per repetition. We keep
			// it populated for fidelity with proto.Unmarshal.
			b, err := w.readLengthDelimited()
			if err != nil {
				return 0, 0, xerrors.Errorf("read raw_input_transactions: %w", err)
			}
			k, v, err := decodeStringBytesMapEntry(b)
			if err != nil {
				return 0, 0, xerrors.Errorf("decode raw_input_transactions map entry: %w", err)
			}
			if blob.RawInputTransactions == nil {
				blob.RawInputTransactions = make(map[string][]byte)
			}
			blob.RawInputTransactions[k] = v
		default:
			return 0, 0, xerrors.Errorf("unknown BitcoinBlobdata field %d (wt=%d): proto changed; update KnownBitcoinBlobFields + switch in blockchain_bitcoin.stream.go", fieldNum, wireType)
		}
	}

	if w.pos != endPos {
		return 0, 0, xerrors.Errorf("bitcoin blob consumed %d bytes, expected %d", w.pos, endPos)
	}
	return headerOffset, headerLength, nil
}

// decodeStringBytesMapEntry decodes a proto map<string, bytes> entry
// message. Map entries have field 1 = key, field 2 = value.
func decodeStringBytesMapEntry(b []byte) (string, []byte, error) {
	var key string
	var val []byte
	br := bufReader(b)
	w := &wireWalker{br: bufio.NewReader(&br)}
	for w.pos < int64(len(b)) {
		fieldNum, wireType, err := w.readTag()
		if err != nil {
			return "", nil, err
		}
		switch fieldNum {
		case 1: // key (string)
			s, err := w.readLengthDelimited()
			if err != nil {
				return "", nil, err
			}
			key = string(s)
		case 2: // value (bytes)
			s, err := w.readLengthDelimited()
			if err != nil {
				return "", nil, err
			}
			val = s
		default:
			if err := w.skipValue(wireType); err != nil {
				return "", nil, err
			}
		}
	}
	return key, val, nil
}

type wireWalker struct {
	br  *bufio.Reader
	pos int64 // running count of bytes consumed from br
}

// bufReader wraps a []byte as a simple io.Reader (no allocation).
type bufReader []byte

func (b *bufReader) Read(p []byte) (int, error) {
	if len(*b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, *b)
	*b = (*b)[n:]
	return n, nil
}

// readTag reads a proto tag varint and decomposes it into a field
// number and wire type.
func (w *wireWalker) readTag() (fieldNum protoreflect.FieldNumber, wireType int, err error) {
	tag, err := w.readVarint()
	if err != nil {
		return 0, 0, err
	}
	return protoreflect.FieldNumber(tag >> 3), int(tag & 0x7), nil
}

// readVarint reads a proto base-128 varint (up to 10 bytes).
func (w *wireWalker) readVarint() (uint64, error) {
	var v uint64
	var shift uint
	for i := 0; i < 10; i++ {
		b, err := w.br.ReadByte()
		if err != nil {
			if err == io.EOF && i > 0 {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		w.pos++
		v |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return v, nil
		}
		shift += 7
	}
	return 0, xerrors.New("varint too long")
}

// readLengthDelimited reads a length-prefixed byte slice into memory.
// Only used for small fields; large payloads (like BitcoinBlobdata.Header)
// are skipped via discardN instead.
func (w *wireWalker) readLengthDelimited() ([]byte, error) {
	length, err := w.readVarint()
	if err != nil {
		return nil, err
	}
	if length > (1 << 30) {
		return nil, xerrors.Errorf("length-delimited payload too large: %d", length)
	}
	b := make([]byte, length)
	n, err := io.ReadFull(w.br, b)
	w.pos += int64(n)
	if err != nil {
		return nil, xerrors.Errorf("readLengthDelimited: %w", err)
	}
	return b, nil
}

// discardN reads and discards the next n bytes.
func (w *wireWalker) discardN(n int64) error {
	m, err := io.CopyN(io.Discard, w.br, n)
	w.pos += m
	if err != nil {
		return xerrors.Errorf("discardN(%d): %w", n, err)
	}
	return nil
}

// skipValue advances past a field value whose wire type is known but
// whose contents we do not care about.
func (w *wireWalker) skipValue(wireType int) error {
	switch wireType {
	case wireVarint:
		_, err := w.readVarint()
		return err
	case wireFixed64:
		n, err := io.CopyN(io.Discard, w.br, 8)
		w.pos += n
		return err
	case wireBytes:
		length, err := w.readVarint()
		if err != nil {
			return err
		}
		return w.discardN(int64(length))
	case wireFixed32:
		n, err := io.CopyN(io.Discard, w.br, 4)
		w.pos += n
		return err
	case wireStartGroup, wireEndGroup:
		return xerrors.Errorf("group wire type %d is not supported (proto2 only)", wireType)
	default:
		return xerrors.Errorf("unknown wire type %d", wireType)
	}
}
