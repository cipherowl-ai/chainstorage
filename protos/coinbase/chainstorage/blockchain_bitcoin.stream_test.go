package chainstorage_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// TestWalkBitcoinEnvelope_Parity compares WalkBitcoinEnvelope output
// against proto.Unmarshal on a small hand-built bitcoin block.
func TestWalkBitcoinEnvelope_Parity(t *testing.T) {
	require := require.New(t)

	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    1,
			Height: 696402,
			Hash:   "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e",
		},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{"hash":"000000","height":696402,"tx":[{"txid":"abc"}]}`),
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{[]byte(`{"txid":"prev1"}`), []byte(`{"txid":"prev2"}`)}},
					{Data: [][]byte{[]byte(`{"txid":"prev3"}`)}},
				},
			},
		},
	}

	raw, err := proto.Marshal(original)
	require.NoError(err)

	r := bytes.NewReader(raw)
	block, headerOffset, headerLength, err := api.WalkBitcoinEnvelope(r)
	require.NoError(err)

	require.Equal(original.Blockchain, block.Blockchain)
	require.Equal(original.Network, block.Network)
	require.Equal(original.Metadata.Tag, block.Metadata.Tag)
	require.Equal(original.Metadata.Height, block.Metadata.Height)
	require.Equal(original.Metadata.Hash, block.Metadata.Hash)

	blob := block.GetBitcoin()
	require.NotNil(blob)
	require.Nil(blob.Header, "header must be left nil — walker exposes it via offset")

	require.Equal(len(original.GetBitcoin().InputTransactions), len(blob.InputTransactions))
	for i := range original.GetBitcoin().InputTransactions {
		require.Equal(original.GetBitcoin().InputTransactions[i].Data, blob.InputTransactions[i].Data)
	}

	require.Greater(headerOffset, int64(0))
	require.Equal(int64(len(original.GetBitcoin().Header)), headerLength)
	require.Equal(original.GetBitcoin().Header, raw[headerOffset:headerOffset+headerLength])
}

// TestWalkBitcoinEnvelope_Fixture walks the proto-marshaled form of a
// real bitcoin block fixture to catch ordering and field-number
// surprises.
func TestWalkBitcoinEnvelope_Fixture(t *testing.T) {
	require := require.New(t)

	header, err := fixtures.ReadFile("parser/bitcoin/get_block.json")
	require.NoError(err)
	tx1, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction.json")
	require.NoError(err)
	tx2, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction_tx2.json")
	require.NoError(err)

	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 696402},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{}},
					{Data: [][]byte{tx1, tx2}},
				},
			},
		},
	}
	raw, err := proto.Marshal(original)
	require.NoError(err)

	block, headerOffset, headerLength, err := api.WalkBitcoinEnvelope(bytes.NewReader(raw))
	require.NoError(err)
	require.Equal(int64(len(header)), headerLength)
	require.Equal(header, raw[headerOffset:headerOffset+headerLength])
	require.Nil(block.GetBitcoin().Header)
	require.Equal(2, len(block.GetBitcoin().InputTransactions))
	require.Equal([][]byte{tx1, tx2}, block.GetBitcoin().InputTransactions[1].Data)
}

// TestWalkBitcoinEnvelope_UnknownBlockField ensures the walker errors
// on a field number that is not registered in its known set, rather
// than silently skipping.
func TestWalkBitcoinEnvelope_UnknownBlockField(t *testing.T) {
	require := require.New(t)

	// Hand-craft a proto wire payload with a single unknown field
	// (number 77, wire type 0 varint).
	buf := []byte{
		// tag = (77 << 3) | 0 = 616 → varint 0xE8, 0x04
		0xE8, 0x04,
		0x2A, // value: varint 42
	}
	_, _, _, err := api.WalkBitcoinEnvelope(bytes.NewReader(buf))
	require.Error(err)
	require.Contains(err.Error(), "unknown Block field 77")
}

// TestWalkBitcoinEnvelope_UnknownBitcoinBlobField checks the same at
// the BitcoinBlobdata level.
func TestWalkBitcoinEnvelope_UnknownBitcoinBlobField(t *testing.T) {
	require := require.New(t)

	// Block.bitcoin = {unknown_field_99 = 1}
	// Outer tag (field 101, wiretype 2) = 0xAA 0x06
	// length = 3; inner: field 99 varint 1 = 0x98 0x06 0x01
	buf := []byte{0xAA, 0x06, 0x03, 0x98, 0x06, 0x01}
	_, _, _, err := api.WalkBitcoinEnvelope(bytes.NewReader(buf))
	require.Error(err)
	require.Contains(err.Error(), "unknown BitcoinBlobdata field 99")
}

// TestWalkBitcoinEnvelope_TeedToFile verifies the tee pattern.
func TestWalkBitcoinEnvelope_TeedToFile(t *testing.T) {
	require := require.New(t)

	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 42},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{"hash":"ab","tx":[{"txid":"xyz"}]}`),
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{[]byte("prev1")}},
				},
			},
		},
	}
	raw, err := proto.Marshal(original)
	require.NoError(err)

	spool, err := os.CreateTemp(t.TempDir(), "chainstorage-spool-*.bin")
	require.NoError(err)
	defer spool.Close()

	tee := io.TeeReader(bytes.NewReader(raw), spool)
	block, headerOffset, headerLength, err := api.WalkBitcoinEnvelope(tee)
	require.NoError(err)
	require.NoError(spool.Sync())

	f, err := os.Open(spool.Name())
	require.NoError(err)
	defer f.Close()
	_, err = f.Seek(headerOffset, io.SeekStart)
	require.NoError(err)
	got, err := io.ReadAll(io.LimitReader(f, headerLength))
	require.NoError(err)
	require.Equal(original.GetBitcoin().Header, got)

	full, err := os.ReadFile(spool.Name())
	require.NoError(err)
	require.Equal(raw, full)

	require.Equal(1, len(block.GetBitcoin().InputTransactions))
	require.Equal([][]byte{[]byte("prev1")}, block.GetBitcoin().InputTransactions[0].Data)
}
