package ethereum

import (
	"context"
	"encoding/json"
	"testing"

	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func newHeaderOnlyTestParser(t *testing.T, blockchain common.Blockchain, network common.Network) internal.Parser {
	var parser internal.Parser
	app := testapp.New(
		t,
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(blockchain, network),
		fx.Populate(&parser),
	)
	t.Cleanup(app.Close)
	return parser
}

func ethereumHeaderOnlyTestBlock(header []byte) *api.Block {
	return &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   ethereumMetadata,
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              header,
				TransactionReceipts: [][]byte{fixtures.MustReadFile("parser/ethereum/raw_block_receipt.json")},
				TransactionTraces:   [][]byte{fixtures.MustReadFile("parser/ethereum/raw_block_traces.json")},
			},
		},
	}
}

// Header-only output must equal the full parse minus receipts, token
// transfers and flattened traces.
func requireHeaderOnlyMatchesFull(t *testing.T, parser internal.Parser, block *api.Block) *api.NativeBlock {
	require := testutil.Require(t)

	full, err := parser.ParseNativeBlock(context.Background(), proto.Clone(block).(*api.Block))
	require.NoError(err)
	headerOnly, err := parser.ParseNativeBlock(context.Background(), proto.Clone(block).(*api.Block), internal.WithHeaderOnly())
	require.NoError(err)

	expected := proto.Clone(full).(*api.NativeBlock)
	for _, tx := range expected.GetEthereum().Transactions {
		tx.Receipt = nil
		tx.TokenTransfers = nil
		tx.FlattenedTraces = nil
	}
	require.True(proto.Equal(expected, headerOnly), "header-only block differs from the trimmed full parse")
	return headerOnly
}

func TestParseEthereumBlock_HeaderOnly(t *testing.T) {
	parser := newHeaderOnlyTestParser(t, common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET)
	actual := requireHeaderOnlyMatchesFull(t, parser, ethereumHeaderOnlyTestBlock(fixtureHeader))

	require := testutil.Require(t)
	require.Len(actual.GetEthereum().Transactions, 1)
	tx := actual.GetEthereum().Transactions[0]
	require.Nil(tx.Receipt)
	require.Empty(tx.FlattenedTraces)
	require.Empty(tx.TokenTransfers)
}

func TestParseEthereumBlock_HeaderOnly_PostShanghai(t *testing.T) {
	parser := newHeaderOnlyTestParser(t, common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET)
	header := fixtures.MustReadFile("parser/ethereum/raw_block_header_post_shanghai.json")
	actual := requireHeaderOnlyMatchesFull(t, parser, ethereumHeaderOnlyTestBlock(header))
	testutil.Require(t).NotEmpty(actual.GetEthereum().Header.Withdrawals)
}

func TestParseEthereumBlock_HeaderOnly_IgnoresReceiptsAndTraces(t *testing.T) {
	require := testutil.Require(t)
	parser := newHeaderOnlyTestParser(t, common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET)

	valid, err := parser.ParseNativeBlock(context.Background(), ethereumHeaderOnlyTestBlock(fixtureHeader), internal.WithHeaderOnly())
	require.NoError(err)

	corrupt := ethereumHeaderOnlyTestBlock(fixtureHeader)
	corrupt.GetEthereum().TransactionReceipts = [][]byte{[]byte("not json")}
	corrupt.GetEthereum().TransactionTraces = [][]byte{[]byte("not json")}

	_, err = parser.ParseNativeBlock(context.Background(), proto.Clone(corrupt).(*api.Block))
	require.Error(err)
	actual, err := parser.ParseNativeBlock(context.Background(), corrupt, internal.WithHeaderOnly())
	require.NoError(err)
	require.True(proto.Equal(valid, actual))
}

func TestParsePolygonBlock_HeaderOnly_KeepsAuthor(t *testing.T) {
	parser := newHeaderOnlyTestParser(t, common.Blockchain_BLOCKCHAIN_POLYGON, common.Network_NETWORK_POLYGON_MAINNET)
	block := ethereumHeaderOnlyTestBlock(fixtures.MustReadFile("parser/polygon/polygon_fixture_header.json"))
	block.Blockchain = common.Blockchain_BLOCKCHAIN_POLYGON
	block.Network = common.Network_NETWORK_POLYGON_MAINNET
	block.GetEthereum().ExtraData = &api.EthereumBlobdata_Polygon{
		Polygon: &api.PolygonExtraData{Author: []byte(blockAuthor)},
	}

	actual := requireHeaderOnlyMatchesFull(t, parser, block)
	testutil.Require(t).Equal("0xf0245f6251bef9447a08766b9da2b07b28ad80b0", actual.GetEthereum().Header.GetAuthor())
}

// Tron rewrites header fields from receipt data, so the option must not apply.
func TestParseTronBlock_HeaderOnlyIgnored(t *testing.T) {
	require := testutil.Require(t)
	parser := newHeaderOnlyTestParser(t, common.Blockchain_BLOCKCHAIN_TRON, common.Network_NETWORK_TRON_MAINNET)

	rawItems := func(path string) [][]byte {
		var items []json.RawMessage
		require.NoError(json.Unmarshal(fixtures.MustReadFile(path), &items))
		out := make([][]byte, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_TRON,
		Network:    common.Network_NETWORK_TRON_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:        2,
			Hash:       "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			ParentHash: "0x0000000004034f5b43c5934257b3d1f1a313bba4af0a4dd2f778fda9e641b615",
			Height:     0x4034f5c,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtures.MustReadFile("parser/tron/raw_block_header.json"),
				TransactionReceipts: rawItems("parser/tron/raw_block_tx_receipt.json"),
				TransactionTraces:   rawItems("parser/tron/raw_block_trace_tx_info.json"),
			},
		},
	}

	full, err := parser.ParseNativeBlock(context.Background(), proto.Clone(block).(*api.Block))
	require.NoError(err)
	headerOnly, err := parser.ParseNativeBlock(context.Background(), block, internal.WithHeaderOnly())
	require.NoError(err)
	require.True(proto.Equal(full, headerOnly))
	require.NotNil(headerOnly.GetEthereum().Transactions[0].Receipt)
}
