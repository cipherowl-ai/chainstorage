package ethereum

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type tronParserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  internal.Parser
}

func TestTronParserTestSuite(t *testing.T) {
	//suite.Run(t, new(tronParserTestSuite))
}

func (s *tronParserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser internal.Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_TRON, common.Network_NETWORK_TRON_MAINNET),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *tronParserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *tronParserTestSuite) TestParseTronBlock() {
	require := testutil.Require(s.T())

	fixtureHeader := fixtures.MustReadFile("parser/tron/raw_block_header.json")

	rawReceipts, err := s.fixtureParsingHelper("parser/tron/raw_block_tx_receipt.json")
	require.NoError(err)

	rawTraces, err := s.fixtureParsingHelper("parser/tron/raw_block_trace_tx_info.json")
	require.NoError(err)

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
				Header:              fixtureHeader,
				TransactionReceipts: rawReceipts,
				TransactionTraces:   rawTraces,
			},
		},
	}

	expectedHeader := &api.EthereumHeader{
		Hash:       "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
		ParentHash: "0x0000000004034f5b43c5934257b3d1f1a313bba4af0a4dd2f778fda9e641b615",
		Number:     0x4034F5C,
		Timestamp:  &timestamppb.Timestamp{Seconds: 1732627338},
		Transactions: []string{
			"0xd581afa9158fbed69fb10d6a2245ad45d912a3da03ff24d59f3d2f6df6fd9529",
			"0xe14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
		},
		Nonce:            "0x0000000000000000",
		Sha3Uncles:       "0x0000000000000000000000000000000000000000000000000000000000000000",
		LogsBloom:        "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
		TransactionsRoot: "0xd270690faa58558c2b03ae600334f71f9d5a0ad42d7313852fb3742e8576eec9",
		StateRoot:        "0x",
		ReceiptsRoot:     "0x0000000000000000000000000000000000000000000000000000000000000000",
		Miner:            "0x8b0359acac03bac62cbf89c4b787cb10b3c3f513",
		TotalDifficulty:  "0",
		ExtraData:        "0x",
		Size:             0x1a366,
		GasLimit:         0x2b3b43dc6,
		GasUsed:          0xb1006d,
		MixHash:          "0x0000000000000000000000000000000000000000000000000000000000000000",
		OptionalBaseFeePerGas: &api.EthereumHeader_BaseFeePerGas{
			BaseFeePerGas: uint64(0),
		},
	}

	expectedFlattenedTraces := []*api.EthereumTransactionFlattenedTrace{
		{
			Type:             "CALL",
			From:             "0x41c60a6f5c81431c97ed01b61698b6853557f3afd4",
			To:               "0x41c60a6f5c81431c97ed01b61698b6853557f3afd4",
			Value:            "200",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "0x499bdbdfaae021dd510c70b433bc48d88d8ca6e0b7aee13ce6d726114e365aaf",
			Status:           1,
			BlockHash:        "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "0xe14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
		},
		{
			Type:             "CALL",
			From:             "0x41c60a6f5c81431c97ed01b61698b6853557f3afd4",
			To:               "0x41e8667633c747066c70672c58207cc745a9860527",
			Value:            "0",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "0x997225b56440a9bd172f05f44a663830b72093a12502551cda99b0bc7c60cbc1",
			Status:           1,
			BlockHash:        "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "0xe14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
		},
		{
			Type:             "CALL",
			From:             "0x41c60a6f5c81431c97ed01b61698b6853557f3afd4",
			To:               "0x41e8667633c747066c70672c58207cc745a9860527",
			Value:            "0",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "0x7ac8dd16dede5c512330f5033c8fd6f5390d742aa51b805f805098109eb54fe9",
			Status:           1,
			BlockHash:        "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "0xe14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
		},
		{
			Type:             "CALL",
			From:             "0x41c60a6f5c81431c97ed01b61698b6853557f3afd4",
			To:               "0x41c64e69acde1c7b16c2a3efcdbbdaa96c3644c2b3",
			Value:            "0",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "0xcf6f699d9bdae8aa25fae310a06bb60a29a7812548cf3c1d83c737fd1a22c0ee",
			Status:           1,
			BlockHash:        "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "0xe14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
		},
		{
			Type:             "CALL",
			From:             "0x41c64e69acde1c7b16c2a3efcdbbdaa96c3644c2b3",
			To:               "0x41c64e69acde1c7b16c2a3efcdbbdaa96c3644c2b3",
			Value:            "0",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "0x95787b9a6558c7b6b624d0c1bece9723a7f4c3d414010b6ac105ae5f5aebffbc",
			Status:           1,
			BlockHash:        "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "0xe14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
		},
		{
			Type:             "CALL",
			From:             "0x41c64e69acde1c7b16c2a3efcdbbdaa96c3644c2b3",
			To:               "0x414d12f87c18a914dddbc2b27f378ad126a79b76b6",
			Value:            "822996311610",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "0x14526162e31d969ef0dca9b902d51ecc0ffab87dc936dce62022f368119043af",
			Status:           1,
			BlockHash:        "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "0xe14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
		},
		{
			Type:             "CALL",
			From:             "0x41c60a6f5c81431c97ed01b61698b6853557f3afd4",
			To:               "0x41e8667633c747066c70672c58207cc745a9860527",
			Value:            "0",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "0x8e088220a26ca8d794786e78096e71259cf8744cccdc4f07a8129aa8ee29bb98",
			Status:           1,
			BlockHash:        "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "0xe14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
		},
		{
			Type:             "CALL",
			From:             "0x41c60a6f5c81431c97ed01b61698b6853557f3afd4",
			To:               "0x4189ae01b878dffc8088222adf1fb08ebadfeea53a",
			Value:            "1424255258",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "0x83b1d41ba953aab4da6e474147f647599ea53bb3213306897127b57e85ddd1ca",
			Status:           1,
			BlockHash:        "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "0xe14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
		},
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_TRON, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_TRON_MAINNET, nativeBlock.Network)
	actualBlock := nativeBlock.GetEthereum()
	require.NotNil(actualBlock)
	require.Equal(expectedHeader, actualBlock.Header)

	require.Equal(2, len(actualBlock.Transactions))
	tx := actualBlock.Transactions[1]
	require.Equal(expectedFlattenedTraces, tx.FlattenedTraces)
}

func (s *tronParserTestSuite) fixtureParsingHelper(filePath string) ([][]byte, error) {

	fixtureParityTrace, _ := fixtures.ReadFile(filePath)

	var tmpItems []json.RawMessage
	err := json.Unmarshal(fixtureParityTrace, &tmpItems)

	items := make([][]byte, len(tmpItems))
	for i, item := range tmpItems {
		items[i] = item
	}
	return items, err
}
