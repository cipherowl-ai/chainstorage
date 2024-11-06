package ethereum

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	restapimocks "github.com/coinbase/chainstorage/internal/blockchain/restapi/mocks"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	tronClientTestSuite struct {
		suite.Suite

		ctrl       *gomock.Controller
		app        testapp.TestApp
		rpcClient  *jsonrpcmocks.MockClient
		restClient *restapimocks.MockClient
		client     internal.Client
	}
)

const (
	tronTestTag = uint32(2)
	// tronTestHeight = uint64(10000)
	tronTestHeight             = ethereumHeight
	fixtureBlockTxInfoResponse = `
	[
		{
			"blockNumber": 11322000,
			"contractResult": [
				""
			],
			"blockTimeStamp": 1725466323000,
			"receipt": {
				"net_usage": 268
			},
			"id": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b"
		},
		{
			"id": "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
			"blockNumber": 11322000,
			"internal_transactions": [
				{
					"hash": "27b42aff06882822a0c84211121e5f98c06a9b074ee84a085c998397b8b2da3a",
					"caller_address": "4158baea0b354f7b333b3b1563c849e979ae4e2002",
					"transferTo_address": "41eed9e56a5cddaa15ef0c42984884a8afcf1bdebb",
					"callValueInfo": [
						{}
					],
					"note": "63616c6c"
				},
				{
					"hash": "3e2b8ca208f6c899afdc74b772a4504cdd6704bbeff6d34045351c9ad83f478d",
					"caller_address": "4158baea0b354f7b333b3b1563c849e979ae4e2002",
					"transferTo_address": "41f5a6eae2fb24b0bda6288e346982fc14e094c19a",
					"callValueInfo": [
						{
							"callValue": 405000000
						}
					],
					"note": "63616c6c"
				}
			]
		}
	]
	`
)

func TestTronClientTestSuite(t *testing.T) {
	suite.Run(t, new(tronClientTestSuite))
}

func (s *tronClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)
	s.restClient = restapimocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.app = testapp.New(
		s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_TRON, common.Network_NETWORK_TRON_MAINNET),
		Module,
		// jsonrpc.Module,
		// restapi.Module,
		testTronApiModule(s.rpcClient, s.restClient),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *tronClientTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func testTronApiModule(rpcClient *jsonrpcmocks.MockClient, restClient *restapimocks.MockClient) fx.Option {
	return fx.Options(
		internal.Module,
		fx.Provide(fx.Annotated{
			Name:   "master",
			Target: func() jsonrpc.Client { return rpcClient },
		}),
		fx.Provide(fx.Annotated{
			Name:   "slave",
			Target: func() jsonrpc.Client { return rpcClient },
		}),
		fx.Provide(fx.Annotated{
			Name:   "validator",
			Target: func() jsonrpc.Client { return rpcClient },
		}),
		fx.Provide(fx.Annotated{
			Name:   "consensus",
			Target: func() jsonrpc.Client { return rpcClient },
		}),
		fx.Provide(fx.Annotated{
			Name:   "additional",
			Target: func() restapi.Client { return restClient },
		}),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
	)
}

func (s *tronClientTestSuite) TestTronClient_New() {
	require := testutil.Require(s.T())

	var tronClientResult TronClientParams
	var clientResutl internal.ClientParams
	app := testapp.New(
		s.T(),
		Module,
		internal.Module,
		jsonrpc.Module,
		restapi.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_TRON, common.Network_NETWORK_TRON_MAINNET),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
		fx.Populate(&tronClientResult),
		fx.Populate(&clientResutl),
	)
	defer app.Close()

	require.NotNil(s.client)
	require.NotNil(tronClientResult.AdditionalClient)
	s.NotNil(clientResutl.Master)
	s.NotNil(clientResutl.Slave)
	s.NotNil(clientResutl.Validator)
	s.NotNil(clientResutl.Consensus)
}

func (s *tronClientTestSuite) TestTronClient_GetBlockByHeight() {
	require := testutil.Require(s.T())
	// mock block jsonrpc request --------------------
	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)
	// mock TxReceipt jsonrpc request --------------------
	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	// mock BlockTxInfo restapi request --------------------
	blockTxInfoPostData := TronBlockTxInfoRequestData{Num: ethereumHeight}
	postData, _ := json.Marshal(blockTxInfoPostData)
	txr := json.RawMessage(fixtureBlockTxInfoResponse)
	s.restClient.EXPECT().Call(gomock.Any(), tronTxInfoMethod, postData).Return(txr, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tronTestTag, tronTestHeight)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_TRON, block.Blockchain)
	require.Equal(common.Network_NETWORK_TRON_MAINNET, block.Network)

	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(ethereumHash, metadata.Hash)
	require.Equal(ethereumParentHash, metadata.ParentHash)
	require.Equal(ethereumHeight, metadata.Height)
	require.Equal(ethereumParentHeight, metadata.ParentHeight)
	require.Equal(tronTestTag, metadata.Tag)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))
	require.NotNil(blobdata.TransactionTraces[0])
	require.NotNil(blobdata.TransactionTraces[1])
	require.Nil(blobdata.Uncles)
}

// TODO: add test case for TronClient.getBlockTraces
