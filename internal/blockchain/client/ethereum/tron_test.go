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
			"id": "baa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b"
		},
		{
			"id": "f5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
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
	fixtureBlockTxResponse = `
	{
		"blockID": "000000000408a36cd32a8e674045e96f895b7708b85fa5141f3c8fd92eb497a8",
		"block_header": {
			"raw_data": {
				"number": 67674988,
				"txTrieRoot": "08203d3094277ae2bfc9981bde29400a87ab5bc6b9aa807ce42d96a2de5ea109",
				"witness_address": "417f5e5aca5332ce5e18414d7f85bb62097cefa453",
				"parentHash": "000000000408a36b033d241520fd155cb0351a27bce043ddb7799ec2790ca1ee",
				"version": 31,
				"timestamp": 1733675346000
			},
			"witness_signature": "241ea0cb69f7e3dd1436c03c557f2b4a005f6ca315d968a11c1115324f795f2875883db4c91e74a0638c9100a4ced4196080fdaf6077881936636e6169d0fbb801"
		},
		"transactions": [
			{
				"ret": [
					{
						"contractRet": "SUCCESS"
					}
				],
				"signature": [
					"e93160d1df484382923db34f02ca196a7dbbd7342948214a739fdf4b96896cfc2ca4d9b706c2393c3f83d269f31901aee43ae8e50a04579b6636c03c321da8e000"
				],
				"txID": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
				"raw_data": {
					"contract": [
						{
							"parameter": {
								"value": {
									"amount": 6,
									"owner_address": "4174d7980a2a3e48e3a863365542e92ab8d646e0aa",
									"to_address": "41c3d34597fb01e25d4d8e7ef34ccdd0f05bf85473"
								},
								"type_url": "type.googleapis.com/protocol.TransferContract"
							},
							"type": "TransferContract"
						}
					],
					"ref_block_bytes": "a358",
					"ref_block_hash": "c773a99ac91938c6",
					"expiration": 1733675400000,
					"timestamp": 1733675342465
				},
				"raw_data_hex": "0a02a3582208c773a99ac91938c640c0f6ecb8ba325a65080112610a2d747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e747261637412300a154174d7980a2a3e48e3a863365542e92ab8d646e0aa121541c3d34597fb01e25d4d8e7ef34ccdd0f05bf8547318067081b5e9b8ba32"
			},
			{
				"ret": [
					{
						"contractRet": "SUCCESS"
					}
				],
				"signature": [
					"27a73675483a50fa52972e7c5ffe33e931ffad1a037a79c3ff2ab97cf08315b557a723d3fc87d5a1e81165775fdfba4b14c7aab31ccd74d06d36bcdd4615f18a1c"
				],
				"txID": "f5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
				"raw_data": {
					"contract": [
						{
							"parameter": {
								"value": {
									"balance": 248167143,
									"receiver_address": "414c614b77e81392450d88f9e339481843abae0e34",
									"owner_address": "410611d6c1d784930f53979f06f10306251919e1ea"
								},
								"type_url": "type.googleapis.com/protocol.UnDelegateResourceContract"
							},
							"type": "UnDelegateResourceContract"
						}
					],
					"ref_block_bytes": "a343",
					"ref_block_hash": "92d31415885cc680",
					"expiration": 1733761442497,
					"fee_limit": 30000000,
					"timestamp": 1733675342497
				},
				"raw_data_hex": "0a02a343220892d31415885cc68040c1c5f0e1ba325a72083a126e0a37747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e556e44656c65676174655265736f75726365436f6e747261637412330a15410611d6c1d784930f53979f06f10306251919e1ea18e7f5aa762215414c614b77e81392450d88f9e339481843abae0e3470a1b5e9b8ba3290018087a70e"
			}
		]
	}
	`
	// Two TransferContract transactions of block 87573393: the first carries a memo (raw_data.data), the second doesn't.
	fixtureBlockTxInfoWithMemoResponse = `
	[
		{
			"id": "4b1345bbc3610321d041619b80e52f16ddfde9108b814ac5af0b7c1ea9240446",
			"fee": 1000000,
			"blockNumber": 87573393,
			"blockTimeStamp": 1793393586000,
			"contractResult": [
				""
			],
			"receipt": {
				"net_usage": 302
			}
		},
		{
			"id": "61cba69bd9b6fb70692aca4f7cdfc45ccc95822da2c00e9b88c5101c6a603ed3",
			"fee": 268000,
			"blockNumber": 87573393,
			"blockTimeStamp": 1793393586000,
			"contractResult": [
				""
			],
			"receipt": {
				"net_fee": 268000
			}
		}
	]
	`
	fixtureBlockTxWithMemoResponse = `
	{
		"blockID": "000000000538439184c6ad65ee294d0dac4651041c8c76c3764b28db9008058a",
		"transactions": [
			{
				"raw_data": {
					"ref_block_bytes": "4390",
					"ref_block_hash": "1bc6674ccdf3c01c",
					"expiration": 1793393643000,
					"data": "6be029c1fa65e2b3a98e23b325b5d4da1c1ee32b39c7ddf2a20ed685776e280b",
					"contract": [
						{
							"parameter": {
								"value": {
									"owner_address": "41f70da97812cb96acdf810712aa562db8dfa3dbef",
									"to_address": "4176f171569726e59d53d0eda4b6650d495c14f10d",
									"amount": 12023410
								},
								"type_url": "type.googleapis.com/protocol.TransferContract"
							},
							"type": "TransferContract"
						}
					],
					"timestamp": 1793393583000
				},
				"signature": [
					"e6d57305dc56d73845a178f57722b2ab55fb49223be22ac89be77b5425bff03c20a8248e284d53c612dbbc1740cc7b9217885929722d00ebe58b22a6b03070641b"
				],
				"ret": [
					{
						"contractRet": "SUCCESS"
					}
				],
				"raw_data_hex": "0a02439022081bc6674ccdf3c01c40f89bddf4983452206be029c1fa65e2b3a98e23b325b5d4da1c1ee32b39c7ddf2a20ed685776e280b5a68080112640a2d747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e747261637412330a1541f70da97812cb96acdf810712aa562db8dfa3dbef12154176f171569726e59d53d0eda4b6650d495c14f10d18f2ecdd057098c7d9f49834",
				"txID": "4b1345bbc3610321d041619b80e52f16ddfde9108b814ac5af0b7c1ea9240446"
			},
			{
				"raw_data": {
					"ref_block_bytes": "4390",
					"ref_block_hash": "1bc6674ccdf3c01c",
					"expiration": 1793429583000,
					"contract": [
						{
							"parameter": {
								"value": {
									"owner_address": "4191a048c65200d96bffec3ac903c92155f085a32b",
									"to_address": "4129228e5d382b7a2c2a0dbc05e6d6507f4dbb3bbe",
									"amount": 5072200
								},
								"type_url": "type.googleapis.com/protocol.TransferContract"
							},
							"type": "TransferContract"
						}
					],
					"timestamp": 1793393583000
				},
				"signature": [
					"ca219de01b3013ee3d144305058063340174f80114948322b29a16bf6ecfcbb5406c99d7f62964b0a81bb403e8d835c64957449b6093df90e9087b4271d58dc900"
				],
				"ret": [
					{
						"contractRet": "SUCCESS"
					}
				],
				"raw_data_hex": "0a02439022081bc6674ccdf3c01c4098e9ee8599345a68080112640a2d747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e747261637412330a154191a048c65200d96bffec3ac903c92155f085a32b12154129228e5d382b7a2c2a0dbc05e6d6507f4dbb3bbe18c8cab5027098c7d9f49834",
				"txID": "61cba69bd9b6fb70692aca4f7cdfc45ccc95822da2c00e9b88c5101c6a603ed3"
			}
		]
	}
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
	blockTxInfoPostData := tronBlockNumRequestData{Num: ethereumHeight}
	postData, _ := json.Marshal(blockTxInfoPostData)
	txr := json.RawMessage(fixtureBlockTxInfoResponse)
	s.restClient.EXPECT().Call(gomock.Any(), tronTxInfoMethod, postData).Return(txr, nil)

	// mock BlockTx restapi request --------------------
	blockTxPostData := tronBlockNumRequestData{Num: ethereumHeight}
	postData, _ = json.Marshal(blockTxPostData)
	txr = json.RawMessage(fixtureBlockTxResponse)
	s.restClient.EXPECT().Call(gomock.Any(), tronBlockTxMethod, postData).Return(txr, nil)

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

func (s *tronClientTestSuite) TestTronClient_GetBlockByHeight_Memo() {
	require := testutil.Require(s.T())
	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)
	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	postData, _ := json.Marshal(tronBlockNumRequestData{Num: ethereumHeight})
	s.restClient.EXPECT().Call(gomock.Any(), tronTxInfoMethod, postData).Return(json.RawMessage(fixtureBlockTxInfoWithMemoResponse), nil)
	s.restClient.EXPECT().Call(gomock.Any(), tronBlockTxMethod, postData).Return(json.RawMessage(fixtureBlockTxWithMemoResponse), nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tronTestTag, tronTestHeight)
	require.NoError(err)

	traces := block.GetEthereum().TransactionTraces
	require.Equal(2, len(traces))

	var withMemo map[string]any
	require.NoError(json.Unmarshal(traces[0], &withMemo))
	require.Equal("4b1345bbc3610321d041619b80e52f16ddfde9108b814ac5af0b7c1ea9240446", withMemo["id"])
	require.Equal("TransferContract", withMemo["type"])
	require.Equal("6be029c1fa65e2b3a98e23b325b5d4da1c1ee32b39c7ddf2a20ed685776e280b", withMemo["memo"])

	var withoutMemo map[string]any
	require.NoError(json.Unmarshal(traces[1], &withoutMemo))
	require.Equal("61cba69bd9b6fb70692aca4f7cdfc45ccc95822da2c00e9b88c5101c6a603ed3", withoutMemo["id"])
	require.Equal("TransferContract", withoutMemo["type"])
	require.NotContains(withoutMemo, "memo")
}

func (s *tronClientTestSuite) TestTronClient_ExtractTransactionTypes_Memo() {
	require := testutil.Require(s.T())
	client := &TronClient{}

	txTypeMap, txMemoMap, err := client.extractTransactionTypes([]byte(`{"transactions": [
		{"txID": "aa", "raw_data": {"data": "68656c6c6f", "contract": [{"type": "TransferContract"}]}},
		{"txID": "bb", "raw_data": {"data": "", "contract": [{"type": "TransferContract"}]}},
		{"txID": "cc", "raw_data": {"contract": [{"type": "TriggerSmartContract"}]}},
		{"txID": "dd", "raw_data": {"data": null, "contract": [{"type": "TransferContract"}]}}
	]}`))
	require.NoError(err)
	require.Equal(map[string]string{"aa": "TransferContract", "bb": "TransferContract", "cc": "TriggerSmartContract", "dd": "TransferContract"}, txTypeMap)
	require.Equal(map[string]string{"aa": "68656c6c6f"}, txMemoMap)

	_, _, err = client.extractTransactionTypes([]byte(`{"transactions": [
		{"txID": "aa", "raw_data": {"data": 1, "contract": [{"type": "TransferContract"}]}}
	]}`))
	require.Error(err)
	require.Contains(err.Error(), "raw_data.data is not a string")

	_, _, err = client.extractTransactionTypes([]byte(`{"transactions": [
		{"txID": "aa", "raw_data": {"data": "hello", "contract": [{"type": "TransferContract"}]}}
	]}`))
	require.Error(err)
	require.Contains(err.Error(), "raw_data.data of transaction aa is not hex")
}
