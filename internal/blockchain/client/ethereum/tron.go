package ethereum

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

type (
	TronClient struct {
		*EthereumClient
		additionalClient restapi.Client
	}

	TronClientParams struct {
		fx.In
		fxparams.Params
		MasterClient     jsonrpc.Client `name:"master"`
		SlaveClient      jsonrpc.Client `name:"slave"`
		ValidatorClient  jsonrpc.Client `name:"validator"`
		ConsensusClient  jsonrpc.Client `name:"consensus"`
		AdditionalClient restapi.Client `name:"additional"`
		DLQ              dlq.DLQ
	}

	tronApiClientFactory struct {
		masterClient    jsonrpc.Client
		slaveClient     jsonrpc.Client
		validatorClient jsonrpc.Client
		consensusClient jsonrpc.Client
		clientFactory   TronApiClientFactoryFn
	}

	TronApiClientFactoryFn func(client jsonrpc.Client) internal.Client
)

type TronBlockTxInfoRequestData struct {
	Num uint64 `json:"num"`
}

var tronTxInfoMethod = &restapi.RequestMethod{
	Name:       "GetTransactionInfoByBlockNum",
	ParamsPath: "/wallet/gettransactioninfobyblocknum", // No parameter URls
	Timeout:    6 * time.Second,
	HTTPMethod: http.MethodPost,
}

func NewTronApiClientFactory(params TronClientParams, clientFactory TronApiClientFactoryFn) internal.ClientFactory {
	return &tronApiClientFactory{
		masterClient:    params.MasterClient,
		slaveClient:     params.SlaveClient,
		validatorClient: params.ValidatorClient,
		consensusClient: params.ConsensusClient,
		clientFactory:   clientFactory,
	}
}

func (f *tronApiClientFactory) Master() internal.Client {
	return f.clientFactory(f.masterClient)
}

func (f *tronApiClientFactory) Slave() internal.Client {
	return f.clientFactory(f.slaveClient)

}

func (f *tronApiClientFactory) Validator() internal.Client {
	return f.clientFactory(f.validatorClient)

}

func (f *tronApiClientFactory) Consensus() internal.Client {
	return f.clientFactory(f.consensusClient)
}

// Tron shares the same data schema as Ethereum since it is an EVM chain, but we retrive trace from another restapi Client which independent from the main jsonrpc client.
// So it need to create a new factory for TronClient and set the additionalClient to the restapi client.
func NewTronClientFactory(params TronClientParams) internal.ClientFactory {
	return NewTronApiClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		ethClient := &EthereumClient{
			config:          params.Config,
			logger:          logger,
			client:          client,
			dlq:             params.DLQ,
			validate:        validator.New(),
			metrics:         newEthereumClientMetrics(params.Metrics),
			nodeType:        types.EthereumNodeType_ARCHIVAL,
			traceType:       types.TraceType_GETH,
			commitmentLevel: types.CommitmentLevelLatest,
		}
		result := &TronClient{
			EthereumClient:   ethClient,
			additionalClient: params.AdditionalClient,
		}
		result.tracer = result
		return result
	})
}

func (c *TronClient) getBlockTraces(ctx context.Context, tag uint32, block *ethereum.EthereumBlockLit) ([][]byte, error) {
	blockNumber := block.Number.Value()

	requestData := TronBlockTxInfoRequestData{
		Num: blockNumber,
	}
	postData, err := json.Marshal(requestData)
	if err != nil {
		return nil, xerrors.Errorf("failed to Marshal Tron requestData: %w", err)
	}
	response, err := c.additionalClient.Call(ctx, tronTxInfoMethod, postData)
	if err != nil {
		return nil, xerrors.Errorf("failed to get Tron TransactionInfo: %w", err)
	}
	var tmpResults []json.RawMessage
	if err := json.Unmarshal(response, &tmpResults); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal TronTxInfo: %w", err)
	}
	results := make([][]byte, len(tmpResults))
	for i, trace := range tmpResults {
		results[i] = trace
	}
	return results, nil
}
