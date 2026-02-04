package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum"
)

func NewMegaethClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	return NewEthereumClientFactory(params,
		WithTransactionFilter(func(txn *ethereum.EthereumTransactionLit) bool {
			return ethereum.IsMegaethFilteredTransaction(string(txn.From), string(txn.To))
		}),
	)
}
