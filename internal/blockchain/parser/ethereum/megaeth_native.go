package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewMegaethNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	opts = append(opts, WithParserTransactionFilter(IsMegaethFilteredTransaction))
	return NewEthereumNativeParser(params, opts...)
}
