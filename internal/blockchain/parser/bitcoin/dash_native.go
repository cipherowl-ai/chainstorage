package bitcoin

import (
	"github.com/go-playground/validator/v10"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

func NewDashNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	v := validator.New()
	v.RegisterStructValidation(validateBitcoinScriptPubKey, BitcoinScriptPubKey{})
	return &bitcoinNativeParserImpl{
		logger:   log.WithPackage(params.Logger),
		validate: v,
		preprocessBlock: func(block *BitcoinBlock) {
			// Dash's getblock response may omit tx.hash entirely. The shared Bitcoin
			// parser still validates hash as required, so normalize the Dash payload
			// first instead of weakening Bitcoin's default validation behavior.
			//
			// Dash is non-SegWit, so txid and hash are equivalent here.
			for _, tx := range block.Tx {
				if tx.Hash == "" {
					tx.Hash = tx.TxId
				}
			}
		},
	}, nil
}
