package bitcoin

import (
	"github.com/go-playground/validator/v10"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

func NewZcashNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	v := validator.New()
	v.RegisterStructValidation(validateBitcoinScriptPubKey, BitcoinScriptPubKey{})
	// Note: validateBitcoinTransactionVinVout is intentionally NOT registered.
	// Zcash shielded transactions can have empty vin (shielded-to-transparent)
	// or empty vout (transparent-to-shielded).
	return &bitcoinNativeParserImpl{
		logger:          log.WithPackage(params.Logger),
		validate:        v,
		preprocessBlock: backfillTxHash,
	}, nil
}
