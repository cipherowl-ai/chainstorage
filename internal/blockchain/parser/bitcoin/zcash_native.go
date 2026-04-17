package bitcoin

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/go-playground/validator/v10"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	zcashNativeParserImpl struct {
		*bitcoinNativeParserImpl
	}

	// zcashBlockForFilter is a lightweight struct used to inspect raw
	// transaction JSON without modifying shared Bitcoin transaction structs.
	zcashBlockForFilter struct {
		Tx []json.RawMessage `json:"tx"`
	}

	zcashTxForFilter map[string]json.RawMessage
)

func NewZcashNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	v := validator.New()
	v.RegisterStructValidation(validateBitcoinScriptPubKey, BitcoinScriptPubKey{})
	// Note: validateBitcoinTransactionVinVout is intentionally NOT registered.
	// Zcash shielded transactions can have empty vin (shielded-to-transparent)
	// or empty vout (transparent-to-shielded).
	base := &bitcoinNativeParserImpl{
		logger:   log.WithPackage(params.Logger),
		validate: v,
	}

	return &zcashNativeParserImpl{
		bitcoinNativeParserImpl: base,
	}, nil
}

func (p *zcashNativeParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block, opts ...internal.ParseOption) (*api.NativeBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, xerrors.New("metadata not found")
	}

	blobdata := rawBlock.GetBitcoin()
	if blobdata == nil {
		return nil, xerrors.New("bitcoin blobdata not found")
	}

	rawHeader := blobdata.GetHeader()

	optView := internal.ResolveParseOptions(opts)

	var block BitcoinBlock
	if err := json.Unmarshal(rawHeader, &block); err != nil {
		return nil, xerrors.Errorf("failed to parse zcash block with %+v: %w", metadata, err)
	}

	backfillTxHash(&block, rawHeader)

	if err := p.validateStruct(block); err != nil {
		return nil, xerrors.Errorf("failed to validate zcash block %+v: %w", metadata, err)
	}

	metadataMap, err := p.buildInputMetadataMap(blobdata, optView)
	if err != nil {
		return nil, xerrors.Errorf("failed to build input metadata for %+v: %w", metadata, err)
	}

	transactions, err := p.buildZcashTransactions(rawHeader, block.Tx, metadataMap, optView)
	if err != nil {
		return nil, xerrors.Errorf("failed to build zcash transactions for %+v: %w", metadata, err)
	}

	header := block.GetApiBitcoinHeader()
	return &api.NativeBlock{
		Blockchain:      rawBlock.Blockchain,
		Network:         rawBlock.Network,
		Tag:             metadata.Tag,
		Hash:            metadata.Hash,
		ParentHash:      metadata.ParentHash,
		Height:          metadata.Height,
		ParentHeight:    metadata.ParentHeight,
		Timestamp:       header.Timestamp,
		NumTransactions: uint64(len(transactions)),
		Block: &api.NativeBlock_Bitcoin{
			Bitcoin: &api.BitcoinBlock{
				Header:       header,
				Transactions: transactions,
			},
		},
	}, nil
}

func (p *zcashNativeParserImpl) buildZcashTransactions(
	rawHeader []byte,
	rawTransactions []*BitcoinTransaction,
	metadataMap map[string][]*api.BitcoinTransactionOutput,
	optView internal.ParseOptionsView,
) ([]*api.BitcoinTransaction, error) {
	keepMask, err := buildTransparentTransactionMask(rawHeader, len(rawTransactions))
	if err != nil {
		return nil, err
	}

	transactions := make([]*api.BitcoinTransaction, 0, len(rawTransactions))
	for i, rawTx := range rawTransactions {
		if !keepMask[i] {
			continue
		}

		transaction, err := rawTx.ToApiBitcoinTransaction(i, metadataMap, p.p2pkhVersionByte, optView)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse tx[%d]: %w", i, err)
		}

		transactions = append(transactions, transaction)
	}

	return transactions, nil
}

func buildTransparentTransactionMask(rawHeader []byte, expectedTxCount int) ([]bool, error) {
	var filterBlock zcashBlockForFilter
	if err := json.Unmarshal(rawHeader, &filterBlock); err != nil {
		return nil, xerrors.Errorf("failed to parse zcash block filter view: %w", err)
	}

	if len(filterBlock.Tx) != expectedTxCount {
		return nil, xerrors.Errorf("zcash tx count mismatch between parsed block and filter view (parsed=%d filter=%d)", expectedTxCount, len(filterBlock.Tx))
	}

	mask := make([]bool, expectedTxCount)
	for i, txRaw := range filterBlock.Tx {
		transparent, err := isTransparentZcashTx(txRaw)
		if err != nil {
			return nil, xerrors.Errorf("failed to classify zcash tx[%d]: %w", i, err)
		}

		mask[i] = transparent
	}

	return mask, nil
}

// isTransparentZcashTx returns true only when the transaction has both vin and
// vout. Coinbase transactions are always kept.
func isTransparentZcashTx(txRaw json.RawMessage) (bool, error) {
	var tx zcashTxForFilter
	if err := json.Unmarshal(txRaw, &tx); err != nil {
		return false, err
	}

	coinbase, err := isZcashCoinbaseTx(tx)
	if err != nil {
		return false, err
	}
	if coinbase {
		return true, nil
	}

	return hasMeaningfulJSONField(tx["vin"]) && hasMeaningfulJSONField(tx["vout"]), nil
}

// isZcashCoinbaseTx checks whether a transaction is a coinbase by inspecting
// the first vin entry for a "coinbase" field.
func isZcashCoinbaseTx(tx zcashTxForFilter) (bool, error) {
	vinRaw, ok := tx["vin"]
	if !ok || !hasMeaningfulJSONField(vinRaw) {
		return false, nil
	}

	var vin []json.RawMessage
	if err := json.Unmarshal(vinRaw, &vin); err != nil {
		return false, err
	}
	if len(vin) == 0 {
		return false, nil
	}

	var firstVin map[string]json.RawMessage
	if err := json.Unmarshal(vin[0], &firstVin); err != nil {
		return false, err
	}

	return hasMeaningfulJSONField(firstVin["coinbase"]), nil
}

func hasMeaningfulJSONField(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return false
	}

	switch string(trimmed) {
	case "null", "[]", "{}":
		return false
	default:
		return true
	}
}
