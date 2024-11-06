package ethereum

import (
	"encoding/json"
	"strconv"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func NewTronNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Tron shares the same data schema as Ethereum since its an EVM chain except skip trace data
	opts = append(opts, WithEthereumNodeType(types.EthereumNodeType_ARCHIVAL), WithTraceType(types.TraceType_PARITY))
	return NewEthereumNativeParser(params, opts...)
}

type TronCallValueInfo struct {
	CallValue int64  `json:"callValue"`
	TokenId   string `json:"tokenId"`
}

type TronTransactionInfo struct {
	InternalTransactions []TronInternalTransaction `json:"internal_transactions"`
	Id                   string                    `json:"id"`
	BlockNumber          int64                     `json:"blockNumber"`
	TransactionHash      string                    `json:"transactionHash"`
}

type TronInternalTransaction struct {
	Hash              string              `json:"hash"`
	CallerAddress     string              `json:"caller_address"`
	TransferToAddress string              `json:"transferTo_address"`
	CallValueInfo     []TronCallValueInfo `json:"callValueInfo"`
	Note              string              `json:"note"`
	Rejected          bool                `json:"rejected"`
}

func toEthereumHexString(data string) string {
	return "0x" + data
}

func convertInternalTransactionToTrace(itx *TronInternalTransaction) *api.EthereumTransactionFlattenedTrace {
	// Calculate total value from CallValueInfo
	var totalValue int64
	for _, callValue := range itx.CallValueInfo {
		totalValue += callValue.CallValue
	}

	trace := &api.EthereumTransactionFlattenedTrace{
		Type:      "CALL",
		TraceType: "CALL",
		CallType:  "CALL",
		From:      toEthereumHexString(itx.CallerAddress),
		To:        toEthereumHexString(itx.TransferToAddress),
		Value:     strconv.FormatInt(totalValue, 10),
		TraceId:   toEthereumHexString(itx.Hash),
	}
	if itx.Rejected {
		trace.Error = "Internal transaction is executed failed"
		trace.Status = 0
	} else {
		trace.Status = 1
	}
	return trace

}

func convertTxInfoToFlattenedTraces(blobData *api.EthereumBlobdata, header *api.EthereumHeader, transactionToFlattenedTracesMap map[string][]*api.EthereumTransactionFlattenedTrace) error {
	if len(blobData.TransactionTraces) == 0 {
		return nil
	}
	for txIndex, rawTxInfo := range blobData.TransactionTraces {
		var txInfo TronTransactionInfo
		if err := json.Unmarshal(rawTxInfo, &txInfo); err != nil {
			return xerrors.Errorf("failed to parse transaction trace: %w", err)
		}
		traceTransactionHash := toEthereumHexString(txInfo.Id)
		traces := make([]*api.EthereumTransactionFlattenedTrace, 0)
		txIdx := uint64(txIndex)
		internalTxs := txInfo.InternalTransactions
		for _, internalTx := range internalTxs {
			trace := convertInternalTransactionToTrace(&internalTx)
			trace.BlockHash = header.Hash
			trace.BlockNumber = header.Number
			trace.TransactionHash = traceTransactionHash
			trace.TransactionIndex = txIdx

			traces = append(traces, trace)
		}
		transactionToFlattenedTracesMap[traceTransactionHash] = traces
	}
	return nil
}
