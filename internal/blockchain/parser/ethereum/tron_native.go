package ethereum

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/mr-tron/base58"
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
		From:      hexToTronAddress(itx.CallerAddress),
		To:        hexToTronAddress(itx.TransferToAddress),
		Value:     strconv.FormatInt(totalValue, 10),
		TraceId:   itx.Hash,
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
		traceTransactionHash := txInfo.Id
		txIdx := uint64(txIndex)
		internalTxs := txInfo.InternalTransactions
		traces := make([]*api.EthereumTransactionFlattenedTrace, len(internalTxs))
		for i, internalTx := range internalTxs {
			trace := convertInternalTransactionToTrace(&internalTx)
			trace.BlockHash = header.Hash
			trace.BlockNumber = header.Number
			trace.TransactionHash = traceTransactionHash
			trace.TransactionIndex = txIdx
			traces[i] = trace
		}
		transactionToFlattenedTracesMap[traceTransactionHash] = traces
	}
	return nil
}

func toTronHash(hexHash string) string {
	return strings.Replace(hexHash, "0x", "", -1)
}

func hexToTronAddress(hexAddress string) string {
	if strings.HasPrefix(hexAddress, "0x") {
		hexAddress = "41" + hexAddress[2:]
	}
	// 
	rawBytes, _ := hex.DecodeString(hexAddress)
	// Compute double SHA-256 checksum
	hash1 := sha256.Sum256(rawBytes)
	hash2 := sha256.Sum256(hash1[:])
	checksum := hash2[:4] // First 4 bytes as checksum
	// Append checksum to the raw bytes
	fullBytes := append(rawBytes, checksum...)
	// Base58Check encode
	tronAddress := base58.Encode(fullBytes)

	return tronAddress
}

func convertTokenTransfer(data *api.EthereumTokenTransfer) {
	data.TokenAddress = hexToTronAddress(data.TokenAddress)
	data.FromAddress = hexToTronAddress(data.FromAddress)
	data.ToAddress = hexToTronAddress(data.ToAddress)

	data.TransactionHash = toTronHash(data.TransactionHash)
	data.BlockHash = toTronHash(data.BlockHash)

	switch v := data.TokenTransfer.(type) {
	case *api.EthereumTokenTransfer_Erc20:
		if v.Erc20 != nil {
			v.Erc20.FromAddress = hexToTronAddress(v.Erc20.FromAddress)
			v.Erc20.ToAddress = hexToTronAddress(v.Erc20.ToAddress)
		}
	case *api.EthereumTokenTransfer_Erc721:
		if v.Erc721 != nil {
			v.Erc721.FromAddress = hexToTronAddress(v.Erc721.FromAddress)
			v.Erc721.ToAddress = hexToTronAddress(v.Erc721.ToAddress)
		}
	}
}

func postProcessTronBlock(metaData *api.BlockMetadata, header *api.EthereumHeader, transactions []*api.EthereumTransaction, txReceipts []*api.EthereumTransactionReceipt, tokenTransfers [][]*api.EthereumTokenTransfer) {
	metaData.Hash = toTronHash(metaData.Hash)
	metaData.ParentHash = toTronHash(metaData.ParentHash)

	header.Hash = toTronHash(header.Hash)
	header.ParentHash = toTronHash(header.ParentHash)
	header.TransactionsRoot = toTronHash(header.TransactionsRoot)
	header.Miner = hexToTronAddress(header.Miner)

	for i := range header.Transactions {
		header.Transactions[i] = toTronHash(header.Transactions[i])
	}

	for _, tx := range transactions {
		tx.BlockHash = toTronHash(tx.BlockHash)
		tx.Hash = toTronHash(tx.Hash)
		if tx.From != "" {
			tx.From = hexToTronAddress(tx.From)
		}
		if tx.To != "" {
			tx.To = hexToTronAddress(tx.To)
		}
	}

	for _, txR := range txReceipts {
		txR.TransactionHash = toTronHash(txR.TransactionHash)
		txR.BlockHash = toTronHash(txR.BlockHash)
		if txR.From != "" {
			txR.From = hexToTronAddress(txR.From)
		}
		if txR.To != "" {
			txR.To = hexToTronAddress(txR.To)
		}
		if txR.Logs != nil {
			for _, txLog := range txR.Logs {
				txLog.TransactionHash = toTronHash(txLog.TransactionHash)
				txLog.BlockHash = toTronHash(txLog.BlockHash)
				txLog.Address = hexToTronAddress(txLog.Address)
			}
		}
	}

	for _, txTokenTransfers := range tokenTransfers {
		for _, tokenTransfer := range txTokenTransfers {
			convertTokenTransfer(tokenTransfer)
		}
	}
}
