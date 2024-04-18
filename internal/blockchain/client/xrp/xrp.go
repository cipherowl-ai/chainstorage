package xrp

import (
	"context"
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/xrp"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strconv"
	"time"
)

type (
	xrpClient struct {
		config   *config.Config
		logger   *zap.Logger
		client   jsonrpc.Client
		validate *validator.Validate
	}
)

type ledgerRequest struct {
	LedgerHash   string   `json:"ledger_hash,omitempty"`
	LedgerIndex  string   `json:"ledger_index,omitempty"`
	Expand       bool     `json:"expand,omitempty"`
	Binary       bool     `json:"binary,omitempty"`
	Transactions bool     `json:"transactions,omitempty"`
	Queued       bool     `json:"queued,omitempty"`
	EntryTypes   []string `json:"entry_types,omitempty"`
	ReturnLinks  bool     `json:"return_links,omitempty"`
}

type ledgerTransactionRequest struct {
	Transaction string `json:"transaction"`
	Binary      bool   `json:"binary"`
}

var _ internal.Client = (*xrpClient)(nil)

var (
	xrpGetLedger = &jsonrpc.RequestMethod{
		Name:    "ledger",
		Timeout: time.Second * 10,
	}
	xrpGetTransaction = &jsonrpc.RequestMethod{
		Name:    "tx",
		Timeout: time.Second * 30,
	}
)

func NewXRPClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	return internal.NewJsonrpcClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		return &xrpClient{
			config:   params.Config,
			logger:   logger,
			client:   client,
			validate: validator.New(),
		}
	})
}

func (b *xrpClient) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, xerrors.Errorf("invalid height range range of [%d, %d)", from, to)
	}

	totalLedger := int(to - from)

	params := makeRequestParams(totalLedger, from)

	responses, err := b.client.BatchCall(ctx, xrpGetLedger, params)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block for ledger indexes: %w", err)
	}

	if len(responses) != totalLedger {
		return nil, xerrors.Errorf("missing blocks in BatchCall to %s", xrpGetLedger.Name)
	}

	ledgerHeaders, err := assembleLedgerHeaders(responses)
	if err != nil {
		return nil, xerrors.Errorf("failed to assemble ledger from responses: %w", err)
	}

	return convertLedgerHeadersToBlockMetadata(ledgerHeaders, from, tag)
}

func makeRequestParams(totalLedger int, from uint64) []jsonrpc.Params {
	params := make([]jsonrpc.Params, totalLedger)
	for i := 0; i < totalLedger; i++ {
		params[i] = jsonrpc.Params{
			ledgerRequest{
				LedgerIndex:  strconv.Itoa(int(uint32(from + uint64(i)))),
				Transactions: true,
			},
		}
	}
	return params
}

func convertLedgerHeadersToBlockMetadata(ledgerHeaders []*xrp.LedgerHeader, from uint64, tag uint32) ([]*api.BlockMetadata, error) {
	results := make([]*api.BlockMetadata, len(ledgerHeaders))
	for i, ledgerHeader := range ledgerHeaders {
		closeTime, err := convertTime(ledgerHeader)
		if err != nil {
			panic(err)
		}
		hash := ledgerHeader.LedgerHash
		height := from + uint64(i)

		results[i] = &api.BlockMetadata{
			Tag:          tag,
			Height:       height,
			ParentHeight: internal.GetParentHeight(height),
			Hash:         hash,
			ParentHash:   ledgerHeader.ParentHash,
			Timestamp:    timestamppb.New(closeTime),
		}
	}
	return results, nil
}

func convertTime(ledgerHeader *xrp.LedgerHeader) (time.Time, error) {
	closeTime, err := time.Parse(time.RFC3339, ledgerHeader.CloseTime)
	if err != nil {
		return time.Time{}, err
	}
	return closeTime, nil
}

func assembleLedgerHeaders(responses []*jsonrpc.Response) ([]*xrp.LedgerHeader, error) {
	ledgerHeaders := make([]*xrp.LedgerHeader, len(responses))
	for i, response := range responses {
		header, err := assembleLedgerHeader(response)
		if err != nil {
			return nil, xerrors.Errorf("failed to assemble ledger header: %w", err)
		}

		ledgerHeaders[i] = header
	}
	return ledgerHeaders, nil
}

func assembleLedgerHeader(response *jsonrpc.Response) (*xrp.LedgerHeader, error) {
	xrpResponse := &xrp.LedgerResponse{}
	if err := response.Unmarshal(xrpResponse); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block header: %w", err)
	}

	header := xrpResponse.Result.LedgerHeader
	return &header, nil
}

func (b *xrpClient) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)
	params := jsonrpc.Params{
		ledgerRequest{
			LedgerIndex:  strconv.Itoa(int(uint32(height))),
			Transactions: true,
		}}

	return b.makeBlock(ctx, tag, params)
}

func (b *xrpClient) makeBlock(ctx context.Context, tag uint32, params jsonrpc.Params) (*api.Block, error) {
	response, err := b.client.Call(ctx, xrpGetLedger, params)
	if err != nil {
		return nil, xerrors.Errorf("failed to make a call for block %v: %w", params, err)
	}

	ledgerHeader, err := assembleLedgerHeader(response)
	if err != nil {
		return nil, xerrors.Errorf("failed to assemble ledger header: %w", err)
	}

	xrpBlock, transactionResponses, err := b.getXRPBlockFromHeader(ctx, ledgerHeader)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block from index: %v ,%w", params, err)
	}

	return b.toBlock(tag, xrpBlock, transactionResponses, response), nil
}

func (b *xrpClient) toBlock(tag uint32, xrpBlock *xrp.XRPBlock, transactionResponses []*jsonrpc.Response, headerResponse *jsonrpc.Response) *api.Block {
	t, err := time.Parse(time.RFC3339, xrpBlock.LedgerHeader.CloseTime)
	if err != nil {
		panic(err)
	}
	height, err := strconv.ParseUint(xrpBlock.LedgerIndex, 10, 64)
	if err != nil {
		panic(err)
	}
	return &api.Block{
		Blockchain: b.config.Chain.Blockchain,
		Network:    b.config.Chain.Network,
		Metadata: &api.BlockMetadata{
			Tag:          tag,
			Height:       height,
			ParentHeight: internal.GetParentHeight(height),
			Hash:         xrpBlock.LedgerHeader.LedgerHash,
			ParentHash:   xrpBlock.LedgerHeader.ParentHash,
			Timestamp:    timestamppb.New(t),
		},
		Blobdata: &api.Block_Xrp{
			Xrp: &api.XRPBlobdata{
				Header:             headerResponse.Result,
				LedgerTransactions: b.extractResultsFromResponses(transactionResponses),
			},
		},
	}
}

func (b *xrpClient) extractResultsFromResponses(responses []*jsonrpc.Response) [][]byte {
	results := make([][]byte, len(responses))
	for i, response := range responses {
		results[i] = response.Result
	}

	return results
}

func (b *xrpClient) getXRPBlockFromHeader(ctx context.Context, header *xrp.LedgerHeader) (*xrp.XRPBlock, []*jsonrpc.Response, error) {
	transactionHashSet := header.Transactions
	if transactionHashSet == nil || len(transactionHashSet) == 0 {
		return nil, nil, xerrors.Errorf("failed to find transactionHashSet for ledger header: %w", header.LedgerIndex)
	}

	params := make([]jsonrpc.Params, len(transactionHashSet))
	for i, transaction := range transactionHashSet {
		params[i] = jsonrpc.Params{
			ledgerTransactionRequest{
				Transaction: transaction,
				Binary:      false,
			},
		}
	}
	responses, err := b.client.BatchCall(ctx, xrpGetTransaction, params)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to get transactionHashSet for ledger header: %w", header.LedgerIndex)
	}

	transactions, err := assembleTransactions(responses)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to assemble transactionHashSet: %w", err)
	}

	return &xrp.XRPBlock{
		LedgerHeader: header,
		LedgerIndex:  header.LedgerIndex,
		Transactions: transactions,
	}, responses, nil
}

func assembleTransactions(responses []*jsonrpc.Response) ([]*xrp.LedgerTransaction, error) {
	transactions := make([]*xrp.LedgerTransaction, len(responses))
	for i, response := range responses {
		transaction, err := assembleTransaction(response)
		if err != nil {
			return nil, xerrors.Errorf("failed to assemble transaction: %w", err)
		}

		transactions[i] = transaction
	}
	return transactions, nil
}

func assembleTransaction(response *jsonrpc.Response) (*xrp.LedgerTransaction, error) {
	var transaction xrp.LedgerTransaction
	xrpResponse := &xrp.TransactionResponse{}
	if err := response.Unmarshal(xrpResponse); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal transaction: %w", err)
	}
	return &transaction, nil
}

func (b *xrpClient) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)
	params := jsonrpc.Params{
		ledgerRequest{
			LedgerHash:   hash,
			Transactions: true,
		},
	}

	return b.makeBlock(ctx, tag, params)
}

func (b *xrpClient) GetLatestHeight(ctx context.Context) (uint64, error) {

	params := jsonrpc.Params{
		ledgerRequest{
			LedgerHash:   "current",
			Transactions: true,
		},
	}
	response, err := b.client.Call(ctx, xrpGetLedger, params)
	if err != nil {
		return 0, xerrors.Errorf("failed to get the height of the most-work fully-validated chain: %w", err)
	}

	ledgerHeader, err := assembleLedgerHeader(response)
	if err != nil {
		return 0, xerrors.Errorf("failed to assemble ledger header: %w", err)
	}
	height, err := strconv.ParseUint(ledgerHeader.LedgerIndex, 10, 64)
	if err != nil {
		panic(err)
	}
	return height, nil
}

func (b *xrpClient) UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error) {
	return nil, internal.ErrNotImplemented
}

func (b *xrpClient) CanReprocess(tag uint32, height uint64) bool {
	return true
}

func (b *xrpClient) GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error) {
	return nil, internal.ErrNotImplemented
}
