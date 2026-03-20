package bitcoin

import (
	"context"

	"github.com/go-playground/validator/v10"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/bitcoin"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type dashClient struct {
	*bitcoinClient
}

var _ internal.Client = (*dashClient)(nil)

func NewDashClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	return internal.NewJsonrpcClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		return &dashClient{
			bitcoinClient: &bitcoinClient{
				config:   params.Config,
				logger:   logger,
				client:   client,
				validate: validator.New(),
			},
		}
	})
}

func (d *dashClient) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)
	params := jsonrpc.Params{height}

	response, err := d.client.Call(ctx, bitcoinGetBlockHashMethod, params)
	if err != nil {
		var rpcErr *jsonrpc.RPCError
		if xerrors.As(err, &rpcErr) &&
			rpcErr.Code == bitcoinErrCodeInvalidParameter &&
			rpcErr.Message == bitcoinErrMessageBlockOutOfRange {
			return nil, xerrors.Errorf("block not found by height %v: %w", height, internal.ErrBlockNotFound)
		}
		return nil, xerrors.Errorf("failed to make a call for block %v: %w", height, err)
	}

	var hash bitcoin.BitcoinHexString
	if err := response.Unmarshal(&hash); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block hash: %w", err)
	}

	block, err := d.GetBlockByHash(ctx, tag, height, hash.Value(), opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block by hash: %s for height: %v, %w", hash, height, err)
	}

	actualHeight := block.Metadata.Height
	if actualHeight != height {
		return nil, xerrors.Errorf("failed to get block due to inconsistent height values, expected: %v, actual: %v", height, actualHeight)
	}

	return block, nil
}

func (d *dashClient) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)
	params := jsonrpc.Params{
		hash,
		bitcoinBlockVerbosity,
	}

	response, err := d.client.Call(ctx, bitcoinGetBlockByHashMethod, params)
	if err != nil {
		var rpcErr *jsonrpc.RPCError
		if xerrors.As(err, &rpcErr) &&
			rpcErr.Code == bitcoinErrCodeInvalidAddressOrKey &&
			rpcErr.Message == bitcoinErrMessageBlockNotFound {
			return nil, xerrors.Errorf("block not found by hash %s: %w", hash, internal.ErrBlockNotFound)
		}
		return nil, xerrors.Errorf("failed to make a call for block hash %s: %w", hash, err)
	}

	headerResult, err := d.getBlockHeader(response)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block hash %s: %w", hash, err)
	}

	actualHash := headerResult.header.Hash.Value()
	if actualHash != hash {
		return nil, xerrors.Errorf("failed to get block due to inconsistent hash values, expected: %s, actual: %s", hash, actualHash)
	}

	return d.getBlockFromHeader(ctx, tag, headerResult)
}

func (d *dashClient) getBlockFromHeader(
	ctx context.Context,
	tag uint32,
	headerResult *bitcoinBlockHeaderResultHolder,
) (*api.Block, error) {
	blockHash := headerResult.header.Hash.Value()

	inputTransactionsData, rawInputTransactionsMap, err := d.bitcoinClient.getInputTransactions(ctx, headerResult.header)
	if err != nil {
		return nil, xerrors.Errorf("failed to get previous transactions for block %s: %w", blockHash, err)
	}

	inputTransactions := make([]*api.RepeatedBytes, len(inputTransactionsData))
	for i, data := range inputTransactionsData {
		inputTransactions[i] = &api.RepeatedBytes{Data: data}
	}

	block := &api.Block{
		Blockchain: d.config.Chain.Blockchain,
		Network:    d.config.Chain.Network,
		Metadata: &api.BlockMetadata{
			Tag:          tag,
			Hash:         blockHash,
			ParentHash:   headerResult.header.PreviousBlockHash.Value(),
			Height:       headerResult.header.Height.Value(),
			ParentHeight: internal.GetParentHeight(headerResult.header.Height.Value()),
			Timestamp:    utils.ToTimestamp(int64(headerResult.header.Time.Value())),
		},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header:               headerResult.rawJson,
				InputTransactions:    inputTransactions,
				RawInputTransactions: rawInputTransactionsMap,
			},
		},
	}

	return block, nil
}
