package sdk

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	downloadermocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader/mocks"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	apimocks "github.com/coinbase/chainstorage/protos/coinbase/chainstorage/mocks"
)

// streamBitcoinClientSuite brings up an SDK Client wired for
// bitcoin-mainnet so StreamBitcoinBlock can actually stream through the
// bitcoin parser.
type streamBitcoinClientSuite struct {
	suite.Suite
	ctrl             *gomock.Controller
	app              testapp.TestApp
	gatewayClient    *apimocks.MockChainStorageClient
	downloaderClient *downloadermocks.MockBlockDownloader
	client           Client
	require          *testutil.Assertions
}

func TestStreamBitcoinClientSuite(t *testing.T) {
	suite.Run(t, new(streamBitcoinClientSuite))
}

func (s *streamBitcoinClientSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.require = testutil.Require(s.T())

	s.gatewayClient = apimocks.NewMockChainStorageClient(s.ctrl)
	s.downloaderClient = downloadermocks.NewMockBlockDownloader(s.ctrl)

	s.app = testapp.New(
		s.T(),
		Module,
		parser.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(func() downloader.BlockDownloader { return s.downloaderClient }),
		fx.Provide(func() gateway.Client { return s.gatewayClient }),
		fx.Populate(&s.client),
	)
	s.require.NotNil(s.client)
}

func (s *streamBitcoinClientSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

// TestStreamBitcoinBlock_EndToEnd verifies that a bitcoin-configured
// SDK client plumbs a streamed block through the downloader into the
// bitcoin parser's iterator and delivers transactions to the caller.
func (s *streamBitcoinClientSuite) TestStreamBitcoinBlock_EndToEnd() {
	const (
		tag    = uint32(1)
		height = uint64(696402)
		hash   = "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e"
	)

	rawBlock := buildBitcoinFixtureBlock(s.T())
	rawBlock.Metadata = &api.BlockMetadata{
		Tag: tag, Height: height, Hash: hash,
		ParentHash: "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3",
	}

	bf := &api.BlockFile{Tag: tag, Height: height, Hash: hash}
	s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), gomock.Any()).Return(&api.GetBlockFileResponse{File: bf}, nil)

	// For bitcoin-configured SDK, sdk.Client.StreamBlock uses the
	// Phase 2 path: DownloadStreamBitcoin delivers an api.Block with
	// Bitcoin.Header = nil plus an openHeaderReader factory.
	headerBytes := rawBlock.GetBitcoin().GetHeader()
	blockForStream := *rawBlock
	blockForStream.Blobdata = &api.Block_Bitcoin{
		Bitcoin: &api.BitcoinBlobdata{
			Header:            nil, // walker would leave this empty
			InputTransactions: nil, // walker now also leaves this empty
		},
	}
	openHeaderReader := func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(headerBytes)), nil
	}
	groups := rawBlock.GetBitcoin().GetInputTransactions()
	loadGroup := func(i int) (*api.RepeatedBytes, error) {
		if i < 0 || i >= len(groups) {
			return nil, nil
		}
		return groups[i], nil
	}
	s.downloaderClient.EXPECT().DownloadStreamBitcoin(gomock.Any(), bf, gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *api.BlockFile, consumer downloader.BitcoinStreamConsumer) error {
			return consumer(ctx, &blockForStream, openHeaderReader, loadGroup)
		},
	)

	var txCount int
	var header *api.BitcoinHeader
	err := s.client.StreamBlock(context.Background(), tag, height, hash, func(view *StreamedBlock) error {
		s.require.NotNil(view.GetMetadata())
		bs := view.GetBitcoin()
		s.require.NotNil(bs)
		for tx, iterErr := range bs.Transactions() {
			if iterErr != nil {
				return iterErr
			}
			_ = tx
			txCount++
		}
		h, herr := bs.Header()
		if herr != nil {
			return herr
		}
		header = h
		return nil
	})
	s.require.NoError(err)
	s.require.Greater(txCount, 0)
	s.require.NotNil(header)
	s.require.Equal(hash, header.Hash)
}

// buildBitcoinFixtureBlock assembles an api.Block with bitcoin blobdata
// from the standard get_block fixtures already used by parser tests.
func buildBitcoinFixtureBlock(t *testing.T) *api.Block {
	t.Helper()
	header, err := fixtures.ReadFile("parser/bitcoin/get_block.json")
	if err != nil {
		t.Fatalf("load get_block.json: %v", err)
	}
	tx1, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction.json")
	if err != nil {
		t.Fatalf("load get_raw_transaction.json: %v", err)
	}
	tx2, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction_tx2.json")
	if err != nil {
		t.Fatalf("load get_raw_transaction_tx2.json: %v", err)
	}

	return &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Height:     696402,
			Hash:       "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e",
			ParentHash: "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3",
		},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{}},
					{Data: [][]byte{tx1, tx2}},
				},
			},
		},
	}
}
