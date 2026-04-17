package downloader

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var (
	expectedBlock = &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
	}

	expectedSkippedBlock = &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:     1,
			Height:  123,
			Skipped: true,
		},
	}

	expectedBlockBytes, _           = proto.Marshal(expectedBlock)
	expectedBlockCompressedBytes, _ = storage_utils.Compress(expectedBlockBytes, api.Compression_GZIP)
)

type (
	blockDownloaderTestSuite struct {
		suite.Suite
		app              testapp.TestApp
		httpServer       *httptest.Server
		downloader       BlockDownloader
		blockFile        *api.BlockFile
		skippedBlockFile *api.BlockFile
	}

	httpServerFunc func() *httptest.Server
	httpClientFunc func() HTTPClient
)

func TestBlockDownloaderSuite(t *testing.T) {
	suite.Run(t, new(blockDownloaderTestSuite))
}

func (s *blockDownloaderTestSuite) SetupTest() {
	s.app = testapp.New(
		s.T(),
	)
	s.blockFile = &api.BlockFile{
		Tag:          1,
		Hash:         "0xabc",
		ParentHash:   "0xdef",
		Height:       123,
		ParentHeight: 122,
		Skipped:      false,
	}
	s.skippedBlockFile = &api.BlockFile{
		Tag:     1,
		Height:  123,
		Skipped: true,
	}
}

func (s *blockDownloaderTestSuite) TearDownTest() {
	s.httpServer.Close()
	s.app.Close()
}

func (s *blockDownloaderTestSuite) TestDownloadFailure() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusInternalServerError, []byte(nil))),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	_, err := s.downloader.Download(context.Background(), s.blockFile)
	require.True(xerrors.Is(err, errors.ErrDownloadFailure))
}

func (s *blockDownloaderTestSuite) TestUnmarshalFailure() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, []byte("foo"))),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	resp, err := s.downloader.Download(context.Background(), s.blockFile)
	require.Nil(resp)
	require.Error(err)
}

func (s *blockDownloaderTestSuite) TestSuccess() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, expectedBlockBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	rawBlock, err := s.downloader.Download(context.Background(), s.blockFile)
	require.NoError(err)
	if diff := cmp.Diff(expectedBlock, rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
}

func (s *blockDownloaderTestSuite) TestSuccess_Gzip() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, expectedBlockCompressedBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	s.blockFile.Compression = api.Compression_GZIP

	rawBlock, err := s.downloader.Download(context.Background(), s.blockFile)
	require.NoError(err)
	if diff := cmp.Diff(expectedBlock, rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
}

func (s *blockDownloaderTestSuite) TestSkipped() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
		fx.Provide(s.newHttpClientFunc()),
	)

	rawBlock, err := s.downloader.Download(context.Background(), s.skippedBlockFile)
	require.NoError(err)
	if diff := cmp.Diff(expectedSkippedBlock, rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
}

func (s *blockDownloaderTestSuite) TestDownloadStream_Success() {
	require := testutil.Require(s.T())

	bitcoinBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 123, Hash: "0xabc"},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{"hash":"0xabc","height":123,"tx":[]}`),
			},
		},
	}
	blockBytes, err := proto.Marshal(bitcoinBlock)
	require.NoError(err)

	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, blockBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	block, err := s.downloader.DownloadStream(context.Background(), s.blockFile)
	require.NoError(err)
	require.NotNil(block.GetMetadata())
	require.Equal(uint64(123), block.GetMetadata().Height)

	blob := block.GetBitcoin()
	require.NotNil(blob)
	require.Equal(`{"hash":"0xabc","height":123,"tx":[]}`, string(blob.GetHeader()))
}

func (s *blockDownloaderTestSuite) TestDownloadStream_Skipped() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
		fx.Provide(s.newHttpClientFunc()),
	)

	block, err := s.downloader.DownloadStream(context.Background(), s.skippedBlockFile)
	require.NoError(err)
	require.NotNil(block.GetMetadata())
	require.True(block.GetMetadata().Skipped)
	require.Nil(block.GetBlobdata())
}

func (s *blockDownloaderTestSuite) TestDownloadStreamBitcoin_Success() {
	require := testutil.Require(s.T())

	headerJSON := []byte(`{"hash":"0xabc","height":42,"tx":[{"txid":"tx1"},{"txid":"tx2"}]}`)
	bitcoinBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 42, Hash: "0xabc"},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: headerJSON,
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{[]byte("prev1-json"), []byte("prev2-json")}},
				},
			},
		},
	}
	blockBytes, err := proto.Marshal(bitcoinBlock)
	require.NoError(err)

	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, blockBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	stream, err := s.downloader.DownloadStreamBitcoin(context.Background(), s.blockFile)
	require.NoError(err)
	defer stream.Close()

	// Header and InputTransactions are BOTH lazy now.
	require.Equal(uint64(42), stream.Block.GetMetadata().Height)
	require.Nil(stream.Block.GetBitcoin().GetHeader(), "header must be nil — exposed via OpenHeaderReader")
	require.Nil(stream.Block.GetBitcoin().GetInputTransactions(), "input_transactions must be nil — loaded on demand via LoadInputTxGroup")

	// OpenHeaderReader should be re-callable.
	for i := 0; i < 2; i++ {
		rc, err := stream.OpenHeaderReader()
		require.NoError(err)
		got, err := io.ReadAll(rc)
		require.NoError(err)
		require.NoError(rc.Close())
		require.Equal(headerJSON, got, "header reader attempt %d", i)
	}

	// LoadInputTxGroup returns the i-th group's prev-tx bytes.
	group, err := stream.LoadInputTxGroup(0)
	require.NoError(err)
	require.NotNil(group)
	require.Equal([][]byte{[]byte("prev1-json"), []byte("prev2-json")}, group.Data)

	// Out-of-range returns nil group, no error.
	group, err = stream.LoadInputTxGroup(99)
	require.NoError(err)
	require.Nil(group)
}

func (s *blockDownloaderTestSuite) TestDownloadStreamBitcoin_Skipped() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
		fx.Provide(s.newHttpClientFunc()),
	)

	stream, err := s.downloader.DownloadStreamBitcoin(context.Background(), s.skippedBlockFile)
	require.NoError(err)
	defer stream.Close()

	require.True(stream.Block.GetMetadata().Skipped)
	require.Nil(stream.Block.GetBlobdata())
	// OpenHeaderReader returns an empty reader for skipped blocks.
	rc, err := stream.OpenHeaderReader()
	require.NoError(err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(err)
	require.Empty(got)
	// LoadInputTxGroup always returns nil for skipped blocks.
	g, err := stream.LoadInputTxGroup(0)
	require.NoError(err)
	require.Nil(g)
}

func (s *blockDownloaderTestSuite) TestDownloadStreamBitcoin_CloseRemovesSpool() {
	require := testutil.Require(s.T())

	bitcoinBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 42, Hash: "0xabc"},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{Header: []byte(`{"hash":"0xabc"}`)},
		},
	}
	blockBytes, err := proto.Marshal(bitcoinBlock)
	require.NoError(err)

	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, blockBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	stream, err := s.downloader.DownloadStreamBitcoin(context.Background(), s.blockFile)
	require.NoError(err)

	// Reader works before Close.
	rc, err := stream.OpenHeaderReader()
	require.NoError(err)
	require.NoError(rc.Close())

	require.NoError(stream.Close())
	// Second Close must be a no-op.
	require.NoError(stream.Close())

	// After Close, OpenHeaderReader fails (spool removed).
	_, err = stream.OpenHeaderReader()
	require.Error(err)
}

func (s *blockDownloaderTestSuite) newHttpClientFunc() httpClientFunc {
	return func() HTTPClient {
		return s.httpServer.Client()
	}
}

func (s *blockDownloaderTestSuite) newHttpServerFunc(httpMethod string, respStatusCode int, bodyBytes []byte) httpServerFunc {
	return func() *httptest.Server {
		server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			s.T().Logf("HELLO WORLD: %s", bodyBytes)
			require := testutil.Require(s.T())
			require.Equal(httpMethod, request.Method)
			writer.WriteHeader(respStatusCode)
			if _, err := writer.Write(bodyBytes); err != nil {
				require.NoError(err)
			}
		}))
		s.blockFile.FileUrl = server.URL
		return server
	}
}
