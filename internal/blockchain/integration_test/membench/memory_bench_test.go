// Memory benchmarks for the per-chain block fetch + upload pipeline.
//
// These benchmarks mock at the HTTP transport level (jsonrpc.HTTPClient),
// exercising the full code path from makeHTTPRequest through chain-client
// logic through proto.Marshal + compress + upload. Each mock response creates
// a fresh byte array to simulate real HTTP behavior where every response has
// its own memory.
//
// Running:
//
//	go test -bench=BenchmarkBlockPipelineMemory -benchmem -count=5 \
//	    ./internal/blockchain/integration_test/membench/...
package membench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	blockchainclient "github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/s3"
	s3mocks "github.com/coinbase/chainstorage/internal/s3/mocks"
	blobstorage "github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const heapSampleInterval = time.Millisecond

func BenchmarkBlockPipelineMemory(b *testing.B) {
	b.Run("Ethereum", benchmarkEthereum)
	b.Run("Solana", benchmarkSolana)
	b.Run("Bitcoin", benchmarkBitcoin)
}

// --- Ethereum ---

const (
	ethBenchTag    = uint32(1)
	ethBenchHeight = uint64(24887296)
	ethBenchHash   = "0xe1dad30aefe8608dd4071c9d4aefa70433fdc5e855935be0b0d1713a9981a024"
)

func benchmarkEthereum(b *testing.B) {
	blockFixture := loadFixtureOrDefault(
		"client/ethereum/large/eth_getblockbynumber.json",
		[]byte(ethSmallBlockFixture),
	)
	receiptArrayFixture := loadFixtureOrDefault(
		"client/ethereum/large/eth_getblockreceipts.json",
		[]byte("["+ethSmallReceiptFixture+"]"),
	)
	individualReceipts := splitReceiptsArray(b, receiptArrayFixture, []byte(ethSmallReceiptFixture))
	traceFixture := loadFixtureOrDefault(
		"client/ethereum/large/eth_traceblockbyhash.json",
		synthesizeEthereumTraces(b, blockFixture),
	)

	httpClient := &fixtureHTTPClient{
		singleHandlers: map[string][]byte{
			"eth_getBlockByNumber":   blockFixture,
			"eth_getBlockByHash":     blockFixture,
			"eth_blockNumber":        []byte(`"0x17bc000"`),
			"debug_traceBlockByHash": traceFixture,
		},
		batchResultFn: func(method string, index int) []byte {
			if index < len(individualReceipts) {
				return individualReceipts[index]
			}
			return individualReceipts[index%len(individualReceipts)]
		},
	}

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	var clientParams blockchainclient.ClientParams
	var storage blobstorage.BlobStorage
	app := testapp.New(
		b,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET),
		blockchainclient.Module,
		jsonrpc.Module,
		restapi.Module,
		dummyEndpoints(),
		fx.Provide(func() jsonrpc.HTTPClient { return httpClient }),
		fx.Provide(parser.NewNop),
		fx.Provide(dlq.NewNop),
		blobStorageModule(ctrl),
		fx.Decorate(func(*zap.Logger) *zap.Logger { return zap.NewNop() }),
		fx.Populate(&clientParams),
		fx.Populate(&storage),
	)
	defer app.Close()

	client := clientParams.Master
	ctx := context.Background()
	runPipeline(b, storage, func() (*api.Block, error) {
		return client.GetBlockByHash(ctx, ethBenchTag, ethBenchHeight, ethBenchHash)
	})
}

// --- Solana ---

const (
	solBenchTag    = uint32(2)
	solBenchHeight = uint64(100_000_000)
)

func benchmarkSolana(b *testing.B) {
	blockFixture := loadFixtureOrDefault(
		"client/solana/large/sol_getblock.json",
		fixtures.MustReadFile("client/solana/block_v2.json"),
	)

	httpClient := &fixtureHTTPClient{
		singleHandlers: map[string][]byte{
			"*": blockFixture,
		},
	}

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	var clientParams blockchainclient.ClientParams
	var storage blobstorage.BlobStorage
	app := testapp.New(
		b,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		blockchainclient.Module,
		jsonrpc.Module,
		restapi.Module,
		dummyEndpoints(),
		fx.Provide(func() jsonrpc.HTTPClient { return httpClient }),
		fx.Provide(parser.NewNop),
		fx.Provide(dlq.NewNop),
		blobStorageModule(ctrl),
		fx.Decorate(func(*zap.Logger) *zap.Logger { return zap.NewNop() }),
		fx.Populate(&clientParams),
		fx.Populate(&storage),
	)
	defer app.Close()

	client := clientParams.Master
	ctx := context.Background()
	runPipeline(b, storage, func() (*api.Block, error) {
		return client.GetBlockByHeight(ctx, solBenchTag, solBenchHeight)
	})
}

// --- Bitcoin ---

const (
	btcBenchTag         = uint32(1)
	btcBenchHeight      = uint64(945252)
	btcBenchHash        = "00000000000000000000ea15d4678fa799f031a61146697cec35a8a332d56c84"
	btcBenchHeightSmall = uint64(696402)
	btcBenchHashSmall   = "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e"
)

func benchmarkBitcoin(b *testing.B) {
	height := btcBenchHeight
	hash := btcBenchHash
	largeBlock, blockErr := fixtures.ReadFile("client/bitcoin/large/btc_getblock.json")
	largeTx, txErr := fixtures.ReadFile("client/bitcoin/large/btc_getrawtx_sample.json")
	var blockFixture, inputTxFixture []byte
	if blockErr == nil && txErr == nil {
		blockFixture = largeBlock
		inputTxFixture = largeTx
	} else {
		height = btcBenchHeightSmall
		hash = btcBenchHashSmall
		blockFixture = fixtures.MustReadFile("client/bitcoin/btc_getblockresponse.json")
		inputTxFixture = fixtures.MustReadFile("client/bitcoin/btc_getinputtx1_resp.json")
	}

	httpClient := &fixtureHTTPClient{
		singleHandlers: map[string][]byte{
			"*": blockFixture,
		},
		batchResultFn: func(method string, index int) []byte {
			return inputTxFixture
		},
	}

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	var clientParams blockchainclient.ClientParams
	var storage blobstorage.BlobStorage
	app := testapp.New(
		b,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		blockchainclient.Module,
		jsonrpc.Module,
		restapi.Module,
		dummyEndpoints(),
		fx.Provide(func() jsonrpc.HTTPClient { return httpClient }),
		fx.Provide(parser.NewNop),
		fx.Provide(dlq.NewNop),
		blobStorageModule(ctrl),
		fx.Decorate(func(*zap.Logger) *zap.Logger { return zap.NewNop() }),
		fx.Populate(&clientParams),
		fx.Populate(&storage),
	)
	defer app.Close()

	client := clientParams.Master
	ctx := context.Background()
	runPipeline(b, storage, func() (*api.Block, error) {
		return client.GetBlockByHash(ctx, btcBenchTag, height, hash)
	})
}

// =============================================================================
// fixtureHTTPClient — mocks at the HTTP transport level
// =============================================================================

// fixtureHTTPClient implements jsonrpc.HTTPClient by dispatching on the
// JSON-RPC method name in each request body. Each response creates a fresh
// byte array to simulate real HTTP behavior (no shared backing arrays).
type fixtureHTTPClient struct {
	// singleHandlers maps RPC method name → result JSON bytes.
	// Use "*" as a catch-all default.
	singleHandlers map[string][]byte

	// batchResultFn returns result bytes for a batch element by method and
	// index. If nil, singleHandlers is used for each element.
	batchResultFn func(method string, index int) []byte
}

func (c *fixtureHTTPClient) Do(req *http.Request) (*http.Response, error) {
	reqBody, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	trimmed := bytes.TrimSpace(reqBody)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		return c.doBatch(trimmed)
	}
	return c.doSingle(trimmed)
}

func (c *fixtureHTTPClient) doSingle(reqBody []byte) (*http.Response, error) {
	var rpcReq struct {
		Method string `json:"method"`
		ID     uint   `json:"id"`
	}
	_ = json.Unmarshal(reqBody, &rpcReq)

	result := c.lookupResult(rpcReq.Method)

	// Build a fresh envelope — distinct allocation per call.
	envelope := fmt.Appendf(nil, `{"jsonrpc":"2.0","id":%d,"result":`, rpcReq.ID)
	envelope = append(envelope, result...)
	envelope = append(envelope, '}')

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(envelope)),
		Header:     http.Header{"Content-Type": {"application/json"}},
	}, nil
}

func (c *fixtureHTTPClient) doBatch(reqBody []byte) (*http.Response, error) {
	var batch []struct {
		Method string `json:"method"`
		ID     uint   `json:"id"`
	}
	_ = json.Unmarshal(reqBody, &batch)

	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, elem := range batch {
		if i > 0 {
			buf.WriteByte(',')
		}
		var result []byte
		if c.batchResultFn != nil {
			result = c.batchResultFn(elem.Method, i)
		} else {
			result = c.lookupResult(elem.Method)
		}
		fmt.Fprintf(&buf, `{"jsonrpc":"2.0","id":%d,"result":`, elem.ID)
		buf.Write(result)
		buf.WriteByte('}')
	}
	buf.WriteByte(']')

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(buf.Bytes())),
		Header:     http.Header{"Content-Type": {"application/json"}},
	}, nil
}

func (c *fixtureHTTPClient) lookupResult(method string) []byte {
	if result, ok := c.singleHandlers[method]; ok {
		return result
	}
	if result, ok := c.singleHandlers["*"]; ok {
		return result
	}
	return []byte("null")
}

// =============================================================================
// shared helpers
// =============================================================================

func runPipeline(b *testing.B, storage blobstorage.BlobStorage, fetch func() (*api.Block, error)) {
	b.Helper()
	if _, err := fetch(); err != nil {
		b.Fatalf("warmup fetch: %v", err)
	}
	runtime.GC()

	b.ResetTimer()
	b.ReportAllocs()

	var peakHeap uint64
	stop := startHeapSampler(&peakHeap)
	defer func() {
		stop()
		b.ReportMetric(float64(peakHeap)/(1024*1024), "peak_heap_MB")
	}()

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		block, err := fetch()
		if err != nil {
			b.Fatalf("fetch failed at iter %d: %v", i, err)
		}
		if _, err := storage.Upload(ctx, block, api.Compression_GZIP); err != nil {
			b.Fatalf("upload failed at iter %d: %v", i, err)
		}
	}
}

func startHeapSampler(peak *uint64) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		ticker := time.NewTicker(heapSampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&m)
				for {
					cur := atomic.LoadUint64(peak)
					if m.HeapInuse <= cur {
						break
					}
					if atomic.CompareAndSwapUint64(peak, cur, m.HeapInuse) {
						break
					}
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func loadFixtureOrDefault(path string, fallback []byte) []byte {
	if data, err := fixtures.ReadFile(path); err == nil {
		return data
	}
	return fallback
}

func synthesizeEthereumTraces(b *testing.B, blockFixture []byte) []byte {
	b.Helper()
	var block struct {
		Transactions []json.RawMessage `json:"transactions"`
	}
	if err := json.Unmarshal(blockFixture, &block); err != nil {
		b.Fatalf("synthesizeEthereumTraces: %v", err)
	}
	n := len(block.Transactions)
	if n == 0 {
		n = 1
	}
	const filler = `"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"`
	var buf []byte
	buf = append(buf, '[')
	for i := 0; i < n; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, []byte(`{"result":{"type":"CALL","from":"0x0000000000000000000000000000000000000000","to":"0x0000000000000000000000000000000000000000","value":"0x0","gas":"0x0","gasUsed":"0x0","input":`)...)
		buf = append(buf, []byte(filler)...)
		buf = append(buf, []byte(`,"output":"0x"}}`)...)
	}
	buf = append(buf, ']')
	return buf
}

func splitReceiptsArray(b *testing.B, arrayJSON []byte, fallback []byte) [][]byte {
	b.Helper()
	var raws []json.RawMessage
	if err := json.Unmarshal(arrayJSON, &raws); err != nil {
		return [][]byte{fallback}
	}
	out := make([][]byte, len(raws))
	for i, r := range raws {
		out[i] = []byte(r)
	}
	return out
}

// dummyEndpoints decorates the config to inject a dummy endpoint into each
// client group (master/slave/validator/consensus). This satisfies the
// endpoints.Module requirement without needing secrets.yml. The actual URL is
// irrelevant because the injected jsonrpc.HTTPClient intercepts all requests.
func dummyEndpoints() fx.Option {
	return fx.Decorate(func(cfg *config.Config) *config.Config {
		dummy := config.Endpoint{Name: "bench", Url: "http://localhost:0"}
		for _, g := range []*config.EndpointGroup{
			&cfg.Chain.Client.Master.EndpointGroup,
			&cfg.Chain.Client.Slave.EndpointGroup,
			&cfg.Chain.Client.Validator.EndpointGroup,
			&cfg.Chain.Client.Consensus.EndpointGroup,
		} {
			if len(g.Endpoints) == 0 {
				g.Endpoints = []config.Endpoint{dummy}
			}
		}
		return cfg
	})
}

func blobStorageModule(ctrl *gomock.Controller) fx.Option {
	uploader := s3mocks.NewMockUploader(ctrl)
	uploader.EXPECT().Upload(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			if input.Body != nil {
				_, _ = io.Copy(io.Discard, input.Body)
			}
			return &manager.UploadOutput{}, nil
		},
	).AnyTimes()

	downloader := s3mocks.NewMockDownloader(ctrl)
	downloader.EXPECT().Download(gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()

	s3Client := s3mocks.NewMockClient(ctrl)

	return fx.Options(
		blobstorage.Module,
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Client { return s3Client }),
	)
}

// --- small inline fixtures ---

const ethSmallBlockFixture = `{
	"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
	"number": "0xacc290",
	"parentHash": "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
	"timestamp": "0x5fbd2fb9",
	"transactions": [
		{"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b"},
		{"hash": "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991"}
	]
}`

const ethSmallReceiptFixture = `{
	"blockHash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
	"blockNumber": "0xacc290"
}`
