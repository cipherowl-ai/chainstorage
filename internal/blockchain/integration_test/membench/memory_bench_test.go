// Memory benchmarks for the per-chain block fetch + upload pipeline.
//
// These benchmarks are the objective yardstick for the streaming work described
// in the plan at /Users/henry/.claude/plans/snappy-floating-spark.md. Run them
// before and after the streaming changes and compare with `benchstat`.
//
// Each sub-benchmark runs a chain's full "fetch block → marshal proto → compress
// → upload" pipeline against fixture data, using a mocked HTTP transport (via
// jsonrpcmocks.MockClient) and a mocked S3 uploader that discards the body.
// There is no network access: CI-friendly.
//
// Fixtures: each sub-benchmark prefers large-block fixtures committed under
//
//	internal/utils/fixtures/client/<chain>/large/
//
// (see internal/utils/fixtures/tools/capture_large_block). If the large variant
// is not present on disk, the benchmark falls back to a small committed fixture
// so the suite still compiles and runs — numbers will be modest but the wiring
// is exercised. Replace the fallback paths with the real large fixtures before
// publishing baseline numbers.
//
// Running:
//
//	go test -bench=BenchmarkBlockPipelineMemory -benchmem -count=5 \
//	    ./internal/blockchain/integration_test/...
//
// The benchmark reports two custom metrics per iteration:
//
//	B/op         standard Go benchmark allocation metric
//	peak_heap_MB high-water mark of runtime.MemStats.HeapInuse, sampled every
//	             millisecond during the benchmark run. This catches transient
//	             peaks that allocations-per-op would miss.
package membench

import (
	"context"
	"encoding/json"
	"io"
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
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
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

// BenchmarkBlockPipelineMemory runs the full per-chain pipeline with mocked
// HTTP transport and mocked S3 uploader. See file-level doc comment for details.
func BenchmarkBlockPipelineMemory(b *testing.B) {
	b.Run("Ethereum", benchmarkEthereum)
	b.Run("Solana", benchmarkSolana)
	b.Run("Bitcoin", benchmarkBitcoin)
}

// --- Ethereum ---

const (
	ethBenchTag = uint32(1)
	// Block 24887296 captured from onchain-dev proxy on 2026-04-16. The hash
	// must match what the fixture reports — the chainstorage Ethereum client
	// validates the returned block hash against the requested hash.
	ethBenchHeight = uint64(24887296)
	ethBenchHash   = "0xe1dad30aefe8608dd4071c9d4aefa70433fdc5e855935be0b0d1713a9981a024"
)

func benchmarkEthereum(b *testing.B) {
	blockFixture := loadFixtureOrDefault(
		"client/ethereum/large/eth_getblockbynumber.json",
		[]byte(ethSmallBlockFixture),
	)
	// eth_getBlockReceipts returns an array; chainstorage uses
	// eth_getTransactionReceipt (batch, one per tx). Slice the array into
	// individual receipt JSONs so the BatchCall mock can dispatch them.
	receiptArrayFixture := loadFixtureOrDefault(
		"client/ethereum/large/eth_getblockreceipts.json",
		[]byte("["+ethSmallReceiptFixture+"]"),
	)
	receiptsByIndex := splitReceiptsArray(b, receiptArrayFixture, []byte(ethSmallReceiptFixture))
	// Trace fixture: large variant requires a QuickNode-routed capture
	// (NowNodes returns "unsupported", llamarpc returns Cloudflare). If not
	// captured, synthesize a trace array sized to match the block's tx count
	// — same JSON shape as the real response, filled with a uniform payload
	// large enough to exercise the trace accumulation code path at realistic
	// scale. Each synthetic entry is ~1KB.
	traceFixture := loadFixtureOrDefault(
		"client/ethereum/large/eth_traceblockbyhash.json",
		synthesizeEthereumTraces(b, blockFixture),
	)

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)
	rpcClient.EXPECT().Call(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
			// Dispatcher on method.Name — handles whatever calls the client makes.
			if method.Name == "debug_traceBlockByHash" || method.Name == "arbtrace_block" || method.Name == "trace_block" {
				return &jsonrpc.Response{Result: json.RawMessage(traceFixture)}, nil
			}
			return &jsonrpc.Response{Result: json.RawMessage(blockFixture)}, nil
		},
	).AnyTimes()
	rpcClient.EXPECT().BatchCall(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, batchParams []jsonrpc.Params, opts ...jsonrpc.Option) ([]*jsonrpc.Response, error) {
			resp := make([]*jsonrpc.Response, len(batchParams))
			for i := range resp {
				resp[i] = &jsonrpc.Response{Result: json.RawMessage(receiptsByIndex[i%len(receiptsByIndex)])}
			}
			return resp, nil
		},
	).AnyTimes()

	var clientParams blockchainclient.ClientParams
	var storage blobstorage.BlobStorage
	app := testapp.New(
		b,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET),
		blockchainclient.Module,
		jsonrpcMockModule(rpcClient),
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
	// TODO: swap to client/solana/large/sol_getblock.json once captured.
	blockFixture := loadFixtureOrDefault(
		"client/solana/large/sol_getblock.json",
		fixtures.MustReadFile("client/solana/block_v2.json"),
	)

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)
	rpcClient.EXPECT().Call(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
			return &jsonrpc.Response{Result: json.RawMessage(blockFixture)}, nil
		},
	).AnyTimes()
	rpcClient.EXPECT().BatchCall(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, batchParams []jsonrpc.Params, opts ...jsonrpc.Option) ([]*jsonrpc.Response, error) {
			resp := make([]*jsonrpc.Response, len(batchParams))
			for i := range resp {
				resp[i] = &jsonrpc.Response{Result: json.RawMessage(blockFixture)}
			}
			return resp, nil
		},
	).AnyTimes()

	var clientParams blockchainclient.ClientParams
	var storage blobstorage.BlobStorage
	app := testapp.New(
		b,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		blockchainclient.Module,
		jsonrpcMockModule(rpcClient),
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
	btcBenchTag = uint32(1)
	// Block 945252 captured from onchain-dev proxy on 2026-04-16 (2976 txs,
	// 3183 unique input txids). Falls back to small fixture (696402) if the
	// large variant has not been captured.
	btcBenchHeight = uint64(945252)
	btcBenchHash   = "00000000000000000000ea15d4678fa799f031a61146697cec35a8a332d56c84"
	// Small-fixture identity for the fallback path.
	btcBenchHeightSmall = uint64(696402)
	btcBenchHashSmall   = "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e"
)

func benchmarkBitcoin(b *testing.B) {
	// The Bitcoin pipeline cross-references each tx's vin (input) entries
	// against vouts in the fetched getrawtransaction results, so a single
	// mocked input tx must have enough vouts to cover every vout index the
	// block's vins reference. The committed sample at
	//   client/bitcoin/large/btc_getrawtx_sample.json
	// has been pre-expanded (scripted, see plan) to 500 vouts, which covers
	// every referenced index in the large captured block.
	height := btcBenchHeight
	hash := btcBenchHash
	largeBlock, blockErr := fixtures.ReadFile("client/bitcoin/large/btc_getblock.json")
	largeTx, txErr := fixtures.ReadFile("client/bitcoin/large/btc_getrawtx_sample.json")
	var blockFixture []byte
	var inputTxFixtures [][]byte
	if blockErr == nil && txErr == nil {
		blockFixture = largeBlock
		inputTxFixtures = [][]byte{largeTx}
	} else {
		height = btcBenchHeightSmall
		hash = btcBenchHashSmall
		blockFixture = fixtures.MustReadFile("client/bitcoin/btc_getblockresponse.json")
		inputTxFixtures = [][]byte{
			fixtures.MustReadFile("client/bitcoin/btc_getinputtx1_resp.json"),
			fixtures.MustReadFile("client/bitcoin/btc_getinputtx2_resp.json"),
			fixtures.MustReadFile("client/bitcoin/btc_getinputtx3_resp.json"),
		}
	}

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)
	rpcClient.EXPECT().Call(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
			return &jsonrpc.Response{Result: json.RawMessage(blockFixture)}, nil
		},
	).AnyTimes()
	rpcClient.EXPECT().BatchCall(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, batchParams []jsonrpc.Params, opts ...jsonrpc.Option) ([]*jsonrpc.Response, error) {
			resp := make([]*jsonrpc.Response, len(batchParams))
			for i := range resp {
				// Rotate through the available input tx fixtures.
				resp[i] = &jsonrpc.Response{Result: json.RawMessage(inputTxFixtures[i%len(inputTxFixtures)])}
			}
			return resp, nil
		},
	).AnyTimes()

	var clientParams blockchainclient.ClientParams
	var storage blobstorage.BlobStorage
	app := testapp.New(
		b,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		blockchainclient.Module,
		jsonrpcMockModule(rpcClient),
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

// --- shared helpers ---

// runPipeline runs `fetch` then storage.Upload `b.N` times and reports B/op
// plus peak heap-in-use (MB).
func runPipeline(b *testing.B, storage blobstorage.BlobStorage, fetch func() (*api.Block, error)) {
	b.Helper()
	// Warm up once so the first iteration doesn't include one-time setup costs.
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

// startHeapSampler samples runtime.MemStats.HeapInuse every heapSampleInterval
// and tracks the max via atomic CAS. Returns a stop function that must be
// called to end sampling.
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

// loadFixtureOrDefault returns the bytes at `path` if it exists, otherwise
// `fallback`. Lets the benchmark suite compile and run before large-block
// fixtures have been captured.
func loadFixtureOrDefault(path string, fallback []byte) []byte {
	if data, err := fixtures.ReadFile(path); err == nil {
		return data
	}
	return fallback
}

// synthesizeEthereumTraces builds a fake callTracer response sized to match
// the block fixture's transaction count. Used when a real trace fixture has
// not been captured. Each element is shaped like the real geth callTracer
// output (`{"result":{...}}`) so chainstorage's unmarshal succeeds.
func synthesizeEthereumTraces(b *testing.B, blockFixture []byte) []byte {
	b.Helper()
	var block struct {
		Transactions []json.RawMessage `json:"transactions"`
	}
	if err := json.Unmarshal(blockFixture, &block); err != nil {
		b.Fatalf("synthesizeEthereumTraces: parse block fixture: %v", err)
	}
	n := len(block.Transactions)
	if n == 0 {
		n = 1
	}
	// ~1KB per trace element: roughly matches realistic call traces for
	// ordinary txs. Larger contracts produce multi-KB traces; this suffices
	// to exercise the per-element copy path.
	const filler = `"0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"`
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

// splitReceiptsArray takes a JSON array of receipts (e.g. from
// eth_getBlockReceipts) and returns each element as raw bytes. Used to
// dispatch one receipt per BatchCall element, since chainstorage uses
// per-tx eth_getTransactionReceipt batching, not the single-call
// getBlockReceipts shape.
func splitReceiptsArray(b *testing.B, arrayJSON []byte, fallback []byte) [][]byte {
	b.Helper()
	var raws []json.RawMessage
	if err := json.Unmarshal(arrayJSON, &raws); err != nil {
		// Array fixture not present or malformed; serve the fallback receipt
		// for every batch element.
		return [][]byte{fallback}
	}
	out := make([][]byte, len(raws))
	for i, r := range raws {
		out[i] = []byte(r)
	}
	return out
}

// jsonrpcMockModule mirrors the per-chain `testModule` helpers in
// internal/blockchain/client/<chain>/<chain>_test.go — same mock client shared
// across all four endpoint names.
func jsonrpcMockModule(client jsonrpc.Client) fx.Option {
	return fx.Options(
		restapi.Module,
		fx.Provide(fx.Annotated{Name: "master", Target: func() jsonrpc.Client { return client }}),
		fx.Provide(fx.Annotated{Name: "slave", Target: func() jsonrpc.Client { return client }}),
		fx.Provide(fx.Annotated{Name: "validator", Target: func() jsonrpc.Client { return client }}),
		fx.Provide(fx.Annotated{Name: "consensus", Target: func() jsonrpc.Client { return client }}),
	)
}

// blobStorageModule wires the real S3 blob-storage factory with mocked
// S3 Client/Uploader/Downloader so Upload runs end-to-end (proto.Marshal +
// gzip + uploader.Upload) while the mock uploader discards the body.
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

// --- small inline fixtures (fallbacks used when large/* is not captured) ---

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

const ethSmallTraceFixture = `[
	{"result": {"type": "CALL"}},
	{"result": {"type": "CALL"}}
]`
