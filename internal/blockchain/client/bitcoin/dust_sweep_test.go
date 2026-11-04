package bitcoin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"runtime/metrics"
	"strings"
	"sync/atomic"
	"testing"

	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	sweepBlockHash   = "00000000000000000001blockhash"
	sweepHeight      = uint64(3541841)
	sweepInputsPerTx = 490
	sweepParents     = 3000
	sweepOutputs     = 300
	sweepTxIDMarker  = "SWEEP_PARENT_TXID"
)

// sweepBlockJSON returns a dust sweep: a block whose inputs each spend one output
// of a different fan-out transaction.
func sweepBlockJSON() json.RawMessage {
	var txs []string
	txs = append(txs, `{"txid": "coinbasetx", "vin": [{"coinbase": "00"}]}`)
	for parent := 0; parent < sweepParents; {
		var vin []string
		for len(vin) < sweepInputsPerTx && parent < sweepParents {
			vin = append(vin, fmt.Sprintf(`{"txid": "%s", "vout": %d}`, sweepParentTxID(parent), parent%sweepOutputs))
			parent++
		}
		txs = append(txs, fmt.Sprintf(`{"txid": "sweep%d", "vin": [%s]}`, len(txs), strings.Join(vin, ",")))
	}
	return json.RawMessage(fmt.Sprintf(`{
		"hash": "%s",
		"previousblockhash": "0000000000000000000000000000000000000000000000000000000000000000",
		"height": %d,
		"time": 1629306034,
		"tx": [%s]
	}`, sweepBlockHash, sweepHeight, strings.Join(txs, ",")))
}

func sweepParentTxID(parent int) string {
	return fmt.Sprintf("%064x", parent)
}

// sweepParentJSON returns a verbose getrawtransaction result with the output
// shape of a fan-out transaction. The txid is left as sweepTxIDMarker so that
// the test holds a single template, not every previous transaction.
func sweepParentJSON() []byte {
	const pubKeyHash = "0123456789abcdef0123456789abcdef01234567"
	vout := make([]string, sweepOutputs)
	for n := range vout {
		vout[n] = fmt.Sprintf(
			`{"value": 0.00010000, "valueZat": 10000, "n": %d, "scriptPubKey": {`+
				`"asm": "OP_DUP OP_HASH160 %[2]s OP_EQUALVERIFY OP_CHECKSIG", `+
				`"hex": "76a914%[2]s88ac", "reqSigs": 1, "type": "pubkeyhash", `+
				`"addresses": ["t1Vz4qgnWkTpTVUxlQsSK7Ei4nVVsy8u5cq"]}}`,
			n, pubKeyHash,
		)
	}
	return []byte(fmt.Sprintf(`{"txid": "%s", "vout": [%s]}`, sweepTxIDMarker, strings.Join(vout, ",")))
}

func sweepParentResult(template []byte, txID string) []byte {
	return bytes.Replace(template, []byte(sweepTxIDMarker), []byte(txID), 1)
}

// peakLiveHeap runs fn while collecting in a loop, and returns how far the live
// heap rose above where it started and how many times it was sampled. It reads
// the bytes marked by the collection, because HeapAlloc also counts whatever was
// allocated since then and would make the result depend on the machine's load.
func peakLiveHeap(fn func()) (peak uint64, samples int) {
	liveHeap := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	readHeap := func() uint64 {
		runtime.GC()
		metrics.Read(liveHeap)
		return liveHeap[0].Value.Uint64()
	}

	baseline := readHeap()
	done := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-done:
				return
			default:
				if heap := readHeap(); heap > baseline {
					peak = max(peak, heap-baseline)
				}
				samples++
			}
		}
	}()

	fn()
	close(done)
	<-sampled
	return peak, samples
}

// syncDustSweep syncs a dust sweep, the shape of Zcash block 3541841: 1.15 MB of
// block whose inputs reference 6,553 previous transactions and 620 MB of JSON
// (INF-2230). It returns the block, how many bytes of previous transactions the
// node served, and how far the live heap rose during the sync.
func syncDustSweep(t *testing.T, blockchain common.Blockchain, network common.Network) (*api.Block, uint64, uint64) {
	require := testutil.Require(t)
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		testapp.WithBlockchainNetwork(blockchain, network),
		fx.Populate(&result),
	)
	t.Cleanup(app.Close)

	rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{sweepBlockHash, bitcoinBlockVerbosity},
	).Return(&jsonrpc.Response{Result: sweepBlockJSON()}, nil)

	// Results are generated on demand so that only the client can keep them live.
	template := sweepParentJSON()
	var served atomic.Int64
	rpcClient.EXPECT().BatchCall(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *jsonrpc.RequestMethod, batchParams []jsonrpc.Params, _ ...jsonrpc.Option) ([]*jsonrpc.Response, error) {
			responses := make([]*jsonrpc.Response, len(batchParams))
			for i, params := range batchParams {
				txID, ok := params[0].(string)
				if !ok {
					return nil, fmt.Errorf("unexpected params: %v", params)
				}
				parent := sweepParentResult(template, txID)
				served.Add(int64(len(parent)))
				responses[i] = &jsonrpc.Response{Result: parent}
			}
			return responses, nil
		},
	).AnyTimes()

	var block *api.Block
	var err error
	peak, samples := peakLiveHeap(func() {
		block, err = result.Master.GetBlockByHash(context.Background(), btcTag, sweepHeight, sweepBlockHash)
	})
	require.NoError(err)

	total := uint64(served.Load())
	t.Logf("served %d MB of previous transactions, peak live heap %d MB, block %d MB", total>>20, peak>>20, proto.Size(block)>>20)
	require.Greater(total, uint64(sweepParents*sweepOutputs*100), "the test must serve fan-out transactions")
	require.GreaterOrEqual(samples, 3, "the heap must have been sampled while the block was fetched")

	// Every input still gets the one output it spends, in order.
	var entry struct {
		TxID string `json:"txid"`
		Vout []struct {
			N uint64 `json:"n"`
		} `json:"vout"`
	}
	parent := 0
	for _, inputs := range block.GetBitcoin().InputTransactions[1:] {
		for _, data := range inputs.Data {
			require.NoError(json.Unmarshal(data, &entry))
			require.Equal(sweepParentTxID(parent), entry.TxID)
			require.Len(entry.Vout, 1)
			require.Equal(uint64(parent%sweepOutputs), entry.Vout[0].N)
			parent++
		}
	}
	require.Equal(sweepParents, parent)

	return block, total, peak
}

// TestBitcoinClient_GetBlockByHash_DustSweepMemory pins that a client which does
// not preserve raw transactions holds previous transactions one batch at a time:
// the live heap and the block follow the block's inputs, not what they reference.
func TestBitcoinClient_GetBlockByHash_DustSweepMemory(t *testing.T) {
	require := testutil.Require(t)

	block, total, peak := syncDustSweep(t, common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET)

	require.Less(peak, total/2, "live heap must not follow the size of the previous transactions")
	require.Less(uint64(proto.Size(block)), total/10, "the block must not carry the previous transactions")
	require.Empty(block.GetBitcoin().RawInputTransactions)
}

// TestZcashClient_GetBlockByHash_DustSweepMemory pins the same shape for a client
// that preserves raw transactions. The raw JSON has to stay, so the live heap
// follows it, but parsed outputs may not stack on top of it: they more than
// doubled the heap before the client parsed one batch at a time.
func TestZcashClient_GetBlockByHash_DustSweepMemory(t *testing.T) {
	require := testutil.Require(t)

	block, total, peak := syncDustSweep(t, common.Blockchain_BLOCKCHAIN_ZCASH, common.Network_NETWORK_ZCASH_MAINNET)

	require.Less(peak, total*3/2, "parsed outputs must not stack on top of the raw transactions")
	rawInputTransactions := block.GetBitcoin().RawInputTransactions
	require.Len(rawInputTransactions, sweepParents)
	txID := sweepParentTxID(sweepParents - 1)
	require.Equal(sweepParentResult(sweepParentJSON(), txID), rawInputTransactions[txID])
}
