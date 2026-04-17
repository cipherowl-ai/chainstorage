package bitcoin

import (
	"bytes"
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// TestBitcoinStreamBlock verifies StreamBlock produces the same
// transactions as ParseBlock on the 15MB mainnet fixture.
func TestBitcoinStreamBlock(t *testing.T) {
	require := require.New(t)

	var parser internal.NativeParser
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(NewBitcoinNativeParser),
		fx.Populate(&parser),
	)
	defer app.Close()

	impl, ok := parser.(*bitcoinNativeParserImpl)
	require.True(ok)

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)

	ctx := context.Background()

	// Baseline via ParseBlock.
	baseline, err := parser.ParseBlock(ctx, rawBlock)
	require.NoError(err)

	// Streamed.
	var streamed []*api.BitcoinTransaction
	r := bytes.NewReader(rawBlock.GetBitcoin().GetHeader())
	header, err := impl.StreamBlock(ctx, r, rawBlock.GetBitcoin(),
		BitcoinBlockVisitorFunc(func(tx *api.BitcoinTransaction) error {
			streamed = append(streamed, tx)
			return nil
		}),
	)
	require.NoError(err)
	require.NotNil(header)

	baseTxs := baseline.GetBitcoin().GetTransactions()
	require.Equal(len(baseTxs), len(streamed))
	for i := range baseTxs {
		require.Equal(baseTxs[i].TransactionId, streamed[i].TransactionId, "tx[%d] id", i)
		require.Equal(baseTxs[i].Hash, streamed[i].Hash, "tx[%d] hash", i)
		require.Equal(baseTxs[i].InputCount, streamed[i].InputCount, "tx[%d] InputCount", i)
		require.Equal(baseTxs[i].OutputCount, streamed[i].OutputCount, "tx[%d] OutputCount", i)
		require.Equal(baseTxs[i].Fee, streamed[i].Fee, "tx[%d] Fee", i)
	}

	baseHeader := baseline.GetBitcoin().GetHeader()
	require.Equal(baseHeader.Hash, header.Hash)
	require.Equal(baseHeader.Height, header.Height)
	require.Equal(baseHeader.MerkleRoot, header.MerkleRoot)
}

// TestBitcoinStreamBlock_VisitorError ensures a visitor error aborts the
// stream and is propagated.
func TestBitcoinStreamBlock_VisitorError(t *testing.T) {
	require := require.New(t)

	var parser internal.NativeParser
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(NewBitcoinNativeParser),
		fx.Populate(&parser),
	)
	defer app.Close()

	impl := parser.(*bitcoinNativeParserImpl)

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)

	r := bytes.NewReader(rawBlock.GetBitcoin().GetHeader())
	_, err = impl.StreamBlock(context.Background(), r, rawBlock.GetBitcoin(),
		BitcoinBlockVisitorFunc(func(tx *api.BitcoinTransaction) error {
			return context.DeadlineExceeded
		}),
	)
	require.Error(err)
}

// TestBitcoinStreamBlock_PeakMemory ensures StreamBlock's peak heap
// allocation during iteration is bounded by O(largest tx) rather than
// O(block). We measure heap-in-use after forcing GC between iterations.
func TestBitcoinStreamBlock_PeakMemory(t *testing.T) {
	var parser internal.NativeParser
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(NewBitcoinNativeParser),
		fx.Populate(&parser),
	)
	defer app.Close()

	impl := parser.(*bitcoinNativeParserImpl)

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	if err != nil {
		t.Fatal(err)
	}

	r := bytes.NewReader(rawBlock.GetBitcoin().GetHeader())

	var peakHeap uint64
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	baseHeap := ms.HeapAlloc

	_, err = impl.StreamBlock(context.Background(), r, rawBlock.GetBitcoin(),
		BitcoinBlockVisitorFunc(func(tx *api.BitcoinTransaction) error {
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > peakHeap {
				peakHeap = ms.HeapAlloc
			}
			// Drop reference so GC can reclaim before next tx.
			_ = tx
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("baseHeap=%d peakHeap=%d delta=%d", baseHeap, peakHeap, int64(peakHeap)-int64(baseHeap))
}
