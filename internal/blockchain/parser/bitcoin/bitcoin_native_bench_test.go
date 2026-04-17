package bitcoin

import (
	"bytes"
	"context"
	"testing"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// BenchmarkBitcoinParseBlock measures memory + allocations for a single
// ParseBlock call on a witness-heavy mainnet block (15MB fixture, block
// 731379). Sub-benchmarks vary which ParseOptions are set. Run with:
//
//	go test -run=^$ -bench=BenchmarkBitcoinParseBlock -benchmem \
//	    ./internal/blockchain/parser/bitcoin/
func BenchmarkBitcoinParseBlock(b *testing.B) {
	var parser internal.NativeParser
	app := testapp.New(b,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(NewBitcoinNativeParser),
		fx.Populate(&parser),
	)
	defer app.Close()

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	if err != nil {
		b.Fatalf("LoadRawBlock: %v", err)
	}

	ctx := context.Background()

	cases := []struct {
		name string
		opts []internal.ParseOption
	}{
		{"baseline", nil},
		{"skip_scripts", []internal.ParseOption{internal.WithSkipScripts()}},
		{"skip_witnesses", []internal.ParseOption{internal.WithSkipWitnesses()}},
		{"skip_scripts_witnesses", []internal.ParseOption{internal.WithSkipScripts(), internal.WithSkipWitnesses()}},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := parser.ParseBlock(ctx, rawBlock, c.opts...); err != nil {
					b.Fatalf("ParseBlock: %v", err)
				}
			}
		})
	}
}

// BenchmarkBitcoinStreamBlock measures StreamBlock on the same 15MB
// witness-heavy mainnet block. The visitor drops each transaction
// reference immediately so allocations-per-op reflect streaming overhead
// rather than aggregate block size.
func BenchmarkBitcoinStreamBlock(b *testing.B) {
	var parser internal.NativeParser
	app := testapp.New(b,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(NewBitcoinNativeParser),
		fx.Populate(&parser),
	)
	defer app.Close()

	impl := parser.(*bitcoinNativeParserImpl)

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	if err != nil {
		b.Fatalf("LoadRawBlock: %v", err)
	}

	headerBytes := rawBlock.GetBitcoin().GetHeader()
	ctx := context.Background()

	visitor := BitcoinBlockVisitorFunc(func(tx *api.BitcoinTransaction) error {
		_ = tx
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(headerBytes)
		if _, err := impl.StreamBlock(ctx, r, NewInMemoryInputTxGroupLoader(rawBlock.GetBitcoin().GetInputTransactions()), visitor); err != nil {
			b.Fatalf("StreamBlock: %v", err)
		}
	}
}
