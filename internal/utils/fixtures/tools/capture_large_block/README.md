# capture_large_block

One-time developer utility for capturing large-block RPC responses as fixtures
for the memory benchmark suite (`internal/blockchain/integration_test/memory_bench_test.go`).

The tool issues a single JSON-RPC request (or a batch call) against the supplied
endpoint URL and writes the raw response body to disk. The captured files are
committed to `internal/utils/fixtures/client/<chain>/large/` and replayed by the
benchmark via a mocked HTTP transport — no network access is required to run the
benchmarks themselves.

## Usage

### Single call

```bash
go run ./internal/utils/fixtures/tools/capture_large_block \
    --url "$QUICKNODE_ETHEREUM_URL" \
    --method debug_traceBlockByHash \
    --params '["0xBLOCKHASH",{"tracer":"callTracer","timeout":"90s"}]' \
    --output internal/utils/fixtures/client/ethereum/large/eth_traceblockbyhash.json
```

### Batch call

Put a JSON array of params arrays in a file:

```json
[
  ["0xtxid1", true],
  ["0xtxid2", true],
  ["0xtxid3", true]
]
```

Then:

```bash
go run ./internal/utils/fixtures/tools/capture_large_block \
    --url "$NOWNODES_BITCOIN_URL" \
    --method getrawtransaction \
    --batch-params-file /tmp/inputs.json \
    --output internal/utils/fixtures/client/bitcoin/large/btc_inputs_batch.json
```

## Credentials

Pass one of:
- `--user` / `--password` flags
- `RPC_BASIC_USER` / `RPC_BASIC_PASSWORD` env vars
- URL with embedded credentials (`https://user:pass@host/...`)

**Do not commit credentials.** The captured response bodies (JSON) are what goes in the repo.

## Suggested captures

Per the plan in `/Users/henry/.claude/plans/snappy-floating-spark.md`:

| Chain | Method | Target | Output path |
|-------|--------|--------|-------------|
| Ethereum | `eth_getBlockByHash` | Recent block with 500+ txs | `internal/utils/fixtures/client/ethereum/large/eth_getblock.json` |
| Ethereum | `eth_getBlockReceipts` | Same block | `internal/utils/fixtures/client/ethereum/large/eth_getblockreceipts.json` |
| Ethereum | `debug_traceBlockByHash` | Same block (the big one) | `internal/utils/fixtures/client/ethereum/large/eth_traceblockbyhash.json` |
| Solana | `getBlock` | Slot with 5000+ transactions | `internal/utils/fixtures/client/solana/large/sol_getblock.json` |
| Bitcoin | `getblock` | Block with consolidation tx (hundreds of inputs) | `internal/utils/fixtures/client/bitcoin/large/btc_getblock.json` |
| Bitcoin | `getrawtransaction` (batch) | All input txids from the Bitcoin block above | `internal/utils/fixtures/client/bitcoin/large/btc_getinputtx_batch.json` |
| Aptos | (per client hot path) | Recent heavy block | `internal/utils/fixtures/client/aptos/large/aptos_getblock.json` |
| Rosetta (Tron) | (per client hot path) | Recent heavy block | `internal/utils/fixtures/client/rosetta/large/rosetta_getblock.json` |

Document the exact block heights/hashes used in the PR description for reproducibility.

## Why not use the chainstorage `jsonrpc.Client`?

This tool is intentionally a standalone binary with no dependency on the
chainstorage config stack. A one-time developer utility should not require
spinning up the full fx graph, loading `secrets.yml`, or matching a specific
chain/network config — you just want to grab a blob of bytes from an RPC endpoint.
The captured fixtures are byte-identical to what the production `jsonrpc.Client`
would receive, so the benchmark correctness is preserved.
