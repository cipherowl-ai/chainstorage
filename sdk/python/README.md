# ChainStorage Python SDK

The Python counterpart of the Go SDK in [`sdk/`](..). It is a generic library:
it has no dependency on any particular deployment, and the server address is
always passed in by the caller.

## Status

| Part | State |
| --- | --- |
| Generated protobuf and gRPC classes (`coinbase.chainstorage`) | Available |
| Client and Session (blocks, chain events, parallel downloads) | Available |
| Block parser (the same native blocks as the Go parser) | Planned |

## Install

The SDK is released as a Git tag of this repository, the same way the Go SDK
is. It is not published to PyPI, so never install it by name from an index.

```bash
pip install "chainstorage-sdk @ git+https://github.com/cipherowl-ai/chainstorage@<tag>#subdirectory=sdk/python"
```

It needs Python 3.10 or newer.

## Use

```python
from chainstorage_sdk import Client, Config, StreamingConfiguration
from coinbase.chainstorage import api_pb2

config = Config(server_address="localhost:9090")

with Client(config) as client:
    height = client.get_latest_block()

    # One block, or a range [start, end). Blocks are raw `Block` messages.
    block = client.get_block(height)
    blocks = client.get_blocks_by_range(height - 10, height)

    # A long range, downloaded a little ahead of the loop, without parsing.
    for raw in client.iter_raw_block_payloads(height - 10_000, height):
        handle(raw.block_file.height, raw.payload)  # or client.parse_raw_block(raw)

    # Chain events, each with its block, resuming after a position.
    request = api_pb2.ChainEventsRequest(sequence_num=last_sequence_num)
    for result in client.stream_chain_events(StreamingConfiguration(request)):
        if result.event.type == api_pb2.BlockchainEvent.BLOCK_REMOVED:
            undo(result.event.block)
        else:
            apply(result.block)
```

### Connection

- **gRPC (the default).** `server_address` is `host:port`. The connection is
  plaintext when the address starts with `http://` or contains `localhost`,
  and TLS otherwise.
- **REST.** Pass `transport="rest"` and the gateway's base URL. The event
  stream is not available over REST.
- **Identity.** `client_id` is sent as `x-client-id`. `auth_header` and
  `auth_token` are sent as one header.

### What the client does for you

- **Parallel downloads.** Block files are downloaded `num_workers` at a time
  (10 by default) and handed over in height order.
- **Bounded memory.** A range is downloaded a little ahead of the caller, not
  all at once. About 256 MB of decompressed data may be on its way at a
  time; while a chunk is being cut into blocks it is held twice.
- **Consolidated block files.** A file that holds many blocks is read with
  range requests, and a chunk that holds several wanted blocks is fetched
  once. Every block is checked against its length, hash and checksum.
  `chunk_cache_bytes` (128 MB by default, 0 to turn it off) keeps the last
  chunks, so reading the blocks of one chunk one by one fetches it once.
- **Retries.** A call or a download that fails for a temporary reason is
  repeated with backoff: 4 attempts, starting at 200 ms. Set `retry` to change
  it.
- **Fallback.** A read from consolidated files that fails continues from
  single-block objects.
- **Expired links.** When storage refuses a signed link, the client asks the
  server for a fresh one.
- **One chain per range.** A range is requested 1,000 blocks at a time. The
  server checks the chain within each request, and the client checks that
  consecutive requests connect. If a reorganization happens in between, the
  read fails with a `RequestError` and can be started again.
- **Stream resume.** When the event stream drops, it is reopened after the
  last event the caller received.

### Errors

A failed call, download or check raises a `ChainStorageError`:

| Error | Meaning |
| --- | --- |
| `RequestError` | A call to the server failed. `code` holds the status. |
| `NotFoundError` | The server has no such block, transaction or event. |
| `DownloadError` | Fetching a block file from storage failed. |
| `BlockFormatError` | A block file is malformed or fails a check. |
| `ConfigError` | The configuration or an argument is invalid. |

### Signed links

A block file's link is signed, so it must not be printed. The SDK leaves the
signature out of its error messages and log lines, and out of the `repr` of a
`RawBlockPayload`. Two things remain yours to watch:

- `raw.block_file.file_url` is the full link.
- With `DEBUG` logging turned on for `urllib3`, that library logs every
  request line, signature included.

### Differences from the Go SDK

- The server address is a required argument. There are no built-in per-chain
  configurations.
- `client_timeout` is applied to every call.
- One retry policy covers every call: 4 attempts from 200 ms. The Go SDK
  retries gRPC calls up to 5 times from 500 ms.
- The event stream raises an error when it fails. It does not yield an error
  value.
- Expired links are refreshed.
- A range of any length can be read, in requests of 1,000 blocks that are
  checked to connect. The Go SDK leaves ranges above the server limit to the
  caller.
- A failed range read continues from the first block that is still missing.
  The Go SDK starts the range again from single-block objects.
- Over REST, only a `NOT_FOUND` reported in the gateway's error body counts as
  "not found". A bare 404 is an error.
- Not available yet, because they need the parser: block validation, the
  native block streams and static chain metadata.

### Generated classes

The protobuf and gRPC classes live under the `coinbase` namespace package,
because that is the package name in the `.proto` files. They run on the
protobuf 5 and 6 runtimes.

## Develop

Run these from the repository root.

```bash
pip install -e 'sdk/python[dev]'
make test-py     # ruff and pytest
make proto-py    # regenerate src/coinbase from protos/
```

`src/coinbase` is generated by `scripts/protogen-py.sh` and is never edited by
hand. After changing a `.proto` file, run `make proto-py` and commit the
result: CI regenerates the classes and fails on any difference.

The tests run the client against a fake server and a fake object store, both
in the test process (`tests/conftest.py`).

The consolidated files under `tests/testdata` are written by the Go encoder
and owned by the Go test `TestPythonSDKFixtures`
(`internal/storage/blobstorage/cscb`). That test reads them with the Go
reader as well, and fails when the format changes. Rewrite them with:

```bash
TEST_TYPE=unit go test ./internal/storage/blobstorage/cscb -run TestPythonSDKFixtures -update-python-sdk-fixtures
```
