"""Fakes for the client tests: a ChainStorage server and an object store.

Both run in this process on a free local port, so the client under test uses
its real gRPC and HTTP code paths.
"""

from __future__ import annotations

import gzip
import json
import threading
import time
from collections import defaultdict, deque
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import grpc
import pytest
import zstandard

from chainstorage_sdk import Client, Config, RetryPolicy
from coinbase.c3.common import common_pb2
from coinbase.chainstorage import api_pb2, api_pb2_grpc, blockchain_pb2

TESTDATA = Path(__file__).parent / "testdata"
TAG = 2


class FakeStorage:
    """An object store that serves objects over HTTP, with range requests.

    A link carries a ``sig`` query parameter, like a signed link. With
    ``rotate`` set, every link gets its own signature, as real signed links
    do. Signatures listed in ``expired`` are refused with ``expired_status``.
    Every answer that is not the object carries ``error_body``.
    """

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.requests: list[tuple[str, str | None]] = []
        # Statuses to answer with, per object, before serving it.
        self.failures: dict[str, deque[int]] = defaultdict(deque)
        # Per object: how many bytes of the next range answers to send.
        self.truncations: dict[str, deque[int]] = defaultdict(deque)
        self.expired: set[str] = set()
        self.expired_status = 403
        # What a failure, a refused link and a missing object are answered
        # with, as a store explains itself.
        self.error_body = b""
        self.ignore_ranges = False
        self.signature = "s1"
        self.rotate = False
        self._issued = 0
        # With a delay, requests overlap, and the most that did is recorded.
        self.delay = 0.0
        self.most_at_once = 0
        self._active = 0
        self._lock = threading.Lock()
        storage = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                with storage._lock:
                    storage._active += 1
                    storage.most_at_once = max(storage.most_at_once, storage._active)
                try:
                    if storage.delay:
                        time.sleep(storage.delay)
                    self._serve()
                finally:
                    with storage._lock:
                        storage._active -= 1

            def _serve(self) -> None:
                parts = urlsplit(self.path)
                name = parts.path.lstrip("/")
                range_header = self.headers.get("Range")
                storage.requests.append((name, range_header))
                signature = parse_qs(parts.query).get("sig", [""])[0]
                if storage.failures[name]:
                    return self._reply(storage.failures[name].popleft(), storage.error_body)
                if signature in storage.expired:
                    return self._reply(storage.expired_status, storage.error_body)
                if name not in storage.objects:
                    return self._reply(404, storage.error_body)
                body = storage.objects[name]
                if range_header and not storage.ignore_ranges:
                    first, last = range_header.removeprefix("bytes=").split("-")
                    body = body[int(first) : int(last) + 1]
                    if storage.truncations[name]:
                        body = body[: storage.truncations[name].popleft()]
                    return self._reply(206, body)
                return self._reply(200, body)

            def _reply(self, status: int, body: bytes) -> None:
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        # A short poll interval, so that shutdown() returns quickly.
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        self._thread.start()

    def url(self, name: str) -> str:
        signature = self.signature
        if self.rotate:
            self._issued += 1
            signature = f"{signature}-{self._issued}"
        port = self._server.server_address[1]
        return f"http://127.0.0.1:{port}/{name}?sig={signature}"

    def requests_for(self, name: str) -> list[str | None]:
        return [range_header for requested, range_header in self.requests if requested == name]

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


class FakeChainStorage(api_pb2_grpc.ChainStorageServicer):
    """A ChainStorage server over the blocks and events a test gives it."""

    def __init__(self, storage: FakeStorage) -> None:
        self.storage = storage
        self.blocks: dict[int, blockchain_pb2.Block] = {}
        self.compression: dict[int, int] = {}
        self.skipped: set[int] = set()
        # Blocks that a reorganization replaced: (height, hash) -> object name.
        self.orphans: dict[tuple[int, str], str] = {}
        # Heights that also sit in a consolidated object: height -> BlockFile.
        self.consolidated: dict[int, api_pb2.BlockFile] = {}
        self.events: list[api_pb2.BlockchainEvent] = []
        self.transactions: dict[str, list[int]] = {}
        self.calls: list[tuple[str, object]] = []
        self.metadata: list[tuple[str, dict[str, str]]] = []
        self.failures: dict[str, deque[grpc.StatusCode]] = defaultdict(deque)
        self.delays: dict[str, float] = {}
        # One entry per stream the server will open: how many events to send
        # before it fails with the given code. Streams beyond the plan send
        # every event and then end.
        self.stream_plans: deque[tuple[int, grpc.StatusCode]] = deque()

    # -- test setup -------------------------------------------------------

    def add_block(self, height: int, *, compression: int = api_pb2.GZIP, size: int = 64) -> None:
        block = make_block(height, size)
        self.blocks[height] = block
        self.compression[height] = compression
        self.storage.objects[f"blocks/{height}"] = compress(block.SerializeToString(), compression)

    def add_orphan(self, height: int, hash: str) -> blockchain_pb2.Block:
        """Adds a block that is no longer on the chain: it is found by hash only."""
        block = make_block(height, 48)
        block.metadata.hash = hash
        name = f"orphans/{height}-{hash}"
        self.orphans[(height, hash)] = name
        self.storage.objects[name] = compress(block.SerializeToString(), api_pb2.GZIP)
        return block

    def add_consolidated(self, codec: str) -> list[dict]:
        """Serves one of the consolidated files written by the Go test
        ``TestPythonSDKFixtures``.

        Each of its heights also gets a single-block object, whose content
        differs, so a test can tell which of the two a read came from.
        """
        manifest = json.loads((TESTDATA / f"cscb_{codec}.json").read_text())
        name = f"consolidated/{manifest['file']}"
        self.storage.objects[name] = (TESTDATA / manifest["file"]).read_bytes()
        for entry in manifest["blocks"]:
            height = entry["height"]
            self.add_block(height)
            file = api_pb2.BlockFile(
                tag=entry["tag"],
                hash=entry["hash"],
                parent_hash=entry["parent_hash"],
                height=height,
                parent_height=entry["parent_height"],
                file_url=name,
                compression=api_pb2.Compression.Value(manifest["codec"]),
                object_format=blockchain_pb2.BLOCK_OBJECT_FORMAT_CSCB_BATCH,
                byte_offset=entry["byte_offset"],
                byte_length=entry["byte_length"],
                uncompressed_length=entry["uncompressed_length"],
            )
            file.block_timestamp.seconds = block_time(height)
            self.consolidated[height] = file
        return manifest["blocks"]

    def add_event(
        self,
        height: int,
        event_type: int = api_pb2.BlockchainEvent.BLOCK_ADDED,
        hash: str | None = None,
    ) -> None:
        number = len(self.events) + 1
        self.events.append(
            api_pb2.BlockchainEvent(
                sequence=str(number),
                sequence_num=number,
                type=event_type,
                event_tag=1,
                block=blockchain_pb2.BlockIdentifier(
                    tag=TAG, height=height, hash=hash or block_hash(height)
                ),
            )
        )

    def calls_to(self, method: str) -> list:
        return [request for called, request in self.calls if called == method]

    def metadata_of(self, method: str) -> list[dict[str, str]]:
        return [sent for called, sent in self.metadata if called == method]

    # -- the service ------------------------------------------------------

    def _enter(self, method: str, request: object, context: grpc.ServicerContext) -> None:
        self.calls.append((method, request))
        if len(self.calls) > 200:
            # A retry loop that never ends should fail a test, not hang it.
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "too many calls in one test")
        self.metadata.append((method, dict(context.invocation_metadata())))
        if method in self.delays:
            time.sleep(self.delays[method])
        if self.failures[method]:
            context.abort(self.failures[method].popleft(), "injected failure")

    def _block_file(self, height: int, hash: str, read_source: int, context) -> api_pb2.BlockFile:
        if hash and hash != block_hash(height):
            if (height, hash) not in self.orphans:
                context.abort(grpc.StatusCode.NOT_FOUND, f"block {height} {hash} not found")
            return api_pb2.BlockFile(
                tag=TAG,
                hash=hash,
                height=height,
                file_url=self.storage.url(self.orphans[(height, hash)]),
                compression=api_pb2.GZIP,
            )
        if height in self.skipped:
            return api_pb2.BlockFile(tag=TAG, height=height, skipped=True)
        if height in self.consolidated and read_source != api_pb2.BLOCK_READ_SOURCE_SINGLE_BLOCK:
            file = api_pb2.BlockFile()
            file.CopyFrom(self.consolidated[height])
            file.file_url = self.storage.url(file.file_url)
            return file
        if height not in self.blocks:
            context.abort(grpc.StatusCode.NOT_FOUND, f"block {height} not found")
        return api_pb2.BlockFile(
            tag=TAG,
            hash=block_hash(height),
            parent_hash=block_hash(height - 1),
            height=height,
            parent_height=height - 1,
            file_url=self.storage.url(f"blocks/{height}"),
            compression=self.compression[height],
        )

    def GetLatestBlock(self, request, context):
        self._enter("GetLatestBlock", request, context)
        height = max([*self.blocks, *self.skipped, *self.consolidated])
        return api_pb2.GetLatestBlockResponse(tag=TAG, height=height, hash=block_hash(height))

    def GetBlockFile(self, request, context):
        self._enter("GetBlockFile", request, context)
        file = self._block_file(request.height, request.hash, request.read_source, context)
        return api_pb2.GetBlockFileResponse(file=file)

    def GetBlockFilesByRange(self, request, context):
        self._enter("GetBlockFilesByRange", request, context)
        if request.end_height - request.start_height > 1000:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "block range size exceeded limit")
        return api_pb2.GetBlockFilesByRangeResponse(
            files=[
                self._block_file(height, "", request.read_source, context)
                for height in range(request.start_height, request.end_height)
            ]
        )

    def GetBlockByTransaction(self, request, context):
        self._enter("GetBlockByTransaction", request, context)
        if request.transaction_hash not in self.transactions:
            context.abort(grpc.StatusCode.NOT_FOUND, "transaction not found")
        return api_pb2.GetBlockByTransactionResponse(
            blocks=[
                blockchain_pb2.BlockIdentifier(tag=TAG, height=height, hash=block_hash(height))
                for height in self.transactions[request.transaction_hash]
            ]
        )

    def GetBlockByTimestamp(self, request, context):
        self._enter("GetBlockByTimestamp", request, context)
        height = request.timestamp // 10
        return api_pb2.GetBlockByTimestampResponse(
            tag=TAG, height=height, hash=block_hash(height), timestamp=height * 10
        )

    def GetChainEvents(self, request, context):
        self._enter("GetChainEvents", request, context)
        after = [event for event in self.events if event.sequence_num > request.sequence_num]
        return api_pb2.GetChainEventsResponse(events=after[: request.max_num_events or 1])

    def StreamChainEvents(self, request, context):
        self._enter("StreamChainEvents", request, context)
        limit, code = self.stream_plans.popleft() if self.stream_plans else (None, None)
        sent = 0
        for event in self.events:
            if event.sequence_num <= request.sequence_num:
                continue
            if limit is not None and sent == limit:
                context.abort(code, "injected stream failure")
            yield api_pb2.ChainEventsResponse(event=event)
            sent += 1
        if limit is not None:
            context.abort(code, "injected stream failure")

    def GetChainMetadata(self, request, context):
        self._enter("GetChainMetadata", request, context)
        return api_pb2.GetChainMetadataResponse(latest_block_tag=TAG, stable_block_tag=TAG)


def block_hash(height: int) -> str:
    return f"hash-{height}"


def block_time(height: int) -> int:
    return 1_700_000_000 + height


def make_block(height: int, size: int = 64) -> blockchain_pb2.Block:
    block = blockchain_pb2.Block(
        blockchain=common_pb2.BLOCKCHAIN_ETHEREUM,
        network=common_pb2.NETWORK_ETHEREUM_MAINNET,
        metadata=blockchain_pb2.BlockMetadata(
            tag=TAG,
            hash=block_hash(height),
            parent_hash=block_hash(height - 1),
            height=height,
            parent_height=height - 1,
        ),
    )
    block.ethereum.header = bytes((height + i) % 251 for i in range(size))
    return block


def compress(data: bytes, compression: int) -> bytes:
    if compression == api_pb2.GZIP:
        return gzip.compress(data)
    if compression == api_pb2.ZSTD:
        return zstandard.ZstdCompressor().compress(data)
    return data


class Deployment:
    """A fake server and store, and clients connected to them."""

    def __init__(self) -> None:
        self.storage = FakeStorage()
        self.server = FakeChainStorage(self.storage)
        self.sleeps: list[float] = []
        self._grpc = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        api_pb2_grpc.add_ChainStorageServicer_to_server(self.server, self._grpc)
        self.port = self._grpc.add_insecure_port("127.0.0.1:0")
        self._grpc.start()
        self._clients: list[Client] = []

    @property
    def address(self) -> str:
        return f"localhost:{self.port}"

    def config(self, **overrides: object) -> Config:
        settings: dict = {
            "server_address": self.address,
            "blockchain": common_pb2.BLOCKCHAIN_ETHEREUM,
            "network": common_pb2.NETWORK_ETHEREUM_MAINNET,
            "tag": TAG,
            "num_workers": 4,
            "retry": RetryPolicy(attempts=4),
        }
        settings.update(overrides)
        return Config(**settings)

    def client(self, **overrides: object) -> Client:
        # Waits are recorded and skipped, so that retries do not slow the tests.
        client = Client(self.config(**overrides), sleep=self.sleeps.append)
        self._clients.append(client)
        return client

    def close(self) -> None:
        for client in self._clients:
            client.close()
        self._grpc.stop(grace=None)
        self.storage.close()


@pytest.fixture
def deployment():
    deployment = Deployment()
    yield deployment
    deployment.close()
