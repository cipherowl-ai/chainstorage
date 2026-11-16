"""The client against a fake server and object store (see conftest.py)."""

from __future__ import annotations

import gzip
import hashlib
import io
import struct
import threading
import time
from concurrent.futures import CancelledError

import grpc
import pytest
import requests
import zstandard
from conftest import TAG, block_hash, block_time, make_block

from chainstorage_sdk import (
    BlockFormatError,
    CallTimeoutError,
    Client,
    Config,
    ConfigError,
    DownloadError,
    NotFoundError,
    RequestError,
    ResponseTooLargeError,
    RetryPolicy,
    Session,
    StreamingConfiguration,
    cscb,
)
from chainstorage_sdk import downloader as downloader_module
from chainstorage_sdk.downloader import BlockDownloader
from coinbase.c3.common import common_pb2
from coinbase.chainstorage import api_pb2, blockchain_pb2

DEFAULT = api_pb2.BLOCK_READ_SOURCE_DEFAULT
CONSOLIDATED = api_pb2.BLOCK_READ_SOURCE_CONSOLIDATED
SINGLE_BLOCK = api_pb2.BLOCK_READ_SOURCE_SINGLE_BLOCK
INDEX_RANGE = "bytes=0-65535"
CONSOLIDATED_OBJECT = "consolidated/cscb_zstd.bin"


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class StubTransport:
    """Answers calls from a function, and records them."""

    def __init__(self, answer=None, stream=None) -> None:
        self.calls: list[tuple[str, object, float]] = []
        self._answer = answer
        self._stream = stream

    def call(self, method, request, *, timeout):
        self.calls.append((method, request, timeout))
        return self._answer(method, request)

    def stream_chain_events(self, request):
        return self._stream(request)

    def close(self):
        pass


class StubDownloader:
    """Hands back empty payloads, or raises the errors a test queued."""

    def __init__(self, errors=()) -> None:
        self.errors = list(errors)
        self.downloads: list[list[int]] = []

    def iter_download(self, block_files):
        self.downloads.append([file.height for file in block_files])
        if self.errors:
            raise self.errors.pop(0)
        for _ in block_files:
            yield b""

    def close(self):
        pass


def chain_files(start: int, end: int, *, fork: str = "") -> list[api_pb2.BlockFile]:
    """Block files of consecutive blocks whose hashes carry the fork's name."""
    return [
        api_pb2.BlockFile(
            tag=TAG,
            height=height,
            hash=f"{fork}{height}",
            parent_hash=f"{fork}{height - 1}",
            file_url=f"http://storage.invalid/{height}",
        )
        for height in range(start, end)
    ]


# -- single blocks --------------------------------------------------------


def test_latest_block(deployment):
    deployment.server.add_block(7)
    deployment.server.add_block(9)

    assert deployment.client().get_latest_block() == 9
    assert deployment.server.calls_to("GetLatestBlock")[0].tag == TAG


@pytest.mark.parametrize("compression", [api_pb2.NONE, api_pb2.GZIP, api_pb2.ZSTD])
def test_get_block_reads_a_single_block_object(deployment, compression):
    deployment.server.add_block(5, compression=compression, size=4096)

    block = deployment.client().get_block(5)

    assert block == make_block(5, 4096)
    request = deployment.server.calls_to("GetBlockFile")[0]
    assert (request.tag, request.height, request.hash) == (TAG, 5, "")


def test_get_block_passes_the_hash_and_tag(deployment):
    deployment.server.add_block(5)

    deployment.client().get_block(5, "hash-5", tag=3)

    request = deployment.server.calls_to("GetBlockFile")[0]
    assert (request.tag, request.height, request.hash) == (3, 5, "hash-5")


def test_a_replaced_block_is_read_by_its_hash(deployment):
    deployment.server.add_block(5)
    orphan = deployment.server.add_orphan(5, "orphan-5")

    assert deployment.client().get_block(5, "orphan-5") == orphan
    assert deployment.client().get_block(5) == make_block(5)


def test_a_skipped_block_has_no_content(deployment):
    deployment.server.skipped.add(8)
    client = deployment.client()

    block = client.get_block(8)

    assert block.metadata.skipped
    assert (block.metadata.tag, block.metadata.height) == (TAG, 8)
    assert block.blockchain == common_pb2.BLOCKCHAIN_ETHEREUM
    assert block.network == common_pb2.NETWORK_ETHEREUM_MAINNET
    assert block.WhichOneof("blobdata") is None
    # As in the Go SDK, the raw payload of a skipped block is empty.
    assert client.open_raw_block_payload(8).payload == b""
    assert deployment.storage.requests == []


def test_a_missing_block_is_not_found(deployment):
    deployment.server.add_block(1)

    with pytest.raises(NotFoundError) as raised:
        deployment.client().get_block(2)

    assert raised.value.code == "NOT_FOUND"


def test_raw_payload_is_the_stored_block(deployment):
    deployment.server.add_block(5)
    client = deployment.client()

    raw = client.open_raw_block_payload(5)

    assert raw.payload == make_block(5).SerializeToString()
    assert raw.block_file.height == 5
    assert client.parse_raw_block(raw) == make_block(5)


def test_a_raw_payload_prints_neither_its_link_nor_its_content(deployment):
    deployment.server.add_block(5)

    raw = deployment.client().open_raw_block_payload(5)

    assert "sig=" in raw.block_file.file_url
    assert repr(raw) == f"RawBlockPayload(height=5, hash='hash-5', bytes={len(raw.payload)})"
    assert raw in {raw}  # usable in sets and as a dictionary key


# -- ranges ---------------------------------------------------------------


def test_range_returns_blocks_in_height_order(deployment):
    for height in range(10, 30):
        deployment.server.add_block(height)

    blocks = deployment.client().get_blocks_by_range(12, 27)

    assert blocks == [make_block(height) for height in range(12, 27)]
    # One request describes the whole range, so it is one consistent chain.
    requests = deployment.server.calls_to("GetBlockFilesByRange")
    assert [(r.start_height, r.end_height) for r in requests] == [(12, 27)]


def test_range_without_an_end_is_one_block(deployment):
    deployment.server.add_block(10)

    blocks = deployment.client().get_blocks_by_range(10)

    assert [block.metadata.height for block in blocks] == [10]


@pytest.mark.parametrize(("start", "end"), [(10, 10), (10, 9)])
def test_an_empty_range_is_refused(deployment, start, end):
    with pytest.raises(ConfigError, match="invalid range"):
        deployment.client().get_blocks_by_range(start, end)


def test_a_long_range_is_split_into_requests_the_server_allows(deployment):
    deployment.server.skipped.update(range(0, 2300))

    payloads = list(deployment.client().iter_raw_block_payloads(0, 2300))

    assert [raw.block_file.height for raw in payloads] == list(range(0, 2300))
    requests = deployment.server.calls_to("GetBlockFilesByRange")
    assert [(r.start_height, r.end_height) for r in requests] == [
        (0, 1000),
        (1000, 2000),
        (2000, 2300),
    ]


def test_a_range_is_downloaded_only_a_little_ahead_of_the_caller(deployment):
    for height in range(100, 140):
        deployment.server.add_block(height)
    payloads = deployment.client(num_workers=4).iter_raw_block_payloads(100, 140)

    assert next(payloads).block_file.height == 100

    # No more than one download per worker has started.
    assert 1 <= len(deployment.storage.requests) <= 4


def test_requests_of_a_long_range_must_connect():
    def answer(method, request):
        # The second request is answered from another fork of the chain.
        fork = "a-" if request.start_height == 0 else "b-"
        return api_pb2.GetBlockFilesByRangeResponse(
            files=chain_files(request.start_height, request.end_height, fork=fork)
        )

    downloader = StubDownloader()
    client = Client(
        Config(server_address="unused:1"), transport=StubTransport(answer), downloader=downloader
    )

    received = []
    with pytest.raises(RequestError, match="block 1000 does not follow block 999"):
        for raw in client.iter_raw_block_payloads(0, 1500):
            received.append(raw.block_file.height)

    # The first request's blocks were handed over; none of the second's were.
    assert received == list(range(0, 1000))
    assert downloader.downloads == [list(range(0, 1000))]


def test_requests_connect_across_skipped_blocks():
    def answer(method, request):
        files = chain_files(request.start_height, request.end_height)
        for file in files:
            if file.height in (999, 1000):
                file.CopyFrom(api_pb2.BlockFile(tag=TAG, height=file.height, skipped=True))
            if file.height == 1001:
                file.parent_hash = "998"
        return api_pb2.GetBlockFilesByRangeResponse(files=files)

    client = Client(
        Config(server_address="unused:1"),
        transport=StubTransport(answer),
        downloader=StubDownloader(),
    )

    assert len(list(client.iter_raw_block_payloads(0, 1500))) == 1500


def test_an_answer_for_other_heights_is_an_error():
    def answer(method, request):
        return api_pb2.GetBlockFilesByRangeResponse(files=chain_files(2, 5))

    client = Client(
        Config(server_address="unused:1"),
        transport=StubTransport(answer),
        downloader=StubDownloader(),
    )

    with pytest.raises(RequestError, match="expected the block file of height 1, got 2"):
        client.get_blocks_by_range(1, 4, read_source=SINGLE_BLOCK)


def test_a_short_answer_from_the_server_is_an_error():
    def answer(method, request):
        return api_pb2.GetBlockFilesByRangeResponse(files=chain_files(1, 2))

    client = Client(
        Config(server_address="unused:1"),
        transport=StubTransport(answer),
        downloader=StubDownloader(),
    )

    with pytest.raises(RequestError, match="expected 3 block files"):
        client.get_blocks_by_range(1, 4, read_source=SINGLE_BLOCK)


# -- consolidated objects -------------------------------------------------


@pytest.mark.parametrize("codec", ["gzip", "zstd"])
def test_get_block_reads_a_consolidated_object(deployment, codec):
    entries = deployment.server.add_consolidated(codec)

    raw = deployment.client().open_raw_block_payload(1004)

    assert sha256(raw.payload) == entries[4]["payload_sha256"]
    name = f"consolidated/cscb_{codec}.bin"
    assert len(deployment.storage.requests_for(name)) == 2  # the index, then one chunk
    assert deployment.storage.requests_for(name)[0] == INDEX_RANGE


def test_a_consolidated_block_takes_its_metadata_from_the_block_file(deployment):
    entries = deployment.server.add_consolidated("zstd")

    metadata = deployment.client().get_block(1001).metadata

    assert metadata.object_format == blockchain_pb2.BLOCK_OBJECT_FORMAT_CSCB_BATCH
    assert metadata.byte_offset == entries[1]["byte_offset"]
    assert metadata.byte_length == entries[1]["byte_length"]
    assert metadata.uncompressed_length == entries[1]["uncompressed_length"]
    assert (metadata.tag, metadata.height, metadata.hash) == (TAG, 1001, "hash-1001")
    assert (metadata.parent_height, metadata.parent_hash) == (1000, "hash-1000")
    assert metadata.timestamp.seconds == block_time(1001)


def test_a_range_fetches_each_chunk_once(deployment):
    entries = deployment.server.add_consolidated("zstd")

    payloads = list(deployment.client().iter_raw_block_payloads(1000, 1006))

    assert [sha256(raw.payload) for raw in payloads] == [e["payload_sha256"] for e in entries]
    ranges = deployment.storage.requests_for(CONSOLIDATED_OBJECT)
    assert len(ranges) == 3  # the index and the two chunks, for six blocks
    assert ranges.count(INDEX_RANGE) == 1


def test_the_index_and_chunk_are_kept_for_the_next_read(deployment):
    deployment.server.add_consolidated("zstd")
    # Every link has its own signature, as real signed links do: the object
    # must still be recognized as the same one.
    deployment.storage.rotate = True
    client = deployment.client()

    client.get_block(1000)
    client.get_block(1001)  # in the same chunk as 1000

    assert len(deployment.storage.requests_for(CONSOLIDATED_OBJECT)) == 2


def test_without_a_chunk_cache_every_read_fetches_its_chunk(deployment):
    deployment.server.add_consolidated("zstd")
    client = deployment.client(chunk_cache_bytes=0)

    client.get_block(1000)
    client.get_block(1001)

    # The index is still kept: one index read, and the chunk twice.
    assert len(deployment.storage.requests_for(CONSOLIDATED_OBJECT)) == 3


@pytest.mark.parametrize(("budget", "expected"), [(1, 1), (None, 2)])
def test_downloads_overlap_only_within_the_memory_budget(deployment, monkeypatch, budget, expected):
    deployment.server.add_consolidated("zstd")
    client = deployment.client()
    client.get_block(1000, read_source=SINGLE_BLOCK)
    deployment.storage.most_at_once = 0
    deployment.storage.delay = 0.2
    if budget is not None:
        monkeypatch.setattr(downloader_module, "_IN_FLIGHT_BYTES", budget)

    list(client.iter_raw_block_payloads(1000, 1006, read_source=CONSOLIDATED))

    # Two chunks: together with a normal budget, one after the other when
    # the budget only has room for one.
    assert deployment.storage.most_at_once == expected


@pytest.mark.parametrize("compression", [api_pb2.GZIP, api_pb2.ZSTD])
@pytest.mark.parametrize("damage", ["empty", "cut off"])
def test_a_damaged_single_block_object_is_refused(deployment, compression, damage):
    deployment.server.add_block(5, compression=compression, size=4096)
    body = deployment.storage.objects["blocks/5"]
    deployment.storage.objects["blocks/5"] = b"" if damage == "empty" else body[:-1]

    with pytest.raises(BlockFormatError, match="failed to decompress"):
        deployment.client().get_block(5, read_source=SINGLE_BLOCK)


def test_a_single_block_of_the_wrong_length_is_refused(deployment):
    deployment.server.add_block(5)
    file = api_pb2.BlockFile(
        height=5,
        file_url=deployment.storage.url("blocks/5"),
        compression=api_pb2.GZIP,
        uncompressed_length=len(make_block(5).SerializeToString()) + 1,
    )
    downloader = BlockDownloader(deployment.config())

    try:
        with pytest.raises(BlockFormatError, match="bytes the server reported"):
            downloader.download(file)
        file.uncompressed_length -= 1
        assert downloader.download(file) == make_block(5).SerializeToString()
    finally:
        downloader.close()


def zstd_without_a_declared_size(data: bytes) -> bytes:
    return zstandard.ZstdCompressor(write_content_size=False).compress(data)


def single_block_file(deployment, name: str, compression: int, length: int = 0):
    return api_pb2.BlockFile(
        height=5,
        file_url=deployment.storage.url(name),
        compression=compression,
        uncompressed_length=length,
    )


def test_a_single_block_declaring_too_many_bytes_is_refused_before_it_is_read(
    deployment, monkeypatch
):
    deployment.server.add_block(5)
    monkeypatch.setattr(cscb, "MAX_CHUNK_BYTES", 1000)
    downloader = BlockDownloader(deployment.config())

    try:
        with pytest.raises(BlockFormatError, match="declares 1001 bytes, more than the 1000"):
            downloader.download(single_block_file(deployment, "blocks/5", api_pb2.GZIP, 1001))
    finally:
        downloader.close()
    assert deployment.storage.requests_for("blocks/5") == []


@pytest.mark.parametrize(
    ("compression", "compress"),
    [(api_pb2.GZIP, gzip.compress), (api_pb2.ZSTD, zstd_without_a_declared_size)],
    ids=["gzip", "zstd without a declared size"],
)
def test_a_single_block_of_unknown_length_may_not_grow_past_the_cap(
    deployment, monkeypatch, compression, compress
):
    payload = make_block(5, 4096).SerializeToString()
    deployment.storage.objects["unsized"] = compress(payload)
    downloader = BlockDownloader(deployment.config())

    try:
        file = single_block_file(deployment, "unsized", compression)
        assert downloader.download(file) == payload
        monkeypatch.setattr(cscb, "MAX_CHUNK_BYTES", len(payload) - 1)
        with pytest.raises(BlockFormatError, match="bytes allowed"):
            downloader.download(file)
    finally:
        downloader.close()


@pytest.mark.parametrize("declares_its_size", [True, False])
def test_a_single_block_file_with_a_second_zstd_frame_is_refused(deployment, declares_its_size):
    compressor = zstandard.ZstdCompressor(write_content_size=declares_its_size)
    deployment.storage.objects["two-frames"] = compressor.compress(
        b"first frame, "
    ) + compressor.compress(b"second frame")
    # The server does not say how long the block is, so nothing else would
    # show that the first frame is not all of it.
    file = api_pb2.BlockFile(
        height=5, file_url=deployment.storage.url("two-frames"), compression=api_pb2.ZSTD
    )
    downloader = BlockDownloader(deployment.config())

    try:
        with pytest.raises(BlockFormatError, match="unused data|more data follows"):
            downloader.download(file)
    finally:
        downloader.close()


def test_a_single_block_object_larger_than_the_cap_is_not_read_whole(deployment, monkeypatch):
    deployment.storage.objects["huge"] = b"x" * 10_000
    monkeypatch.setattr(cscb, "MAX_CHUNK_BYTES", 1000)
    downloader = BlockDownloader(deployment.config())

    try:
        with pytest.raises(DownloadError, match="longer than the 1000 bytes allowed"):
            downloader.download(single_block_file(deployment, "huge", api_pb2.NONE))
    finally:
        downloader.close()


class _EndlessResponse:
    """A streamed response whose body never ends; counts what is read."""

    def __init__(self) -> None:
        self.pieces_read = 0

    def iter_content(self, chunk_size: int):
        while True:
            self.pieces_read += 1
            yield b"x" * chunk_size


def test_a_response_is_read_no_further_than_needed(monkeypatch):
    monkeypatch.setattr(downloader_module, "_RESPONSE_PIECE", 10)
    response = _EndlessResponse()

    assert downloader_module._read_at_most(response, 25) == b"x" * 25
    assert response.pieces_read == 3


def test_a_store_that_ignores_ranges_is_read_only_as_far_as_the_range(deployment, monkeypatch):
    entries = deployment.server.add_consolidated("zstd")
    deployment.storage.ignore_ranges = True
    monkeypatch.setattr(downloader_module, "_RESPONSE_PIECE", 1024)
    read: list[int] = []
    real = downloader_module._read_at_most

    def recording(response, size, limits):
        body = real(response, size, limits)
        read.append(len(body))
        return body

    monkeypatch.setattr(downloader_module, "_read_at_most", recording)
    whole = len(deployment.storage.objects[CONSOLIDATED_OBJECT])
    monkeypatch.setattr(cscb, "INITIAL_INDEX_READ_SIZE", 128)
    downloader = BlockDownloader(deployment.config())
    file = api_pb2.BlockFile()
    file.CopyFrom(deployment.server.consolidated[entries[0]["height"]])
    file.file_url = deployment.storage.url(file.file_url)

    try:
        with pytest.raises(DownloadError, match="range request was not honoured"):
            downloader.download(file)
    finally:
        downloader.close()
    # The index read starts at 0, so the whole object would be accepted, but
    # only what the range asked for (and one byte more) was read.
    assert read[0] == 129 < whole


def test_a_long_index_is_read_in_a_second_request(deployment, monkeypatch):
    entries = deployment.server.add_consolidated("zstd")
    monkeypatch.setattr(cscb, "INITIAL_INDEX_READ_SIZE", 128)
    index_length = cscb.parse_header(deployment.storage.objects[CONSOLIDATED_OBJECT]).index_length

    raw = deployment.client().open_raw_block_payload(1005)

    assert sha256(raw.payload) == entries[5]["payload_sha256"]
    ranges = deployment.storage.requests_for(CONSOLIDATED_OBJECT)
    assert ranges[:2] == ["bytes=0-127", f"bytes=128-{index_length - 1}"]


def test_a_cut_off_chunk_is_fetched_again(deployment):
    entries = deployment.server.add_consolidated("zstd")
    client = deployment.client()
    client.get_block(1000)  # reads the index
    deployment.storage.truncations[CONSOLIDATED_OBJECT].append(10)

    raw = client.open_raw_block_payload(1004)

    assert sha256(raw.payload) == entries[4]["payload_sha256"]
    assert len(deployment.sleeps) == 1


def test_a_chunk_read_needs_a_store_that_honours_ranges(deployment):
    deployment.server.add_consolidated("zstd")
    deployment.storage.ignore_ranges = True
    file = api_pb2.BlockFile()
    file.CopyFrom(deployment.server.consolidated[1004])
    file.file_url = deployment.storage.url(file.file_url)
    downloader = BlockDownloader(deployment.config())

    try:
        # The index is read from the start of the object, so the whole
        # object is an acceptable answer; a chunk in the middle is not.
        with pytest.raises(DownloadError, match="range request was not honoured"):
            downloader.download(file)
    finally:
        downloader.close()


# -- read-source fallback -------------------------------------------------


@pytest.mark.parametrize("read_source", [DEFAULT, CONSOLIDATED])
def test_a_failed_consolidated_read_falls_back_to_single_block(deployment, read_source):
    deployment.server.add_consolidated("zstd")
    deployment.storage.objects[CONSOLIDATED_OBJECT] = b"damaged"

    block = deployment.client().get_block(1002, read_source=read_source)

    # The single-block object of this height has the fake's own content.
    assert block == make_block(1002)
    sources = [r.read_source for r in deployment.server.calls_to("GetBlockFile")]
    assert sources == [read_source, SINGLE_BLOCK]


def test_a_range_falls_back_to_single_block(deployment):
    deployment.server.add_consolidated("zstd")
    deployment.storage.objects[CONSOLIDATED_OBJECT] = b"damaged"

    blocks = deployment.client().get_blocks_by_range(1000, 1006)

    assert blocks == [make_block(height) for height in range(1000, 1006)]


def test_a_range_that_fails_midway_continues_without_starting_over(deployment):
    entries = deployment.server.add_consolidated("zstd")
    # Damage the second chunk only: blocks 1000-1002 still read from the
    # consolidated object, and 1003-1005 have to come from single blocks.
    data = deployment.storage.objects[CONSOLIDATED_OBJECT]
    chunk = cscb.parse_index(data).chunks[1]
    position = chunk.compressed_payload_offset + chunk.compressed_length // 2
    deployment.storage.objects[CONSOLIDATED_OBJECT] = (
        data[:position] + bytes([data[position] ^ 0xFF]) + data[position + 1 :]
    )

    payloads = list(deployment.client().iter_raw_block_payloads(1000, 1006))

    assert [raw.block_file.height for raw in payloads] == list(range(1000, 1006))
    assert [sha256(raw.payload) for raw in payloads[:3]] == [
        entry["payload_sha256"] for entry in entries[:3]
    ]
    assert [raw.payload for raw in payloads[3:]] == [
        make_block(height).SerializeToString() for height in range(1003, 1006)
    ]
    # Only the blocks that were still missing were asked for again.
    again = deployment.server.calls_to("GetBlockFilesByRange")[1]
    assert (again.start_height, again.end_height, again.read_source) == (1003, 1006, SINGLE_BLOCK)


def test_a_range_whose_links_stay_refused_falls_back_too(deployment):
    deployment.server.add_consolidated("zstd")
    deployment.storage.failures[CONSOLIDATED_OBJECT].extend([403] * 50)

    blocks = deployment.client().get_blocks_by_range(1000, 1006)

    assert blocks == [make_block(height) for height in range(1000, 1006)]
    sources = [r.read_source for r in deployment.server.calls_to("GetBlockFilesByRange")]
    assert sources == [DEFAULT, DEFAULT, DEFAULT, SINGLE_BLOCK]


def test_a_range_fails_when_the_fallback_fails_too():
    def answer(method, request):
        return api_pb2.GetBlockFilesByRangeResponse(files=chain_files(10, 14))

    transport = StubTransport(answer)
    downloader = StubDownloader([BlockFormatError("first"), BlockFormatError("second")])
    client = Client(Config(server_address="unused:1"), transport=transport, downloader=downloader)

    with pytest.raises(BlockFormatError, match="second"):
        list(client.iter_raw_block_payloads(10, 14))

    assert [request.read_source for _, request, _ in transport.calls] == [DEFAULT, SINGLE_BLOCK]


def test_a_single_block_read_has_no_fallback(deployment):
    deployment.server.add_block(3)
    deployment.storage.objects["blocks/3"] = b"damaged"

    with pytest.raises(BlockFormatError):
        deployment.client().get_block(3, read_source=SINGLE_BLOCK)

    assert len(deployment.server.calls_to("GetBlockFile")) == 1


def test_when_the_fallback_fails_too_its_error_is_raised(deployment):
    deployment.server.add_consolidated("zstd")
    deployment.storage.objects[CONSOLIDATED_OBJECT] = b"damaged"
    deployment.storage.objects["blocks/1002"] = b"damaged too"

    with pytest.raises(BlockFormatError) as raised:
        deployment.client().get_block(1002)

    assert "failed to decompress" in str(raised.value)
    assert isinstance(raised.value.__cause__, BlockFormatError)


def test_a_range_stops_when_the_chain_changes_under_it():
    answers = [chain_files(10, 14, fork="a-"), chain_files(10, 14, fork="b-")]

    def answer(method, request):
        return api_pb2.GetBlockFilesByRangeResponse(files=answers.pop(0))

    client = Client(
        Config(server_address="unused:1"),
        transport=StubTransport(answer),
        downloader=StubDownloader([DownloadError("refused", status_code=403)]),
    )

    # The links expired, and by the time fresh ones were asked for, the
    # blocks at these heights had been replaced.
    with pytest.raises(RequestError, match="block 10 was replaced"):
        list(client.iter_raw_block_payloads(10, 14))


# -- downloads: retries and expired links ---------------------------------


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_a_download_is_retried_on_a_temporary_failure(deployment, status):
    deployment.server.add_block(5)
    deployment.storage.failures["blocks/5"].extend([status, status])

    block = deployment.client().get_block(5, read_source=SINGLE_BLOCK)

    assert block == make_block(5)
    assert len(deployment.storage.requests_for("blocks/5")) == 3
    assert len(deployment.sleeps) == 2


def test_a_download_gives_up_after_the_policy_allows(deployment):
    deployment.server.add_block(5)
    deployment.storage.failures["blocks/5"].extend([503] * 10)

    with pytest.raises(DownloadError) as raised:
        deployment.client(retry=RetryPolicy(attempts=3)).get_block(5, read_source=SINGLE_BLOCK)

    assert raised.value.status_code == 503
    assert len(deployment.storage.requests_for("blocks/5")) == 3


def test_a_download_is_not_retried_on_a_permanent_failure(deployment):
    deployment.server.add_block(5)
    del deployment.storage.objects["blocks/5"]

    with pytest.raises(DownloadError) as raised:
        deployment.client().get_block(5, read_source=SINGLE_BLOCK)

    assert raised.value.status_code == 404
    assert len(deployment.storage.requests_for("blocks/5")) == 1


def test_a_store_that_cannot_be_reached_is_retried_and_its_link_stays_hidden(deployment):
    sleeps: list[float] = []
    downloader = BlockDownloader(deployment.config(), sleep=sleeps.append)
    # Port 1 refuses connections.
    file = api_pb2.BlockFile(height=5, file_url="http://127.0.0.1:1/blocks/5?sig=secret")

    try:
        with pytest.raises(DownloadError) as raised:
            downloader.download(file)
    finally:
        downloader.close()

    assert len(sleeps) == 3  # four attempts
    assert raised.value.status_code is None
    assert "blocks/5" in str(raised.value)
    assert "secret" not in str(raised.value)
    # The library's own error names the full link, so it must not be attached.
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__


@pytest.mark.parametrize("status", [400, 403])
def test_an_expired_link_is_replaced_by_a_fresh_one(deployment, status):
    deployment.server.add_block(5)
    deployment.storage.expired.add("s1")
    deployment.storage.expired_status = status
    client = deployment.client()
    original_enter = deployment.server._enter

    def issue_new_links_from_the_second_call(method, request, context):
        original_enter(method, request, context)
        if len(deployment.server.calls_to("GetBlockFile")) == 2:
            deployment.storage.signature = "s2"

    deployment.server._enter = issue_new_links_from_the_second_call

    block = client.get_block(5, read_source=SINGLE_BLOCK)

    assert block == make_block(5)
    assert len(deployment.server.calls_to("GetBlockFile")) == 2


def test_expired_links_of_a_range_are_replaced(deployment):
    for height in range(20, 26):
        deployment.server.add_block(height)
    deployment.storage.expired.add("s1")
    original_enter = deployment.server._enter

    def issue_new_links_from_the_second_call(method, request, context):
        original_enter(method, request, context)
        if len(deployment.server.calls_to("GetBlockFilesByRange")) == 2:
            deployment.storage.signature = "s2"

    deployment.server._enter = issue_new_links_from_the_second_call

    blocks = deployment.client().get_blocks_by_range(20, 26, read_source=SINGLE_BLOCK)

    assert blocks == [make_block(height) for height in range(20, 26)]
    assert len(deployment.server.calls_to("GetBlockFilesByRange")) == 2


def test_refused_links_are_counted_from_the_last_progress():
    class OneBlockThenRefused:
        """Hands over one block, then meets an expired link, every time."""

        def iter_download(self, block_files):
            yield b""
            raise DownloadError("refused", status_code=403)

        def close(self):
            pass

    def answer(method, request):
        return api_pb2.GetBlockFilesByRangeResponse(
            files=chain_files(request.start_height, request.end_height)
        )

    transport = StubTransport(answer)
    client = Client(
        Config(server_address="unused:1"),
        transport=transport,
        downloader=OneBlockThenRefused(),
    )

    # Ten refusals in total, but never three in a row without a block in
    # between, so the read goes on, always from the first missing block.
    payloads = list(client.iter_raw_block_payloads(10, 20, read_source=SINGLE_BLOCK))

    assert [raw.block_file.height for raw in payloads] == list(range(10, 20))
    asked = [(request.start_height, request.end_height) for _, request, _ in transport.calls]
    assert asked == [(10, 20)] + [(first, 20) for first in range(11, 20)]


@pytest.mark.parametrize("method", ["get_block", "get_blocks_by_range"])
def test_links_that_stay_refused_fail_after_three_tries(deployment, method):
    deployment.server.add_block(5)
    deployment.storage.expired.add("s1")

    with pytest.raises(DownloadError) as raised:
        getattr(deployment.client(), method)(5, read_source=SINGLE_BLOCK)

    assert raised.value.status_code == 403
    asked = "GetBlockFile" if method == "get_block" else "GetBlockFilesByRange"
    assert len(deployment.server.calls_to(asked)) == 3


def test_an_error_never_shows_the_signature_of_a_link(deployment):
    deployment.server.add_block(5)
    deployment.storage.expired.add("s1")

    with pytest.raises(DownloadError) as raised:
        deployment.client().get_block(5, read_source=SINGLE_BLOCK)

    assert "blocks/5" in str(raised.value)
    assert "sig=" not in str(raised.value)


# What a store answers a refused link with. Only the code may be kept: the
# rest of such a body can repeat parts of the request.
STORE_REFUSAL = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Error><Code>ExpiredToken</Code>"
    b"<Message>The provided token has expired.</Message>"
    b"<Token-0>sig=secret</Token-0></Error>"
)


def test_a_refused_link_reports_the_code_the_store_gave(deployment):
    deployment.server.add_block(5)
    deployment.storage.expired.add("s1")
    deployment.storage.error_body = STORE_REFUSAL

    with pytest.raises(DownloadError) as raised:
        deployment.client().get_block(5, read_source=SINGLE_BLOCK)

    assert raised.value.status_code == 403
    assert raised.value.error_code == "ExpiredToken"
    assert "received status 403 (ExpiredToken) for " in str(raised.value)
    assert "has expired" not in str(raised.value)
    assert "secret" not in str(raised.value)


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"<html>Forbidden</html>",
        b"<Code>not a code</Code>",
        b"<Code>" + b"A" * 65 + b"</Code>",
        # Only the first 2 KiB of a refusal are looked at.
        b" " * 2048 + b"<Code>ExpiredToken</Code>",
    ],
)
def test_a_refusal_without_a_code_at_its_start_reports_none(deployment, body):
    deployment.server.add_block(5)
    deployment.storage.expired.add("s1")
    deployment.storage.error_body = body

    with pytest.raises(DownloadError) as raised:
        deployment.client().get_block(5, read_source=SINGLE_BLOCK)

    assert raised.value.status_code == 403
    assert raised.value.error_code == ""
    assert "received status 403 for " in str(raised.value)


def test_a_missing_object_reports_the_stores_code(deployment):
    deployment.server.add_block(5)
    del deployment.storage.objects["blocks/5"]
    deployment.storage.error_body = b"<Error><Code>NoSuchKey</Code></Error>"

    with pytest.raises(DownloadError) as raised:
        deployment.client().get_block(5, read_source=SINGLE_BLOCK)

    assert (raised.value.status_code, raised.value.error_code) == (404, "NoSuchKey")


def test_a_temporary_failure_with_a_code_is_still_retried(deployment):
    deployment.server.add_block(5)
    deployment.storage.failures["blocks/5"].extend([503] * 10)
    deployment.storage.error_body = b"<Error><Code>SlowDown</Code></Error>"

    with pytest.raises(DownloadError) as raised:
        deployment.client(retry=RetryPolicy(attempts=3)).get_block(5, read_source=SINGLE_BLOCK)

    assert (raised.value.status_code, raised.value.error_code) == (503, "SlowDown")
    assert raised.value.retryable
    assert len(deployment.storage.requests_for("blocks/5")) == 3


@pytest.mark.parametrize("codec", ["gzip", "zstd"])
def test_a_refused_read_of_a_consolidated_file_reports_the_stores_code(deployment, codec):
    height = deployment.server.add_consolidated(codec)[0]["height"]
    deployment.storage.expired.add("s1")
    deployment.storage.error_body = STORE_REFUSAL
    file = api_pb2.BlockFile()
    file.CopyFrom(deployment.server.consolidated[height])
    file.file_url = deployment.storage.url(file.file_url)
    downloader = BlockDownloader(deployment.config())

    try:
        with pytest.raises(DownloadError) as raised:
            downloader.download(file)
    finally:
        downloader.close()

    assert (raised.value.status_code, raised.value.error_code) == (403, "ExpiredToken")
    assert "sig=" not in str(raised.value)


class _UnreadableRefusal:
    """A refusal whose body fails while it is read, naming the signed link."""

    status_code = 403

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def iter_content(self, chunk_size=None):
        raise OSError("connection lost reading http://store/blocks/5?sig=secret")


class _RefusingSession:
    def get(self, url, headers=None, timeout=None, stream=False):
        return _UnreadableRefusal()

    def close(self):
        pass


def test_a_refusal_whose_body_cannot_be_read_still_reports_its_status(deployment):
    downloader = BlockDownloader(deployment.config(), session=_RefusingSession())
    file = api_pb2.BlockFile(height=5, file_url="http://store/blocks/5?sig=secret")

    try:
        with pytest.raises(DownloadError) as raised:
            downloader.download(file)
    finally:
        downloader.close()

    assert (raised.value.status_code, raised.value.error_code) == (403, "")
    assert "secret" not in str(raised.value)
    assert raised.value.__cause__ is None and raised.value.__context__ is None


@pytest.mark.parametrize(
    ("limits", "read"),
    [({}, 2048), ({"max_response_bytes": 64}, 64), ({"max_response_bytes": 10**6}, 2048)],
)
def test_a_refusal_is_read_in_small_pieces(deployment, limits, read):
    """A store may answer a refusal with a large body; little of it is taken,
    and no more than the caller allows a response."""
    asked = []

    class Refusal(_UnreadableRefusal):
        def iter_content(self, chunk_size=None):
            asked.append(chunk_size)
            while True:
                yield b"x" * chunk_size

    class Session(_RefusingSession):
        def get(self, url, headers=None, timeout=None, stream=False):
            return Refusal()

    downloader = BlockDownloader(deployment.config(), session=Session())

    try:
        with pytest.raises(DownloadError) as raised:
            downloader.download(
                api_pb2.BlockFile(height=5, file_url="http://store/blocks/5"), **limits
            )
    finally:
        downloader.close()

    assert asked == [read]
    assert raised.value.error_code == ""


@pytest.mark.parametrize(
    "method, asking",
    [("get_block", "asking for a fresh one"), ("get_blocks_by_range", "asking for fresh ones")],
)
def test_asking_for_fresh_links_logs_the_status_and_the_stores_code(
    deployment, caplog, method, asking
):
    deployment.server.add_block(5)
    deployment.storage.expired.add("s1")
    deployment.storage.error_body = STORE_REFUSAL

    with caplog.at_level("INFO", logger="chainstorage_sdk.client"):
        with pytest.raises(DownloadError):
            getattr(deployment.client(), method)(5, read_source=SINGLE_BLOCK)

    refused = [r for r in caplog.records if "was refused" in r.getMessage()]
    assert [r.getMessage() for r in refused] == [
        f"a block file link was refused (403 ExpiredToken); {asking}"
    ] * 2
    assert {r.levelname for r in refused} == {"WARNING"}
    assert "secret" not in caplog.text


# -- server calls: retries, timeouts, headers -----------------------------


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    ],
)
def test_a_call_is_retried_on_a_temporary_failure(deployment, code):
    deployment.server.add_block(4)
    deployment.server.failures["GetLatestBlock"].extend([code, code])

    assert deployment.client().get_latest_block() == 4
    assert len(deployment.server.calls_to("GetLatestBlock")) == 3
    assert len(deployment.sleeps) == 2


def test_a_call_is_not_retried_on_a_permanent_failure(deployment):
    deployment.server.add_block(4)
    deployment.server.failures["GetLatestBlock"].append(grpc.StatusCode.INVALID_ARGUMENT)

    with pytest.raises(RequestError) as raised:
        deployment.client().get_latest_block()

    assert raised.value.code == "INVALID_ARGUMENT"
    assert not raised.value.retryable
    assert len(deployment.server.calls_to("GetLatestBlock")) == 1


def test_a_call_gives_up_after_the_policy_allows(deployment):
    deployment.server.add_block(4)
    deployment.server.failures["GetLatestBlock"].extend([grpc.StatusCode.UNAVAILABLE] * 10)

    with pytest.raises(RequestError) as raised:
        deployment.client(retry=RetryPolicy(attempts=2)).get_latest_block()

    assert raised.value.code == "UNAVAILABLE"
    assert len(deployment.server.calls_to("GetLatestBlock")) == 2


def test_the_client_timeout_is_applied(deployment):
    deployment.server.add_block(4)
    deployment.server.delays["GetLatestBlock"] = 0.5
    client = deployment.client(client_timeout=0.05, retry=RetryPolicy(attempts=1))

    with pytest.raises(RequestError) as raised:
        client.get_latest_block()

    assert raised.value.code == "DEADLINE_EXCEEDED"


def test_calls_that_return_more_get_more_time():
    def answer(method, request):
        if method == "GetBlockFilesByRange":
            return api_pb2.GetBlockFilesByRangeResponse(
                files=chain_files(request.start_height, request.end_height)
            )
        if method == "GetChainEvents":
            return api_pb2.GetChainEventsResponse()
        return api_pb2.GetLatestBlockResponse(height=1)

    transport = StubTransport(answer)
    client = Client(
        Config(server_address="unused:1", client_timeout=2),
        transport=transport,
        downloader=StubDownloader(),
    )

    client.get_latest_block()
    list(client.iter_raw_block_payloads(1, 3))
    list(client.iter_raw_block_payloads(1, 4))
    client.get_chain_events(api_pb2.GetChainEventsRequest(max_num_events=10))
    client.get_chain_events(api_pb2.GetChainEventsRequest(max_num_events=11))

    # Short is t, medium is 2t + 1 and long is 4t + 3, as in the Go SDK.
    assert [timeout for _, _, timeout in transport.calls] == [2, 5, 11, 2, 5]


def test_the_client_id_and_auth_header_are_sent(deployment):
    deployment.server.add_block(4)
    deployment.server.add_event(4)
    client = deployment.client(client_id="indexer", auth_header="X-Api-Key", auth_token="t0ken")

    client.get_latest_block()
    stream = client.stream_chain_events(
        StreamingConfiguration(api_pb2.ChainEventsRequest(), number_of_events=1, event_only=True)
    )
    assert len(list(stream)) == 1

    for method in ("GetLatestBlock", "StreamChainEvents"):
        sent = deployment.server.metadata_of(method)[0]
        assert sent["x-client-id"] == "indexer"
        assert sent["x-api-key"] == "t0ken"


def test_no_identity_headers_are_sent_by_default(deployment):
    deployment.server.add_block(4)

    deployment.client().get_latest_block()

    assert "x-client-id" not in deployment.server.metadata_of("GetLatestBlock")[0]


# -- lookups --------------------------------------------------------------


def test_blocks_by_transaction(deployment):
    deployment.server.add_block(5)
    deployment.server.add_block(6)
    deployment.server.transactions["0xabc"] = [5, 6]

    blocks = deployment.client().get_block_by_transaction("0xabc")

    assert blocks == [make_block(5), make_block(6)]
    assert deployment.server.calls_to("GetBlockByTransaction")[0].transaction_hash == "0xabc"
    requests = deployment.server.calls_to("GetBlockFile")
    assert [(r.tag, r.height, r.hash) for r in requests] == [(TAG, 5, "hash-5"), (TAG, 6, "hash-6")]


def test_an_unknown_transaction_has_no_blocks(deployment):
    assert deployment.client().get_block_by_transaction("0xmissing") == []


def test_block_by_timestamp(deployment):
    deployment.server.add_block(42)

    block = deployment.client().get_block_by_timestamp(425)

    assert block == make_block(42)
    assert deployment.server.calls_to("GetBlockByTimestamp")[0].timestamp == 425
    request = deployment.server.calls_to("GetBlockFile")[0]
    assert (request.tag, request.height, request.hash) == (TAG, 42, "hash-42")


def test_chain_metadata(deployment):
    assert deployment.client().get_chain_metadata().latest_block_tag == TAG


# -- chain events ---------------------------------------------------------


def stream(deployment_or_client, number_of_events=0, event_only=False, **request):
    client = getattr(deployment_or_client, "client", lambda: deployment_or_client)()
    config = StreamingConfiguration(
        chain_events_request=api_pb2.ChainEventsRequest(**request),
        number_of_events=number_of_events,
        event_only=event_only,
    )
    return client.stream_chain_events(config)


def add_blocks_with_events(deployment, heights) -> None:
    for height in heights:
        deployment.server.add_block(height)
        deployment.server.add_event(height)


def test_get_chain_events(deployment):
    add_blocks_with_events(deployment, range(1, 6))

    events = deployment.client().get_chain_events(
        api_pb2.GetChainEventsRequest(sequence_num=2, max_num_events=2)
    )

    assert [event.sequence_num for event in events] == [3, 4]


def test_the_stream_yields_each_event_with_its_block(deployment):
    add_blocks_with_events(deployment, range(1, 4))

    results = list(stream(deployment, number_of_events=3))

    assert [result.event.sequence_num for result in results] == [1, 2, 3]
    assert [result.block for result in results] == [make_block(h) for h in range(1, 4)]


def test_the_stream_starts_after_the_given_position(deployment):
    add_blocks_with_events(deployment, range(1, 6))

    results = list(stream(deployment, number_of_events=2, sequence_num=3))

    assert [result.event.block.height for result in results] == [4, 5]


def test_event_only_downloads_no_blocks(deployment):
    add_blocks_with_events(deployment, range(1, 4))

    results = list(stream(deployment, number_of_events=3, event_only=True))

    assert [result.block for result in results] == [None, None, None]
    assert deployment.server.calls_to("GetBlockFile") == []


def test_a_removed_block_event_comes_with_the_removed_block(deployment):
    # Block 5 was replaced: the chain now has another block at that height.
    deployment.server.add_block(5)
    orphan = deployment.server.add_orphan(5, "orphan-5")
    deployment.server.add_event(5, hash="orphan-5")
    deployment.server.add_event(5, api_pb2.BlockchainEvent.BLOCK_REMOVED, hash="orphan-5")
    deployment.server.add_event(5)

    results = list(stream(deployment, number_of_events=3))

    assert [result.event.type for result in results] == [
        api_pb2.BlockchainEvent.BLOCK_ADDED,
        api_pb2.BlockchainEvent.BLOCK_REMOVED,
        api_pb2.BlockchainEvent.BLOCK_ADDED,
    ]
    assert [result.block for result in results] == [orphan, orphan, make_block(5)]
    requests = deployment.server.calls_to("GetBlockFile")
    assert [(r.tag, r.hash) for r in requests] == [
        (TAG, "orphan-5"),
        (TAG, "orphan-5"),
        (TAG, block_hash(5)),
    ]


def test_a_broken_stream_resumes_after_the_last_event(deployment):
    add_blocks_with_events(deployment, range(1, 7))
    # The first stream fails after two events, the second after one more.
    deployment.server.stream_plans.extend(
        [(2, grpc.StatusCode.UNAVAILABLE), (1, grpc.StatusCode.INTERNAL)]
    )

    results = list(stream(deployment, number_of_events=6))

    # Every event arrives exactly once, in order.
    assert [result.event.sequence_num for result in results] == [1, 2, 3, 4, 5, 6]
    opened = deployment.server.calls_to("StreamChainEvents")
    assert [(r.sequence, r.sequence_num) for r in opened] == [("", 0), ("2", 2), ("3", 3)]


def test_every_event_has_its_own_retry_budget(deployment):
    add_blocks_with_events(deployment, range(1, 4))
    # Each stream fails after one event. With two attempts per event, one
    # reconnect is allowed per event, which is enough here; a budget shared
    # by the whole stream would run out at the second failure.
    unavailable = grpc.StatusCode.UNAVAILABLE
    deployment.server.stream_plans.extend([(1, unavailable), (1, unavailable), (1, unavailable)])
    client = deployment.client(retry=RetryPolicy(attempts=2))

    results = list(stream(client, number_of_events=3))

    assert [result.event.sequence_num for result in results] == [1, 2, 3]
    assert len(deployment.server.calls_to("StreamChainEvents")) == 3


def test_a_stream_that_keeps_failing_gives_up(deployment):
    add_blocks_with_events(deployment, range(1, 3))
    # The first stream fails after one event; every later one fails at once.
    unavailable = grpc.StatusCode.UNAVAILABLE
    deployment.server.stream_plans.extend([(1, unavailable)] + [(0, unavailable)] * 10)
    client = deployment.client(retry=RetryPolicy(attempts=3))
    received = []

    with pytest.raises(RequestError) as raised:
        for result in stream(client):
            received.append(result.event.sequence_num)

    assert raised.value.code == "UNAVAILABLE"
    assert received == [1]
    # One stream at the start, then two reconnects for the second event.
    assert len(deployment.server.calls_to("StreamChainEvents")) == 3


def test_a_stream_from_an_initial_position_fails_fast_before_its_first_event(deployment):
    add_blocks_with_events(deployment, range(1, 3))
    deployment.server.stream_plans.append((0, grpc.StatusCode.UNAVAILABLE))

    with pytest.raises(RequestError) as raised:
        list(stream(deployment, initial_position_in_stream="EARLIEST"))

    assert raised.value.code == "UNAVAILABLE"
    assert len(deployment.server.calls_to("StreamChainEvents")) == 1


def test_a_stream_from_an_initial_position_resumes_once_it_has_an_event(deployment):
    add_blocks_with_events(deployment, range(1, 4))
    deployment.server.stream_plans.append((1, grpc.StatusCode.UNAVAILABLE))

    results = list(stream(deployment, number_of_events=3, initial_position_in_stream="EARLIEST"))

    assert [result.event.sequence_num for result in results] == [1, 2, 3]


def test_a_permanent_stream_failure_is_not_retried(deployment):
    add_blocks_with_events(deployment, range(1, 3))
    deployment.server.stream_plans.append((1, grpc.StatusCode.PERMISSION_DENIED))

    with pytest.raises(RequestError) as raised:
        list(stream(deployment))

    assert raised.value.code == "PERMISSION_DENIED"
    assert len(deployment.server.calls_to("StreamChainEvents")) == 1


def test_a_stream_the_server_ends_is_an_error(deployment):
    add_blocks_with_events(deployment, range(1, 3))
    received = []

    with pytest.raises(RequestError, match="closed the event stream"):
        for result in stream(deployment):
            received.append(result.event.sequence_num)

    assert received == [1, 2]


def test_a_block_that_cannot_be_downloaded_stops_the_stream(deployment):
    deployment.server.add_event(1)  # an event for a block the server does not have

    with pytest.raises(NotFoundError):
        list(stream(deployment))


def test_a_negative_event_count_is_refused(deployment):
    with pytest.raises(ConfigError):
        stream(deployment, number_of_events=-1)


class RecordingStream:
    """An event stream over a list, which records that it was cancelled."""

    def __init__(self, count: int) -> None:
        self._responses = iter(
            api_pb2.ChainEventsResponse(
                event=api_pb2.BlockchainEvent(sequence=str(number), sequence_num=number)
            )
            for number in range(1, count + 1)
        )
        self.cancelled = False

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._responses)

    def cancel(self):
        self.cancelled = True


@pytest.mark.parametrize("how", ["count reached", "abandoned", "failed"])
def test_the_server_stream_is_cancelled_when_the_caller_is_done(how):
    recording = RecordingStream(count=2)
    client = Client(
        Config(server_address="unused:1"),
        transport=StubTransport(stream=lambda request: recording),
        downloader=StubDownloader(),
    )
    config = StreamingConfiguration(
        api_pb2.ChainEventsRequest(),
        number_of_events=1 if how == "count reached" else 0,
        event_only=True,
    )
    events = client.stream_chain_events(config)

    if how == "count reached":
        assert len(list(events)) == 1
    elif how == "abandoned":
        next(events)
        events.close()
    else:
        with pytest.raises(RequestError):
            list(events)

    assert recording.cancelled


# -- session --------------------------------------------------------------


def test_a_session_holds_the_client(deployment):
    deployment.server.add_block(4)
    client = deployment.client()

    with Session(client.config, client=client) as session:
        assert session.client.get_latest_block() == 4
        assert session.config is client.config
        with pytest.raises(NotImplementedError, match="parser"):
            _ = session.parser


def test_consolidated_fixture_layout_is_what_these_tests_assume(deployment):
    # Two chunks of three blocks: the tests above rely on it.
    deployment.server.add_consolidated("zstd")
    data = deployment.storage.objects[CONSOLIDATED_OBJECT]
    assert struct.unpack_from("<II", data, 8) == (6, 2)


# -- block files, without a download --------------------------------------


class DeadlineTransport(StubTransport):
    """A transport that takes a deadline, and records the one of each call."""

    def __init__(self, answer=None) -> None:
        super().__init__(answer)
        self.deadlines: list[float | None] = []

    def call(self, method, request, *, timeout, deadline=None):
        self.deadlines.append(deadline)
        return super().call(method, request, timeout=timeout)


def range_answer(method, request):
    return api_pb2.GetBlockFilesByRangeResponse(
        files=chain_files(request.start_height, request.end_height)
    )


def stub_client(transport) -> Client:
    return Client(
        Config(server_address="unused:1"), transport=transport, downloader=StubDownloader()
    )


def test_the_latest_block_comes_with_its_tag_hashes_and_time(deployment):
    deployment.server.add_block(8)
    deployment.server.add_block(9)

    latest = deployment.client().get_latest_block_info()

    assert (latest.tag, latest.height) == (TAG, 9)
    assert (latest.hash, latest.parent_hash) == (block_hash(9), block_hash(8))
    assert latest.timestamp.seconds == block_time(9)
    assert deployment.server.calls_to("GetLatestBlock")[0].tag == TAG


def test_a_block_file_is_returned_without_a_download(deployment):
    deployment.server.add_block(5)

    file = deployment.client().get_block_file(5, read_source=SINGLE_BLOCK)

    assert (file.tag, file.height, file.hash, file.skipped) == (TAG, 5, block_hash(5), False)
    assert file.file_url
    assert deployment.storage.requests == []
    request = deployment.server.calls_to("GetBlockFile")[0]
    assert (request.tag, request.height, request.hash) == (TAG, 5, "")
    assert request.read_source == SINGLE_BLOCK


def test_a_block_file_is_found_by_hash(deployment):
    deployment.server.add_block(5)
    deployment.server.add_orphan(5, "replaced")

    file = deployment.client().get_block_file(5, "replaced", tag=TAG)

    assert (file.height, file.hash) == (5, "replaced")
    assert deployment.server.calls_to("GetBlockFile")[0].hash == "replaced"


def test_the_block_file_of_a_skipped_block_says_so(deployment):
    deployment.server.skipped.add(6)

    file = deployment.client().get_block_file(6)

    assert (file.height, file.skipped, file.file_url) == (6, True, "")


@pytest.mark.parametrize("read_source", [DEFAULT, CONSOLIDATED])
def test_a_block_file_is_not_looked_for_in_another_source(deployment, read_source):
    # get_block would read the block from single-block objects instead.
    with pytest.raises(NotFoundError):
        deployment.client().get_block_file(5, read_source=read_source)

    assert [request.read_source for request in deployment.server.calls_to("GetBlockFile")] == [
        read_source
    ]


def test_the_block_files_of_a_range_come_in_height_order(deployment):
    for height in (10, 11, 13):
        deployment.server.add_block(height)
    deployment.server.skipped.add(12)

    files = deployment.client().get_block_files_by_range(10, 14, read_source=SINGLE_BLOCK)

    assert [file.height for file in files] == [10, 11, 12, 13]
    assert [file.skipped for file in files] == [False, False, True, False]
    assert [file.hash for file in files] == [block_hash(10), block_hash(11), "", block_hash(13)]
    assert deployment.storage.requests == []
    request = deployment.server.calls_to("GetBlockFilesByRange")[0]
    assert (request.tag, request.start_height, request.end_height) == (TAG, 10, 14)
    assert request.read_source == SINGLE_BLOCK


def test_a_range_of_block_files_without_an_end_is_one_block(deployment):
    deployment.server.add_block(10)
    deployment.server.add_block(11)

    files = deployment.client().get_block_files_by_range(10)

    assert [file.height for file in files] == [10]


@pytest.mark.parametrize("end_height", [5, 4])
def test_an_empty_range_of_block_files_is_refused(deployment, end_height):
    with pytest.raises(ConfigError):
        deployment.client().get_block_files_by_range(5, end_height)

    assert deployment.server.calls == []


def test_a_long_range_of_block_files_is_asked_for_in_requests_of_a_thousand():
    transport = StubTransport(range_answer)

    files = stub_client(transport).get_block_files_by_range(100, 2600)

    assert [file.height for file in files] == list(range(100, 2600))
    assert [(request.start_height, request.end_height) for _, request, _ in transport.calls] == [
        (100, 1100),
        (1100, 2100),
        (2100, 2600),
    ]


def test_block_files_of_two_requests_that_do_not_connect_are_refused():
    def answer(method, request):
        fork = "a" if request.start_height == 0 else "b"
        return api_pb2.GetBlockFilesByRangeResponse(
            files=chain_files(request.start_height, request.end_height, fork=fork)
        )

    with pytest.raises(RequestError, match="block 1000 does not follow block 999"):
        stub_client(StubTransport(answer)).get_block_files_by_range(0, 1500)


def test_a_range_of_block_files_that_misses_a_height_is_refused():
    def answer(method, request):
        return api_pb2.GetBlockFilesByRangeResponse(files=chain_files(10, 13))

    with pytest.raises(RequestError, match="expected 4 block files"):
        stub_client(StubTransport(answer)).get_block_files_by_range(10, 14)


@pytest.mark.parametrize("read_source", [DEFAULT, CONSOLIDATED])
def test_a_range_of_block_files_is_not_looked_for_in_another_source(read_source):
    # iter_raw_block_payloads would read the range from single-block objects.
    def answer(method, request):
        raise RequestError("GetBlockFilesByRange failed", code="FAILED_PRECONDITION")

    transport = StubTransport(answer)

    with pytest.raises(RequestError):
        stub_client(transport).get_block_files_by_range(10, 14, read_source=read_source)

    assert [request.read_source for _, request, _ in transport.calls] == [read_source]


# -- a time limit for a whole call ----------------------------------------


def test_a_call_within_its_timeout_is_retried_as_usual(deployment):
    deployment.server.add_block(4)
    unavailable = grpc.StatusCode.UNAVAILABLE
    deployment.server.failures["GetLatestBlock"].extend([unavailable, unavailable])

    assert deployment.client().get_latest_block(timeout=30) == 4
    assert len(deployment.server.calls_to("GetLatestBlock")) == 3
    assert len(deployment.sleeps) == 2


def test_a_timeout_cuts_an_attempt_short(deployment):
    deployment.server.add_block(4)
    deployment.server.delays["GetLatestBlock"] = 2.0
    client = deployment.client(client_timeout=30)
    started = time.monotonic()

    with pytest.raises(CallTimeoutError) as raised:
        client.get_latest_block(timeout=0.3)

    assert time.monotonic() - started < 1.5
    assert str(raised.value) == (
        "GetLatestBlock did not finish in time; the last attempt failed with DEADLINE_EXCEEDED"
    )
    # Both, so that code written for either kind of error handles it.
    assert isinstance(raised.value, RequestError)
    assert isinstance(raised.value, TimeoutError)
    assert raised.value.code == "DEADLINE_EXCEEDED"
    assert not raised.value.retryable
    assert len(deployment.server.calls_to("GetLatestBlock")) == 1
    assert deployment.sleeps == []


def test_a_timeout_stops_a_retry_that_could_not_start_in_time(deployment):
    deployment.server.add_block(4)
    deployment.server.failures["GetLatestBlock"].append(grpc.StatusCode.UNAVAILABLE)
    client = deployment.client(retry=RetryPolicy(initial_interval=60, jitter=0, max_interval=60))

    with pytest.raises(CallTimeoutError, match="the last attempt failed with UNAVAILABLE"):
        client.get_latest_block(timeout=5)

    assert len(deployment.server.calls_to("GetLatestBlock")) == 1
    assert deployment.sleeps == []


@pytest.mark.parametrize("timeout", [0, -1])
def test_a_timeout_that_has_run_out_fails_before_the_server_is_asked(deployment, timeout):
    deployment.server.add_block(4)

    with pytest.raises(CallTimeoutError):
        deployment.client().get_latest_block(timeout=timeout)

    assert deployment.server.calls == []


@pytest.mark.parametrize("attempts", [1, 2])
def test_a_timeout_that_runs_out_on_the_last_attempt_is_a_timeout_error(deployment, attempts):
    deployment.server.add_block(4)
    # The attempts before the last fail at once; the last one is cut short.
    deployment.server.failures["GetLatestBlock"].extend(
        [grpc.StatusCode.UNAVAILABLE] * (attempts - 1)
    )
    deployment.server.delays["GetLatestBlock"] = 1.0
    policy = RetryPolicy(attempts=attempts, initial_interval=0.01, jitter=0)
    client = deployment.client(client_timeout=30, retry=policy)

    with pytest.raises(CallTimeoutError, match="the last attempt failed with DEADLINE_EXCEEDED"):
        client.get_latest_block(timeout=0.3)

    assert len(deployment.server.calls_to("GetLatestBlock")) == attempts


def test_the_client_timeout_still_limits_an_attempt_of_a_call_with_a_timeout(deployment):
    deployment.server.add_block(4)
    deployment.server.delays["GetLatestBlock"] = 2.0
    client = deployment.client(client_timeout=0.2, retry=RetryPolicy(attempts=1))
    started = time.monotonic()

    with pytest.raises(RequestError) as raised:
        client.get_latest_block(timeout=30)

    # The attempt ran out, not the call: the caller's limit was far away.
    assert not isinstance(raised.value, CallTimeoutError)
    assert raised.value.code == "DEADLINE_EXCEEDED"
    assert time.monotonic() - started < 1.5


@pytest.mark.parametrize("timeout", [float("nan"), "5", True])
def test_a_timeout_that_is_not_a_number_of_seconds_is_refused(deployment, timeout):
    deployment.server.add_block(4)

    with pytest.raises(ConfigError, match="timeout must be a number of seconds"):
        deployment.client().get_latest_block(timeout=timeout)

    assert deployment.server.calls == []


def test_a_timeout_without_end_is_no_limit(deployment):
    deployment.server.add_block(4)

    assert deployment.client().get_latest_block(timeout=float("inf")) == 4


def test_a_transport_without_deadlines_gets_the_time_left_for_each_attempt():
    # StubTransport.call takes no deadline, like a transport written before
    # deadlines existed.
    transport = StubTransport(range_answer)
    client = Client(
        Config(server_address="unused:1", client_timeout=30),
        transport=transport,
        downloader=StubDownloader(),
    )

    client.get_block_files_by_range(0, 5, timeout=7)
    client.get_block_files_by_range(0, 5)

    with_timeout, without = [timeout for _, _, timeout in transport.calls]
    assert 6 < with_timeout <= 7
    assert without == 30 * 4 + 3


@pytest.mark.parametrize("transport_type", ["old", "new"])
def test_a_timeout_that_runs_out_between_two_requests_stops_the_range(transport_type):
    def slow_answer(method, request):
        time.sleep(0.06)
        return range_answer(method, request)

    transport = (StubTransport if transport_type == "old" else DeadlineTransport)(slow_answer)

    with pytest.raises(CallTimeoutError, match="GetBlockFilesByRange did not finish in time"):
        stub_client(transport).get_block_files_by_range(0, 5000, timeout=0.1)

    assert len(transport.calls) == 2


def test_every_call_that_does_not_download_takes_a_timeout(deployment):
    deployment.server.add_block(4)
    deployment.server.add_event(4)
    client = deployment.client()
    events = api_pb2.GetChainEventsRequest(max_num_events=1)

    assert client.get_latest_block(timeout=30) == 4
    assert client.get_latest_block_info(timeout=30).height == 4
    assert client.get_block_file(4, timeout=30).height == 4
    assert len(client.get_block_files_by_range(4, 5, timeout=30)) == 1
    assert len(client.get_chain_events(events, timeout=30)) == 1
    assert client.get_chain_metadata(timeout=30).latest_block_tag == TAG

    for call in (
        lambda: client.get_latest_block(timeout=0),
        lambda: client.get_latest_block_info(timeout=0),
        lambda: client.get_block_file(4, timeout=0),
        lambda: client.get_block_files_by_range(4, 5, timeout=0),
        lambda: client.get_chain_events(events, timeout=0),
        lambda: client.get_chain_metadata(timeout=0),
    ):
        with pytest.raises(CallTimeoutError):
            call()


def test_the_requests_of_a_long_range_share_one_timeout():
    transport = DeadlineTransport(range_answer)
    before = time.monotonic()

    stub_client(transport).get_block_files_by_range(0, 2500, timeout=20)

    assert len(transport.deadlines) == 3
    assert len(set(transport.deadlines)) == 1
    assert before + 20 <= transport.deadlines[0] <= time.monotonic() + 20


def test_a_call_without_a_timeout_has_no_deadline():
    transport = DeadlineTransport(range_answer)

    stub_client(transport).get_block_files_by_range(0, 5)

    assert transport.deadlines == [None]


# -- a connection that was reset, and the log of retries ------------------


@pytest.mark.parametrize("details", ["Stream removed", "Received RST_STREAM with error code 2"])
def test_a_call_is_retried_when_its_connection_was_reset(deployment, details):
    deployment.server.add_block(4)
    reset = (grpc.StatusCode.UNKNOWN, details)
    deployment.server.failures["GetLatestBlock"].extend([reset, reset])

    assert deployment.client().get_latest_block() == 4
    assert len(deployment.server.calls_to("GetLatestBlock")) == 3


def test_a_call_is_not_retried_on_another_unknown_failure(deployment):
    deployment.server.add_block(4)
    deployment.server.failures["GetLatestBlock"].append(
        (grpc.StatusCode.UNKNOWN, "the handler raised an exception")
    )

    with pytest.raises(RequestError) as raised:
        deployment.client().get_latest_block()

    assert raised.value.code == "UNKNOWN"
    assert not raised.value.retryable
    assert len(deployment.server.calls_to("GetLatestBlock")) == 1


def test_a_stream_whose_connection_was_reset_is_reopened(deployment):
    add_blocks_with_events(deployment, range(1, 4))
    deployment.server.stream_plans.append((1, grpc.StatusCode.UNKNOWN, "Stream removed"))

    results = list(stream(deployment, number_of_events=3))

    assert [result.event.sequence_num for result in results] == [1, 2, 3]
    assert len(deployment.server.calls_to("StreamChainEvents")) == 2


def test_a_retried_call_is_logged(deployment, caplog):
    deployment.server.add_block(4)
    failures = [grpc.StatusCode.UNAVAILABLE, (grpc.StatusCode.UNKNOWN, "Stream removed")]
    deployment.server.failures["GetLatestBlock"].extend(failures)
    client = deployment.client(retry=RetryPolicy(jitter=0))

    with caplog.at_level("WARNING", logger="chainstorage_sdk.retry"):
        assert client.get_latest_block() == 4

    assert [record.getMessage() for record in caplog.records] == [
        "GetLatestBlock failed with UNAVAILABLE on attempt 1 of 4; trying again in 0.20 s",
        "GetLatestBlock failed with UNKNOWN on attempt 2 of 4; trying again in 0.40 s",
    ]


def test_a_call_that_succeeds_at_once_logs_nothing(deployment, caplog):
    deployment.server.add_block(5)

    with caplog.at_level("WARNING", logger="chainstorage_sdk"):
        deployment.client().get_block(5)

    assert caplog.records == []


def test_a_retried_download_is_logged_without_its_link(deployment, caplog):
    deployment.server.add_block(5)
    deployment.storage.signature = "secret"
    deployment.storage.failures["blocks/5"].append(503)
    deployment.storage.error_body = STORE_REFUSAL
    client = deployment.client(retry=RetryPolicy(jitter=0))

    with caplog.at_level("WARNING", logger="chainstorage_sdk.retry"):
        client.get_block(5, read_source=SINGLE_BLOCK)

    assert [record.getMessage() for record in caplog.records] == [
        "a block file download failed with status 503 ExpiredToken on attempt 1 of 4; "
        "trying again in 0.20 s"
    ]
    assert "secret" not in caplog.text
    assert "blocks/5" not in caplog.text


# -- blocks at chosen heights ----------------------------------------------


class LimitedDownloader(StubDownloader):
    """A stub that takes the limits of a download, and records them."""

    def __init__(self, errors=()) -> None:
        super().__init__(errors)
        self.limits: list[dict] = []
        self.closed = False

    def iter_download(self, block_files, **limits):
        self.limits.append(limits)
        return super().iter_download(block_files)

    def download(self, block_file, **limits):
        return list(self.iter_download([block_file], **limits))[0]

    def to_block(self, block_file, payload):
        return blockchain_pb2.Block()

    def close(self):
        self.closed = True


def asked_ranges(transport) -> list[tuple[int, int]]:
    return [(request.start_height, request.end_height) for _, request, _ in transport.calls]


def test_blocks_at_heights_come_in_height_order_each_once(deployment):
    for height in range(10, 20):
        deployment.server.add_block(height)
    client = deployment.client()

    blocks = client.get_blocks_by_heights([15, 11, 15, 19], read_source=SINGLE_BLOCK)

    assert blocks == [make_block(11), make_block(15), make_block(19)]
    (asked,) = deployment.server.calls_to("GetBlockFilesByRange")
    assert (asked.start_height, asked.end_height) == (11, 20)
    # Only the wanted blocks were downloaded.
    assert sorted(name for name, _ in deployment.storage.requests) == [
        "blocks/11",
        "blocks/15",
        "blocks/19",
    ]


def test_no_heights_ask_the_server_nothing():
    transport = StubTransport(range_answer)

    assert list(stub_client(transport).iter_raw_block_payloads_by_heights([])) == []
    assert stub_client(transport).get_blocks_by_heights(iter(())) == []
    assert transport.calls == []


def test_heights_are_asked_for_in_stretches_the_server_allows():
    transport = StubTransport(range_answer)

    payloads = list(
        stub_client(transport).iter_raw_block_payloads_by_heights([3000, 5, 1004, 1005])
    )

    assert [raw.block_file.height for raw in payloads] == [5, 1004, 1005, 3000]
    # 5 to 1004 is the 1,000 heights one request may span. 1005 is one more.
    assert asked_ranges(transport) == [(5, 1005), (1005, 1006), (3000, 3001)]


def test_a_stretch_is_asked_for_just_before_its_blocks_are_downloaded():
    transport = StubTransport(range_answer)
    payloads = stub_client(transport).iter_raw_block_payloads_by_heights([7, 5000, 9000])

    assert transport.calls == []
    assert next(payloads).block_file.height == 7
    assert asked_ranges(transport) == [(7, 8)]
    assert next(payloads).block_file.height == 5000
    assert asked_ranges(transport) == [(7, 8), (5000, 5001)]


class _Integer:
    """A whole number that is not an ``int``, as a NumPy integer is."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __index__(self) -> int:
        return self.value


def test_a_height_may_be_any_integer_type():
    transport = StubTransport(range_answer)

    payloads = list(stub_client(transport).iter_raw_block_payloads_by_heights([_Integer(7), 7]))

    assert [raw.block_file.height for raw in payloads] == [7]


@pytest.mark.parametrize("height", [-1, 1.5, "7", True, None, 2**64, _Integer(-3)])
def test_a_height_that_is_not_a_whole_number_is_refused_at_the_call(height):
    transport = StubTransport(range_answer)

    with pytest.raises(ConfigError, match="a height must be a whole number"):
        stub_client(transport).iter_raw_block_payloads_by_heights([3, height])

    assert transport.calls == []


def test_a_skipped_block_among_the_heights_has_an_empty_payload(deployment):
    deployment.server.add_block(10)
    deployment.server.add_block(12)
    deployment.server.skipped.add(11)
    client = deployment.client()

    payloads = list(client.iter_raw_block_payloads_by_heights([12, 11], read_source=SINGLE_BLOCK))

    assert [(raw.block_file.height, raw.block_file.skipped) for raw in payloads] == [
        (11, True),
        (12, False),
    ]
    assert payloads[0].payload == b""
    assert client.parse_raw_block(payloads[0]).metadata.skipped


def test_a_height_the_server_does_not_have_is_not_found(deployment):
    deployment.server.add_block(10)

    with pytest.raises(NotFoundError):
        deployment.client().get_blocks_by_heights([10, 14], read_source=SINGLE_BLOCK)


@pytest.mark.parametrize("codec", ["gzip", "zstd"])
def test_heights_in_a_consolidated_object_fetch_each_chunk_once(deployment, codec):
    entries = deployment.server.add_consolidated(codec)

    payloads = list(deployment.client().iter_raw_block_payloads_by_heights([1004, 1000, 1002]))

    assert [raw.block_file.height for raw in payloads] == [1000, 1002, 1004]
    assert [sha256(raw.payload) for raw in payloads] == [
        entries[position]["payload_sha256"] for position in (0, 2, 4)
    ]
    # The index, the chunk of 1000 and 1002, and the chunk of 1004.
    assert len(deployment.storage.requests_for(f"consolidated/cscb_{codec}.bin")) == 3


def test_heights_fall_back_to_single_block(deployment):
    deployment.server.add_consolidated("zstd")
    deployment.storage.objects[CONSOLIDATED_OBJECT] = b"damaged"

    blocks = deployment.client().get_blocks_by_heights([1001, 1005])

    assert blocks == [make_block(1001), make_block(1005)]
    asked = deployment.server.calls_to("GetBlockFilesByRange")
    assert [(r.start_height, r.end_height, r.read_source) for r in asked] == [
        (1001, 1006, DEFAULT),
        (1001, 1006, SINGLE_BLOCK),
    ]


def test_refused_links_of_heights_are_replaced_from_the_first_missing_block():
    class OneBlockThenRefused(LimitedDownloader):
        """Hands over one block, then meets an expired link, every time."""

        def iter_download(self, block_files, **limits):
            self.downloads.append([file.height for file in block_files])
            yield b""
            raise DownloadError("refused", status_code=403)

    transport = StubTransport(range_answer)
    downloader = OneBlockThenRefused()
    client = Client(Config(server_address="unused:1"), transport=transport, downloader=downloader)

    payloads = list(
        client.iter_raw_block_payloads_by_heights([10, 13, 17], read_source=SINGLE_BLOCK)
    )

    assert [raw.block_file.height for raw in payloads] == [10, 13, 17]
    # No block is downloaded twice, and only wanted blocks are downloaded.
    assert downloader.downloads == [[10, 13, 17], [13, 17], [17]]
    assert asked_ranges(transport) == [(10, 18), (13, 18), (17, 18)]


def test_heights_stop_when_a_wanted_block_is_replaced_under_them():
    forks = ["a-", "b-"]

    def answer(method, request):
        return api_pb2.GetBlockFilesByRangeResponse(
            files=chain_files(request.start_height, request.end_height, fork=forks.pop(0))
        )

    client = Client(
        Config(server_address="unused:1"),
        transport=StubTransport(answer),
        downloader=StubDownloader([DownloadError("refused", status_code=403)]),
    )

    with pytest.raises(RequestError, match="block 10 was replaced"):
        list(client.iter_raw_block_payloads_by_heights([10, 12]))


# -- limits on a read: time and size ---------------------------------------


def downloading_calls(client, **limits):
    """Every call that downloads, each reading block 4."""
    return [
        lambda: client.get_block(4, **limits),
        lambda: client.get_blocks_by_range(4, 5, **limits),
        lambda: client.get_blocks_by_heights([4], **limits),
        lambda: client.open_raw_block_payload(4, **limits),
        lambda: list(client.iter_raw_block_payloads(4, 5, **limits)),
        lambda: list(client.iter_raw_block_payloads_by_heights([4], **limits)),
        lambda: client.get_block_by_transaction("0xabc", **limits),
        lambda: client.get_block_by_timestamp(40, **limits),
    ]


def test_every_call_that_downloads_takes_a_timeout(deployment):
    deployment.server.add_block(4)
    deployment.server.transactions["0xabc"] = [4]
    client = deployment.client()

    for call in downloading_calls(client, timeout=30):
        call()
    for call in downloading_calls(client, timeout=0):
        with pytest.raises(CallTimeoutError):
            call()


def test_a_call_and_its_downloads_share_one_timeout():
    transport = DeadlineTransport(range_answer)
    downloader = LimitedDownloader()
    client = Client(Config(server_address="unused:1"), transport=transport, downloader=downloader)
    before = time.monotonic()

    list(client.iter_raw_block_payloads_by_heights([1, 5000], timeout=20, max_response_bytes=64))

    deadlines = {*transport.deadlines, *(limits["deadline"] for limits in downloader.limits)}
    assert len(transport.deadlines) == len(downloader.limits) == 2
    assert len(deadlines) == 1
    assert before + 20 <= deadlines.pop() <= time.monotonic() + 20
    assert [limits["max_response_bytes"] for limits in downloader.limits] == [64, 64]


@pytest.mark.parametrize("method", ["get_block", "open_raw_block_payload"])
def test_one_block_and_its_download_share_one_timeout(method):
    def answer(method, request):
        return api_pb2.GetBlockFileResponse(file=chain_files(4, 5)[0])

    transport = DeadlineTransport(answer)
    downloader = LimitedDownloader()
    client = Client(Config(server_address="unused:1"), transport=transport, downloader=downloader)
    before = time.monotonic()

    getattr(client, method)(4, timeout=20, max_response_bytes=64)

    (deadline,) = transport.deadlines
    assert before + 20 <= deadline <= time.monotonic() + 20
    assert downloader.limits == [{"deadline": deadline, "max_response_bytes": 64}]


class _RecordingSession:
    """Records the wait each request allows, and answers with a small object."""

    def __init__(self) -> None:
        self.timeouts: list[float] = []

    def get(self, url, headers=None, timeout=None, stream=False):
        self.timeouts.append(timeout)
        response = requests.Response()
        response.status_code = 200
        response.raw = io.BytesIO(gzip.compress(make_block(5).SerializeToString()))
        return response

    def close(self):
        pass


def test_a_wait_on_the_store_is_cut_to_the_time_left(deployment):
    session = _RecordingSession()
    downloader = BlockDownloader(deployment.config(http_timeout=30.0), session=session)
    file = api_pb2.BlockFile(height=5, file_url="http://store/blocks/5", compression=api_pb2.GZIP)

    try:
        downloader.download(file)
        downloader.download(file, deadline=time.monotonic() + 4)
        downloader.download(file, deadline=time.monotonic() + 400)
    finally:
        downloader.close()

    without, short, long = session.timeouts
    assert without == 30.0
    assert 3 < short <= 4
    # A deadline far away leaves the usual wait.
    assert long == 30.0


def test_a_read_without_limits_gives_the_downloader_none():
    downloader = LimitedDownloader()
    client = Client(
        Config(server_address="unused:1"),
        transport=StubTransport(range_answer),
        downloader=downloader,
    )

    list(client.iter_raw_block_payloads(1, 3))

    assert downloader.limits == [{}]


def test_the_timeout_of_an_iterator_starts_at_the_call(deployment):
    deployment.server.add_block(4)
    payloads = deployment.client().iter_raw_block_payloads(4, 5, timeout=0.05)

    time.sleep(0.1)

    with pytest.raises(CallTimeoutError):
        next(payloads)
    assert deployment.server.calls == []


@pytest.mark.parametrize("read_source", [DEFAULT, SINGLE_BLOCK])
def test_a_timeout_cuts_a_wait_on_the_store_short(deployment, read_source):
    deployment.server.add_block(5)
    deployment.storage.delay = 3.0
    client = deployment.client()
    before = time.monotonic()

    with pytest.raises(CallTimeoutError):
        client.get_block(5, read_source=read_source, timeout=0.5)

    assert time.monotonic() - before < 2.0
    # Nothing more was tried: no second attempt, and no other source.
    assert len(deployment.server.calls_to("GetBlockFile")) == 1
    assert deployment.sleeps == []


def test_a_timeout_stops_a_range_that_is_under_way(deployment):
    for height in range(20, 24):
        deployment.server.add_block(height)
    payloads = deployment.client(num_workers=1).iter_raw_block_payloads(20, 24, timeout=1.0)

    assert next(payloads).block_file.height == 20
    # From here on the store answers too slowly for the time that is left.
    deployment.storage.delay = 3.0
    before = time.monotonic()
    with pytest.raises(CallTimeoutError):
        list(payloads)

    assert time.monotonic() - before < 2.0
    assert len(deployment.server.calls_to("GetBlockFilesByRange")) == 1


def test_a_block_downloaded_ahead_is_not_yielded_after_the_timeout(deployment):
    for height in range(20, 24):
        deployment.server.add_block(height)
    payloads = deployment.client(num_workers=4).iter_raw_block_payloads(20, 24, timeout=0.5)

    assert next(payloads).block_file.height == 20
    # The caller takes its time. The other blocks were downloaded meanwhile.
    time.sleep(0.7)
    assert len(deployment.storage.requests) == 4
    with pytest.raises(CallTimeoutError):
        next(payloads)

    assert len(deployment.server.calls_to("GetBlockFilesByRange")) == 1
    assert deployment.sleeps == []


def test_a_block_of_a_chunk_already_read_is_not_yielded_after_the_timeout(deployment):
    deployment.server.add_consolidated("zstd")
    payloads = deployment.client().iter_raw_block_payloads(1000, 1006, timeout=0.5)

    assert next(payloads).block_file.height == 1000
    requests = len(deployment.storage.requests)
    time.sleep(0.7)
    with pytest.raises(CallTimeoutError):
        next(payloads)

    # The next block came with the first one's chunk: nothing was left to read.
    assert len(deployment.storage.requests) == requests
    assert deployment.sleeps == []


@pytest.mark.parametrize("timeout", [float("inf"), 1e12])
def test_a_timeout_without_end_does_not_limit_a_download(deployment, timeout):
    deployment.server.add_block(5)
    client = deployment.client()

    assert client.get_block(5, timeout=timeout) == make_block(5)
    assert client.get_blocks_by_heights([5], timeout=timeout) == [make_block(5)]


def test_the_time_left_is_read_once_for_an_attempt(monkeypatch):
    # A second reading could find the deadline passed, and hand the
    # connection a wait of no time at all.
    readings = iter([0.25])
    monkeypatch.setattr(downloader_module, "time_left", lambda deadline: next(readings))

    limits = downloader_module._Limits(deadline=1.0)

    assert downloader_module._attempt_timeout(30.0, limits) == 0.25


@pytest.mark.parametrize("deadline", [None, 600.0])
def test_a_download_that_waits_ends_when_the_downloader_is_closed(deployment, deadline):
    deployment.server.add_block(5)
    deployment.server.add_block(6)
    deployment.storage.delay = 0.5
    files = deployment.client().get_block_files_by_range(5, 7, read_source=SINGLE_BLOCK)
    downloader = BlockDownloader(deployment.config(num_workers=1))
    outcomes: list[BaseException | bytes] = []

    def download(block_file):
        limits = {} if deadline is None else {"deadline": time.monotonic() + deadline}
        try:
            outcomes.append(downloader.download(block_file, **limits))
        except BaseException as exc:
            outcomes.append(exc)

    # The first download holds the only worker; the second one waits for it.
    # Daemon threads: if a download ever waited for good again, the test
    # would fail and the test run would still end.
    first = threading.Thread(target=download, args=(files[0],), daemon=True)
    first.start()
    while not deployment.storage.most_at_once:
        time.sleep(0.01)
    second = threading.Thread(target=download, args=(files[1],), daemon=True)
    second.start()
    time.sleep(0.1)

    downloader.close()
    second.join(timeout=5)

    waiting = second.is_alive()
    first.join()
    assert not waiting
    assert deployment.storage.requests_for("blocks/6") == []
    assert [type(outcome) for outcome in outcomes if not isinstance(outcome, bytes)] == [
        CancelledError
    ]


class _SlowResponse:
    """A streamed response whose body arrives one slow piece at a time."""

    def __init__(self) -> None:
        self.pieces_read = 0

    def iter_content(self, chunk_size: int):
        while True:
            time.sleep(0.03)
            self.pieces_read += 1
            yield b"x" * chunk_size


def test_a_timeout_stops_a_body_that_arrives_slowly():
    response = _SlowResponse()
    limits = downloader_module._Limits(deadline=time.monotonic() + 0.1)

    with pytest.raises(CallTimeoutError):
        downloader_module._read_at_most(response, 100 * 64 * 1024, limits)

    assert 1 <= response.pieces_read <= 5


@pytest.mark.parametrize("ends", ["at the size wanted", "before it"])
def test_a_body_whose_last_piece_arrives_late_is_not_returned(ends):
    class Response:
        def iter_content(self, chunk_size: int):
            time.sleep(0.2)
            yield b"x" * 10

    limits = downloader_module._Limits(deadline=time.monotonic() + 0.05)
    size = 10 if ends == "at the size wanted" else 100

    with pytest.raises(CallTimeoutError):
        downloader_module._read_at_most(Response(), size, limits)


def test_a_refusal_that_arrives_late_is_a_timeout(deployment):
    class Session(_RefusingSession):
        def get(self, url, headers=None, timeout=None, stream=False):
            time.sleep(0.2)
            return super().get(url, headers=headers, timeout=timeout, stream=stream)

    downloader = BlockDownloader(deployment.config(), session=Session())
    file = api_pb2.BlockFile(height=5, file_url="http://store/blocks/5")

    try:
        with pytest.raises(DownloadError):
            downloader.download(file)
        # The download itself, not the wait for it, which ends at the
        # deadline whatever the download does.
        limits = downloader_module._Limits(deadline=time.monotonic() + 0.05)
        with pytest.raises(CallTimeoutError):
            downloader._download_single(file, limits)
    finally:
        downloader.close()


def test_a_body_is_read_in_smaller_pieces_when_there_is_a_deadline():
    asked: list[int] = []

    class Response:
        def iter_content(self, chunk_size: int):
            asked.append(chunk_size)
            yield b"x"

    size = 100 * 1024 * 1024
    downloader_module._read_at_most(Response(), size)
    limits = downloader_module._Limits(deadline=time.monotonic() + 60)
    downloader_module._read_at_most(Response(), size, limits)

    assert asked == [1024 * 1024, 64 * 1024]


class _CountingResponse:
    """A streamed response with an endless body; records what is asked and read."""

    def __init__(self) -> None:
        self.piece_sizes: list[int] = []
        self.bytes_read = 0

    def iter_content(self, chunk_size: int):
        self.piece_sizes.append(chunk_size)
        while True:
            self.bytes_read += chunk_size
            yield b"x" * chunk_size


@pytest.mark.parametrize("deadline", [None, 60.0], ids=["no deadline", "a deadline"])
@pytest.mark.parametrize(
    ("size", "most_read"),
    [
        (1, 1),
        (65, 65),
        (64 * 1024, 64 * 1024),
        # Longer than a piece: at most one byte per piece is read beyond it.
        (64 * 1024 + 1, 64 * 1024 + 2),
        (3 * 1024 * 1024 + 1, 3 * 1024 * 1024 + 1 + 49),
    ],
)
def test_a_body_is_read_no_further_than_the_bytes_wanted(deadline, size, most_read):
    response = _CountingResponse()
    limits = downloader_module._Limits(
        deadline=None if deadline is None else time.monotonic() + deadline
    )

    body = downloader_module._read_at_most(response, size, limits)

    assert len(body) == size
    assert size <= response.bytes_read <= most_read
    assert len(response.piece_sizes) == 1


def test_a_store_that_sends_short_pieces_passes_the_bytes_wanted_by_less_than_a_piece():
    class Response:
        def iter_content(self, chunk_size: int):
            yield b"x" * (chunk_size - 1)
            yield b"x" * chunk_size

    body = downloader_module._read_at_most(Response(), 65)

    assert len(body) == 65


@pytest.mark.parametrize("timeout", [None, 60.0], ids=["no timeout", "a timeout"])
def test_a_small_size_limit_reads_only_that_much_of_a_large_object(deployment, timeout):
    deployment.server.add_block(5)
    deployment.storage.objects["blocks/5"] = b"x" * (2 * 1024 * 1024)
    read: list[int] = []
    real = requests.Response.iter_content

    def recording(self, chunk_size=1, decode_unicode=False):
        for piece in real(self, chunk_size, decode_unicode):
            read.append(len(piece))
            yield piece

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(requests.Response, "iter_content", recording)
        with pytest.raises(ResponseTooLargeError):
            deployment.client().get_block(
                5, read_source=SINGLE_BLOCK, timeout=timeout, max_response_bytes=64
            )

    assert read == [65]


def test_a_timeout_stops_the_retries_of_a_download(deployment):
    deployment.server.add_block(5)
    deployment.storage.failures["blocks/5"].extend([503] * 3)
    # The first wait before a retry is at least 5 s: more than is left.
    client = deployment.client(retry=RetryPolicy(attempts=4, initial_interval=10.0))

    with pytest.raises(CallTimeoutError, match="the last attempt failed with status 503"):
        client.get_block(5, read_source=SINGLE_BLOCK, timeout=2)

    assert len(deployment.storage.requests_for("blocks/5")) == 1
    assert deployment.sleeps == []


def test_a_download_waiting_behind_others_stops_at_the_deadline(deployment):
    deployment.server.add_block(5)
    deployment.server.add_block(6)
    deployment.storage.delay = 2.0
    client = deployment.client(num_workers=1)
    files = client.get_block_files_by_range(5, 7, read_source=SINGLE_BLOCK)
    downloader = BlockDownloader(deployment.config(num_workers=1))
    # Another caller's download, with no deadline, holds the only worker.
    other = threading.Thread(target=downloader.download, args=(files[0],), daemon=True)
    other.start()
    while not deployment.storage.most_at_once:
        time.sleep(0.01)
    before = time.monotonic()

    try:
        with pytest.raises(CallTimeoutError):
            downloader.download(files[1], deadline=time.monotonic() + 0.2)
        waited = time.monotonic() - before
        other.join()
    finally:
        downloader.close()

    assert waited < 1.5
    # The queued download never started.
    assert deployment.storage.requests_for("blocks/6") == []


@pytest.mark.parametrize("method", ["get_block", "get_blocks_by_range"])
@pytest.mark.parametrize(
    ("status", "dead_link_wait", "waits"),
    [(400, 15.0, [15.0, 30.0]), (400, 0.0, []), (403, 15.0, [])],
)
def test_a_link_that_never_worked_waits_before_a_new_one(
    deployment, method, status, dead_link_wait, waits
):
    deployment.server.add_block(5)
    deployment.storage.expired.add("s1")
    deployment.storage.expired_status = status
    client = deployment.client(dead_link_wait=dead_link_wait)

    with pytest.raises(DownloadError) as raised:
        getattr(client, method)(5, read_source=SINGLE_BLOCK)

    assert raised.value.status_code == status
    # A wait before the second and the third link, and none after the last.
    assert deployment.sleeps == waits


def test_the_wait_for_a_new_link_starts_over_after_a_block_arrives():
    class OneBlockThenDead(LimitedDownloader):
        def iter_download(self, block_files, **limits):
            yield b""
            raise DownloadError("refused", status_code=400)

    sleeps: list[float] = []
    client = Client(
        Config(server_address="unused:1"),
        transport=StubTransport(range_answer),
        downloader=OneBlockThenDead(),
        sleep=sleeps.append,
    )

    list(client.iter_raw_block_payloads(10, 14, read_source=SINGLE_BLOCK))

    assert sleeps == [15.0, 15.0, 15.0, 15.0]


@pytest.mark.parametrize("method", ["get_block", "get_blocks_by_range"])
def test_a_timeout_stops_the_wait_for_a_new_link(deployment, method):
    deployment.server.add_block(5)
    deployment.storage.expired.add("s1")
    deployment.storage.expired_status = 400
    deployment.storage.error_body = STORE_REFUSAL

    with pytest.raises(CallTimeoutError) as raised:
        getattr(deployment.client(), method)(5, timeout=10)

    assert "its link was refused (400 ExpiredToken)" in str(raised.value)
    assert "secret" not in str(raised.value)
    assert deployment.sleeps == []
    # No new link was asked for, from this source or another.
    assert len(deployment.server.calls) == 1


def test_every_call_that_downloads_takes_a_size_limit(deployment):
    deployment.server.add_block(4, compression=api_pb2.NONE, size=4096)
    deployment.server.transactions["0xabc"] = [4]
    client = deployment.client()

    for call in downloading_calls(client, max_response_bytes=10_000):
        call()
    for call in downloading_calls(client, max_response_bytes=1000):
        with pytest.raises(ResponseTooLargeError):
            call()


@pytest.mark.parametrize("read_source", [DEFAULT, SINGLE_BLOCK])
def test_an_object_above_the_size_limit_is_read_no_further_and_not_tried_again(
    deployment, read_source
):
    deployment.server.add_block(5, compression=api_pb2.NONE, size=4096)
    client = deployment.client()

    with pytest.raises(ResponseTooLargeError, match="longer than the 1000 bytes allowed") as raised:
        client.get_block(5, read_source=read_source, max_response_bytes=1000)

    assert isinstance(raised.value, DownloadError)
    assert "sig=" not in str(raised.value)
    assert len(deployment.storage.requests_for("blocks/5")) == 1
    assert len(deployment.server.calls_to("GetBlockFile")) == 1
    assert deployment.sleeps == []


@pytest.mark.parametrize("method", ["get_blocks_by_range", "get_blocks_by_heights"])
def test_a_range_above_the_size_limit_is_not_read_from_another_source(deployment, method):
    deployment.server.add_block(5, compression=api_pb2.NONE, size=4096)
    wanted = (5, 6) if method == "get_blocks_by_range" else ([5],)

    with pytest.raises(ResponseTooLargeError):
        getattr(deployment.client(), method)(*wanted, max_response_bytes=1000)

    assert len(deployment.storage.requests_for("blocks/5")) == 1
    assert [r.read_source for r in deployment.server.calls_to("GetBlockFilesByRange")] == [DEFAULT]


def test_a_range_that_runs_out_of_time_is_not_read_from_another_source():
    class OutOfTime(LimitedDownloader):
        def iter_download(self, block_files, **limits):
            raise CallTimeoutError("a block file download did not finish in time")
            yield b""

    transport = StubTransport(range_answer)
    client = Client(Config(server_address="unused:1"), transport=transport, downloader=OutOfTime())

    # The time is not checked again by this stub, so only the error itself
    # can stop the read.
    with pytest.raises(CallTimeoutError):
        list(client.iter_raw_block_payloads(10, 14, timeout=600))

    assert [request.read_source for _, request, _ in transport.calls] == [DEFAULT]


def test_a_size_limit_is_the_size_an_object_may_just_reach(deployment):
    deployment.server.add_block(5, compression=api_pb2.NONE, size=4096)
    stored = len(deployment.storage.objects["blocks/5"])
    client = deployment.client()

    assert client.get_block(5, max_response_bytes=stored) == make_block(5, 4096)
    with pytest.raises(ResponseTooLargeError):
        client.get_block(5, max_response_bytes=stored - 1)


def test_a_chunk_above_the_size_limit_is_refused_before_it_is_asked_for(deployment):
    deployment.server.add_consolidated("zstd")
    chunk = cscb.parse_index(deployment.storage.objects[CONSOLIDATED_OBJECT]).chunks[1]
    client = deployment.client()
    # Reads the index and the first chunk, and keeps the index.
    client.get_block(1000)
    requests = len(deployment.storage.requests)

    with pytest.raises(ResponseTooLargeError, match="is more than the") as raised:
        client.get_block(1004, max_response_bytes=chunk.compressed_length - 1)

    assert "sig=" not in str(raised.value)
    assert len(deployment.storage.requests) == requests
    # The single-block object of 1004 is small enough, but is not tried.
    assert [r.read_source for r in deployment.server.calls_to("GetBlockFile")] == [DEFAULT] * 2

    block = client.get_block(1004, max_response_bytes=chunk.compressed_length)
    assert block.metadata.height == 1004


def test_a_size_limit_below_the_index_read_refuses_a_consolidated_object(deployment):
    deployment.server.add_consolidated("zstd")

    with pytest.raises(ResponseTooLargeError):
        deployment.client().get_block(1000, max_response_bytes=cscb.INITIAL_INDEX_READ_SIZE - 1)

    assert deployment.storage.requests == []


@pytest.mark.parametrize("max_response_bytes", [0, -1, 1.5, "64", True])
def test_a_size_limit_that_is_not_a_positive_number_of_bytes_is_refused(
    deployment, max_response_bytes
):
    client = deployment.client()
    downloader = BlockDownloader(deployment.config())

    try:
        for call in downloading_calls(client, max_response_bytes=max_response_bytes):
            with pytest.raises(ConfigError, match="max_response_bytes must be a positive number"):
                call()
        with pytest.raises(ConfigError, match="max_response_bytes must be a positive number"):
            downloader.iter_download([], max_response_bytes=max_response_bytes)
    finally:
        downloader.close()

    assert deployment.server.calls == []


def test_a_client_does_not_close_a_downloader_it_was_given():
    downloader = LimitedDownloader()
    client = Client(
        Config(server_address="unused:1"),
        transport=StubTransport(range_answer),
        downloader=downloader,
    )

    client.close()

    assert not downloader.closed
