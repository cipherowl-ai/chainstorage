"""Downloads block files from storage and returns their block payloads.

The server answers a block request with a ``BlockFile``: a link to an object
in storage plus where the block sits in it. There are two kinds of object:

- a single-block object, whose whole content is one compressed block;
- a consolidated object (CSCB), which holds many blocks in compressed chunks.

A payload is the serialized ``Block`` message, decompressed. A skipped block
has no stored content, so its payload is empty.

Several block files are downloaded in parallel and their payloads are handed
back in order. A chunk that holds several of the wanted blocks is fetched and
decompressed once.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Hashable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests
from google.protobuf.message import DecodeError
from requests.adapters import HTTPAdapter

from chainstorage_sdk import cscb
from chainstorage_sdk.config import Config
from chainstorage_sdk.errors import BlockFormatError, DownloadError
from chainstorage_sdk.retry import call_with_retry
from coinbase.chainstorage import api_pb2, blockchain_pb2

logger = logging.getLogger(__name__)

# How much decompressed data may be on its way at once. One download is
# always allowed, however large. While a chunk is being cut into blocks it is
# held about twice, so the peak is roughly double this.
_IN_FLIGHT_BYTES = 256 * 1024 * 1024

# What a single-block object counts for when the server does not say how
# long its block is.
_UNKNOWN_BLOCK_BYTES = 8 * 1024 * 1024

# Responses are read in pieces of this size, and never past what is needed.
_RESPONSE_PIECE = 1024 * 1024

# Parsed indexes and decompressed chunks kept for the next read. The chunk
# cache is capped in bytes (Config.chunk_cache_bytes), and a chunk larger
# than the cap is not kept.
_INDEX_CACHE_ENTRIES = 8
_CHUNK_CACHE_ENTRIES = 4


@dataclass
class _Unit:
    """One download: the payloads at ``positions``, produced by ``run``."""

    positions: list[int]
    cost: int
    run: Callable[[], list[bytes]]


class BlockDownloader:
    def __init__(
        self,
        config: Config,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config = config
        self._sleep = sleep
        if session is None:
            session = requests.Session()
            adapter = HTTPAdapter(
                pool_connections=config.num_workers, pool_maxsize=config.num_workers
            )
            session.mount("https://", adapter)
            session.mount("http://", adapter)
        self._session = session
        self._executor = ThreadPoolExecutor(
            max_workers=config.num_workers, thread_name_prefix="chainstorage-download"
        )
        self._indexes = _Cache(_INDEX_CACHE_ENTRIES)
        self._chunks = _Cache(_CHUNK_CACHE_ENTRIES, max_bytes=config.chunk_cache_bytes)

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._session.close()

    def download(self, block_file: api_pb2.BlockFile) -> bytes:
        return self.download_many([block_file])[0]

    def download_many(self, block_files: Sequence[api_pb2.BlockFile]) -> list[bytes]:
        """Returns one payload per block file, in the order given."""
        return list(self.iter_download(block_files))

    def iter_download(self, block_files: Sequence[api_pb2.BlockFile]) -> Iterator[bytes]:
        """Yields one payload per block file, in the order given.

        Downloads run ahead of the caller, up to ``num_workers`` at a time and
        within a memory budget, so a long list never has to fit in memory.
        """
        units = iter(self._plan(block_files))
        upcoming = next(units, None)
        in_flight: deque[tuple[_Unit, Future[list[bytes]]]] = deque()
        in_flight_bytes = 0
        ready: dict[int, bytes] = {}
        position = 0
        try:
            while in_flight or upcoming is not None:
                while upcoming is not None and (
                    not in_flight
                    or (
                        len(in_flight) < self._config.num_workers
                        and in_flight_bytes + upcoming.cost <= _IN_FLIGHT_BYTES
                    )
                ):
                    in_flight.append((upcoming, self._executor.submit(upcoming.run)))
                    in_flight_bytes += upcoming.cost
                    upcoming = next(units, None)
                unit, task = in_flight.popleft()
                ready.update(zip(unit.positions, task.result(), strict=True))
                in_flight_bytes -= unit.cost
                # Drop the finished download now: its payloads must not stay
                # alive, through these names, while the next ones arrive.
                del unit, task
                while position in ready:
                    yield ready.pop(position)
                    position += 1
        finally:
            for _, task in in_flight:
                task.cancel()

    def to_block(self, block_file: api_pb2.BlockFile, payload: bytes) -> blockchain_pb2.Block:
        """Parses a payload into a ``Block``.

        A skipped block is built from the block file. A block from a
        consolidated object gets its metadata from the block file. Both are
        what the Go SDK does.
        """
        if block_file.skipped:
            return blockchain_pb2.Block(
                blockchain=self._config.blockchain,
                network=self._config.network,
                side_chain=self._config.sidechain,
                metadata=blockchain_pb2.BlockMetadata(
                    tag=block_file.tag, height=block_file.height, skipped=True
                ),
            )
        block = blockchain_pb2.Block()
        try:
            block.ParseFromString(payload)
        except DecodeError as exc:
            raise BlockFormatError(
                f"failed to parse block at height {block_file.height}: {exc}"
            ) from exc
        if is_consolidated(block_file):
            block.metadata.CopyFrom(_metadata_from_block_file(block_file))
        return block

    def _plan(self, block_files: Sequence[api_pb2.BlockFile]) -> list[_Unit]:
        """Groups the block files into downloads, in the order of their first
        block: one per single-block object, and one per chunk of a
        consolidated object."""
        # Every consolidated object's index is needed to know its chunks.
        urls: dict[str, str] = {}
        for block_file in block_files:
            if not block_file.skipped and is_consolidated(block_file):
                urls.setdefault(_object_key(block_file.file_url), block_file.file_url)
        indexes = dict(
            zip(urls, self._executor.map(self._index, urls.keys(), urls.values()), strict=True)
        )

        units: list[_Unit] = []
        chunk_units: dict[tuple[str, int], tuple[_Unit, list[cscb.BlockRecord]]] = {}
        for position, block_file in enumerate(block_files):
            if block_file.skipped:
                units.append(_Unit([position], 0, lambda: [b""]))
            elif not is_consolidated(block_file):
                run = partial(self._download_single, block_file)
                cost = block_file.uncompressed_length or _UNKNOWN_BLOCK_BYTES
                units.append(_Unit([position], cost, run))
            else:
                key = _object_key(block_file.file_url)
                index = indexes[key]
                record, chunk = cscb.lookup_block(index, block_file)
                planned = chunk_units.get((key, chunk.index))
                if planned is None:
                    records: list[cscb.BlockRecord] = []
                    run = partial(
                        self._download_chunk, key, urls[key], index.header.codec, chunk, records
                    )
                    unit = _Unit([], chunk.uncompressed_length, run)
                    units.append(unit)
                    planned = chunk_units[(key, chunk.index)] = (unit, records)
                planned[0].positions.append(position)
                planned[1].append(record)
        return units

    def _download_single(self, block_file: api_pb2.BlockFile) -> list[bytes]:
        # No block is larger than the largest chunk. The server does not
        # always say how long the block is; when it does not, the output may
        # still not grow past that.
        cap = cscb.MAX_CHUNK_BYTES
        declared = block_file.uncompressed_length or None
        if declared is not None and declared > cap:
            raise BlockFormatError(
                f"block {block_file.height} declares {declared} bytes, more than the {cap} allowed"
            )
        body = self._request(block_file.file_url, None, limit=cap)
        if declared is not None:
            payload = cscb.decompress(body, block_file.compression, max_size=declared)
        else:
            payload = cscb.decompress(body, block_file.compression, limit=cap)
        payload = bytes(payload)
        if declared is not None and len(payload) != declared:
            raise BlockFormatError(
                f"block {block_file.height} is {len(payload)} bytes long, not the {declared} "
                "bytes the server reported"
            )
        return [payload]

    def _index(self, key: str, url: str) -> cscb.Index:
        index = self._indexes.get(key)
        if index is None:
            first = self._get_range(url, 0, cscb.INITIAL_INDEX_READ_SIZE - 1)
            needed = cscb.parse_header(first).index_length
            if needed > len(first):
                first += self._get_range(url, len(first), needed - 1)
            index = cscb.parse_index(first[:needed])
            self._indexes.put(key, index)
        return index

    def _download_chunk(
        self,
        key: str,
        url: str,
        codec: int,
        chunk: cscb.ChunkRecord,
        records: Sequence[cscb.BlockRecord],
    ) -> list[bytes]:
        cache_key = (key, chunk.index)
        chunk_payload = self._chunks.get(cache_key)
        if chunk_payload is None:
            if chunk.compressed_length <= 0:
                raise BlockFormatError(f"CSCB chunk {chunk.index} has no data")
            start = chunk.compressed_payload_offset
            frame = self._get_range(url, start, start + chunk.compressed_length - 1)
            chunk_payload = cscb.decode_chunk(frame, codec, chunk)
            self._chunks.put(cache_key, chunk_payload, size=len(chunk_payload))
        return cscb.extract_blocks(chunk_payload, chunk, records)

    def _get_range(self, url: str, start: int, end: int) -> bytes:
        """Reads bytes ``start..end`` (inclusive) of an object."""
        expected = end - start + 1

        def check(status: int, body: bytes) -> bytes:
            if status == 200:
                # The store ignored the range and sent the whole object. That
                # is only usable for a read from the start.
                if start != 0:
                    raise DownloadError(
                        f"range request was not honoured for {_redact(url)}", status_code=status
                    )
                return body[:expected]
            if len(body) > expected or (start != 0 and len(body) != expected):
                # A cut-off transfer: worth another attempt.
                raise DownloadError(
                    f"range body length mismatch for {_redact(url)}: "
                    f"got {len(body)} want {expected}",
                    status_code=status,
                    retryable=True,
                )
            return body

        # One byte more than wanted shows a store that sent too much.
        return self._request(url, {"Range": f"bytes={start}-{end}"}, check, limit=expected)

    def _request(
        self,
        url: str,
        headers: dict[str, str] | None,
        check: Callable[[int, bytes], bytes] | None = None,
        *,
        limit: int,
    ) -> bytes:
        """Gets ``url`` and returns its body, read up to ``limit`` + 1 bytes.

        Reading stops there, so a store that ignores a range or sends a huge
        object never makes us hold more. Without ``check``, a longer body is
        refused.
        """
        timeout = self._config.http_timeout

        def attempt() -> bytes:
            try:
                with self._session.get(
                    url, headers=headers, timeout=timeout, stream=True
                ) as response:
                    status = response.status_code
                    if status not in (200, 206):
                        raise DownloadError(
                            f"received status {status} for {_redact(url)}",
                            status_code=status,
                            retryable=status in (408, 429) or status >= 500,
                        )
                    body = _read_at_most(response, limit + 1)
            except requests.RequestException as exc:
                # The exception text can hold the signed link, so only its
                # type is reported, and it is not chained.
                raise DownloadError(
                    f"failed to download {_redact(url)}: {type(exc).__name__}", retryable=True
                ) from None
            if check is not None:
                return check(status, body)
            if len(body) > limit:
                raise DownloadError(
                    f"{_redact(url)} is longer than the {limit} bytes allowed", status_code=status
                )
            return body

        return call_with_retry(
            attempt,
            retryable=lambda exc: isinstance(exc, DownloadError) and exc.retryable,
            policy=self._config.retry,
            sleep=self._sleep,
        )


def _read_at_most(response: requests.Response, size: int) -> bytes:
    """Reads the body of a streamed response, stopping after ``size`` bytes."""
    pieces: list[bytes] = []
    read = 0
    for piece in response.iter_content(chunk_size=_RESPONSE_PIECE):
        pieces.append(piece)
        read += len(piece)
        if read >= size:
            break
    body = b"".join(pieces)
    return body if len(body) <= size else body[:size]


def is_consolidated(block_file: api_pb2.BlockFile) -> bool:
    return (
        block_file.object_format == blockchain_pb2.BLOCK_OBJECT_FORMAT_CSCB_BATCH
        and block_file.byte_length != 0
    )


def _metadata_from_block_file(block_file: api_pb2.BlockFile) -> blockchain_pb2.BlockMetadata:
    metadata = blockchain_pb2.BlockMetadata(
        tag=block_file.tag,
        hash=block_file.hash,
        parent_hash=block_file.parent_hash,
        height=block_file.height,
        parent_height=block_file.parent_height,
        skipped=block_file.skipped,
        object_format=block_file.object_format,
        byte_offset=block_file.byte_offset,
        byte_length=block_file.byte_length,
        uncompressed_length=block_file.uncompressed_length,
    )
    if block_file.HasField("block_timestamp"):
        metadata.timestamp.CopyFrom(block_file.block_timestamp)
    return metadata


def _object_key(url: str) -> str:
    """Identifies an object across links: a signed link changes each time it
    is issued, but only in its query string."""
    return _redact(url)


def _redact(url: str) -> str:
    """Drops the query string, which holds the signature of a signed link."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


class _Cache:
    """Keeps the most recently used entries, within a count and a byte limit."""

    def __init__(self, max_entries: int, *, max_bytes: int | None = None) -> None:
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self._entries: OrderedDict[Hashable, tuple[Any, int]] = OrderedDict()
        self._bytes = 0

    def get(self, key: Hashable) -> Any:
        with self._lock:
            if key not in self._entries:
                return None
            self._entries.move_to_end(key)
            return self._entries[key][0]

    def put(self, key: Hashable, value: Any, *, size: int = 0) -> None:
        if self._max_bytes is not None and size > self._max_bytes:
            return
        with self._lock:
            if key in self._entries:
                self._bytes -= self._entries.pop(key)[1]
            self._entries[key] = (value, size)
            self._bytes += size
            while len(self._entries) > self._max_entries or (
                self._max_bytes is not None and self._bytes > self._max_bytes
            ):
                self._bytes -= self._entries.popitem(last=False)[1][1]
