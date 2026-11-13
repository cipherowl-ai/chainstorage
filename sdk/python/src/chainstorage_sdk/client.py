"""The ChainStorage client: blocks and chain events.

The methods follow the Go SDK's ``Client`` (``sdk/client.go``), with Python
names. Blocks come back as raw ``Block`` messages, exactly as stored; the
block parser turns them into native blocks.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

from chainstorage_sdk.config import Config
from chainstorage_sdk.downloader import BlockDownloader
from chainstorage_sdk.errors import (
    ChainStorageError,
    ConfigError,
    DownloadError,
    NotFoundError,
    RequestError,
)
from chainstorage_sdk.transport import EventStream, Transport, new_transport
from coinbase.chainstorage import api_pb2, blockchain_pb2

logger = logging.getLogger(__name__)

T = TypeVar("T")

# The server refuses a request for more block files than this.
MAX_BLOCK_FILES_PER_REQUEST = 1000

# A signed link that has expired answers 400 or 403. The client then asks the
# server for fresh links. It gives up after this many refusals in a row.
_LINK_ATTEMPTS = 3
_EXPIRED_LINK_STATUSES = (400, 403)

_SOURCES_WITH_FALLBACK = (
    api_pb2.BLOCK_READ_SOURCE_DEFAULT,
    api_pb2.BLOCK_READ_SOURCE_CONSOLIDATED,
)


@dataclass(frozen=True, eq=False)
class RawBlockPayload:
    """One block as stored: the serialized ``Block`` message, not parsed.

    The payload of a skipped block is empty.
    """

    # Neither field is printed: the block file holds a signed link, and the
    # payload can be megabytes long.
    block_file: api_pb2.BlockFile = field(repr=False)
    payload: bytes = field(repr=False)

    def __repr__(self) -> str:
        return (
            f"RawBlockPayload(height={self.block_file.height}, "
            f"hash={self.block_file.hash!r}, bytes={len(self.payload)})"
        )


@dataclass(frozen=True)
class StreamingConfiguration:
    """What to stream.

    ``chain_events_request`` says where to start: after ``sequence_num`` (or
    the older ``sequence``), or at ``initial_position_in_stream`` (``EARLIEST``,
    ``LATEST`` or a block height).

    ``number_of_events``: stop after this many events. 0 never stops.

    ``event_only``: do not download the block of each event.
    """

    chain_events_request: api_pb2.ChainEventsRequest
    number_of_events: int = 0
    event_only: bool = False


@dataclass(frozen=True)
class ChainEventResult:
    """One event of the stream and, unless ``event_only`` was set, its block."""

    event: api_pb2.BlockchainEvent
    block: blockchain_pb2.Block | None


class Client:
    """Reads blocks and chain events from one ChainStorage deployment.

    A client holds a connection and a download pool. Close it when done, or
    use it as a context manager. It may be shared between threads.
    """

    def __init__(
        self,
        config: Config,
        *,
        transport: Transport | None = None,
        downloader: BlockDownloader | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config = config
        self._sleep = sleep
        self._transport = transport or new_transport(config, sleep=sleep)
        self._downloader = downloader or BlockDownloader(config, sleep=sleep)

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self._downloader.close()
        self._transport.close()

    @property
    def config(self) -> Config:
        return self._config

    # -- blocks -----------------------------------------------------------

    def get_latest_block(self, *, tag: int | None = None) -> int:
        """Returns the height of the latest block."""
        request = api_pb2.GetLatestBlockRequest(tag=self._tag(tag))
        return self._call("GetLatestBlock", request, self._short_timeout).height

    def get_block(
        self,
        height: int,
        hash: str = "",
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
    ) -> blockchain_pb2.Block:
        """Returns one block. Pass ``hash`` to read a block that a
        reorganization has since replaced at that height."""
        raw = self.open_raw_block_payload(height, hash, tag=tag, read_source=read_source)
        return self.parse_raw_block(raw)

    def get_blocks_by_range(
        self,
        start_height: int,
        end_height: int = 0,
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
    ) -> list[blockchain_pb2.Block]:
        """Returns the blocks in ``[start_height, end_height)`` on the current
        chain. ``end_height == 0`` means one block.

        Every block is held in memory at once; for a long range, iterate
        ``iter_raw_block_payloads`` instead.
        """
        return [
            self.parse_raw_block(raw)
            for raw in self.iter_raw_block_payloads(
                start_height, end_height, tag=tag, read_source=read_source
            )
        ]

    def open_raw_block_payload(
        self,
        height: int,
        hash: str = "",
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
    ) -> RawBlockPayload:
        """Returns one block as stored, without parsing it."""
        resolved_tag = self._tag(tag)

        def read(source: int) -> RawBlockPayload:
            request = api_pb2.GetBlockFileRequest(
                tag=resolved_tag, height=height, hash=hash, read_source=source
            )
            for attempt in range(1, _LINK_ATTEMPTS + 1):
                block_file = self._call("GetBlockFile", request, self._short_timeout).file
                try:
                    payload = self._downloader.download(block_file)
                except DownloadError as exc:
                    if not _is_expired_link(exc) or attempt == _LINK_ATTEMPTS:
                        raise
                    logger.info("a block file link was refused; asking for a fresh one")
                    continue
                return RawBlockPayload(block_file=block_file, payload=payload)
            raise AssertionError("unreachable")

        return self._with_fallback(read, read_source)

    def iter_raw_block_payloads(
        self,
        start_height: int,
        end_height: int = 0,
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
    ) -> Iterator[RawBlockPayload]:
        """Yields the blocks in ``[start_height, end_height)`` as stored, in
        height order, without parsing them.

        Blocks are downloaded ahead of the caller, a bounded number at a time,
        so a long range never has to fit in memory.

        A range of any length is accepted. It is requested from the server
        1,000 blocks at a time; each such request describes one consistent
        chain, and the client checks that consecutive requests connect.
        """
        if end_height == 0:
            end_height = start_height + 1
        if end_height <= start_height:
            raise ConfigError(f"invalid range: start_height={start_height} end_height={end_height}")
        return self._iter_raw_block_payloads(start_height, end_height, self._tag(tag), read_source)

    def _iter_raw_block_payloads(
        self, start_height: int, end_height: int, tag: int, read_source: int
    ) -> Iterator[RawBlockPayload]:
        previous: api_pb2.BlockFile | None = None
        for first in range(start_height, end_height, MAX_BLOCK_FILES_PER_REQUEST):
            last = min(first + MAX_BLOCK_FILES_PER_REQUEST, end_height)
            source, files = self._page(tag, first, last, read_source)
            # The server checks the chain within one request. Between two
            # requests the chain can change, so check that they connect.
            stored = [block_file for block_file in files if not block_file.skipped]
            if stored:
                if (
                    previous is not None
                    and stored[0].parent_hash
                    and stored[0].parent_hash != previous.hash
                ):
                    raise RequestError(
                        f"the chain changed while reading: block {stored[0].height} does "
                        f"not follow block {previous.height}; read the range again"
                    )
                previous = stored[-1]
            yield from self._download_page(tag, files, source)

    def _page(
        self, tag: int, start_height: int, end_height: int, read_source: int
    ) -> tuple[int, list[api_pb2.BlockFile]]:
        """Returns the block files of one request, and the source they are for."""

        def read(source: int) -> tuple[int, list[api_pb2.BlockFile]]:
            return source, self._block_files(tag, start_height, end_height, source)

        return self._with_fallback(read, read_source)

    def _download_page(
        self, tag: int, files: list[api_pb2.BlockFile], source: int
    ) -> Iterator[RawBlockPayload]:
        """Downloads one request's block files, in order.

        A failure does not start over: the blocks not yet yielded are asked
        for again, with fresh links when a link has expired, and from
        single-block objects when the wanted source fails.
        """
        done = 0
        refusals = 0
        while done < len(files):
            progress = done
            try:
                for payload in self._downloader.iter_download(files[done:]):
                    yield RawBlockPayload(block_file=files[done], payload=payload)
                    done += 1
            except DownloadError as exc:
                if not _is_expired_link(exc):
                    source = self._fallback_source(source, exc)
                else:
                    refusals = 1 if done > progress else refusals + 1
                    if refusals == _LINK_ATTEMPTS:
                        # Fresh links keep being refused: try the other source.
                        source = self._fallback_source(source, exc)
                        refusals = 0
                    else:
                        logger.info("a block file link was refused; asking for fresh ones")
            except ChainStorageError as exc:
                source = self._fallback_source(source, exc)
            else:
                return
            if done < len(files):
                files[done:] = self._same_blocks(files[done:], tag, source)

    def _fallback_source(self, source: int, exc: ChainStorageError) -> int:
        """Returns the source to continue with after ``exc``, or raises it."""
        if source not in _SOURCES_WITH_FALLBACK:
            raise exc
        logger.warning("block read failed (%s); continuing from single-block objects", exc)
        return api_pb2.BLOCK_READ_SOURCE_SINGLE_BLOCK

    def _same_blocks(
        self, files: Sequence[api_pb2.BlockFile], tag: int, source: int
    ) -> list[api_pb2.BlockFile]:
        """Asks again for the block files of ``files`` and checks that they
        still describe the same blocks."""
        fresh = self._block_files(tag, files[0].height, files[-1].height + 1, source)
        for old, new in zip(files, fresh, strict=True):
            if (old.height, old.hash, old.skipped) != (new.height, new.hash, new.skipped):
                raise RequestError(
                    f"the chain changed while reading: block {old.height} was replaced; "
                    "read the range again"
                )
        return fresh

    def parse_raw_block(self, raw: RawBlockPayload) -> blockchain_pb2.Block:
        """Parses a raw payload into the ``Block`` that ``get_block`` returns."""
        return self._downloader.to_block(raw.block_file, raw.payload)

    def get_block_by_transaction(
        self, transaction_hash: str, *, tag: int | None = None
    ) -> list[blockchain_pb2.Block]:
        """Returns the blocks that hold the transaction, or an empty list when
        the server does not know it."""
        request = api_pb2.GetBlockByTransactionRequest(
            tag=self._tag(tag), transaction_hash=transaction_hash
        )
        try:
            response = self._call("GetBlockByTransaction", request, self._short_timeout)
        except NotFoundError:
            return []
        return [
            self.get_block(block.height, block.hash, tag=block.tag) for block in response.blocks
        ]

    def get_block_by_timestamp(
        self, timestamp: int, *, tag: int | None = None
    ) -> blockchain_pb2.Block:
        """Returns the block the server finds for ``timestamp`` (Unix seconds)."""
        request = api_pb2.GetBlockByTimestampRequest(tag=self._tag(tag), timestamp=timestamp)
        found = self._call("GetBlockByTimestamp", request, self._short_timeout)
        return self.get_block(found.height, found.hash, tag=found.tag)

    # -- chain events -----------------------------------------------------

    def get_chain_events(
        self, request: api_pb2.GetChainEventsRequest
    ) -> list[api_pb2.BlockchainEvent]:
        """Returns up to ``request.max_num_events`` events after a position."""
        timeout = self._medium_timeout if request.max_num_events > 10 else self._short_timeout
        return list(self._call("GetChainEvents", request, timeout).events)

    def stream_chain_events(self, config: StreamingConfiguration) -> Iterator[ChainEventResult]:
        """Yields chain events as the server reports them, each with its block.

        A ``BLOCK_REMOVED`` event is yielded like any other: the caller undoes
        the block it names.

        When the connection drops, the stream is reopened after the last
        event that was yielded, so no event is lost or repeated. That needs a
        position: a stream that started from ``initial_position_in_stream``
        and fails before its first event raises instead.

        Raises ``RequestError`` when the stream fails for good, and whatever
        ``get_block`` raises when a block cannot be downloaded.
        """
        if config.number_of_events < 0:
            raise ConfigError("number_of_events must not be negative")
        request = api_pb2.ChainEventsRequest()
        request.CopyFrom(config.chain_events_request)
        # Open the stream now, so that a transport that cannot stream fails
        # here and not at the first event.
        stream = self._transport.stream_chain_events(request)
        return self._stream_chain_events(config, request, stream)

    def _stream_chain_events(
        self,
        config: StreamingConfiguration,
        request: api_pb2.ChainEventsRequest,
        stream: EventStream,
    ) -> Iterator[ChainEventResult]:
        streams = [stream]
        try:
            delivered = 0
            while config.number_of_events == 0 or delivered < config.number_of_events:
                event = self._next_event(request, streams)
                block = None
                if not config.event_only:
                    # By hash: a removed block is no longer at its height.
                    block = self.get_block(
                        event.block.height, event.block.hash, tag=event.block.tag
                    )
                yield ChainEventResult(event=event, block=block)
                # The caller has the event: resume after it from now on.
                request.sequence = event.sequence
                request.sequence_num = event.sequence_num
                delivered += 1
        finally:
            streams[0].cancel()

    def _next_event(
        self, request: api_pb2.ChainEventsRequest, streams: list[EventStream]
    ) -> api_pb2.BlockchainEvent:
        # Each event gets the full retry budget, as in the Go SDK.
        waits = self._config.retry.waits()
        while True:
            try:
                response = next(streams[0])
            except StopIteration:
                raise RequestError("the server closed the event stream") from None
            except RequestError as exc:
                if not request.sequence and request.initial_position_in_stream:
                    # No event was received yet, so there is no position to
                    # reopen the stream at.
                    raise
                wait = next(waits, None)
                if not exc.retryable or wait is None:
                    raise
                logger.info("reconnecting the event stream after %s", exc.code)
                # A broken stream cannot be read again: open a new one after
                # the last event that was yielded.
                try:
                    streams[0] = self._transport.stream_chain_events(request)
                except RequestError:
                    raise exc from None
                self._sleep(wait)
                continue
            if not response.HasField("event"):
                raise RequestError("received an empty event from the stream")
            return response.event

    # -- metadata ---------------------------------------------------------

    def get_chain_metadata(self) -> api_pb2.GetChainMetadataResponse:
        """Returns what the server reports about its chain and its tags."""
        return self._call(
            "GetChainMetadata", api_pb2.GetChainMetadataRequest(), self._short_timeout
        )

    # -- internals --------------------------------------------------------

    def _tag(self, tag: int | None) -> int:
        return self._config.tag if tag is None else tag

    @property
    def _short_timeout(self) -> float:
        return self._config.client_timeout

    @property
    def _medium_timeout(self) -> float:
        return self._config.client_timeout * 2 + 1

    @property
    def _long_timeout(self) -> float:
        return self._config.client_timeout * 4 + 3

    def _call(self, method: str, request: Any, timeout: float) -> Any:
        return self._transport.call(method, request, timeout=timeout)

    def _block_files(
        self, tag: int, start_height: int, end_height: int, read_source: int
    ) -> list[api_pb2.BlockFile]:
        count = end_height - start_height
        request = api_pb2.GetBlockFilesByRangeRequest(
            tag=tag, start_height=start_height, end_height=end_height, read_source=read_source
        )
        timeout = self._long_timeout if count > 2 else self._medium_timeout
        files = list(self._call("GetBlockFilesByRange", request, timeout).files)
        if len(files) != count:
            raise RequestError(
                f"expected {count} block files for [{start_height}, {end_height}), got {len(files)}"
            )
        for height, block_file in zip(range(start_height, end_height), files, strict=True):
            if block_file.height != height:
                raise RequestError(
                    f"expected the block file of height {height}, got {block_file.height}"
                )
        return files

    def _with_fallback(self, read: Callable[[int], T], read_source: int) -> T:
        """Runs ``read`` with the wanted source. When that fails and the source
        allows it, reads once more from single-block objects, as the Go SDK
        does."""
        try:
            return read(read_source)
        except ChainStorageError as exc:
            if read_source not in _SOURCES_WITH_FALLBACK:
                raise
            logger.warning("block read failed (%s); retrying from single-block objects", exc)
            try:
                return read(api_pb2.BLOCK_READ_SOURCE_SINGLE_BLOCK)
            except ChainStorageError as fallback_exc:
                raise fallback_exc from exc


def _is_expired_link(exc: DownloadError) -> bool:
    return exc.status_code in _EXPIRED_LINK_STATUSES
