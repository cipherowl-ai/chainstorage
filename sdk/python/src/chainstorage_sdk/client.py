"""The ChainStorage client: blocks and chain events.

The methods follow the Go SDK's ``Client`` (``sdk/client.go``), with Python
names. Blocks come back as raw ``Block`` messages, exactly as stored; the
block parser turns them into native blocks.
"""

from __future__ import annotations

import inspect
import logging
import math
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

from chainstorage_sdk.config import Config
from chainstorage_sdk.downloader import BlockDownloader
from chainstorage_sdk.errors import (
    CallTimeoutError,
    ChainStorageError,
    ConfigError,
    DownloadError,
    NotFoundError,
    RequestError,
    ResponseTooLargeError,
)
from chainstorage_sdk.retry import time_left
from chainstorage_sdk.transport import EventStream, Transport, new_transport
from coinbase.chainstorage import api_pb2, blockchain_pb2

logger = logging.getLogger(__name__)

T = TypeVar("T")

# The server refuses a request for more block files than this.
MAX_BLOCK_FILES_PER_REQUEST = 1000

# Storage refuses a signed link with 400 or 403. The client then asks the
# server for fresh links. It gives up after this many refusals in a row.
_LINK_ATTEMPTS = 3
_EXPIRED_LINK_STATUSES = (400, 403)
# A link that has expired answers 403, and its replacement works at once. A
# 400 is a link that never worked: the server signed it with credentials that
# were about to expire, and needs a moment to get new ones
# (Config.dead_link_wait).
_DEAD_LINK_STATUS = 400

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
class _Limits:
    """What the caller allows one read.

    ``deadline``: the ``time.monotonic()`` value the read must be done by.
    ``max_response_bytes``: the most bytes one request to storage may return.
    """

    deadline: float | None = None
    max_response_bytes: int | None = None

    def for_download(self) -> dict[str, Any]:
        """The limits as arguments of a download. Without limits there are
        none, so a downloader written before limits existed still works."""
        if self == _NO_LIMITS:
            return {}
        return {"deadline": self.deadline, "max_response_bytes": self.max_response_bytes}


_NO_LIMITS = _Limits()


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

    Every method but ``stream_chain_events`` takes ``timeout``: a limit in
    seconds for the whole call, with its downloads, retries, new links and
    waits. When it runs out the call raises ``CallTimeoutError``, and nothing
    more is tried. Without it, each attempt is limited by ``client_timeout``
    or ``http_timeout``, and the retries by ``retry``.

    - With little time left, a retry is not made when the wait before it
      would not end in time.
    - Over REST, and while a block file downloads, the limit is looser: an
      answer that keeps arriving slowly can run past it, and is then not
      returned.
    - For the methods that return an iterator, the time starts when the
      method is called, so it includes the time the caller spends between
      two blocks.

    The methods that download take ``max_response_bytes``: the most bytes one
    request to storage may return (a single-block object, the index of a
    consolidated object, or one of its chunks). A larger one raises
    ``ResponseTooLargeError``, and nothing more is tried.
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
        self._transport_takes_deadline = _takes_deadline(self._transport)
        # A downloader that was passed in is the caller's to close.
        self._owns_downloader = downloader is None
        self._downloader = downloader or BlockDownloader(config, sleep=sleep)

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_downloader:
            self._downloader.close()
        self._transport.close()

    @property
    def config(self) -> Config:
        return self._config

    # -- blocks -----------------------------------------------------------

    def get_latest_block(self, *, tag: int | None = None, timeout: float | None = None) -> int:
        """Returns the height of the latest block."""
        return self.get_latest_block_info(tag=tag, timeout=timeout).height

    def get_latest_block_info(
        self, *, tag: int | None = None, timeout: float | None = None
    ) -> api_pb2.GetLatestBlockResponse:
        """Returns the latest block's tag, hash, parent hash, height and
        timestamp."""
        request = api_pb2.GetLatestBlockRequest(tag=self._tag(tag))
        return self._call("GetLatestBlock", request, self._short_timeout, _deadline(timeout))

    def get_block_file(
        self,
        height: int,
        hash: str = "",
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
        timeout: float | None = None,
    ) -> api_pb2.BlockFile:
        """Returns where one block is stored, without downloading it.

        The answer is for ``read_source`` as asked: unlike ``get_block``, this
        does not try single-block objects when the wanted source fails.

        A block file holds a signed link (``file_url``). Do not log it.
        """
        request = api_pb2.GetBlockFileRequest(
            tag=self._tag(tag), height=height, hash=hash, read_source=read_source
        )
        return self._call("GetBlockFile", request, self._short_timeout, _deadline(timeout)).file

    def get_block_files_by_range(
        self,
        start_height: int,
        end_height: int = 0,
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
        timeout: float | None = None,
    ) -> list[api_pb2.BlockFile]:
        """Returns where the blocks in ``[start_height, end_height)`` are
        stored, in height order, without downloading them. ``end_height == 0``
        means one block. A skipped block has a file with ``skipped`` set.

        A range of any length is accepted, requested from the server 1,000
        blocks at a time and checked as ``iter_raw_block_payloads`` checks it:
        every height is present, and consecutive requests connect. The block
        files of the whole range are held in memory at once.

        The answer is for ``read_source`` as asked: unlike
        ``iter_raw_block_payloads``, this does not try single-block objects
        when the wanted source fails.

        A block file holds a signed link (``file_url``). Do not log it.
        """
        start_height, end_height = _range(start_height, end_height)
        resolved_tag = self._tag(tag)
        deadline = _deadline(timeout)
        files: list[api_pb2.BlockFile] = []
        previous: api_pb2.BlockFile | None = None
        for first in range(start_height, end_height, MAX_BLOCK_FILES_PER_REQUEST):
            last = min(first + MAX_BLOCK_FILES_PER_REQUEST, end_height)
            page = self._block_files(resolved_tag, first, last, read_source, deadline)
            previous = _check_connects(previous, page)
            files.extend(page)
        return files

    def get_block(
        self,
        height: int,
        hash: str = "",
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
        timeout: float | None = None,
        max_response_bytes: int | None = None,
    ) -> blockchain_pb2.Block:
        """Returns one block. Pass ``hash`` to read a block that a
        reorganization has since replaced at that height."""
        raw = self.open_raw_block_payload(
            height,
            hash,
            tag=tag,
            read_source=read_source,
            timeout=timeout,
            max_response_bytes=max_response_bytes,
        )
        return self.parse_raw_block(raw)

    def get_blocks_by_range(
        self,
        start_height: int,
        end_height: int = 0,
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
        timeout: float | None = None,
        max_response_bytes: int | None = None,
    ) -> list[blockchain_pb2.Block]:
        """Returns the blocks in ``[start_height, end_height)`` on the current
        chain. ``end_height == 0`` means one block.

        Every block is held in memory at once; for a long range, iterate
        ``iter_raw_block_payloads`` instead.
        """
        return [
            self.parse_raw_block(raw)
            for raw in self.iter_raw_block_payloads(
                start_height,
                end_height,
                tag=tag,
                read_source=read_source,
                timeout=timeout,
                max_response_bytes=max_response_bytes,
            )
        ]

    def get_blocks_by_heights(
        self,
        heights: Iterable[int],
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
        timeout: float | None = None,
        max_response_bytes: int | None = None,
    ) -> list[blockchain_pb2.Block]:
        """Returns the blocks at ``heights`` on the current chain, in height
        order. A height given twice is returned once.

        Every block is held in memory at once; for many heights, iterate
        ``iter_raw_block_payloads_by_heights`` instead.
        """
        return [
            self.parse_raw_block(raw)
            for raw in self.iter_raw_block_payloads_by_heights(
                heights,
                tag=tag,
                read_source=read_source,
                timeout=timeout,
                max_response_bytes=max_response_bytes,
            )
        ]

    def open_raw_block_payload(
        self,
        height: int,
        hash: str = "",
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
        timeout: float | None = None,
        max_response_bytes: int | None = None,
    ) -> RawBlockPayload:
        """Returns one block as stored, without parsing it."""
        return self._open_raw_block_payload(
            height, hash, self._tag(tag), read_source, _limits(timeout, max_response_bytes)
        )

    def _open_raw_block_payload(
        self, height: int, hash: str, tag: int, read_source: int, limits: _Limits
    ) -> RawBlockPayload:
        def read(source: int) -> RawBlockPayload:
            request = api_pb2.GetBlockFileRequest(
                tag=tag, height=height, hash=hash, read_source=source
            )
            for attempt in range(1, _LINK_ATTEMPTS + 1):
                block_file = self._call(
                    "GetBlockFile", request, self._short_timeout, limits.deadline
                ).file
                try:
                    payload = self._downloader.download(block_file, **limits.for_download())
                except DownloadError as exc:
                    if not _is_expired_link(exc) or attempt == _LINK_ATTEMPTS:
                        raise
                    self._before_fresh_links(exc, attempt, limits, "asking for a fresh one")
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
        timeout: float | None = None,
        max_response_bytes: int | None = None,
    ) -> Iterator[RawBlockPayload]:
        """Yields the blocks in ``[start_height, end_height)`` as stored, in
        height order, without parsing them.

        Blocks are downloaded ahead of the caller, a bounded number at a time,
        so a long range never has to fit in memory.

        A range of any length is accepted. It is requested from the server
        1,000 blocks at a time; each such request describes one consistent
        chain, and the client checks that consecutive requests connect.
        """
        start_height, end_height = _range(start_height, end_height)
        limits = _limits(timeout, max_response_bytes)
        return self._iter_raw_block_payloads(
            start_height, end_height, self._tag(tag), read_source, limits
        )

    def _iter_raw_block_payloads(
        self, start_height: int, end_height: int, tag: int, read_source: int, limits: _Limits
    ) -> Iterator[RawBlockPayload]:
        previous: api_pb2.BlockFile | None = None
        for first in range(start_height, end_height, MAX_BLOCK_FILES_PER_REQUEST):
            last = min(first + MAX_BLOCK_FILES_PER_REQUEST, end_height)
            source, files = self._page(tag, first, last, read_source, limits)
            previous = _check_connects(previous, files)
            yield from self._download_page(tag, files, source, limits)

    def iter_raw_block_payloads_by_heights(
        self,
        heights: Iterable[int],
        *,
        tag: int | None = None,
        read_source: int = api_pb2.BLOCK_READ_SOURCE_DEFAULT,
        timeout: float | None = None,
        max_response_bytes: int | None = None,
    ) -> Iterator[RawBlockPayload]:
        """Yields the blocks at ``heights`` as stored, in height order, without
        parsing them. A height given twice is yielded once. A skipped block
        is yielded with an empty payload.

        Blocks are downloaded ahead of the caller, a bounded number at a time,
        so many heights never have to fit in memory.

        The server is asked for a stretch of up to 1,000 heights at a time,
        just before its blocks are downloaded, and only the wanted ones are
        downloaded. Each stretch describes one consistent chain. Heights that
        lie far apart are asked for separately, so a reorganization between
        two requests is not noticed here as it is in a range.
        """
        wanted = _heights(heights)
        limits = _limits(timeout, max_response_bytes)
        return self._iter_raw_block_payloads_by_heights(wanted, self._tag(tag), read_source, limits)

    def _iter_raw_block_payloads_by_heights(
        self, heights: list[int], tag: int, read_source: int, limits: _Limits
    ) -> Iterator[RawBlockPayload]:
        for stretch in _stretches(heights):
            source, files = self._page(tag, stretch[0], stretch[-1] + 1, read_source, limits)
            yield from self._download_page(tag, _at(files, stretch), source, limits)

    def _page(
        self, tag: int, start_height: int, end_height: int, read_source: int, limits: _Limits
    ) -> tuple[int, list[api_pb2.BlockFile]]:
        """Returns the block files of one request, and the source they are for."""

        def read(source: int) -> tuple[int, list[api_pb2.BlockFile]]:
            files = self._block_files(tag, start_height, end_height, source, limits.deadline)
            return source, files

        return self._with_fallback(read, read_source)

    def _download_page(
        self, tag: int, files: list[api_pb2.BlockFile], source: int, limits: _Limits
    ) -> Iterator[RawBlockPayload]:
        """Downloads block files of one request, in order.

        A failure does not start over: the blocks not yet yielded are asked
        for again, with fresh links when a link has expired, and from
        single-block objects when the wanted source fails.
        """
        done = 0
        refusals = 0
        while done < len(files):
            progress = done
            try:
                downloads = self._downloader.iter_download(files[done:], **limits.for_download())
                for payload in downloads:
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
                        self._before_fresh_links(exc, refusals, limits, "asking for fresh ones")
            except ChainStorageError as exc:
                source = self._fallback_source(source, exc)
            else:
                return
            if done < len(files):
                files[done:] = self._same_blocks(files[done:], tag, source, limits)

    def _before_fresh_links(
        self, exc: DownloadError, refusals: int, limits: _Limits, asking: str
    ) -> None:
        """Logs a refused link and, for one that never worked, waits before
        new links are asked for. ``refusals`` counts the refusals in a row."""
        logger.info("a block file link was refused (%s); %s", _refusal(exc), asking)
        if exc.status_code != _DEAD_LINK_STATUS:
            return
        wait = self._config.dead_link_wait * refusals
        left = time_left(limits.deadline)
        if left is not None and wait >= left:
            raise CallTimeoutError(
                "a block read did not finish in time; its link was refused "
                f"({_refusal(exc)}) and the wait for a new one would not end in time",
                code="DEADLINE_EXCEEDED",
            ) from exc
        if wait > 0:
            self._sleep(wait)

    def _fallback_source(self, source: int, exc: ChainStorageError) -> int:
        """Returns the source to continue with after ``exc``, or raises it."""
        if source not in _SOURCES_WITH_FALLBACK or _is_callers_limit(exc):
            raise exc
        logger.warning("block read failed (%s); continuing from single-block objects", exc)
        return api_pb2.BLOCK_READ_SOURCE_SINGLE_BLOCK

    def _same_blocks(
        self, files: Sequence[api_pb2.BlockFile], tag: int, source: int, limits: _Limits
    ) -> list[api_pb2.BlockFile]:
        """Asks again for the block files of ``files`` and checks that they
        still describe the same blocks."""
        stretch = [block_file.height for block_file in files]
        fresh = _at(
            self._block_files(tag, stretch[0], stretch[-1] + 1, source, limits.deadline), stretch
        )
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
        self,
        transaction_hash: str,
        *,
        tag: int | None = None,
        timeout: float | None = None,
        max_response_bytes: int | None = None,
    ) -> list[blockchain_pb2.Block]:
        """Returns the blocks that hold the transaction, or an empty list when
        the server does not know it."""
        limits = _limits(timeout, max_response_bytes)
        request = api_pb2.GetBlockByTransactionRequest(
            tag=self._tag(tag), transaction_hash=transaction_hash
        )
        try:
            response = self._call(
                "GetBlockByTransaction", request, self._short_timeout, limits.deadline
            )
        except NotFoundError:
            return []
        return [
            self._get_block(block.height, block.hash, block.tag, limits)
            for block in response.blocks
        ]

    def get_block_by_timestamp(
        self,
        timestamp: int,
        *,
        tag: int | None = None,
        timeout: float | None = None,
        max_response_bytes: int | None = None,
    ) -> blockchain_pb2.Block:
        """Returns the block the server finds for ``timestamp`` (Unix seconds)."""
        limits = _limits(timeout, max_response_bytes)
        request = api_pb2.GetBlockByTimestampRequest(tag=self._tag(tag), timestamp=timestamp)
        found = self._call("GetBlockByTimestamp", request, self._short_timeout, limits.deadline)
        return self._get_block(found.height, found.hash, found.tag, limits)

    def _get_block(self, height: int, hash: str, tag: int, limits: _Limits) -> blockchain_pb2.Block:
        """Returns one block from the default source, within ``limits``."""
        raw = self._open_raw_block_payload(
            height, hash, tag, api_pb2.BLOCK_READ_SOURCE_DEFAULT, limits
        )
        return self.parse_raw_block(raw)

    # -- chain events -----------------------------------------------------

    def get_chain_events(
        self, request: api_pb2.GetChainEventsRequest, *, timeout: float | None = None
    ) -> list[api_pb2.BlockchainEvent]:
        """Returns up to ``request.max_num_events`` events after a position."""
        attempt_timeout = (
            self._medium_timeout if request.max_num_events > 10 else self._short_timeout
        )
        response = self._call("GetChainEvents", request, attempt_timeout, _deadline(timeout))
        return list(response.events)

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

    def get_chain_metadata(
        self, *, timeout: float | None = None
    ) -> api_pb2.GetChainMetadataResponse:
        """Returns what the server reports about its chain and its tags."""
        request = api_pb2.GetChainMetadataRequest()
        return self._call("GetChainMetadata", request, self._short_timeout, _deadline(timeout))

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

    def _call(
        self, method: str, request: Any, timeout: float, deadline: float | None = None
    ) -> Any:
        """Sends one call. ``timeout`` limits each attempt; ``deadline``, a
        ``time.monotonic()`` value, limits the call as a whole."""
        if deadline is None:
            return self._transport.call(method, request, timeout=timeout)
        left = deadline - time.monotonic()
        if left <= 0:
            raise CallTimeoutError(f"{method} did not finish in time", code="DEADLINE_EXCEEDED")
        if self._transport_takes_deadline:
            return self._transport.call(method, request, timeout=timeout, deadline=deadline)
        # A transport written before deadlines existed: the time left limits
        # each of its attempts, and its retries are its own.
        return self._transport.call(method, request, timeout=min(timeout, left))

    def _block_files(
        self,
        tag: int,
        start_height: int,
        end_height: int,
        read_source: int,
        deadline: float | None = None,
    ) -> list[api_pb2.BlockFile]:
        count = end_height - start_height
        request = api_pb2.GetBlockFilesByRangeRequest(
            tag=tag, start_height=start_height, end_height=end_height, read_source=read_source
        )
        timeout = self._long_timeout if count > 2 else self._medium_timeout
        files = list(self._call("GetBlockFilesByRange", request, timeout, deadline).files)
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
            if read_source not in _SOURCES_WITH_FALLBACK or _is_callers_limit(exc):
                raise
            logger.warning("block read failed (%s); retrying from single-block objects", exc)
            try:
                return read(api_pb2.BLOCK_READ_SOURCE_SINGLE_BLOCK)
            except ChainStorageError as fallback_exc:
                raise fallback_exc from exc


def _range(start_height: int, end_height: int) -> tuple[int, int]:
    """Returns the range a caller asked for; ``end_height == 0`` is one block."""
    if end_height == 0:
        end_height = start_height + 1
    if end_height <= start_height:
        raise ConfigError(f"invalid range: start_height={start_height} end_height={end_height}")
    return start_height, end_height


def _heights(heights: Iterable[int]) -> list[int]:
    """Returns the heights a caller asked for, each once, in order."""
    wanted = set()
    for height in heights:
        if isinstance(height, bool) or not isinstance(height, int) or height < 0:
            raise ConfigError(f"a height must be a whole number, 0 or more, got {height!r}")
        wanted.add(height)
    return sorted(wanted)


def _stretches(heights: Sequence[int]) -> Iterator[list[int]]:
    """Splits sorted heights into the stretches the server is asked for: each
    spans no more heights than one request may."""
    stretch: list[int] = []
    for height in heights:
        if stretch and height - stretch[0] >= MAX_BLOCK_FILES_PER_REQUEST:
            yield stretch
            stretch = []
        stretch.append(height)
    if stretch:
        yield stretch


def _at(files: Sequence[api_pb2.BlockFile], heights: Sequence[int]) -> list[api_pb2.BlockFile]:
    """Returns the block files at ``heights`` out of those of one request."""
    if len(files) == len(heights):
        return list(files)
    wanted = set(heights)
    return [block_file for block_file in files if block_file.height in wanted]


def _limits(timeout: float | None, max_response_bytes: int | None) -> _Limits:
    """Turns what a caller allows a read into the limits it runs under."""
    if max_response_bytes is not None and (
        isinstance(max_response_bytes, bool)
        or not isinstance(max_response_bytes, int)
        or max_response_bytes < 1
    ):
        raise ConfigError(
            f"max_response_bytes must be a positive number of bytes, got {max_response_bytes!r}"
        )
    return _Limits(_deadline(timeout), max_response_bytes)


def _is_callers_limit(exc: ChainStorageError) -> bool:
    """Whether a read stopped at a limit the caller set. Another source would
    meet the same limit, so nothing more is tried."""
    return isinstance(exc, (CallTimeoutError, ResponseTooLargeError))


def _deadline(timeout: float | None) -> float | None:
    """Turns a caller's ``timeout`` into the moment the call must be done by."""
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or math.isnan(timeout):
        raise ConfigError(f"timeout must be a number of seconds, got {timeout!r}")
    return time.monotonic() + timeout


def _takes_deadline(transport: Transport) -> bool:
    """Whether a transport's ``call`` accepts ``deadline``. One written
    before deadlines existed does not."""
    try:
        parameters = inspect.signature(transport.call).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == "deadline" or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _check_connects(
    previous: api_pb2.BlockFile | None, files: Sequence[api_pb2.BlockFile]
) -> api_pb2.BlockFile | None:
    """Checks that one request's block files follow ``previous``, the last
    stored block of the request before it, and returns the block the next
    request has to follow.

    The server checks the chain within one request. Between two requests the
    chain can change.
    """
    stored = [block_file for block_file in files if not block_file.skipped]
    if not stored:
        return previous
    if previous is not None and stored[0].parent_hash and stored[0].parent_hash != previous.hash:
        raise RequestError(
            f"the chain changed while reading: block {stored[0].height} does "
            f"not follow block {previous.height}; read the range again"
        )
    return stored[-1]


def _is_expired_link(exc: DownloadError) -> bool:
    return exc.status_code in _EXPIRED_LINK_STATUSES


def _refusal(exc: DownloadError) -> str:
    """The status and the store's error code, for a log line."""
    return f"{exc.status_code} {exc.error_code}".rstrip()
