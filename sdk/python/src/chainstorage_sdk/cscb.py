"""Reader for consolidated block files (the CSCB format).

A consolidated file holds many blocks. Its layout, little-endian throughout:

    header    96 bytes   magic "CSCB", version, codec, counts, height range,
                         where the envelope and the payload are
    envelope             magic "ENV1", then one 96-byte record per block and
                         one 72-byte record per chunk
    payload              the chunks, each compressed on its own

A block is read by fetching the header and envelope, finding the block's
record by height, fetching the one chunk that holds it and cutting the block
out of the decompressed chunk. Every step is checked: the envelope and each
chunk and block carry a CRC32, and the block record carries the SHA-256 of
the block hash.

The format is defined by the Go code in
``internal/storage/blobstorage/cscb``; this module must follow it.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import struct
import zlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import zstandard

from chainstorage_sdk.errors import BlockFormatError
from coinbase.chainstorage import api_pb2

HEADER_SIZE = 96
ENVELOPE_HEADER_SIZE = 80
BLOCK_RECORD_SIZE = 96
CHUNK_RECORD_SIZE = 72

# A first read of this many bytes usually covers the header and the envelope.
INITIAL_INDEX_READ_SIZE = 64 * 1024

_FORMAT_VERSION = 1
_SCOPE_CHUNK = 2
_CODECS = {1: api_pb2.GZIP, 2: api_pb2.ZSTD}

# No chunk the server writes is larger than this. A file that declares a
# larger one is refused before any memory is set aside for it.
MAX_CHUNK_BYTES = 2**30

# The largest window a zstd frame may declare. libzstd refuses larger frames
# by default at 128 MiB; the server's encoder may use up to 512 MiB. The Go
# reader uses the same limit.
ZSTD_MAX_WINDOW_SIZE = 2**29

# Pieces in which output of unknown length is produced under a limit. A zstd
# block holds at most 128 KiB and takes at least 3 bytes, so one 1 KiB slice of
# input can produce at most ~43 MiB beyond the limit before it is refused.
_READ_PIECE = 1024 * 1024
_ZSTD_INPUT_SLICE = 1024


@dataclass(frozen=True)
class Header:
    codec: int
    block_count: int
    chunk_count: int
    start_height: int
    end_height: int
    envelope_offset: int
    envelope_length: int
    envelope_crc32: int
    payload_offset: int
    payload_compressed_length: int
    payload_uncompressed_length: int
    payload_crc32: int

    @property
    def index_length(self) -> int:
        """Bytes from the start of the file that hold the header and envelope."""
        return self.envelope_offset + self.envelope_length


@dataclass(frozen=True)
class BlockRecord:
    height: int
    logical_payload_offset: int
    payload_length: int
    payload_crc32: int
    chunk_index: int
    chunk_relative_offset: int
    hash_sha256: bytes
    metadata_id: int


@dataclass(frozen=True)
class ChunkRecord:
    index: int
    start_height: int
    end_height: int
    compressed_payload_offset: int
    compressed_length: int
    uncompressed_offset: int
    uncompressed_length: int
    crc32: int
    block_count: int


@dataclass(frozen=True)
class Index:
    header: Header
    blocks: tuple[BlockRecord, ...]
    chunks: tuple[ChunkRecord, ...]
    blocks_by_height: Mapping[int, int]


def parse_header(data: bytes) -> Header:
    """Parses and checks the fixed-size header at the start of a file."""
    if len(data) < HEADER_SIZE:
        raise BlockFormatError(f"CSCB header requires {HEADER_SIZE} bytes, got {len(data)}")
    if data[0:4] != b"CSCB":
        raise BlockFormatError("invalid CSCB magic")
    if data[4] != _FORMAT_VERSION:
        raise BlockFormatError(f"unsupported CSCB format version: {data[4]}")
    codec = _CODECS.get(data[5])
    if codec is None:
        raise BlockFormatError(f"unsupported CSCB codec id: {data[5]}")
    if data[6] != _SCOPE_CHUNK:
        raise BlockFormatError(f"unsupported CSCB compression scope: {data[6]}")

    (block_count, chunk_count, start_height, end_height) = struct.unpack_from("<IIQQ", data, 8)
    (envelope_offset, envelope_length, envelope_crc32) = struct.unpack_from("<QQI", data, 32)
    (block_record_size, chunk_record_size) = struct.unpack_from("<II", data, 52)
    (payload_offset, compressed_length, uncompressed_length, payload_crc32) = struct.unpack_from(
        "<QQQI", data, 60
    )

    if block_count == 0:
        raise BlockFormatError("CSCB header has zero blocks")
    if chunk_count == 0:
        raise BlockFormatError("CSCB header has zero chunks")
    if envelope_offset != HEADER_SIZE:
        raise BlockFormatError(
            f"unexpected CSCB envelope offset: got {envelope_offset} want {HEADER_SIZE}"
        )
    if block_record_size != BLOCK_RECORD_SIZE:
        raise BlockFormatError(
            f"unexpected CSCB block record size: got {block_record_size} want {BLOCK_RECORD_SIZE}"
        )
    if chunk_record_size != CHUNK_RECORD_SIZE:
        raise BlockFormatError(
            f"unexpected CSCB chunk record size: got {chunk_record_size} want {CHUNK_RECORD_SIZE}"
        )
    if payload_offset != envelope_offset + envelope_length:
        raise BlockFormatError(
            f"unexpected CSCB payload offset: got {payload_offset} "
            f"want {envelope_offset + envelope_length}"
        )
    return Header(
        codec=codec,
        block_count=block_count,
        chunk_count=chunk_count,
        start_height=start_height,
        end_height=end_height,
        envelope_offset=envelope_offset,
        envelope_length=envelope_length,
        envelope_crc32=envelope_crc32,
        payload_offset=payload_offset,
        payload_compressed_length=compressed_length,
        payload_uncompressed_length=uncompressed_length,
        payload_crc32=payload_crc32,
    )


def parse_index(data: bytes) -> Index:
    """Parses the header and envelope. ``data`` must hold at least
    ``header.index_length`` bytes from the start of the file."""
    header = parse_header(data)
    if header.index_length > len(data):
        raise BlockFormatError(f"CSCB index requires {header.index_length} bytes, got {len(data)}")

    envelope = data[header.envelope_offset : header.index_length]
    if _crc32(envelope) != header.envelope_crc32:
        raise BlockFormatError("CSCB envelope CRC mismatch")
    if len(envelope) < ENVELOPE_HEADER_SIZE:
        raise BlockFormatError(
            f"CSCB envelope requires {ENVELOPE_HEADER_SIZE} bytes, got {len(envelope)}"
        )
    if envelope[0:4] != b"ENV1":
        raise BlockFormatError("invalid CSCB envelope magic")

    (block_count, chunk_count, start_height, end_height) = struct.unpack_from("<QQQQ", envelope, 8)
    for name, in_header, in_envelope in (
        ("block count", header.block_count, block_count),
        ("chunk count", header.chunk_count, chunk_count),
        ("start height", header.start_height, start_height),
        ("end height", header.end_height, end_height),
    ):
        if in_header != in_envelope:
            raise BlockFormatError(
                f"CSCB {name} mismatch: header={in_header} envelope={in_envelope}"
            )

    (block_index_offset, block_index_length, chunk_index_offset, chunk_index_length) = (
        struct.unpack_from("<QQQQ", envelope, 40)
    )
    if block_index_offset != ENVELOPE_HEADER_SIZE:
        raise BlockFormatError(
            f"unexpected CSCB block index offset: got {block_index_offset} "
            f"want {ENVELOPE_HEADER_SIZE}"
        )
    if block_index_length != block_count * BLOCK_RECORD_SIZE:
        raise BlockFormatError("unexpected CSCB block index length")
    if chunk_index_offset != block_index_offset + block_index_length:
        raise BlockFormatError("unexpected CSCB chunk index offset")
    if chunk_index_length != chunk_count * CHUNK_RECORD_SIZE:
        raise BlockFormatError("unexpected CSCB chunk index length")
    if chunk_index_offset + chunk_index_length != header.envelope_length:
        raise BlockFormatError("unexpected CSCB envelope length")

    blocks = tuple(
        _parse_block_record(envelope, block_index_offset + i * BLOCK_RECORD_SIZE)
        for i in range(block_count)
    )
    chunks = tuple(
        _parse_chunk_record(envelope, chunk_index_offset + i * CHUNK_RECORD_SIZE, i)
        for i in range(chunk_count)
    )
    blocks_by_height: dict[int, int] = {}
    for position, block in enumerate(blocks):
        if block.height in blocks_by_height:
            raise BlockFormatError(f"duplicate CSCB block height: {block.height}")
        blocks_by_height[block.height] = position
    return Index(header=header, blocks=blocks, chunks=chunks, blocks_by_height=blocks_by_height)


def lookup_block(index: Index, block_file: api_pb2.BlockFile) -> tuple[BlockRecord, ChunkRecord]:
    """Finds the record of the block that ``block_file`` points at, and checks
    that the file agrees with what the server said about the block."""
    height = block_file.height
    position = index.blocks_by_height.get(height)
    if position is None:
        raise BlockFormatError(f"CSCB block height {height} not found")

    block = index.blocks[position]
    if block.logical_payload_offset != block_file.byte_offset:
        raise BlockFormatError(
            f"CSCB block offset mismatch at height {height}: "
            f"index={block.logical_payload_offset} metadata={block_file.byte_offset}"
        )
    if block.payload_length != block_file.byte_length:
        raise BlockFormatError(
            f"CSCB block length mismatch at height {height}: "
            f"index={block.payload_length} metadata={block_file.byte_length}"
        )
    uncompressed_length = block_file.uncompressed_length
    if uncompressed_length != 0 and uncompressed_length != block.payload_length:
        raise BlockFormatError(
            f"CSCB block uncompressed length mismatch at height {height}: "
            f"index={block.payload_length} metadata={uncompressed_length}"
        )
    if hashlib.sha256(block_file.hash.encode()).digest() != block.hash_sha256:
        raise BlockFormatError(f"CSCB block hash mismatch at height {height}")
    if block.chunk_index >= len(index.chunks):
        raise BlockFormatError(
            f"CSCB chunk index {block.chunk_index} out of range for height {height}"
        )
    chunk = index.chunks[block.chunk_index]
    if not chunk.start_height <= height < chunk.end_height:
        raise BlockFormatError(
            f"CSCB block height {height} outside chunk range "
            f"[{chunk.start_height}, {chunk.end_height})"
        )
    return block, chunk


def decode_chunk(frame: bytes, codec: int, chunk: ChunkRecord) -> bytes | bytearray:
    """Decompresses one chunk and checks its length and CRC.

    No more than the chunk's declared length is ever decompressed, so a
    damaged or hostile frame cannot exhaust memory.
    """
    if chunk.uncompressed_length > MAX_CHUNK_BYTES:
        raise BlockFormatError(
            f"CSCB chunk declares {chunk.uncompressed_length} bytes, "
            f"more than the {MAX_CHUNK_BYTES} allowed"
        )
    payload = decompress(frame, codec, max_size=chunk.uncompressed_length)
    if len(payload) != chunk.uncompressed_length:
        raise BlockFormatError(
            f"CSCB chunk length mismatch: got {len(payload)} want {chunk.uncompressed_length}"
        )
    if _crc32(payload) != chunk.crc32:
        raise BlockFormatError("CSCB chunk CRC mismatch")
    return payload


def extract_blocks(
    chunk_payload: bytes | bytearray, chunk: ChunkRecord, blocks: Sequence[BlockRecord]
) -> list[bytes]:
    """Cuts the given blocks out of a decompressed chunk and checks each CRC."""
    view = memoryview(chunk_payload)
    payloads = []
    for block in blocks:
        if block.chunk_index != chunk.index:
            raise BlockFormatError(
                f"CSCB block chunk mismatch at height {block.height}: "
                f"block={block.chunk_index} chunk={chunk.index}"
            )
        end = block.chunk_relative_offset + block.payload_length
        if end > chunk.uncompressed_length:
            raise BlockFormatError(
                f"CSCB block payload exceeds chunk bounds: "
                f"end={end} chunk_length={chunk.uncompressed_length}"
            )
        payload = bytes(view[block.chunk_relative_offset : end])
        if _crc32(payload) != block.payload_crc32:
            raise BlockFormatError(f"CSCB block CRC mismatch at height {block.height}")
        payloads.append(payload)
    return payloads


def read_block(data: bytes, block_file: api_pb2.BlockFile) -> bytes:
    """Reads one block out of a whole consolidated file held in memory."""
    index = parse_index(data)
    block, chunk = lookup_block(index, block_file)
    start = chunk.compressed_payload_offset
    frame = data[start : start + chunk.compressed_length]
    return extract_blocks(decode_chunk(frame, index.header.codec, chunk), chunk, [block])[0]


def decompress(
    data: bytes,
    compression: int,
    *,
    max_size: int | None = None,
    limit: int | None = None,
) -> bytes | bytearray:
    """Decompresses ``data`` according to an ``api_pb2.Compression`` value.

    With ``max_size``, output that would be longer is refused, and the result
    is written straight into one buffer of that size. Use it when the length
    is known. When it is not, ``limit`` refuses output longer than ``limit``
    without setting any memory aside for it: the output grows as it is made.

    Zstd data must be exactly one frame. Bytes after it are refused, since
    the first frame would otherwise be returned as if it were the whole
    block. One case may not be caught here: with ``max_size``, a frame that
    does not declare its size and holds fewer than ``max_size`` bytes. The
    result is then shorter than ``max_size``, which the caller must check, as
    the callers in this package do.
    """
    if max_size is not None and limit is not None:
        raise ValueError("pass max_size or limit, not both")
    if compression in (api_pb2.GZIP, api_pb2.ZSTD) and not data:
        # Neither library complains about empty input; it is never a block.
        raise BlockFormatError("failed to decompress block data: it is empty")
    try:
        if compression == api_pb2.NONE:
            payload: bytes | bytearray = data
            bound = max_size if max_size is not None else limit
            if bound is not None and len(payload) > bound:
                raise _TooLong
        elif compression == api_pb2.GZIP:
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as reader:
                if limit is not None:
                    payload = _read_limited(reader, limit)
                else:
                    payload = _read(reader, max_size)
        elif compression == api_pb2.ZSTD:
            decompressor = zstandard.ZstdDecompressor(max_window_size=ZSTD_MAX_WINDOW_SIZE)
            if limit is not None:
                declared = zstandard.frame_content_size(data)
                if declared > limit:
                    raise _TooLong
                if declared > 0:
                    # A frame that declares its size is read in one pass.
                    max_size, limit = declared, None
            if limit is not None:
                payload = _decompress_zstd_limited(decompressor, data, limit)
            elif max_size is None:
                stream = decompressor.decompressobj()
                payload = stream.decompress(data)
                if not stream.eof:
                    # A stream that was cut off decompresses without an
                    # error, to whatever it held up to the cut.
                    raise BlockFormatError("failed to decompress block data: it is cut off")
                if stream.unused_data:
                    raise BlockFormatError(_DATA_AFTER_FRAME)
            else:
                # In one pass the output buffer doubles as the window, which a
                # streaming read would allocate on top (up to hundreds of MB).
                # A frame that declares its size is trusted over the buffer
                # size by the library, so the declared size is checked first.
                declared = zstandard.frame_content_size(data)
                if declared > max_size:
                    raise _TooLong
                if declared == 0:
                    # The library answers an empty frame without looking at
                    # what follows it, so this one is read as a stream.
                    payload = _decompress_zstd_limited(decompressor, data, max_size)
                else:
                    payload = decompressor.decompress(
                        data, max_output_size=max(max_size, 1), allow_extra_data=False
                    )
                    if len(payload) > max_size:
                        raise _TooLong
        else:
            raise BlockFormatError(f"unsupported compression: {compression}")
    except _TooLong:
        if limit is not None:
            message = f"decompressed data is longer than the {limit} bytes allowed"
        else:
            message = f"decompressed data is longer than the declared {max_size} bytes"
        raise BlockFormatError(message) from None
    except (OSError, EOFError, zlib.error, zstandard.ZstdError) as exc:
        raise BlockFormatError(f"failed to decompress block data: {exc}") from exc
    return payload


class _TooLong(Exception):
    pass


_DATA_AFTER_FRAME = "failed to decompress block data: more data follows its zstd frame"


def _read(reader: io.BufferedIOBase, max_size: int | None) -> bytes | bytearray:
    """Reads everything a gzip stream has.

    With ``max_size``, the output goes into one buffer of that size, so a
    large chunk is held once, and ``_TooLong`` is raised if there is more.
    """
    if max_size is None:
        return reader.read()
    buffer = bytearray(max_size)
    view = memoryview(buffer)
    filled = 0
    while filled < max_size:
        count = reader.readinto(view[filled:])
        if not count:
            # Shorter than declared: the caller reports the length mismatch.
            return bytes(view[:filled])
        filled += count
    if reader.read(1):
        raise _TooLong
    return buffer


def _read_limited(reader: io.BufferedIOBase, limit: int) -> bytearray:
    """Reads a gzip stream in pieces; ``_TooLong`` past ``limit`` bytes."""
    payload = bytearray()
    while piece := reader.read(_READ_PIECE):
        payload += piece
        if len(payload) > limit:
            raise _TooLong
    return payload


def _decompress_zstd_limited(
    decompressor: zstandard.ZstdDecompressor, data: bytes, limit: int
) -> bytearray:
    """Decompresses a zstd frame of unknown size; ``_TooLong`` past ``limit``.

    The input goes in in small slices, so the output is checked as it grows.
    """
    stream = decompressor.decompressobj()
    view = memoryview(data)
    payload = bytearray()
    given = 0
    for start in range(0, len(view), _ZSTD_INPUT_SLICE):
        payload += stream.decompress(view[start : start + _ZSTD_INPUT_SLICE])
        given = start + _ZSTD_INPUT_SLICE
        if len(payload) > limit:
            raise _TooLong
        if stream.eof:
            break
    if not stream.eof:
        # A stream that was cut off decompresses without an error, to
        # whatever it held up to the cut.
        raise BlockFormatError("failed to decompress block data: it is cut off")
    if stream.unused_data or given < len(view):
        raise BlockFormatError(_DATA_AFTER_FRAME)
    return payload


def _parse_block_record(envelope: bytes, offset: int) -> BlockRecord:
    (height, logical_offset, length, crc, chunk_index, relative_offset) = struct.unpack_from(
        "<QQQIIQ", envelope, offset
    )
    return BlockRecord(
        height=height,
        logical_payload_offset=logical_offset,
        payload_length=length,
        payload_crc32=crc,
        chunk_index=chunk_index,
        chunk_relative_offset=relative_offset,
        hash_sha256=bytes(envelope[offset + 48 : offset + 80]),
        metadata_id=struct.unpack_from("<Q", envelope, offset + 80)[0],
    )


def _parse_chunk_record(envelope: bytes, offset: int, expected_index: int) -> ChunkRecord:
    index = struct.unpack_from("<I", envelope, offset)[0]
    if index != expected_index:
        raise BlockFormatError(f"unexpected CSCB chunk index: got {index} want {expected_index}")
    (
        start_height,
        end_height,
        compressed_offset,
        compressed_length,
        uncompressed_offset,
        uncompressed_length,
        crc,
        block_count,
    ) = struct.unpack_from("<QQQQQQII", envelope, offset + 8)
    return ChunkRecord(
        index=index,
        start_height=start_height,
        end_height=end_height,
        compressed_payload_offset=compressed_offset,
        compressed_length=compressed_length,
        uncompressed_offset=uncompressed_offset,
        uncompressed_length=uncompressed_length,
        crc32=crc,
        block_count=block_count,
    )


def _crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF
