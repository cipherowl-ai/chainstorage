"""The consolidated-file reader, against files written by the Go encoder.

The files under ``testdata`` are owned by the Go test
``TestPythonSDKFixtures``, which also reads them with the Go reader.
"""

from __future__ import annotations

import dataclasses
import gzip
import hashlib
import json
import random
import struct
import zlib

import pytest
import zstandard
from conftest import TESTDATA

from chainstorage_sdk import BlockFormatError, cscb
from coinbase.chainstorage import api_pb2, blockchain_pb2

CODECS = ["gzip", "zstd"]


def load(codec: str) -> tuple[bytes, list[dict]]:
    manifest = json.loads((TESTDATA / f"cscb_{codec}.json").read_text())
    return (TESTDATA / manifest["file"]).read_bytes(), manifest["blocks"]


def block_file(entry: dict, **changes: object) -> api_pb2.BlockFile:
    fields = {
        "tag": entry["tag"],
        "hash": entry["hash"],
        "height": entry["height"],
        "byte_offset": entry["byte_offset"],
        "byte_length": entry["byte_length"],
        "uncompressed_length": entry["uncompressed_length"],
        "object_format": blockchain_pb2.BLOCK_OBJECT_FORMAT_CSCB_BATCH,
    }
    fields.update(changes)
    return api_pb2.BlockFile(**fields)


def with_envelope_crc(data: bytes) -> bytes:
    """Recomputes the envelope CRC, so that a test can change the envelope
    and reach the checks behind the CRC."""
    envelope_length = struct.unpack_from("<Q", data, 40)[0]
    crc = zlib.crc32(data[96 : 96 + envelope_length]) & 0xFFFFFFFF
    return data[:48] + struct.pack("<I", crc) + data[52:]


@pytest.mark.parametrize("codec", CODECS)
def test_index_describes_the_file(codec):
    data, blocks = load(codec)
    index = cscb.parse_index(data)

    assert index.header.codec == api_pb2.Compression.Value(codec.upper())
    assert index.header.block_count == len(blocks) == 6
    assert index.header.chunk_count == 2
    assert (index.header.start_height, index.header.end_height) == (1000, 1006)
    assert [block.height for block in index.blocks] == [entry["height"] for entry in blocks]
    assert [chunk.block_count for chunk in index.chunks] == [3, 3]
    assert index.header.index_length == index.header.payload_offset
    assert len(data) == index.header.payload_offset + index.header.payload_compressed_length


@pytest.mark.parametrize("codec", CODECS)
def test_every_block_reads_back(codec):
    data, blocks = load(codec)
    for entry in blocks:
        payload = cscb.read_block(data, block_file(entry))
        assert hashlib.sha256(payload).hexdigest() == entry["payload_sha256"]
        block = blockchain_pb2.Block.FromString(payload)
        assert block.metadata.height == entry["height"]
        assert block.metadata.hash == entry["hash"]


@pytest.mark.parametrize("codec", CODECS)
def test_blocks_of_one_chunk_are_cut_from_one_decompression(codec):
    data, blocks = load(codec)
    index = cscb.parse_index(data)
    chunk = index.chunks[1]
    frame = data[
        chunk.compressed_payload_offset : chunk.compressed_payload_offset + chunk.compressed_length
    ]
    chunk_payload = cscb.decode_chunk(frame, index.header.codec, chunk)
    records = [cscb.lookup_block(index, block_file(entry))[0] for entry in blocks[3:]]

    payloads = cscb.extract_blocks(chunk_payload, chunk, records)

    assert [hashlib.sha256(payload).hexdigest() for payload in payloads] == [
        entry["payload_sha256"] for entry in blocks[3:]
    ]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda data: b"XXXX" + data[4:], "invalid CSCB magic"),
        (lambda data: data[:4] + b"\x02" + data[5:], "unsupported CSCB format version"),
        (lambda data: data[:5] + b"\x09" + data[6:], "unsupported CSCB codec id"),
        (lambda data: data[:6] + b"\x01" + data[7:], "unsupported CSCB compression scope"),
        (lambda data: data[:50], "CSCB header requires 96 bytes"),
        (lambda data: data[:200], "CSCB index requires"),
        (lambda data: data[:100] + b"\xff" + data[101:], "CSCB envelope CRC mismatch"),
        (
            lambda data: with_envelope_crc(data[:96] + b"ENV2" + data[100:]),
            "invalid CSCB envelope magic",
        ),
    ],
)
def test_a_damaged_index_is_refused(change, message):
    data, _ = load("zstd")
    with pytest.raises(BlockFormatError, match=message):
        cscb.parse_index(change(data))


def patched(data: bytes, offset: int, fmt: str, value: int) -> bytes:
    """Overwrites one field of the envelope and repairs the envelope CRC, so
    that the check behind the CRC is the one that fires."""
    changed = bytearray(data)
    struct.pack_into(fmt, changed, offset, value)
    return with_envelope_crc(bytes(changed))


ENVELOPE = 96
BLOCK_RECORDS = ENVELOPE + 80
CHUNK_RECORDS = BLOCK_RECORDS + 6 * 96


@pytest.mark.parametrize(
    ("offset", "fmt", "value", "message"),
    [
        (ENVELOPE + 8, "<Q", 7, "CSCB block count mismatch: header=6 envelope=7"),
        (ENVELOPE + 16, "<Q", 3, "CSCB chunk count mismatch: header=2 envelope=3"),
        (ENVELOPE + 24, "<Q", 999, "CSCB start height mismatch"),
        (ENVELOPE + 32, "<Q", 2000, "CSCB end height mismatch"),
        (ENVELOPE + 40, "<Q", 81, "unexpected CSCB block index offset"),
        (ENVELOPE + 48, "<Q", 95, "unexpected CSCB block index length"),
        (ENVELOPE + 56, "<Q", 1, "unexpected CSCB chunk index offset"),
        (ENVELOPE + 64, "<Q", 71, "unexpected CSCB chunk index length"),
        (BLOCK_RECORDS + 96, "<Q", 1000, "duplicate CSCB block height: 1000"),
        (CHUNK_RECORDS + 72, "<I", 5, "unexpected CSCB chunk index: got 5 want 1"),
    ],
)
def test_an_envelope_that_contradicts_itself_is_refused(offset, fmt, value, message):
    data, _ = load("zstd")
    with pytest.raises(BlockFormatError, match=message):
        cscb.parse_index(patched(data, offset, fmt, value))


def test_a_block_outside_its_chunk_is_refused():
    data, blocks = load("zstd")
    # Shrink the first chunk's height range to [1000, 1001).
    index = cscb.parse_index(patched(data, CHUNK_RECORDS + 16, "<Q", 1001))
    with pytest.raises(BlockFormatError, match="outside chunk range"):
        cscb.lookup_block(index, block_file(blocks[2]))


def test_a_block_that_points_at_a_missing_chunk_is_refused():
    data, blocks = load("zstd")
    index = cscb.parse_index(patched(data, BLOCK_RECORDS + 28, "<I", 9))
    with pytest.raises(BlockFormatError, match="chunk index 9 out of range"):
        cscb.lookup_block(index, block_file(blocks[0]))


def test_a_block_that_runs_past_its_chunk_is_refused():
    data, blocks = load("gzip")
    index = cscb.parse_index(data)
    chunk = index.chunks[0]
    record, _ = cscb.lookup_block(index, block_file(blocks[2]))
    content = b"x" * chunk.uncompressed_length

    too_long = dataclasses.replace(record, payload_length=chunk.uncompressed_length + 1)
    with pytest.raises(BlockFormatError, match="exceeds chunk bounds"):
        cscb.extract_blocks(content, chunk, [too_long])
    with pytest.raises(BlockFormatError, match="block chunk mismatch"):
        cscb.extract_blocks(content, index.chunks[1], [record])


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"height": 2000}, "block height 2000 not found"),
        ({"byte_offset": 1}, "block offset mismatch"),
        ({"byte_length": 1}, "block length mismatch"),
        ({"uncompressed_length": 1}, "uncompressed length mismatch"),
        ({"hash": "another-hash"}, "block hash mismatch"),
    ],
)
def test_a_block_file_that_disagrees_with_the_index_is_refused(changes, message):
    data, blocks = load("zstd")
    index = cscb.parse_index(data)
    with pytest.raises(BlockFormatError, match=message):
        cscb.lookup_block(index, block_file(blocks[1], **changes))


def test_an_unset_uncompressed_length_is_accepted():
    data, blocks = load("zstd")
    index = cscb.parse_index(data)
    block, _ = cscb.lookup_block(index, block_file(blocks[1], uncompressed_length=0))
    assert block.height == blocks[1]["height"]


@pytest.mark.parametrize("codec", CODECS)
def test_a_damaged_chunk_is_refused(codec):
    data, blocks = load(codec)
    index = cscb.parse_index(data)
    # Flip a byte in the middle of the first chunk's compressed data.
    position = index.chunks[0].compressed_payload_offset + index.chunks[0].compressed_length // 2
    damaged = data[:position] + bytes([data[position] ^ 0xFF]) + data[position + 1 :]

    with pytest.raises(BlockFormatError):
        cscb.read_block(damaged, block_file(blocks[0]))
    # The second chunk is untouched and still reads.
    assert cscb.read_block(damaged, block_file(blocks[4]))


def test_a_chunk_with_the_wrong_content_fails_its_checks():
    data, _ = load("gzip")
    index = cscb.parse_index(data)
    chunk = index.chunks[0]

    same_length = gzip.compress(b"x" * chunk.uncompressed_length)
    with pytest.raises(BlockFormatError, match="CSCB chunk CRC mismatch"):
        cscb.decode_chunk(same_length, api_pb2.GZIP, chunk)
    with pytest.raises(BlockFormatError, match="CSCB chunk length mismatch"):
        cscb.decode_chunk(gzip.compress(b"short"), api_pb2.GZIP, chunk)


def test_a_block_that_fails_its_crc_is_refused():
    data, blocks = load("gzip")
    index = cscb.parse_index(data)
    chunk = index.chunks[0]
    record, _ = cscb.lookup_block(index, block_file(blocks[0]))

    with pytest.raises(BlockFormatError, match="CSCB block CRC mismatch at height 1000"):
        cscb.extract_blocks(b"x" * chunk.uncompressed_length, chunk, [record])


@pytest.mark.parametrize(
    ("compression", "compress"),
    [
        (api_pb2.NONE, lambda data: data),
        (api_pb2.GZIP, gzip.compress),
        (api_pb2.ZSTD, zstandard.ZstdCompressor().compress),
    ],
)
def test_decompress_handles_every_compression(compression, compress):
    assert cscb.decompress(compress(b"block" * 100), compression) == b"block" * 100


@pytest.mark.parametrize("compression", [api_pb2.GZIP, api_pb2.ZSTD])
def test_decompress_refuses_garbage(compression):
    with pytest.raises(BlockFormatError, match="failed to decompress"):
        cscb.decompress(b"not compressed data", compression)


def zstd_without_a_declared_size(data: bytes) -> bytes:
    return zstandard.ZstdCompressor(write_content_size=False).compress(data)


@pytest.mark.parametrize(
    "compress",
    [gzip.compress, zstandard.ZstdCompressor().compress, zstd_without_a_declared_size],
    ids=["gzip", "zstd", "zstd without a declared size"],
)
def test_decompress_stops_at_the_declared_size(compress):
    compression = api_pb2.GZIP if compress is gzip.compress else api_pb2.ZSTD
    data = compress(b"x" * 100_000)
    assert bytes(cscb.decompress(data, compression, max_size=100_000)) == b"x" * 100_000
    # One byte too many is refused, whatever the frame says about its size.
    with pytest.raises(BlockFormatError):
        cscb.decompress(data, compression, max_size=99_999)


def test_a_chunk_that_expands_past_its_declared_length_is_refused():
    data, _ = load("gzip")
    chunk = cscb.parse_index(data).chunks[0]
    bomb = gzip.compress(b"x" * (chunk.uncompressed_length + 1))
    with pytest.raises(BlockFormatError, match="longer than the declared"):
        cscb.decode_chunk(bomb, api_pb2.GZIP, chunk)


def test_a_chunk_that_declares_an_absurd_length_is_refused_before_it_is_read():
    data, _ = load("gzip")
    chunk = dataclasses.replace(
        cscb.parse_index(data).chunks[0], uncompressed_length=cscb.MAX_CHUNK_BYTES + 1
    )
    with pytest.raises(BlockFormatError, match="more than the 1073741824 allowed"):
        cscb.decode_chunk(b"", api_pb2.GZIP, chunk)


@pytest.mark.parametrize(
    "compress",
    [gzip.compress, zstandard.ZstdCompressor().compress, zstd_without_a_declared_size],
    ids=["gzip", "zstd", "zstd without a declared size"],
)
@pytest.mark.parametrize(
    "bound",
    [{}, {"max_size": 100_000}, {"limit": 100_000}],
    ids=["unbounded", "bounded", "limited"],
)
def test_cut_off_or_empty_data_is_never_a_block(compress, bound):
    compression = api_pb2.GZIP if compress is gzip.compress else api_pb2.ZSTD
    data = compress(bytes(range(256)) * 390)
    for damaged in (b"", data[:-1], data[: len(data) // 2]):
        with pytest.raises(BlockFormatError):
            cscb.decompress(damaged, compression, **bound)


@pytest.mark.parametrize(
    "compress",
    [zstandard.ZstdCompressor().compress, zstd_without_a_declared_size],
    ids=["zstd", "zstd without a declared size"],
)
@pytest.mark.parametrize(
    "bound",
    [{}, {"max_size": 50_000}, {"limit": 100_000}],
    ids=["unbounded", "bounded", "limited"],
)
@pytest.mark.parametrize(
    "extra",
    [zstandard.ZstdCompressor().compress(b"second frame"), b"x", b"\x00" * 5000],
    ids=["a second frame", "one byte", "padding"],
)
def test_data_after_a_zstd_frame_is_refused_not_dropped(compress, bound, extra):
    # Data that does not compress, so the limited reader, which takes its
    # input in slices, meets the end of the frame in a late slice.
    payload = random.Random(0).randbytes(50_000)
    frame = compress(payload)
    assert len(frame) > 20_000
    assert bytes(cscb.decompress(frame, api_pb2.ZSTD, **bound)) == payload

    with pytest.raises(BlockFormatError, match="failed to decompress block data"):
        cscb.decompress(frame + extra, api_pb2.ZSTD, **bound)


def test_a_short_unsized_zstd_frame_with_data_after_it_is_never_taken_for_whole():
    """The one case that may not be refused: the result is then shorter than
    ``max_size``, and the caller's length check reports it (see
    ``decode_chunk``)."""
    payload = random.Random(0).randbytes(50_000)
    data = zstd_without_a_declared_size(payload) + b"more"

    try:
        result = cscb.decompress(data, api_pb2.ZSTD, max_size=50_001)
    except BlockFormatError:
        return
    assert len(result) == 50_000 < 50_001


@pytest.mark.parametrize("slices", [1, 2, 4])
def test_a_frame_that_ends_on_an_input_slice_boundary_is_read_and_its_extra_refused(slices):
    """The limited reader takes its input in slices of 1024 bytes."""
    rng = random.Random(slices)
    for size in range(900, 6000):
        payload = rng.randbytes(size)
        frame = zstd_without_a_declared_size(payload)
        if len(frame) == slices * 1024:
            break
    else:
        pytest.fail("no frame of the wanted length")

    assert bytes(cscb.decompress(frame, api_pb2.ZSTD, limit=size)) == payload
    with pytest.raises(BlockFormatError, match="more data follows its zstd frame"):
        cscb.decompress(frame + b"x", api_pb2.ZSTD, limit=size)


@pytest.mark.parametrize("bound", [{}, {"max_size": 0}, {"max_size": 10}, {"limit": 10}], ids=str)
@pytest.mark.parametrize(
    "extra",
    [zstandard.ZstdCompressor().compress(b"real block"), b"x"],
    ids=["a second frame", "one byte"],
)
def test_data_after_an_empty_zstd_frame_is_refused(bound, extra):
    empty = zstandard.ZstdCompressor().compress(b"")
    assert zstandard.frame_content_size(empty) == 0
    assert bytes(cscb.decompress(empty, api_pb2.ZSTD, **bound)) == b""

    with pytest.raises(BlockFormatError, match="failed to decompress block data"):
        cscb.decompress(empty + extra, api_pb2.ZSTD, **bound)


def test_gzip_data_of_several_members_is_read_whole():
    data = gzip.compress(b"first-") + gzip.compress(b"second")
    for bound in ({}, {"max_size": 12}, {"limit": 12}):
        assert bytes(cscb.decompress(data, api_pb2.GZIP, **bound)) == b"first-second"


@pytest.mark.parametrize(
    ("compression", "compress"),
    [
        (api_pb2.NONE, lambda data: data),
        (api_pb2.GZIP, gzip.compress),
        (api_pb2.ZSTD, zstandard.ZstdCompressor().compress),
        (api_pb2.ZSTD, zstd_without_a_declared_size),
    ],
    ids=["none", "gzip", "zstd", "zstd without a declared size"],
)
def test_decompress_refuses_output_past_its_limit(compression, compress):
    # Several pieces of every reader, so the limit is checked as output grows.
    payload = bytes(range(256)) * 12_000
    data = compress(payload)
    assert bytes(cscb.decompress(data, compression, limit=len(payload))) == payload
    with pytest.raises(BlockFormatError, match=f"longer than the {len(payload) - 1} bytes allowed"):
        cscb.decompress(data, compression, limit=len(payload) - 1)


def test_decompress_takes_a_size_or_a_limit_not_both():
    with pytest.raises(ValueError, match="not both"):
        cscb.decompress(b"data", api_pb2.NONE, max_size=4, limit=4)


def test_decompress_refuses_an_unknown_compression():
    with pytest.raises(BlockFormatError, match="unsupported compression"):
        cscb.decompress(b"data", 99)
