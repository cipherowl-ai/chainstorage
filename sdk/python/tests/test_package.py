"""The package installs, and its generated classes import and work."""

import importlib
from pathlib import Path

import grpc

import chainstorage_sdk
import coinbase
from coinbase.c3.common import common_pb2
from coinbase.chainstorage import api_pb2, api_pb2_grpc, blockchain_pb2


def test_version_is_set():
    assert chainstorage_sdk.__version__


def test_every_generated_module_imports():
    # The generated tree is a namespace package (no __init__.py), which
    # pkgutil does not walk, so the modules are found on disk.
    names = []
    for root in map(Path, coinbase.__path__):
        for file in sorted(root.rglob("*.py")):
            relative = file.relative_to(root.parent).with_suffix("")
            names.append(".".join(relative.parts))
    # A rename or a dropped .proto shows up here before it breaks a consumer.
    assert "coinbase.chainstorage.api_pb2_grpc" in names
    assert "coinbase.crypto.rosetta.types.block_pb2" in names
    for name in names:
        importlib.import_module(name)


def test_block_round_trips():
    block = blockchain_pb2.Block(
        blockchain=common_pb2.BLOCKCHAIN_ETHEREUM,
        network=common_pb2.NETWORK_ETHEREUM_MAINNET,
        metadata=blockchain_pb2.BlockMetadata(tag=1, height=17_000_000, hash="0xabc"),
    )
    block.ethereum.header = b'{"number":"0x1036640"}'
    decoded = blockchain_pb2.Block.FromString(block.SerializeToString())
    assert decoded == block
    assert decoded.WhichOneof("blobdata") == "ethereum"


def test_generated_classes_match_the_current_protos():
    # A field the previously checked-in classes lacked, because nothing regenerated them.
    assert "storage_generation" in blockchain_pb2.BlockMetadata.DESCRIPTOR.fields_by_name
    # Values the client's read-source fallback depends on.
    assert api_pb2.BLOCK_READ_SOURCE_CONSOLIDATED == 2
    assert api_pb2.BLOCK_READ_SOURCE_SINGLE_BLOCK == api_pb2.BLOCK_READ_SOURCE_LEGACY


def test_grpc_stub_exposes_the_calls_the_client_uses():
    with grpc.insecure_channel("localhost:1") as channel:
        stub = api_pb2_grpc.ChainStorageStub(channel)
        for method in (
            "GetLatestBlock",
            "GetBlockFile",
            "GetBlockFilesByRange",
            "GetBlockByTransaction",
            "GetBlockByTimestamp",
            "GetChainEvents",
            "StreamChainEvents",
            "GetChainMetadata",
        ):
            assert callable(getattr(stub, method))
