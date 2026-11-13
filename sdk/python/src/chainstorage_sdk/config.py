"""Client configuration."""

from __future__ import annotations

from dataclasses import dataclass, field

from chainstorage_sdk.errors import ConfigError
from chainstorage_sdk.retry import RetryPolicy
from coinbase.c3.common import common_pb2
from coinbase.chainstorage import blockchain_pb2

TRANSPORT_GRPC = "grpc"
TRANSPORT_REST = "rest"


@dataclass(frozen=True)
class Config:
    """How to reach one ChainStorage deployment.

    Unlike the Go SDK, there are no built-in per-chain defaults: the caller
    always passes ``server_address``.

    ``server_address``: for gRPC, ``host:port``. The connection is plaintext
    when the address starts with ``http://`` or contains ``localhost``, and
    TLS otherwise. For REST, the base URL of the gateway.

    ``blockchain``, ``network``, ``sidechain``: the chain the deployment
    serves (``common_pb2.Blockchain``, ``common_pb2.Network``,
    ``blockchain_pb2.SideChain``). They are stamped on skipped blocks, which
    have no stored content.

    ``tag``: the default block tag. 0 lets the server choose.

    ``client_id``: sent as the ``x-client-id`` header when set.

    ``auth_header``, ``auth_token``: sent as one header when both are set.

    ``client_timeout``: seconds allowed for a short call. Calls that return
    more data get ``2t + 1`` or ``4t + 3`` seconds, as in the Go SDK.

    ``http_timeout``: seconds allowed for one block file request.

    ``num_workers``: how many block files are downloaded at once.

    ``chunk_cache_bytes``: how much decompressed data of consolidated files
    the client keeps between reads, so that reading the blocks of one chunk
    one after another fetches the chunk once. 0 keeps nothing.
    """

    server_address: str
    blockchain: int = common_pb2.BLOCKCHAIN_UNKNOWN
    network: int = common_pb2.NETWORK_UNKNOWN
    sidechain: int = blockchain_pb2.SIDECHAIN_NONE
    tag: int = 0
    client_id: str = ""
    auth_header: str = ""
    auth_token: str = field(default="", repr=False)
    transport: str = TRANSPORT_GRPC
    client_timeout: float = 2.0
    http_timeout: float = 30.0
    num_workers: int = 10
    chunk_cache_bytes: int = 128 * 1024 * 1024
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        if not self.server_address:
            raise ConfigError("server_address is required")
        if self.transport not in (TRANSPORT_GRPC, TRANSPORT_REST):
            raise ConfigError(f"transport must be 'grpc' or 'rest', got {self.transport!r}")
        if self.tag < 0:
            raise ConfigError("tag must not be negative")
        if self.client_timeout <= 0 or self.http_timeout <= 0:
            raise ConfigError("timeouts must be positive")
        if self.num_workers < 1:
            raise ConfigError("num_workers must be at least 1")
        if self.chunk_cache_bytes < 0:
            raise ConfigError("chunk_cache_bytes must not be negative")
        if bool(self.auth_header) != bool(self.auth_token):
            raise ConfigError("auth_header and auth_token must be set together")
