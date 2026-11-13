"""Python SDK for ChainStorage.

The counterpart of the Go SDK in ``sdk/``::

    from chainstorage_sdk import Client, Config

    with Client(Config(server_address="localhost:9090")) as client:
        height = client.get_latest_block()
        block = client.get_block(height)

The generated protobuf and gRPC classes ship with it, under
``coinbase.chainstorage``. The block parser is not part of the SDK yet.
"""

from chainstorage_sdk.client import (
    ChainEventResult,
    Client,
    RawBlockPayload,
    StreamingConfiguration,
)
from chainstorage_sdk.config import Config
from chainstorage_sdk.errors import (
    BlockFormatError,
    ChainStorageError,
    ConfigError,
    DownloadError,
    NotFoundError,
    RequestError,
)
from chainstorage_sdk.retry import RetryPolicy
from chainstorage_sdk.session import Session

__version__ = "0.1.0"

__all__ = [
    "BlockFormatError",
    "ChainEventResult",
    "ChainStorageError",
    "Client",
    "Config",
    "ConfigError",
    "DownloadError",
    "NotFoundError",
    "RawBlockPayload",
    "RequestError",
    "RetryPolicy",
    "Session",
    "StreamingConfiguration",
]
