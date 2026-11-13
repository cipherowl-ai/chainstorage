"""A session: the client and the parser for one deployment."""

from __future__ import annotations

from chainstorage_sdk.client import Client
from chainstorage_sdk.config import Config


class Session:
    """Holds the ``Client`` for a deployment, as the Go SDK's ``Session`` does.

    Close it when done, or use it as a context manager.
    """

    def __init__(self, config: Config, *, client: Client | None = None) -> None:
        self._config = config
        self._client = client or Client(config)

    def __enter__(self) -> Session:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    @property
    def config(self) -> Config:
        return self._config

    @property
    def client(self) -> Client:
        return self._client

    @property
    def parser(self) -> None:
        """The block parser. It is not part of the SDK yet."""
        raise NotImplementedError("the block parser is not available in this version")
