"""Errors raised by the SDK."""

from __future__ import annotations


class ChainStorageError(Exception):
    """Base class of every error the SDK raises."""


class ConfigError(ChainStorageError, ValueError):
    """The configuration, or an argument to a client call, is invalid."""


class RequestError(ChainStorageError):
    """A call to the ChainStorage server failed.

    ``code`` is the gRPC status name (``"UNAVAILABLE"``) or ``"HTTP <status>"``
    for the REST transport. ``retryable`` says whether the same call may
    succeed if it is sent again.
    """

    def __init__(self, message: str, *, code: str = "", retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class NotFoundError(RequestError):
    """The server has no such block, transaction or event."""


class DownloadError(ChainStorageError):
    """Fetching a block file from storage failed.

    ``status_code`` is the HTTP status, or ``None`` when no response arrived.
    ``error_code`` is the store's own code for a refusal, such as S3's
    ``ExpiredToken`` or ``AccessDenied``, and empty when the store gave none.
    It can help tell why a link was refused; stores differ in the codes they
    give.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: str = "",
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.retryable = retryable


class BlockFormatError(ChainStorageError, ValueError):
    """A block file is malformed, or fails a length, hash or checksum check."""
