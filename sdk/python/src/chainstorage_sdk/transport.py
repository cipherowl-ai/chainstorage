"""How the client talks to the ChainStorage server: gRPC or REST.

Both transports expose the same two operations, so the client does not know
which one it is using:

- ``call(method, request, timeout=..., deadline=...)`` for a unary call.
  ``timeout`` is for one attempt. ``deadline`` is a ``time.monotonic()``
  value for the whole call, retries included; ``None`` means no limit;
- ``stream_chain_events(request)`` for the event stream (gRPC only).

Every failure is raised as ``RequestError``; a transport never leaks a gRPC
or HTTP library error.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from typing import Any, Protocol

import grpc
import requests
from google.protobuf import json_format, message_factory
from google.protobuf.message import Message

from chainstorage_sdk.config import TRANSPORT_REST, Config
from chainstorage_sdk.errors import CallTimeoutError, NotFoundError, RequestError
from chainstorage_sdk.retry import RetryPolicy, call_with_retry, time_left
from coinbase.chainstorage import api_pb2, api_pb2_grpc

logger = logging.getLogger(__name__)

SERVICE_NAME = "coinbase.chainstorage.ChainStorage"
CLIENT_ID_HEADER = "x-client-id"

# The limits the Go SDK sets on its gRPC connection.
_MAX_SEND_MESSAGE_BYTES = 1024 * 1024
_MAX_RECEIVE_MESSAGE_BYTES = 100 * 1024 * 1024

_RETRYABLE_GRPC_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }
)

# grpc reports a connection that was reset in the middle of a call as UNKNOWN
# with one of these texts. Every call of the SDK is a read, so it is repeated.
_RETRYABLE_UNKNOWN_DETAILS = ("Stream removed", "RST_STREAM")

_SERVICE = api_pb2.DESCRIPTOR.services_by_name["ChainStorage"]


class Transport(Protocol):
    def call(
        self, method: str, request: Message, *, timeout: float, deadline: float | None = None
    ) -> Any: ...

    def stream_chain_events(self, request: api_pb2.ChainEventsRequest) -> EventStream: ...

    def close(self) -> None: ...


class EventStream(Protocol):
    """Yields events until the server ends the stream; ``cancel`` stops it."""

    def __iter__(self) -> Iterator[api_pb2.ChainEventsResponse]: ...

    def __next__(self) -> api_pb2.ChainEventsResponse: ...

    def cancel(self) -> None: ...


def new_transport(config: Config, *, sleep: Callable[[float], None] = time.sleep) -> Transport:
    if config.transport == TRANSPORT_REST:
        return RestTransport(config, sleep=sleep)
    return GrpcTransport(config, sleep=sleep)


class GrpcTransport:
    def __init__(self, config: Config, *, sleep: Callable[[float], None] = time.sleep) -> None:
        self._policy = config.retry
        self._sleep = sleep
        metadata = []
        if config.client_id:
            metadata.append((CLIENT_ID_HEADER, config.client_id))
        if config.auth_header and config.auth_token:
            # gRPC only accepts lower-case metadata keys.
            metadata.append((config.auth_header.lower(), config.auth_token))
        self._metadata = tuple(metadata)

        target, secure = grpc_target(config.server_address)
        options = [
            ("grpc.max_send_message_length", _MAX_SEND_MESSAGE_BYTES),
            ("grpc.max_receive_message_length", _MAX_RECEIVE_MESSAGE_BYTES),
        ]
        if secure:
            self._channel = grpc.secure_channel(target, grpc.ssl_channel_credentials(), options)
        else:
            self._channel = grpc.insecure_channel(target, options)
        self._stub = api_pb2_grpc.ChainStorageStub(self._channel)

    def call(
        self, method: str, request: Message, *, timeout: float, deadline: float | None = None
    ) -> Any:
        rpc = getattr(self._stub, method)

        def attempt() -> Any:
            attempt_timeout = _attempt_timeout(method, timeout, deadline)
            try:
                return rpc(request, timeout=attempt_timeout, metadata=self._metadata)
            except grpc.RpcError as exc:
                raise _from_grpc_error(method, exc) from exc

        return _call_with_retry(method, attempt, self._policy, self._sleep, deadline)

    def stream_chain_events(self, request: api_pb2.ChainEventsRequest) -> EventStream:
        return _GrpcEventStream(self._stub.StreamChainEvents(request, metadata=self._metadata))

    def close(self) -> None:
        self._channel.close()


class _GrpcEventStream:
    def __init__(self, call: Any) -> None:
        self._call = call

    def __iter__(self) -> Iterator[api_pb2.ChainEventsResponse]:
        return self

    def __next__(self) -> api_pb2.ChainEventsResponse:
        try:
            return next(self._call)
        except grpc.RpcError as exc:
            raise _from_grpc_error("StreamChainEvents", exc) from exc

    def cancel(self) -> None:
        self._call.cancel()


class RestTransport:
    """Unary calls as ``POST <address>/<service>/<method>`` with JSON bodies."""

    def __init__(
        self,
        config: Config,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._base_url = config.server_address.rstrip("/")
        self._policy = config.retry
        self._sleep = sleep
        self._session = session or requests.Session()
        self._headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if config.client_id:
            self._headers[CLIENT_ID_HEADER] = config.client_id
        if config.auth_header and config.auth_token:
            self._headers[config.auth_header] = config.auth_token
            # The Go SDK's REST client sends the token under this name as well.
            self._headers["cb-nft-api-token"] = config.auth_token

    def call(
        self, method: str, request: Message, *, timeout: float, deadline: float | None = None
    ) -> Any:
        response_type = message_factory.GetMessageClass(
            _SERVICE.methods_by_name[method].output_type
        )
        url = f"{self._base_url}/{SERVICE_NAME}/{method}"
        body = json_format.MessageToJson(request)

        def attempt() -> Any:
            attempt_timeout = _attempt_timeout(method, timeout, deadline)
            try:
                response = self._session.post(
                    url, data=body, headers=self._headers, timeout=attempt_timeout
                )
            except requests.RequestException as exc:
                raise RequestError(
                    f"{method} failed: {type(exc).__name__}", retryable=True
                ) from exc
            status = response.status_code
            if status != 200:
                message = f"{method} failed with HTTP {status}"
                if _is_grpc_not_found(response):
                    raise NotFoundError(message, code=f"HTTP {status}")
                raise RequestError(
                    message, code=f"HTTP {status}", retryable=status == 429 or status >= 500
                )
            try:
                return json_format.Parse(
                    response.content, response_type(), ignore_unknown_fields=True
                )
            except (json_format.ParseError, ValueError) as exc:
                raise RequestError(f"{method} returned an invalid body", retryable=True) from exc

        return _call_with_retry(method, attempt, self._policy, self._sleep, deadline)

    def stream_chain_events(self, request: api_pb2.ChainEventsRequest) -> EventStream:
        raise RequestError("StreamChainEvents is not available over REST; use gRPC")

    def close(self) -> None:
        self._session.close()


def grpc_target(server_address: str) -> tuple[str, bool]:
    """Returns the address to dial and whether to use TLS.

    As in the Go SDK, the connection is plaintext when the address starts
    with ``http://`` or contains ``localhost``. An ``https://`` prefix is
    accepted and removed.
    """
    if server_address.startswith("http://"):
        return server_address[len("http://") :], False
    if server_address.startswith("https://"):
        return server_address[len("https://") :], True
    return server_address, "localhost" not in server_address


def _is_grpc_not_found(response: requests.Response) -> bool:
    """Whether a gateway reports the gRPC status NOT_FOUND (code 5) in its
    error body. A bare 404 is not enough: a wrong address answers that too."""
    try:
        body = response.json()
    except ValueError:
        return False
    return isinstance(body, dict) and body.get("code") == grpc.StatusCode.NOT_FOUND.value[0]


def _call_with_retry(
    method: str,
    attempt: Callable[[], Any],
    policy: RetryPolicy,
    sleep: Callable[[float], None],
    deadline: float | None,
) -> Any:
    return call_with_retry(
        attempt,
        retryable=_is_retryable_request,
        policy=policy,
        sleep=sleep,
        what=method,
        reason=_reason,
        deadline=deadline,
    )


def _attempt_timeout(method: str, timeout: float, deadline: float | None) -> float:
    """Returns how long one attempt may take: ``timeout``, or the time left
    until ``deadline`` when that is less."""
    left = time_left(deadline)
    if left is None:
        return timeout
    if left <= 0:
        raise CallTimeoutError(f"{method} did not finish in time", code="DEADLINE_EXCEEDED")
    return min(timeout, left)


def _is_retryable_request(exc: Exception) -> bool:
    return isinstance(exc, RequestError) and exc.retryable


def _reason(exc: Exception) -> str:
    """The few words about a failed attempt that go into the retry log line."""
    return getattr(exc, "code", "") or "no valid response"


def _from_grpc_error(method: str, exc: grpc.RpcError) -> RequestError:
    code = exc.code() if callable(getattr(exc, "code", None)) else None
    details = exc.details() if callable(getattr(exc, "details", None)) else ""
    name = code.name if code is not None else "UNKNOWN"
    message = f"{method} failed with {name}: {details}"
    if code == grpc.StatusCode.NOT_FOUND:
        return NotFoundError(message, code=name)
    retryable = code in _RETRYABLE_GRPC_CODES or (
        code == grpc.StatusCode.UNKNOWN
        and any(text in (details or "") for text in _RETRYABLE_UNKNOWN_DETAILS)
    )
    return RequestError(message, code=name, retryable=retryable)
