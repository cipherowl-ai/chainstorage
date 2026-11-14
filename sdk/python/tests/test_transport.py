"""Configuration, the retry policy, and the REST transport."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from chainstorage_sdk import (
    CallTimeoutError,
    Client,
    Config,
    ConfigError,
    NotFoundError,
    RequestError,
    RetryPolicy,
)
from chainstorage_sdk.retry import call_with_retry
from chainstorage_sdk.transport import grpc_target
from coinbase.chainstorage import api_pb2

# -- configuration --------------------------------------------------------


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"server_address": ""}, "server_address is required"),
        ({"transport": "carrier-pigeon"}, "transport must be"),
        ({"tag": -1}, "tag must not be negative"),
        ({"client_timeout": 0}, "timeouts must be positive"),
        ({"http_timeout": -1}, "timeouts must be positive"),
        ({"num_workers": 0}, "num_workers must be at least 1"),
        ({"chunk_cache_bytes": -1}, "chunk_cache_bytes must not be negative"),
        ({"auth_header": "x-api-key"}, "must be set together"),
        ({"auth_token": "t0ken"}, "must be set together"),
    ],
)
def test_an_invalid_config_is_refused(changes, message):
    settings = {"server_address": "localhost:9090", **changes}
    with pytest.raises(ConfigError, match=message):
        Config(**settings)


def test_the_auth_token_is_not_printed():
    config = Config(server_address="localhost:9090", auth_header="x-api-key", auth_token="t0ken")
    assert "t0ken" not in repr(config)


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        ("localhost:9090", ("localhost:9090", False)),
        ("http://chainstorage.internal:80", ("chainstorage.internal:80", False)),
        ("chainstorage.example.com:443", ("chainstorage.example.com:443", True)),
        ("https://chainstorage.example.com:443", ("chainstorage.example.com:443", True)),
    ],
)
def test_the_address_decides_between_plaintext_and_tls(address, expected):
    assert grpc_target(address) == expected


# -- retry ----------------------------------------------------------------


def test_the_default_policy_waits_three_times_within_its_bounds():
    for _ in range(50):
        first, second, third = RetryPolicy().waits()
        assert 0.1 <= first <= 0.3
        assert 0.2 <= second <= 0.6
        assert 0.4 <= third <= 1.2


def test_no_wait_is_longer_than_the_maximum():
    policy = RetryPolicy(attempts=12, initial_interval=1, max_interval=5, jitter=0)
    assert list(policy.waits()) == [1, 2, 4, 5, 5, 5, 5, 5, 5, 5, 5]


@pytest.mark.parametrize("changes", [{"attempts": 0}, {"jitter": 1}, {"jitter": -0.1}])
def test_an_invalid_policy_is_refused(changes):
    with pytest.raises(ValueError):
        RetryPolicy(**changes)


def test_a_call_is_repeated_until_it_succeeds():
    outcomes = [KeyError("first"), KeyError("second"), "done"]
    sleeps: list[float] = []

    def call():
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    result = call_with_retry(
        call, retryable=lambda exc: True, policy=RetryPolicy(jitter=0), sleep=sleeps.append
    )

    assert result == "done"
    assert sleeps == [0.2, 0.4]


def test_the_last_error_is_raised_when_attempts_run_out():
    calls = []

    def call():
        calls.append(1)
        raise KeyError(len(calls))

    with pytest.raises(KeyError, match="3"):
        call_with_retry(
            call,
            retryable=lambda exc: True,
            policy=RetryPolicy(attempts=3),
            sleep=lambda _: None,
        )
    assert len(calls) == 3


def test_an_error_that_is_not_retryable_is_raised_at_once():
    calls = []

    def call():
        calls.append(1)
        raise KeyError("no")

    with pytest.raises(KeyError):
        call_with_retry(
            call, retryable=lambda exc: False, policy=RetryPolicy(), sleep=lambda _: None
        )
    assert len(calls) == 1


def failing_call():
    raise KeyError("the text of the error")


def test_a_retry_is_logged_with_what_failed_and_why(caplog):
    outcomes = [KeyError("the text of the error"), "done"]

    def call():
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    with caplog.at_level("WARNING", logger="chainstorage_sdk.retry"):
        call_with_retry(
            call,
            retryable=lambda exc: True,
            policy=RetryPolicy(jitter=0),
            sleep=lambda _: None,
            what="a lookup",
            reason=lambda exc: "a reason",
        )

    assert [record.getMessage() for record in caplog.records] == [
        "a lookup failed with a reason on attempt 1 of 4; trying again in 0.20 s"
    ]
    # The error itself is not logged: its text could hold a signed link.
    assert "the text of the error" not in caplog.text


def test_a_retry_before_the_deadline_is_made():
    outcomes = [KeyError("first"), "done"]
    sleeps: list[float] = []

    def call():
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    result = call_with_retry(
        call,
        retryable=lambda exc: True,
        policy=RetryPolicy(jitter=0),
        sleep=sleeps.append,
        deadline=time.monotonic() + 60,
    )

    assert result == "done"
    assert sleeps == [0.2]


def test_a_retry_that_could_not_start_before_the_deadline_is_not_made():
    sleeps: list[float] = []

    with pytest.raises(CallTimeoutError) as raised:
        call_with_retry(
            failing_call,
            retryable=lambda exc: True,
            policy=RetryPolicy(jitter=0),
            sleep=sleeps.append,
            what="a lookup",
            deadline=time.monotonic() + 0.1,
        )

    assert str(raised.value) == (
        "a lookup did not finish in time; the last attempt failed with KeyError"
    )
    assert raised.value.code == "DEADLINE_EXCEEDED"
    assert isinstance(raised.value.__cause__, KeyError)
    assert sleeps == []


def test_a_last_attempt_that_fails_after_the_deadline_is_a_timeout_error():
    with pytest.raises(CallTimeoutError, match="the last attempt failed with KeyError"):
        call_with_retry(
            failing_call,
            retryable=lambda exc: True,
            policy=RetryPolicy(attempts=1),
            sleep=lambda _: None,
            what="a lookup",
            deadline=time.monotonic() - 1,
        )


def test_an_error_that_is_not_retryable_is_raised_as_is_after_the_deadline():
    with pytest.raises(KeyError):
        call_with_retry(
            failing_call,
            retryable=lambda exc: False,
            policy=RetryPolicy(),
            sleep=lambda _: None,
            deadline=time.monotonic() - 1,
        )


def test_the_last_error_is_raised_as_is_when_attempts_run_out_before_the_deadline():
    with pytest.raises(KeyError):
        call_with_retry(
            failing_call,
            retryable=lambda exc: True,
            policy=RetryPolicy(attempts=2),
            sleep=lambda _: None,
            deadline=time.monotonic() + 60,
        )


# -- REST -----------------------------------------------------------------


class RestServer:
    """Answers ``POST /<service>/<method>`` with the JSON a test queued."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, dict, dict]] = []
        self.answers: list[tuple[int, str]] = []
        # Seconds to wait before answering, and between the bytes of the answer.
        self.delay = 0.0
        self.drip = 0.0
        rest = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length) or b"{}")
                rest.requests.append((self.path, dict(self.headers.items()), body))
                status, answer = rest.answers.pop(0)
                payload = answer.encode("utf-8", "surrogateescape")
                time.sleep(rest.delay)
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                try:
                    for position in range(len(payload)) if rest.drip else ():
                        self.wfile.write(payload[position : position + 1])
                        self.wfile.flush()
                        time.sleep(rest.drip)
                    if not rest.drip:
                        self.wfile.write(payload)
                except OSError:
                    pass  # the client gave up

            def log_message(self, *args: object) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        ).start()
        self.address = f"http://127.0.0.1:{self._server.server_address[1]}/api/"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def rest():
    server = RestServer()
    yield server
    server.close()


def rest_client(rest: RestServer, sleeps: list[float] | None = None, **settings) -> Client:
    config = Config(server_address=rest.address, transport="rest", **settings)
    return Client(config, sleep=(sleeps if sleeps is not None else []).append)


def test_a_rest_call_posts_json_to_the_method_path(rest):
    rest.answers.append((200, '{"height": "17", "futureField": true}'))

    with rest_client(rest, tag=3) as client:
        assert client.get_latest_block() == 17

    path, headers, body = rest.requests[0]
    assert path == "/api/coinbase.chainstorage.ChainStorage/GetLatestBlock"
    assert body == {"tag": 3}
    assert headers["Content-Type"] == "application/json"


def test_a_rest_call_sends_the_identity_headers(rest):
    rest.answers.append((200, "{}"))

    with rest_client(rest, client_id="indexer", auth_header="X-Api-Key", auth_token="t0ken") as c:
        c.get_chain_metadata()

    headers = {name.lower(): value for name, value in rest.requests[0][1].items()}
    assert headers["x-client-id"] == "indexer"
    assert headers["x-api-key"] == "t0ken"
    # The Go SDK's REST client sends the token under this name as well.
    assert headers["cb-nft-api-token"] == "t0ken"


@pytest.mark.parametrize("status", [429, 500, 503])
def test_a_rest_call_is_retried_on_a_temporary_failure(rest, status):
    rest.answers.extend([(status, ""), (200, '{"height": "5"}')])
    sleeps: list[float] = []

    with rest_client(rest, sleeps) as client:
        assert client.get_latest_block() == 5

    assert len(rest.requests) == 2
    assert len(sleeps) == 1


def test_a_rest_call_is_not_retried_on_a_permanent_failure(rest):
    rest.answers.append((400, "bad request"))

    with rest_client(rest) as client, pytest.raises(RequestError) as raised:
        client.get_latest_block()

    assert raised.value.code == "HTTP 400"
    assert len(rest.requests) == 1


def test_a_rest_gateway_reports_not_found_in_its_body(rest):
    not_found = (404, '{"code": 5, "message": "transaction not found"}')
    rest.answers.extend([not_found, not_found])

    with rest_client(rest) as client:
        assert client.get_block_by_transaction("0xmissing") == []
        with pytest.raises(NotFoundError):
            client.get_latest_block()


def test_a_bare_rest_404_is_an_error_and_not_an_empty_answer(rest):
    # A wrong address answers 404 too: that must not look like "no such transaction".
    rest.answers.append((404, "<html>no such page</html>"))

    with rest_client(rest) as client, pytest.raises(RequestError) as raised:
        client.get_block_by_transaction("0xabc")

    assert not isinstance(raised.value, NotFoundError)
    assert raised.value.code == "HTTP 404"


def test_a_rest_body_that_is_not_json_is_an_error(rest):
    rest.answers.extend([(200, "<html>"), (200, "<html>")])

    with rest_client(rest, retry=RetryPolicy(attempts=2)) as client:
        with pytest.raises(RequestError, match="invalid body"):
            client.get_latest_block()


def test_a_rest_body_that_is_not_text_is_an_error(rest):
    rest.answers.append((200, "\udcff"))

    with rest_client(rest, retry=RetryPolicy(attempts=1)) as client:
        with pytest.raises(RequestError, match="invalid body"):
            client.get_latest_block()


def test_the_event_stream_is_not_available_over_rest(rest):
    from chainstorage_sdk import StreamingConfiguration

    config = StreamingConfiguration(chain_events_request=api_pb2.ChainEventsRequest())
    with rest_client(rest) as client, pytest.raises(RequestError, match="not available over REST"):
        client.stream_chain_events(config)


def test_a_rest_call_within_its_timeout_is_retried_as_usual(rest):
    rest.answers.extend([(503, ""), (200, '{"height": "5"}')])
    sleeps: list[float] = []

    with rest_client(rest, sleeps) as client:
        assert client.get_latest_block(timeout=30) == 5

    assert len(sleeps) == 1


def test_a_rest_timeout_stops_a_retry_that_could_not_start_in_time(rest):
    rest.answers.append((503, ""))
    slow = RetryPolicy(initial_interval=60, jitter=0, max_interval=60)
    sleeps: list[float] = []

    with rest_client(rest, sleeps, retry=slow) as client:
        with pytest.raises(CallTimeoutError, match="the last attempt failed with HTTP 503"):
            client.get_latest_block(timeout=5)

    assert len(rest.requests) == 1
    assert sleeps == []


def test_a_rest_timeout_that_has_run_out_fails_before_the_server_is_asked(rest):
    with rest_client(rest) as client, pytest.raises(CallTimeoutError):
        client.get_latest_block(timeout=0)

    assert rest.requests == []


def test_a_rest_call_that_gets_no_response_is_logged_without_its_error(rest, caplog):
    rest.close()

    with caplog.at_level("WARNING", logger="chainstorage_sdk.retry"):
        with rest_client(rest, retry=RetryPolicy(attempts=2, jitter=0)) as client:
            with pytest.raises(RequestError):
                client.get_latest_block()

    assert [record.getMessage() for record in caplog.records] == [
        "GetLatestBlock failed with no valid response on attempt 1 of 2; trying again in 0.20 s"
    ]


def test_a_rest_timeout_cuts_an_attempt_short(rest):
    rest.answers.append((200, '{"height": "5"}'))
    rest.delay = 2.0
    started = time.monotonic()

    with rest_client(rest, client_timeout=30, retry=RetryPolicy(attempts=1)) as client:
        with pytest.raises(CallTimeoutError, match="the last attempt failed with"):
            client.get_latest_block(timeout=0.3)

    assert time.monotonic() - started < 1.5


def test_a_rest_answer_that_keeps_arriving_past_the_timeout_is_not_returned(rest):
    # The answer drips in for about 0.5 s: every byte arrives well within the
    # wait the connection allows, the whole answer does not arrive in time.
    answer = '{"height": "5"}'
    rest.answers.append((200, answer))
    rest.drip = 0.03
    started = time.monotonic()

    with rest_client(rest, client_timeout=30) as client:
        with pytest.raises(CallTimeoutError) as raised:
            client.get_latest_block(timeout=0.2)

    assert str(raised.value) == "GetLatestBlock did not finish in time"
    # The limit of the guarantee over REST: the attempt was not cut off at
    # the timeout. It ran until the answer was complete, and was not retried.
    assert time.monotonic() - started >= rest.drip * (len(answer) - 1)
    assert len(rest.requests) == 1
