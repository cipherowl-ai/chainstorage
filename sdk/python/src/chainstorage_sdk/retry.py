"""Retry with exponential backoff, the same policy as the Go SDK."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TypeVar

from chainstorage_sdk.errors import CallTimeoutError

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """How often and how long to wait between attempts.

    The defaults match the Go SDK: 4 attempts, a first wait of 200 ms that
    doubles, each wait spread by +/-50%, and no wait longer than 15 s.
    """

    attempts: int = 4
    initial_interval: float = 0.2
    multiplier: float = 2.0
    jitter: float = 0.5
    max_interval: float = 15.0

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be at least 1")
        if not 0 <= self.jitter < 1:
            raise ValueError("jitter must be in [0, 1)")

    def waits(self) -> Iterator[float]:
        """Yields the wait before each retry: ``attempts - 1`` values."""
        interval = self.initial_interval
        for _ in range(self.attempts - 1):
            spread = interval * self.jitter
            yield min(self.max_interval, random.uniform(interval - spread, interval + spread))
            interval = min(self.max_interval, interval * self.multiplier)


def time_left(deadline: float | None) -> float | None:
    """Returns the seconds until ``deadline``, a ``time.monotonic()`` value,
    or ``None`` when there is no deadline."""
    return None if deadline is None else deadline - time.monotonic()


def call_with_retry(
    fn: Callable[[], T],
    *,
    retryable: Callable[[Exception], bool],
    policy: RetryPolicy,
    sleep: Callable[[float], None] = time.sleep,
    what: str = "call",
    reason: Callable[[Exception], str] = lambda exc: type(exc).__name__,
    deadline: float | None = None,
) -> T:
    """Calls ``fn`` until it returns, raises an error that is not retryable,
    or the policy's attempts are used up. The last error is raised as is.

    Every retry is logged as a warning: ``what`` names the call, and
    ``reason`` turns the error into the few words that are logged. The error
    itself is not logged, because its text may hold a signed link.

    ``deadline`` is a ``time.monotonic()`` value. A retry that could not
    start before it is not made: ``CallTimeoutError`` is raised instead.
    """
    waits = policy.waits()
    attempt = 1
    while True:
        try:
            return fn()
        except Exception as exc:
            wait = next(waits, None)
            if wait is None or not retryable(exc):
                raise
            left = time_left(deadline)
            if left is not None and wait >= left:
                raise CallTimeoutError(
                    f"{what} did not finish in time; the last attempt failed with {reason(exc)}",
                    code="DEADLINE_EXCEEDED",
                ) from exc
            logger.warning(
                "%s failed with %s on attempt %d of %d; trying again in %.1f s",
                what,
                reason(exc),
                attempt,
                policy.attempts,
                wait,
            )
            sleep(wait)
            attempt += 1
