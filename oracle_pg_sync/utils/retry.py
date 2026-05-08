from __future__ import annotations

import logging
import os
import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

TRANSIENT_CONNECT_ERROR_PATTERNS = (
    "temporary failure in name resolution",
    "failed to resolve host",
    "could not translate host name",
    "name or service not known",
    "nodename nor servname",
    "getaddrinfo",
    "connection timed out",
    "timeout expired",
    "server closed the connection unexpectedly",
    "could not connect to server",
    "ora-12154",
    "ora-12514",
    "ora-12541",
    "ora-12545",
)


def retry(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    should_retry: Callable[[Exception], bool] | None = None,
) -> T:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            if attempt >= attempts or (should_retry and not should_retry(exc)):
                raise
            time.sleep(delay_seconds)
    raise RuntimeError("retry exhausted") from last_error


def connect_retry(
    fn: Callable[[], T],
    *,
    label: str = "database connect",
    attempts: int | None = None,
    delay_seconds: float | None = None,
    should_retry: Callable[[Exception], bool] | None = None,
    logger: logging.Logger | None = None,
) -> T:
    retry_attempts = attempts if attempts is not None else _env_int("ORACLE_PG_SYNC_CONNECT_RETRIES", 8)
    retry_delay = delay_seconds if delay_seconds is not None else _env_float("ORACLE_PG_SYNC_CONNECT_RETRY_DELAY_SECONDS", 1.0)
    retry_max_delay = _env_float("ORACLE_PG_SYNC_CONNECT_RETRY_MAX_DELAY_SECONDS", 30.0)
    retry_jitter = max(0.0, _env_float("ORACLE_PG_SYNC_CONNECT_RETRY_JITTER_SECONDS", 0.0))
    active_logger = logger or logging.getLogger(__name__)
    last_error: Exception | None = None
    for attempt in range(1, max(1, retry_attempts) + 1):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            predicate = should_retry or is_transient_connect_error
            if attempt >= retry_attempts or not predicate(exc):
                raise
            delay = min(retry_max_delay, retry_delay * (2 ** (attempt - 1)))
            if retry_jitter:
                delay += random.uniform(0, retry_jitter)
            active_logger.warning("%s failed attempt=%s retry_in=%.1fs error=%s", label, attempt, delay, exc)
            time.sleep(delay)
    raise RuntimeError(f"{label} retry exhausted") from last_error


def is_transient_connect_error(exc: Exception) -> bool:
    text = f"{exc.__class__.__name__}: {exc}".lower()
    return any(pattern in text for pattern in TRANSIENT_CONNECT_ERROR_PATTERNS)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default
