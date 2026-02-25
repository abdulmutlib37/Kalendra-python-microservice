"""
Structured logging for Cloud Run.

On Cloud Run: integrates with google-cloud-logging so JSON logs appear
natively in the Cloud Logging console with correct severity levels.

Locally: falls back to human-readable JSON on stdout.

Every request gets a correlation_id (from X-Request-ID header or a
generated UUID) that is attached to all log entries for traceability.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from contextvars import ContextVar

import google.cloud.logging as cloud_logging

SERVICE_NAME = os.getenv("SERVICE_NAME", "calendai-py-service")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ContextVar lets each async request carry its own correlation_id
# without leaking across concurrent requests.
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


def _is_cloud_run() -> bool:
    """K_SERVICE is always set by Cloud Run."""
    return "K_SERVICE" in os.environ


class CorrelationFilter(logging.Filter):
    """Injects correlation_id and service_name into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id_var.get("")  # type: ignore[attr-defined]
        record.service_name = SERVICE_NAME  # type: ignore[attr-defined]
        return True


def setup_logging() -> logging.Logger:
    """
    Returns a configured logger.

    Cloud Run:  google-cloud-logging handler → Cloud Logging console.
    Local:      StreamHandler with JSON-like formatting → stdout.
    """
    logger = logging.getLogger(SERVICE_NAME)
    logger.setLevel(LOG_LEVEL)

    if logger.handlers:
        return logger

    if _is_cloud_run():
        client = cloud_logging.Client()
        client.setup_logging(log_level=getattr(logging, LOG_LEVEL))
    else:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '{"severity":"%(levelname)s","message":"%(message)s",'
            '"correlation_id":"%(correlation_id)s",'
            '"service":"%(service_name)s","time":"%(asctime)s"}'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.addFilter(CorrelationFilter())
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger(SERVICE_NAME)


def get_or_create_correlation_id(request_id: str | None = None) -> str:
    """Set correlation_id for the current request context."""
    cid = request_id or str(uuid.uuid4())
    correlation_id_var.set(cid)
    return cid


def log_event(event: str, **kwargs) -> None:
    """
    Log a structured event with arbitrary metadata.

    Usage:
        log_event('email_sent', thread_id='abc', recipient='xyz@gmail.com', latency_ms=450)
    """
    logger = get_logger()
    data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "service": SERVICE_NAME,
        "correlation_id": correlation_id_var.get(""),
        **kwargs,
    }
    logger.info(json.dumps(data))
