"""
EdgeCloudX Shared — Structured JSON Logging
==============================================
Replaces plain-text logging with structured JSON output.

Every log line includes: timestamp, level, service, trace_id, message, extras.

Usage:
    from shared.logging import setup_logging
    setup_logging("traffic-service")

    logger = logging.getLogger(__name__)
    logger.info("Processed event", extra={"trace_id": "abc", "intersection": "int-0-0"})
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from typing import Any


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def __init__(self, service_name: str = "unknown"):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Attach trace context if present
        for key in ("trace_id", "event_id", "parent_id"):
            val = getattr(record, key, None)
            if val:
                log_entry[key] = val

        # Attach any extra fields the caller passed
        skip_keys = {
            "name", "msg", "args", "created", "relativeCreated",
            "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "filename", "module", "pathname", "thread", "threadName",
            "process", "processName", "levelname", "levelno",
            "trace_id", "event_id", "parent_id", "message", "taskName",
        }
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in skip_keys and not k.startswith("_")
        }
        if extras:
            log_entry["extra"] = extras

        # Attach exception info
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info),
            }

        return json.dumps(log_entry, default=str)


def setup_logging(service_name: str, level: int = logging.INFO) -> None:
    """
    Configure the root logger with structured JSON output.

    Call this once at service startup, before any other logging calls.
    """
    root = logging.getLogger()

    # Remove existing handlers to avoid duplicate output
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter(service_name=service_name))
    root.addHandler(handler)
    root.setLevel(level)

    # Suppress noisy third-party loggers
    for noisy in ("aiokafka", "kafka", "urllib3", "asyncio", "watchfiles"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
