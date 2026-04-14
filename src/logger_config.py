"""
logger_config.py
================
Centralized production-grade logging configuration for all OpsResolver services.

Features:
  - Structured JSON output (machine-parseable for Datadog, Splunk, ELK)
  - Service name injected into every log line
  - Correlation/Trace ID support (set per-message via contextvars)
  - Sensitive data masking (passwords, API keys, tokens)
  - Log level controlled via Config.LOG_LEVEL env variable
  - Docker-friendly (outputs to stdout only)
"""

import logging
import json
import re
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Correlation ID context variable (set once per error payload per service)
# ---------------------------------------------------------------------------
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="-")


def set_correlation_id(cid: str) -> None:
    """Set the correlation/trace ID for the current async context."""
    _correlation_id.set(cid)


def get_correlation_id() -> str:
    return _correlation_id.get()


# ---------------------------------------------------------------------------
# Sensitive data patterns to mask in log output
# ---------------------------------------------------------------------------
_SENSITIVE_PATTERNS = [
    # Generic password/secret/key/token patterns in key=value or JSON style
    (re.compile(r'(password|passwd|secret|api.?key|token|apikey|auth)(["\s:=]+)([^\s,"\'}{]+)', re.IGNORECASE),
     r'\1\2[REDACTED]'),
    # Bearer tokens
    (re.compile(r'(Bearer\s+)[A-Za-z0-9\-._~+/]+=*', re.IGNORECASE),
     r'\1[REDACTED]'),
    # SMTP App passwords (16-char lowercase runs often used by Office365)
    (re.compile(r'(?<=["\'])[a-z]{16}(?=["\'])'),
     '[REDACTED]'),
]


def _mask_sensitive(text: str) -> str:
    for pattern, replacement in _SENSITIVE_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


# ---------------------------------------------------------------------------
# Custom JSON Formatter
# ---------------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    """
    Formats each log record as a single JSON object.
    Output includes: timestamp, level, service, correlation_id, logger, message, exc_info.
    """

    def __init__(self, service_name: str):
        super().__init__()
        self._service = service_name

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        message = _mask_sensitive(message)

        log_obj: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self._service,
            "correlation_id": get_correlation_id(),
            "logger": record.name,
            "message": message,
        }

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        if record.stack_info:
            log_obj["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(log_obj, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Sensitive Data Log Filter (extra guard on the handler level)
# ---------------------------------------------------------------------------
class SensitiveDataFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _mask_sensitive(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {k: _mask_sensitive(str(v)) if isinstance(v, str) else v
                               for k, v in record.args.items()}
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    _mask_sensitive(str(a)) if isinstance(a, str) else a
                    for a in record.args
                )
        return True


# ---------------------------------------------------------------------------
# Public factory function — call once at module top of each service
# ---------------------------------------------------------------------------
def get_logger(service_name: str, log_level: Optional[str] = None) -> logging.Logger:
    """
    Configure and return the root logger for a given service.

    Usage (at top of each service module, replacing basicConfig):
        from src.logger_config import get_logger
        logger = get_logger("consumer")

    Args:
        service_name:  Short identifier shown in every log line (e.g. "consumer", "extractor")
        log_level:     Override log level (defaults to Config.LOG_LEVEL env var)
    """
    from src.config import Config  # local import to avoid circular import

    level_str = (log_level or Config.LOG_LEVEL or "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)

    # Remove any existing handlers to avoid duplicate output when re-called
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # JSON stdout handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter(service_name))
    handler.addFilter(SensitiveDataFilter())

    root_logger.setLevel(level)
    root_logger.addHandler(handler)

    # Silence noisy third-party loggers
    _noisy_libs = (
        "pika",
        "urllib3",
        "httpx",
        "httpcore",
        "apscheduler",
        "psycopg2",
        "presidio-analyzer",          # Silences 40+ "Loaded recognizer: X" lines on startup
        "presidio_analyzer",          # Some versions use underscore
        "langchain",
        "langchain_core",
        "langchain_google_genai",
        "google.api_core",
        "google.auth",
        "googleapiclient",
    )
    for noisy in _noisy_libs:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger(service_name)
