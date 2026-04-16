"""
logger.py
---------
Centralized JSON structured logging for all services.

Features
--------
- JSON output (python-json-logger) — compatible with ELK, CloudWatch, Datadog
- Session ID injected into every log line via contextvars (zero-call-site change)
- Service name stamped on every record
- Third-party libraries silenced to WARNING (pika, urllib3, bs4, httpx)
- Single call to setup_logging() at each service entrypoint replaces
  all per-module logging.basicConfig() calls

Usage
-----
    from src.logger import setup_logging, set_session_id, clear_session_id

    # At service startup:
    setup_logging(service_name="email-extractor", level=Config.LOG_LEVEL)

    # At the start of each message/request (for correlation):
    token = set_session_id(ctx.sessionid)
    try:
        ...all log calls automatically include session_id...
    finally:
        clear_session_id(token)
"""

import logging
import contextvars
from typing import Optional

try:
    from pythonjsonlogger import jsonlogger
    _JSON_AVAILABLE = True
except ImportError:
    _JSON_AVAILABLE = False

# ---------------------------------------------------------------------------
# Context variable — holds the current message/request session ID.
# Set this at the start of each RabbitMQ callback or HTTP request handler
# and every logger.* call within that scope will automatically include it.
# ---------------------------------------------------------------------------
_session_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "session_id", default=""
)


def set_session_id(session_id: str) -> contextvars.Token:
    """Bind a session ID to the current context. Returns a token to clear it."""
    return _session_id_var.set(session_id)


def clear_session_id(token: contextvars.Token) -> None:
    """Remove the session ID binding after the message/request is done."""
    _session_id_var.reset(token)


if _JSON_AVAILABLE:
    class _ContextualJsonFormatter(jsonlogger.JsonFormatter):
        """
        Extends JsonFormatter to inject:
          - session_id  : from contextvars (per-message correlation)
          - service     : static label set at setup_logging() time
        """
        def __init__(self, service_name: str, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._service_name = service_name

        def add_fields(self, log_record, record, message_dict):
            super().add_fields(log_record, record, message_dict)
            session_id = _session_id_var.get()
            if session_id:
                log_record["session_id"] = session_id
            log_record["service"] = self._service_name
            log_record.pop("color_message", None)

else:
    class _ContextualJsonFormatter(logging.Formatter):
        """Plain-text fallback — used when python-json-logger is not installed."""
        def __init__(self, service_name: str, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._service_name = service_name

        def format(self, record):
            session_id = _session_id_var.get()
            sid = f"[session={session_id}] " if session_id else ""
            record.msg = f"[{self._service_name}] {sid}{record.msg}"
            return super().format(record)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def setup_logging(service_name: str, level: str = "INFO") -> None:
    """
    Configure production JSON logging for the given service.

    Call ONCE at the entrypoint of each service (email-extract-app,
    opensearch-extract-app, error-solution-create, ops_solution).

    Parameters
    ----------
    service_name : Label stamped on every log line, e.g. "email-extractor"
    level        : Log level string from Config.LOG_LEVEL (default "INFO")
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    handler = logging.StreamHandler()

    if _JSON_AVAILABLE:
        fmt = _ContextualJsonFormatter(
            service_name=service_name,
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            timestamp=True,
        )
    else:
        # Fallback to plain-text if package not installed
        logging.warning(
            "python-json-logger not installed — falling back to plain-text logging. "
            "Run: pip install python-json-logger"
        )
        fmt = logging.Formatter(
            f"%(asctime)s %(levelname)s [{service_name}] [%(name)s] "
            "[session=%(session_id)s] %(message)s"
        )

    handler.setFormatter(fmt)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(numeric_level)

    # ------------------------------------------------------------------
    # Silence noisy third-party libraries — they emit at INFO/DEBUG by
    # default which floods production logs with irrelevant heartbeat and
    # HTTP-connection noise.
    # ------------------------------------------------------------------
    _third_party_silence = {
        "pika":              logging.WARNING,   # RabbitMQ heartbeat minutiae
        "urllib3":           logging.WARNING,   # HTTP connection pool messages
        "requests":          logging.WARNING,   # urllib3 peer
        "bs4":               logging.WARNING,   # BeautifulSoup parse warnings
        "httpx":             logging.WARNING,   # async HTTP client
        "httpcore":          logging.WARNING,
        "openai":            logging.WARNING,
        "google":            logging.WARNING,   # Gemini SDK internals
        "qdrant_client":     logging.WARNING,   # Qdrant SDK verbose init
        "apscheduler":       logging.WARNING,   # Scheduler job tick noise
        "msal":              logging.WARNING,   # Azure token-cache spam
        "presidio-analyzer": logging.WARNING,   # PII analysis debug output
        "presidio-anonymizer": logging.WARNING,  # PII anonymizer noise
    }
    for lib, lib_level in _third_party_silence.items():
        logging.getLogger(lib).setLevel(lib_level)

    logging.getLogger(__name__).debug(
        f"Logging configured: service={service_name}, level={level.upper()}, "
        f"json={'yes' if _JSON_AVAILABLE else 'no (fallback)'}"
    )
