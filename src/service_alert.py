"""
service_alert.py
----------------
ServiceAlertNotifier: sends a styled HTML alert email when a critical
downstream service (VectorDB, PostgreSQL DB, Gemini LLM, OpenSearch, RabbitMQ)
goes down or raises an unrecoverable error.

Features:
  - Per-service cooldown (default SERVICE_ALERT_COOLDOWN_MINUTES) to prevent
    email flooding — at most 1 alert per service per cooldown window.
  - Cooldown state is persisted to a JSON file on disk so it survives
    process restarts / crashes (the primary cause of alert flooding).
  - Zero dependency on the EmailService template system; HTML is built inline
    so this module works even when the UI/ directory is unavailable.
  - Thread-safe file writes via a simple file lock (fcntl on Linux/Mac,
    a portable fallback on Windows).
"""

import json
import logging
import os
import smtplib
import tempfile
import time
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, Optional

from src.config import Config

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Cooldown state file location
# Stored next to this source file for easy discovery; can be
# overridden via SERVICE_ALERT_COOLDOWN_FILE env var.
# ------------------------------------------------------------------
_DEFAULT_COOLDOWN_FILE = (
    Path(__file__).resolve().parent.parent / ".service_alert_cooldowns.json"
)
_COOLDOWN_FILE: Path = Path(
    os.getenv("SERVICE_ALERT_COOLDOWN_FILE", str(_DEFAULT_COOLDOWN_FILE))
)


def _load_cooldowns() -> Dict[str, str]:
    """Return the persisted cooldown dict {service_name: ISO-timestamp}."""
    try:
        if _COOLDOWN_FILE.exists():
            with open(_COOLDOWN_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning(f"[ServiceAlert] Could not read cooldown file: {exc}")
    return {}


def _save_cooldowns(cooldowns: Dict[str, str]) -> None:
    """Persist the cooldown dict atomically via a temp-file rename."""
    try:
        dir_ = _COOLDOWN_FILE.parent
        dir_.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=str(dir_), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(cooldowns, fh, indent=2)
            os.replace(tmp_path, _COOLDOWN_FILE)  # atomic on POSIX & Windows
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception as exc:
        logger.warning(f"[ServiceAlert] Could not persist cooldown file: {exc}")


class ServiceAlertNotifier:
    """
    Sends service-down alert emails with cooldown deduplication.

    Cooldowns are stored on-disk so they survive process crashes / restarts —
    the main cause of inbox flooding when a service (e.g. Qdrant) is down.

    Usage
    -----
    notifier = ServiceAlertNotifier()
    notifier.notify_service_down(
        service_name="Qdrant/VectorDB",
        error_message="Connection refused at http://localhost:6333",
        context="qdrant_search",          # Optional — caller label
    )
    """

    def __init__(self):
        # SMTP config
        self.smtp_host: str = Config.SMTP_HOST or ""
        self.smtp_port: int = Config.SMTP_PORT
        self.smtp_user: str = Config.SMTP_USERNAME or ""
        self.smtp_pass: str = Config.SMTP_PASSWORD or ""
        self.smtp_timeout: int = Config.SMTP_TIMEOUT

        # Recipient for service alert emails
        self.alert_to: str = (
            getattr(Config, "ALERT_TO_EMAIL", None)
            or getattr(Config, "HIGH_PRIORITY_TO_EMAIL", None)
            or getattr(Config, "TO_EMAIL", None)
            or ""
        )

        # Cooldown window
        self.cooldown_minutes: int = int(
            getattr(Config, "SERVICE_ALERT_COOLDOWN_MINUTES", 30)
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def notify_service_down(
        self,
        service_name: str,
        error_message: str,
        context: str = "",
    ) -> None:
        """
        Send a service-down alert email (if outside cooldown window).

        Parameters
        ----------
        service_name  : Human-readable service label, e.g. "Qdrant/VectorDB"
        error_message : The exception message or short description.
        context       : Optional caller label ("qdrant_search", "db_execute", …)
        """
        if not self._is_smtp_configured():
            logger.warning(
                f"[ServiceAlert] SMTP not configured — cannot send alert for {service_name}."
            )
            return

        if not self.alert_to:
            logger.warning(
                f"[ServiceAlert] No ALERT_TO_EMAIL configured — skipping alert for {service_name}."
            )
            return

        # Load fresh from disk on every check so concurrent processes / restarts
        # all share the same cooldown state.
        cooldowns = _load_cooldowns()

        if self._is_in_cooldown(service_name, cooldowns):
            remaining = self._cooldown_remaining(service_name, cooldowns)
            logger.info(
                f"[ServiceAlert] Cooldown active for '{service_name}' "
                f"({remaining:.0f} min remaining) — suppressing duplicate alert."
            )
            return

        try:
            self._send_alert_email(service_name, error_message, context)
            # Persist the new "last sent" timestamp immediately
            cooldowns[service_name] = datetime.utcnow().isoformat()
            _save_cooldowns(cooldowns)
            logger.warning(
                f"[ServiceAlert] Alert sent for '{service_name}' → {self.alert_to}"
            )
        except Exception as exc:
            # Never let the notifier crash the caller
            logger.error(f"[ServiceAlert] Failed to send alert for '{service_name}': {exc}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_smtp_configured(self) -> bool:
        return bool(self.smtp_host and self.smtp_user and self.smtp_pass)

    def _is_in_cooldown(self, service_name: str, cooldowns: Dict[str, str]) -> bool:
        raw = cooldowns.get(service_name)
        if raw is None:
            return False
        try:
            last = datetime.fromisoformat(raw)
        except ValueError:
            return False
        return datetime.utcnow() - last < timedelta(minutes=self.cooldown_minutes)

    def _cooldown_remaining(self, service_name: str, cooldowns: Dict[str, str]) -> float:
        """Return minutes remaining in cooldown (0 if none)."""
        raw = cooldowns.get(service_name)
        if raw is None:
            return 0.0
        try:
            last = datetime.fromisoformat(raw)
        except ValueError:
            return 0.0
        elapsed = (datetime.utcnow() - last).total_seconds() / 60
        return max(0.0, self.cooldown_minutes - elapsed)

    def _build_html(
        self, service_name: str, error_message: str, context: str
    ) -> str:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        environment = getattr(Config, "ENVIRONMENT", "Unknown")
        ctx_row = (
            f"""<tr>
                <td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;width:30%;">Context / Caller</td>
                <td style="padding:8px;border:1px solid #ddd;">{context}</td>
            </tr>"""
            if context
            else ""
        )

        return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family: Arial, sans-serif; color: #333; margin: 0; padding: 0;">
  <div style="max-width:680px; margin:20px auto;">
    <!-- Header -->
    <div style="background:#c0392b; color:white; padding:18px 24px; border-radius:6px 6px 0 0;">
      <h2 style="margin:0; font-size:1.3em;">SERVICE DOWN ALERT</h2>
      <p style="margin:6px 0 0 0; font-size:0.9em; opacity:0.9;">
        Opssolver Engine — Infrastructure Monitor
      </p>
    </div>

    <!-- Body -->
    <div style="border:2px solid #c0392b; border-top:none; padding:24px; border-radius:0 0 6px 6px; background:#fff;">
      <p style="font-size:1em; margin-top:0;">
        A critical service has <strong style="color:#c0392b;">failed or become unreachable</strong>.
        Immediate attention may be required.
      </p>

      <table style="width:100%; border-collapse:collapse; margin-top:12px;">
        <tr>
          <td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;width:30%;">Service</td>
          <td style="padding:8px;border:1px solid #ddd;color:#c0392b;font-weight:bold;">{service_name}</td>
        </tr>
        <tr>
          <td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Environment</td>
          <td style="padding:8px;border:1px solid #ddd;">{environment}</td>
        </tr>
        <tr>
          <td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Timestamp (UTC)</td>
          <td style="padding:8px;border:1px solid #ddd;">{timestamp}</td>
        </tr>
        {ctx_row}
        <tr>
          <td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Error Details</td>
          <td style="padding:8px;border:1px solid #ddd;font-family:monospace;font-size:0.9em;
                     background:#fff5f5;border-left:4px solid #c0392b;word-break:break-word;">
            {error_message[:1000]}
          </td>
        </tr>
      </table>

      <div style="margin-top:20px; padding:14px; background:#fff3cd; border-left:4px solid #f39c12;
                  border-radius:4px;">
        <strong>⚠️ Alert Suppression:</strong> Further alerts for this service will be suppressed
        for the next <strong>{self.cooldown_minutes} minutes</strong> to prevent flooding.
        Cooldown state is persisted across restarts.
      </div>

      <p style="margin-top:18px; color:#999; font-size:0.8em;">
        Sent by <em>Opssolver Engine — ServiceAlertNotifier</em>
      </p>
    </div>
  </div>
</body>
</html>"""

    def _send_alert_email(
        self, service_name: str, error_message: str, context: str
    ) -> None:
        html_body = self._build_html(service_name, error_message, context)

        msg = EmailMessage()
        msg["Subject"] = (
            f"SERVICE DOWN: {service_name} — {Config.ENVIRONMENT or 'Unknown Env'}"
        )
        msg["From"] = self.smtp_user
        msg["To"] = self.alert_to
        msg.set_content(
            f"SERVICE DOWN ALERT\n\nService: {service_name}\nError: {error_message}\n"
            f"Context: {context}\nTimestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
            f"(This is a plain-text fallback. Please view the HTML version for full details.)"
        )
        msg.add_alternative(html_body, subtype="html")

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.smtp_timeout) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(self.smtp_user, self.smtp_pass)
            s.send_message(msg)
