import os
import json
import logging
import time
import signal
import sys
import msal
import requests
import pika
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from apscheduler.schedulers.blocking import BlockingScheduler

from src.config import Config
from src.structuraldb import DB
from src.service_alert import ServiceAlertNotifier
from src.incident_manager import IncidentManager
from src.logger_config import get_logger, set_correlation_id

# Structured JSON logger — service-tagged, sensitive-data masked
logger = get_logger("email-extractor")

# Global variables
rabbitmq_connection: Optional[pika.BlockingConnection] = None
rabbitmq_channel: Optional[pika.channel.Channel] = None
scheduler: Optional[BlockingScheduler] = None
msal_app: Optional[msal.ConfidentialClientApplication] = None

# Optimization 1: Persistent DB connection — reused across all cycles (no open/close per email)
_db_conn: Optional[psycopg2.extensions.connection] = None

# In-memory cooldown tracker: { (app_name, error_code) -> last_alert_datetime }
escalation_cooldown: Dict[tuple, datetime] = {}

# Cross-cycle async gap cache: { (app_name, code, desc) → (first_published_at UTC, count) }
# Bridges the window between "published to RabbitMQ" and "consumer inserts DB record"
# Prevents duplicate emails arriving before consumer finishes from being re-published
_published_cache: Dict[tuple, Tuple[datetime, int]] = {}

# Module-level service alert notifier (shared, cooldown-aware)
_alert_notifier: ServiceAlertNotifier = ServiceAlertNotifier()

# ITSM Provider (ServiceNow)
_incident_manager = IncidentManager()

# Graph API Configuration
# For Client Credentials, we usually use the /.default scope
SCOPES = ["https://graph.microsoft.com/.default"]


def get_persistent_db() -> psycopg2.extensions.connection:
    """Return the module-level persistent DB connection, reconnecting if closed."""
    global _db_conn
    try:
        if _db_conn is None or _db_conn.closed:
            _db_conn = psycopg2.connect(Config.DB_URL)
            logger.debug("Persistent DB connection established")
    except Exception as e:
        logger.error(f"Failed to establish persistent DB connection: {e}")
        _alert_notifier.notify_service_down(
            "PostgreSQL/DB", str(e), context="get_persistent_db"
        )
        _db_conn = None
        raise
    return _db_conn


def setup_rabbitmq_connection():
    """
    Setup persistent RabbitMQ connection with heartbeat.
    - Reuses existing connection/channel if both are healthy
    - Reconnects if connection dropped
    - Recreates channel if connection alive but channel closed
    """
    global rabbitmq_connection, rabbitmq_channel

    try:
        # Check if connection is alive
        conn_alive = (
            rabbitmq_connection is not None
            and not rabbitmq_connection.is_closed
        )

        # Check if channel is alive (connection can be open but channel closed)
        channel_alive = (
            conn_alive
            and rabbitmq_channel is not None
            and rabbitmq_channel.is_open
        )

        if channel_alive:
            # Both healthy — nothing to do
            return

        if conn_alive and not channel_alive:
            # Connection alive but channel dead — just recreate channel
            logger.debug("RabbitMQ: connection alive, recreating channel...")
            rabbitmq_channel = rabbitmq_connection.channel()
            rabbitmq_channel.exchange_declare(
                exchange=Config.EXCHANGE,
                exchange_type=Config.EXCHANGE_TYPE,
                durable=True
            )
            rabbitmq_channel.queue_declare(queue=Config.QUEUE, durable=True)
            logger.debug("RabbitMQ channel restored")
            return

        # Full reconnect needed
        logger.debug("Establishing new RabbitMQ connection...")
        params = pika.URLParameters(Config.RABBIT_URL)
        params.heartbeat = 120               # Heartbeat every 120s (> 60s poll interval)
        params.blocked_connection_timeout = 30
        params.socket_timeout = 10
        params.connection_attempts = 3
        params.retry_delay = 2

        rabbitmq_connection = pika.BlockingConnection(params)
        rabbitmq_channel = rabbitmq_connection.channel()

        rabbitmq_channel.exchange_declare(
            exchange=Config.EXCHANGE,
            exchange_type=Config.EXCHANGE_TYPE,
            durable=True
        )
        rabbitmq_channel.queue_declare(queue=Config.QUEUE, durable=True)
        logger.debug("RabbitMQ connection established (heartbeat=120s)")

    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        _alert_notifier.notify_service_down(
            "RabbitMQ", str(e), context="setup_rabbitmq_connection"
        )
        rabbitmq_connection = None
        rabbitmq_channel = None  # Ensure stale channel isn't reused


def keep_rabbitmq_alive():
    """
    Process pending heartbeat frames to keep the connection alive between cycles.
    Call once at the start of each poll cycle.
    """
    global rabbitmq_connection, rabbitmq_channel
    try:
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            rabbitmq_connection.process_data_events(time_limit=0)  # Non-blocking flush
    except Exception as e:
        logger.warning(f"RabbitMQ heartbeat flush failed: {e}. Will reconnect on next publish.")
        rabbitmq_connection = None
        rabbitmq_channel = None


def get_graph_token() -> Optional[str]:
    """Get Access Token for Graph API (Client Credentials Flow)"""
    global msal_app
    
    # Initialize MSAL App if needed
    if not msal_app:
        authority_url = f"https://login.microsoftonline.com/{Config.AZURE_TENANT_ID}"
        msal_app = msal.ConfidentialClientApplication(
            Config.AZURE_CLIENT_ID,
            authority=authority_url,
            client_credential=Config.AZURE_CLIENT_SECRET
        )

    # Acquire token (MSAL handles caching internally for ConfidentialClientApplication)
    result = msal_app.acquire_token_for_client(scopes=SCOPES)
    
    if "access_token" in result:
        return result['access_token']
    else:
        logger.error(f"Failed to acquire token: {result.get('error')}: {result.get('error_description')}")
        return None

def parse_tibco_email(body: str, subject: str) -> Dict[str, Any]:
    """
    Parse TIBCO HTML alert email body.
    
    Extracts:
      - applicationName  <- 'Project Name' cell
      - correlationId    <- 'Exception ID' cell
      - code             <- 'Message Code' cell
      - description      <- 'Message' + 'ERROR DUMP' sections combined
    """
    # Defaults
    app_name = "TIBCO_APP"
    correlation_id = "UNKNOWN"
    error_code = "UNKNOWN_ERROR"
    description = subject or "No description"

    try:
        soup = BeautifulSoup(body, "lxml")

        def get_cell_after_label(label_text: str) -> str:
            """Find a <td> with label text and return the next sibling <td> value."""
            for td in soup.find_all("td"):
                if td.get_text(strip=True).lower() == label_text.lower():
                    # Value is the next <td> sibling
                    next_td = td.find_next_sibling("td")
                    if next_td:
                        return next_td.get_text(strip=True)
            return ""

        # --- Extract fields from HTML tables ---
        project_name = get_cell_after_label("Project Name")
        if project_name:
            app_name = project_name

        exception_id = get_cell_after_label("Exception ID")
        if exception_id:
            correlation_id = exception_id

        msg_code = get_cell_after_label("Message Code")
        if msg_code:
            error_code = msg_code

        # Timestamp Extraction
        timestamp_str = get_cell_after_label("Timestamp UTC")
        if timestamp_str:
            try:
                # Parse ISO format e.g. 2026-02-18T05:16:35Z
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                email_timestamp = dt.timestamp()
            except ValueError:
                logger.warning(f"Failed to parse timestamp '{timestamp_str}', using current time")
                email_timestamp = datetime.now(timezone.utc).timestamp()
        else:
            email_timestamp = datetime.now(timezone.utc).timestamp()

        # --- Build description: Message + ERROR DUMP ---
        message_text = get_cell_after_label("Message")

        # ERROR DUMP is in its own table section - find the div with that title
        error_dump_text = ""
        for div in soup.find_all("div", class_="section-title"):
            if "ERROR DUMP" in div.get_text(strip=True).upper():
                # The next table after this div contains the dump
                next_table = div.find_next_sibling("table")
                if next_table:
                    error_dump_text = next_table.get_text(separator=" ", strip=True)
                break

        # Combine Message + ERROR DUMP
        parts = [p for p in [message_text, error_dump_text] if p]
        if parts:
            description = " | ERROR DUMP: ".join(parts) if error_dump_text else message_text

    except Exception as e:
        logger.error(f"Error parsing HTML email body: {e}")
        # Fallback: use subject as description
        description = f"{subject} - (parse error)"

    logger.debug(
        f"Parsed email: app={app_name}, code={error_code}, desc_len={len(description)}"
    )

    return {
        "applicationName": app_name,
        "correlationId": correlation_id,
        "code": error_code,
        "description": description,
        "timestamp": email_timestamp,
        "source": "email"
    }

def check_occurrence_count(app_name, code, desc, timestamp) -> int:
    """
    Optimization 2: Single atomic UPDATE...RETURNING.
    - If record NOT found → UPDATE affects 0 rows → return 0 (new error)
    - If record FOUND → atomically increments occurrence_count and returns new value
    Eliminates the separate SELECT round trip entirely.
    """
    global _db_conn
    try:
        conn = get_persistent_db()

        if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is not None:
            local_ts = timestamp.astimezone().replace(tzinfo=None)
        else:
            local_ts = timestamp

        sql = """
            UPDATE errorsolutiontable
               SET occurrence_count = occurrence_count + 1
             WHERE application_name = %s
               AND error_code = %s
               AND error_description = %s
               AND error_timestamp >= %s - INTERVAL '%s minutes'
               AND error_timestamp <= %s + INTERVAL '1 minute'
            RETURNING occurrence_count
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (
                app_name, code, desc,
                local_ts, Config.DB_DUPLICATE_WINDOW_MINUTES, local_ts
            ))
            rows = cur.fetchall()
        conn.commit()

        if not rows:
            logger.debug(f"No existing record for {app_name}/{code} — new error")
            return 0

        new_count = max(int(r['occurrence_count']) for r in rows)
        logger.info(f"{app_name}/{code}: occurrence_count → {new_count}")
        return new_count

    except Exception as e:
        logger.error(f"DB occurrence count check failed: {e}")
        try:
            if _db_conn:
                _db_conn.rollback()
        except Exception:
            pass
        _db_conn = None  # Force reconnect next call
        return 0  # Fail open — treat as new error


def batch_fetch_occurrence_counts(
    error_keys: List[Tuple[str, str, str]],
    local_timestamps: List[datetime]
) -> Dict[Tuple[str, str, str], int]:
    """
    Optimization 3: Pre-fetch occurrence counts for ALL emails in ONE query.
    Returns { (app_name, code, description) -> current occurrence_count }
    Reduces N per-email DB queries to a single batch SELECT at cycle start.
    """
    if not error_keys:
        return {}
    global _db_conn
    try:
        conn = get_persistent_db()

        # Use earliest timestamp for the window lower bound
        min_ts = min(local_timestamps)

        # Build IN clause for unique (app_name, error_code) pairs
        unique_pairs = list({(k[0], k[1]) for k in error_keys})
        placeholders = ','.join(['(%s,%s)'] * len(unique_pairs))
        pair_params: List = []
        for pair in unique_pairs:
            pair_params.extend(pair)

        sql = f"""
            SELECT application_name, error_code, error_description, occurrence_count
              FROM errorsolutiontable
             WHERE (application_name, error_code) IN ({placeholders})
               AND error_timestamp >= %s - INTERVAL '{Config.DB_DUPLICATE_WINDOW_MINUTES} minutes'
             ORDER BY error_timestamp DESC
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, pair_params + [min_ts])
            rows = cur.fetchall()
        conn.commit()

        # Build result: keyed by (app, code, desc) — take latest record per key
        result: Dict[Tuple[str, str, str], int] = {}
        for row in rows:
            key = (row['application_name'], row['error_code'], row['error_description'])
            if key not in result:  # Already ordered DESC, first hit is most recent
                result[key] = int(row['occurrence_count'] or 1)

        logger.debug(f"Batch pre-fetch: found {len(result)}/{len(error_keys)} errors in DB")
        return result

    except Exception as e:
        logger.error(f"Batch fetch occurrence counts failed: {e}")
        _db_conn = None
        return {}  # Fail open — all emails treated as new


def deduplicate_emails(emails: List[Dict]) -> List[Dict]:
    """Remove duplicate emails within the same batch"""
    seen = set()
    unique = []
    
    for email in emails:
        try:
            body = email.get('body', {}).get('content', '')
            subject = email.get('subject', '')
            payload = parse_tibco_email(body, subject)
            
            key = (payload['applicationName'], payload['code'], payload['description'])
            if key not in seen:
                seen.add(key)
                unique.append(email)
            else:
                logger.debug(f"⏭️ Filtered duplicate in batch: {payload['code']}")
        except Exception as e:
            logger.warning(f"Failed to parse email for dedup: {e}")
            unique.append(email)  # Include if parsing fails
    
    if len(emails) != len(unique):
        logger.info(f"Batch deduplication: {len(emails)} raw → {len(unique)} unique")
    
    return unique

def send_high_priority_alert(
    app_name: str, code: str, description: str, count: int, timestamp: datetime,
    incident_url: str = "#", incident_display: str = "N/A"
):
    """Send a high-priority escalation email directly via SMTP, bypassing RabbitMQ."""
    try:
        import smtplib
        from email.message import EmailMessage

        to_email = Config.HIGH_PRIORITY_TO_EMAIL or Config.TO_EMAIL
        if not to_email:
            logger.error("HIGH_PRIORITY_TO_EMAIL not configured. Cannot send escalation alert.")
            return

        html_body = f"""
        <html><body style="font-family: Arial, sans-serif; color: #333;">
        <div style="background:#b0002a;color:white;padding:16px;border-radius:6px 6px 0 0;">
            <h2 style="margin:0;">HIGH-PRIORITY ERROR ALERT</h2>
        </div>
        <div style="border:2px solid #b0002a;border-top:none;padding:20px;border-radius:0 0 6px 6px;">
            <p><strong>This error has occurred <span style="color:#b0002a;font-size:1.4em;">{count}x</span>
            in the last {Config.DB_DUPLICATE_WINDOW_MINUTES} minutes and requires immediate attention.</strong></p>
            <table style="width:100%;border-collapse:collapse;margin-top:12px;">
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;width:30%;">Application</td>
                <td style="padding:8px;border:1px solid #ddd;">{app_name}</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Error Code</td>
                <td style="padding:8px;border:1px solid #ddd;">{code}</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Occurrences</td>
                <td style="padding:8px;border:1px solid #ddd;color:#b0002a;font-weight:bold;">{count} times</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Time Window</td>
                <td style="padding:8px;border:1px solid #ddd;">Last {Config.DB_DUPLICATE_WINDOW_MINUTES} minutes</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Last Seen</td>
                <td style="padding:8px;border:1px solid #ddd;">{timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Description</td>
                <td style="padding:8px;border:1px solid #ddd;">{description[:500]}</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Incident Ticket</td>
                <td style="padding:8px;border:1px solid #ddd;"><a href="{incident_url}" style="color:#007bff;text-decoration:none;">{incident_display}</a></td></tr>
            </table>
            <p style="margin-top:16px;color:#666;font-size:0.85em;">Sent by RAG Error Handling Framework — Escalation Engine</p>
        </div>
        </body></html>
        """

        msg = EmailMessage()
        msg["Subject"] = f"HIGH PRIORITY: {code} occurred {count}x in {Config.DB_DUPLICATE_WINDOW_MINUTES}min [{app_name}]"
        msg["From"] = Config.SMTP_USERNAME
        msg["To"] = to_email
        msg.set_content(f"HIGH PRIORITY: {code} for {app_name} occurred {count} times. Check HTML version.")
        msg.add_alternative(html_body, subtype="html")

        with smtplib.SMTP(Config.SMTP_HOST, Config.SMTP_PORT, timeout=Config.SMTP_TIMEOUT) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(Config.SMTP_USERNAME, Config.SMTP_PASSWORD)
            s.send_message(msg)

        logger.warning(f"HIGH-PRIORITY ALERT SENT: {app_name}/{code} ({count}x) → {to_email}")

    except Exception as e:
        logger.error(f"Failed to send high-priority alert: {e}")

def mark_email_read(message_id: str, id_url: str, access_token: str):
    """Mark email as read in Graph API"""
    # Specifically for /users/{id}/messages/{id} endpoint
    url = f"https://graph.microsoft.com/v1.0/users/{Config.AZURE_TARGET_EMAIL}/messages/{message_id}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    data = {'isRead': True}
    resp = requests.patch(url, headers=headers, json=data)
    if resp.status_code != 200:
        logger.warning(f"Failed to mark email read: {resp.status_code} - {resp.text}")

def process_email_cycle():
    """Main polling logic with metrics"""
    cycle_start = datetime.now()
    logger.info("Starting Email Poll Cycle (Client Credentials)...")

    # Optimization 5: Flush heartbeat frames to keep RabbitMQ connection alive
    keep_rabbitmq_alive()

    token = get_graph_token()
    if not token:
        logger.error("Could not obtain Graph Token. Skipping cycle.")
        _alert_notifier.notify_service_down(
            "Microsoft Graph API", "Failed to acquire access token (check Azure credentials)",
            context="process_email_cycle:get_graph_token"
        )
        return

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    # URL for accessing specific user's mail via Application Permissions
    url = f"https://graph.microsoft.com/v1.0/users/{Config.AZURE_TARGET_EMAIL}/messages"
    
    # Build time filter: only emails from the last POLL_INTERVAL_SECONDS seconds
    since_dt = datetime.now(timezone.utc).replace(microsecond=0)
    since_dt = since_dt - timedelta(seconds=Config.EMAIL_POLL_INTERVAL)
    since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO 8601 UTC for OData

    # FIX 1: Poll ALL emails (read + unread) in the 60s window
    # We do NOT filter isRead — the sliding time window prevents re-processing
    odata_filter = f"receivedDateTime ge {since_str}"

    params = {
        '$filter': odata_filter,
        '$top': 50,
        '$select': 'id,subject,body,receivedDateTime,from',
        '$orderby': 'receivedDateTime desc'
    }
    logger.info(f"Polling ALL emails (read+unread) since {since_str}")
    
    # Metrics
    processed = 0
    published = 0
    skipped_duplicate = 0
    skipped_invalid = 0
    
    try:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            err_msg = f"Graph API Error: {resp.status_code} - {resp.text[:300]}"
            logger.error(err_msg)
            _alert_notifier.notify_service_down(
                "Microsoft Graph API", err_msg, context="process_email_cycle:fetch_emails"
            )
            return
            
        emails = resp.json().get('value', [])
        if not emails:
            logger.info("No emails in the last poll window.")
            return

        # FIX 2: No batch deduplication — each email increments occurrence_count individually
        # This ensures 3 identical error emails correctly count as 3 occurrences
        # Filter by Sender (Python side)
        target_sender = Config.EMAIL_SENDER_FILTER.lower()
        filtered_emails = []

        for email in emails:
            sender = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
            if sender == target_sender:
                filtered_emails.append(email)
            else:
                logger.debug(f"Skipping email from '{sender}' (not {target_sender})")

        if not filtered_emails:
            logger.info(f"Found {len(emails)} emails, but NONE from {target_sender}.")
            return

        logger.info(f"Found {len(filtered_emails)} matching emails from {target_sender}")

        setup_rabbitmq_connection()

        # ── Optimization 3: Pre-parse all emails, then batch-fetch DB state in ONE query ──
        parsed_batch: List[Tuple[Dict, Dict, datetime]] = []
        for email in filtered_emails:
            body_content = email.get('body', {}).get('content', '')
            subject = email.get('subject', '')
            payload = parse_tibco_email(body_content, subject)

            if not all([payload.get('applicationName'), payload.get('code'), payload.get('description')]):
                logger.warning(f"⚠️ Skipping email with missing fields: {email.get('id')}")
                skipped_invalid += 1
                continue

            try:
                error_timestamp = datetime.fromtimestamp(payload['timestamp'], tz=timezone.utc)
            except (ValueError, OSError, TypeError) as e:
                logger.error(f"❌ Invalid timestamp {payload['timestamp']}: {e}")
                skipped_invalid += 1
                continue

            parsed_batch.append((email, payload, error_timestamp))

        if not parsed_batch:
            logger.info("No valid emails after parsing.")
            return

        # Pre-count occurrences of each (app, code, desc) key in this batch.
        # Used to publish a single message with the accumulated batch count for
        # brand-new errors, so the consumer inserts with the correct occurrence_count
        # instead of always hardcoding 1.
        batch_occurrence_counts: Dict[tuple, int] = {}
        for _, p, _ in parsed_batch:
            k = (p['applicationName'], p['code'], p['description'])
            batch_occurrence_counts[k] = batch_occurrence_counts.get(k, 0) + 1

        # ONE batch DB query for all error keys — Optimization 3
        all_keys = [(p['applicationName'], p['code'], p['description']) for _, p, _ in parsed_batch]
        all_ts = [
            ts.astimezone().replace(tzinfo=None) if ts.tzinfo else ts
            for _, _, ts in parsed_batch
        ]
        prefetched_counts = batch_fetch_occurrence_counts(all_keys, all_ts)
        logger.info(
            f"Batch pre-fetch: {len(prefetched_counts)} known errors, "
            f"{len(set(all_keys)) - len(prefetched_counts)} new"
        )

        # Within-cycle tracker: catches duplicates before consumer inserts to DB
        within_cycle_counts: Dict[tuple, int] = {}
        # Records the DB count at first-occurrence of each key this cycle.
        # 0 = brand-new error (batch count was encoded in published payload).
        # >0 = existing error (individual DB increments still needed).
        within_cycle_first_db_count: Dict[tuple, int] = {}

        for email, payload, error_timestamp in parsed_batch:
            try:
                # --- Within-cycle deduplication (catches same-batch duplicates) ---
                cycle_key = (payload['applicationName'], payload['code'], payload['description'])
                if cycle_key in within_cycle_counts:
                    within_cycle_counts[cycle_key] += 1
                    cycle_count = within_cycle_counts[cycle_key]
                    logger.info(f"⏭️ Same-cycle duplicate: {payload['code']} (#{cycle_count} this cycle)")

                    first_db_count = within_cycle_first_db_count.get(cycle_key, -1)

                    if first_db_count == 0:
                        # Brand-new error: batch count was already encoded in the
                        # single published message — no DB UPDATE needed here.
                        # Consumer will insert with the correct occurrence_count.
                        logger.debug(
                            f"Batch-counted dup for new error {payload['code']} "
                            f"— no individual DB update needed"
                        )
                    else:
                        # Existing error: individual DB increment still required.
                        new_db_count = check_occurrence_count(
                            payload['applicationName'], payload['code'],
                            payload['description'], error_timestamp
                        )
                        now_utc = datetime.now(timezone.utc)
                        # Bug 2 fix carried forward: check escalation for existing errors
                        if new_db_count >= Config.HIGH_PRIORITY_THRESHOLD:
                            error_key = (payload['applicationName'], payload['code'])
                            last_alert = escalation_cooldown.get(error_key)
                            cooldown_delta = timedelta(minutes=Config.ESCALATION_COOLDOWN_MINUTES)
                            if last_alert is None or (now_utc - last_alert) >= cooldown_delta:
                                logger.warning(
                                    f"ESCALATION TRIGGERED (within-cycle): "
                                    f"{payload['applicationName']}/{payload['code']} "
                                    f"occurred {new_db_count}x "
                                    f"(threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                                )
                                send_high_priority_alert(
                                    app_name=payload['applicationName'],
                                    code=payload['code'],
                                    description=payload['description'],
                                    count=new_db_count,
                                    timestamp=error_timestamp
                                )
                                escalation_cooldown[error_key] = now_utc
                            else:
                                minutes_since = (now_utc - last_alert).total_seconds() / 60
                                logger.info(
                                    f"Escalation cooldown active for {payload['code']} "
                                    f"({minutes_since:.1f}min ago, cooldown={Config.ESCALATION_COOLDOWN_MINUTES}min)"
                                )

                    skipped_duplicate += 1
                    continue

                # ── 3-Layer Decision ──────────────────────────────────────────────────
                # Layer 1: DB check — atomic UPDATE RETURNING
                count = check_occurrence_count(
                    payload['applicationName'], payload['code'],
                    payload['description'], error_timestamp
                )

                now_utc = datetime.now(timezone.utc)
                cache_ttl = timedelta(minutes=Config.DB_DUPLICATE_WINDOW_MINUTES)

                if count > 0:
                    # ✅ DB has the record — source of truth
                    # Clean up in-memory cache (consumer has inserted, cache no longer needed)
                    _published_cache.pop(cycle_key, None)

                elif count == 0:
                    # DB has no record yet — could be:
                    #   (a) Brand-new error  OR
                    #   (b) Async gap: we already published but consumer hasn't inserted yet

                    # Layer 2: Check in-memory published cache
                    cache_entry = _published_cache.get(cycle_key)
                    if cache_entry:
                        pub_time, mem_count = cache_entry
                        age = now_utc - pub_time

                        if age > cache_ttl:
                            # Cache entry expired — treat as a fresh new error
                            logger.info(
                                f"Cache expired for {payload['code']} "
                                f"({age.total_seconds()/60:.1f}min > {Config.DB_DUPLICATE_WINDOW_MINUTES}min TTL). "
                                f"Treating as new."
                            )
                            _published_cache.pop(cycle_key, None)
                            # count stays 0 → will publish below
                        else:
                            # ⚠️ Async gap duplicate — consumer hasn't inserted yet
                            mem_count += 1
                            _published_cache[cycle_key] = (pub_time, mem_count)
                            count = mem_count
                            logger.info(
                                f"Async gap duplicate: {payload['code']} "
                                f"(in-memory count={mem_count}, DB not yet updated by consumer)"
                            )

                # Register in cycle tracker + first-occurrence DB count
                within_cycle_counts[cycle_key] = max(count, 1)
                within_cycle_first_db_count[cycle_key] = count

                # ── Act on final count ────────────────────────────────────────────────
                if count == 0:
                    # Brand-new error — publish ONE message with the total batch
                    # occurrence count so the consumer inserts the correct value.
                    batch_count = batch_occurrence_counts.get(cycle_key, 1)
                    publish_payload = {**payload, 'occurrence_count': batch_count}
                    logger.info(f"✅ New error: {payload['applicationName']}/{payload['code']} (batch_count={batch_count})")
                    if rabbitmq_channel:
                        rabbitmq_channel.basic_publish(
                            exchange=Config.EXCHANGE,
                            routing_key=Config.ROUTING_KEY,
                            body=json.dumps(publish_payload),
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                                content_type='application/json'
                            )
                        )
                        _published_cache[cycle_key] = (now_utc, batch_count)
                        published += 1
                        logger.info(f"✅ Published: {payload['code']} | batch_count={batch_count} | Cached for async gap")
                        processed += 1
                        # Escalate immediately if batch itself crosses the threshold
                        if batch_count >= Config.HIGH_PRIORITY_THRESHOLD:
                            error_key = (payload['applicationName'], payload['code'])
                            last_alert = escalation_cooldown.get(error_key)
                            cooldown_delta = timedelta(minutes=Config.ESCALATION_COOLDOWN_MINUTES)
                            
                            # ITSM: open/escalate ServiceNow incident for high-priority burst
                            _itsm_key = f"{payload['applicationName']}_{payload['code']}"
                            incident_data = _incident_manager.handle_high_priority(
                                error_key=_itsm_key,
                                app_name=payload['applicationName'],
                                error_code=payload['code'],
                                description=payload['description'],
                                count=batch_count,
                            )
                            incident_display = incident_data.get("incident_display", "N/A") if incident_data else "N/A"
                            incident_url = incident_data.get("incident_url", "#") if incident_data else "#"

                            if last_alert is None or (now_utc - last_alert) >= cooldown_delta:
                                logger.warning(
                                    f"ESCALATION TRIGGERED (batch count={batch_count}): "
                                    f"{payload['applicationName']}/{payload['code']} "
                                    f"(threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                                )
                                send_high_priority_alert(
                                    app_name=payload['applicationName'],
                                    code=payload['code'],
                                    description=payload['description'],
                                    count=batch_count,
                                    timestamp=error_timestamp,
                                    incident_url=incident_url,
                                    incident_display=incident_display
                                )
                                escalation_cooldown[error_key] = now_utc
                    else:
                        logger.error(f"❌ RabbitMQ unavailable. Will retry next cycle.")

                elif count < Config.HIGH_PRIORITY_THRESHOLD:
                    # Known duplicate (DB or in-memory) — skip silently
                    logger.info(
                        f"⏭️ Duplicate: {payload['applicationName']}/{payload['code']} "
                        f"(seen {count}x, threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                    )
                    
                    # ITSM: update the existing ticket with recurrence note
                    _itsm_key = f"{payload['applicationName']}_{payload['code']}"
                    _incident_manager.handle_error(
                        error_key=_itsm_key,
                        app_name=payload['applicationName'],
                        error_code=payload['code'],
                        description=payload['description'],
                        count=count,
                    )

                    skipped_duplicate += 1

                else:
                    # count >= HIGH_PRIORITY_THRESHOLD — escalate!
                    error_key = (payload['applicationName'], payload['code'])
                    last_alert = escalation_cooldown.get(error_key)
                    cooldown_delta = timedelta(minutes=Config.ESCALATION_COOLDOWN_MINUTES)

                    # ITSM: always update to high-priority (has its own suppression logic)
                    _itsm_key = f"{payload['applicationName']}_{payload['code']}"
                    incident_data = _incident_manager.handle_high_priority(
                        error_key=_itsm_key,
                        app_name=payload['applicationName'],
                        error_code=payload['code'],
                        description=payload['description'],
                        count=count,
                    )
                    incident_display = incident_data.get("incident_display", "N/A") if incident_data else "N/A"
                    incident_url = incident_data.get("incident_url", "#") if incident_data else "#"

                    if last_alert is None or (now_utc - last_alert) >= cooldown_delta:
                        logger.warning(
                            f"ESCALATION TRIGGERED: {payload['applicationName']}/{payload['code']} "
                            f"occurred {count}x (threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                        )
                        send_high_priority_alert(
                            app_name=payload['applicationName'],
                            code=payload['code'],
                            description=payload['description'],
                            count=count,
                            timestamp=error_timestamp,
                            incident_url=incident_url,
                            incident_display=incident_display
                        )
                        escalation_cooldown[error_key] = now_utc
                    else:
                        minutes_since = (now_utc - last_alert).total_seconds() / 60
                        logger.info(
                            f"Escalation cooldown active for {payload['code']} "
                            f"({minutes_since:.1f}min ago, cooldown={Config.ESCALATION_COOLDOWN_MINUTES}min)"
                        )

                    skipped_duplicate += 1

            except Exception as e:
                logger.error(f"Failed to process email {email.get('id')}: {e}")

    except Exception as e:
        logger.error(f"Poll cycle failed: {e}")
    
    # Cycle metrics
    duration = (datetime.now() - cycle_start).total_seconds()
    logger.info(
        f"Cycle completed in {duration:.2f}s: "
        f"total={len(emails) if 'emails' in locals() else 0}, "
        f"filtered={len(filtered_emails) if 'filtered_emails' in locals() else 0}, "
        f"processed={processed}, published={published}, "
        f"skipped_duplicate={skipped_duplicate}, skipped_invalid={skipped_invalid}"
    )

def cleanup_and_exit():
    if scheduler and scheduler.running:
        scheduler.shutdown()
    if rabbitmq_connection:
        rabbitmq_connection.close()
    sys.exit(0)

def signal_handler(signum, frame):
    cleanup_and_exit()

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Validation
    if not Config.AZURE_CLIENT_ID or not Config.AZURE_CLIENT_SECRET:
        logger.error("Missing Azure Credentials in Config.")
        sys.exit(1)
        
    if "your-tenant-id" in Config.AZURE_TENANT_ID:
         logger.warning("AZURE_TENANT_ID is set to default 'your-tenant-id'. Connection may fail.")

    logger.info(f"Starting Email Extractor (Service Principal) for {Config.AZURE_TARGET_EMAIL}")
    
    scheduler = BlockingScheduler()
    # Runs every X seconds
    scheduler.add_job(process_email_cycle, 'interval', seconds=Config.EMAIL_POLL_INTERVAL)
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        cleanup_and_exit()
