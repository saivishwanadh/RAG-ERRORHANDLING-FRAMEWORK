import os
import json
import logging
import time
import signal
import sys
import re
import requests
import pika
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from apscheduler.schedulers.blocking import BlockingScheduler

from src.config import Config
from src.structuraldb import DB
from src.service_alert import ServiceAlertNotifier
from src.incident_manager import IncidentManager

# Module-level incident manager (disabled when ITSM_PROVIDER=none)
_incident_manager = IncidentManager()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

# RabbitMQ persistent connection
rabbitmq_connection: Optional[pika.BlockingConnection] = None
rabbitmq_channel: Optional[pika.channel.Channel] = None

# APScheduler instance
scheduler: Optional[BlockingScheduler] = None

# Persistent PostgreSQL connection — reused across all poll cycles
_db_conn: Optional[psycopg2.extensions.connection] = None

# In-memory escalation cooldown tracker: { (app_name, error_code) -> last_alert_datetime }
escalation_cooldown: Dict[tuple, datetime] = {}

# Cross-cycle async-gap cache: { (app_name, code, desc) -> (first_published_at UTC, count) }
# Bridges the window between "published to RabbitMQ" and "consumer inserts DB record".
# Prevents re-publishing the same error while the consumer is still processing it.
_published_cache: Dict[tuple, Tuple[datetime, int]] = {}

# Rolling set of OpenSearch document IDs that have already been published this window.
# Keyed by doc _id, value is the UTC datetime when we first saw it.
# Entries expire after DB_DUPLICATE_WINDOW_MINUTES (same TTL as other dedup layers).
_seen_doc_ids: Dict[str, datetime] = {}

# Module-level service alert notifier (shared across all poll cycles)
_alert_notifier: ServiceAlertNotifier = ServiceAlertNotifier()


# ---------------------------------------------------------------------------
# Persistent PostgreSQL connection helpers
# ---------------------------------------------------------------------------

def get_persistent_db() -> psycopg2.extensions.connection:
    """Return the module-level persistent DB connection, reconnecting if closed."""
    global _db_conn
    try:
        if _db_conn is None or _db_conn.closed:
            _db_conn = psycopg2.connect(Config.DB_URL)
            logger.info("✅ Persistent DB connection established")
    except Exception as e:
        logger.error(f"Failed to establish persistent DB connection: {e}")
        _alert_notifier.notify_service_down(
            "PostgreSQL/DB", str(e), context="get_persistent_db"
        )
        _db_conn = None
        raise
    return _db_conn


# ---------------------------------------------------------------------------
# RabbitMQ connection helpers (identical pattern to email-extract-app)
# ---------------------------------------------------------------------------

def setup_rabbitmq_connection():
    """
    Setup persistent RabbitMQ connection with heartbeat.
    - Reuses existing connection/channel if both healthy.
    - Recreates channel if connection is alive but channel closed.
    - Full reconnect if connection is dead.
    """
    global rabbitmq_connection, rabbitmq_channel

    try:
        conn_alive = (
            rabbitmq_connection is not None
            and not rabbitmq_connection.is_closed
        )
        channel_alive = (
            conn_alive
            and rabbitmq_channel is not None
            and rabbitmq_channel.is_open
        )

        if channel_alive:
            return  # Both healthy — nothing to do

        if conn_alive and not channel_alive:
            # Connection alive but channel dead — recreate channel only
            logger.info("RabbitMQ: connection alive, recreating channel...")
            rabbitmq_channel = rabbitmq_connection.channel()
            rabbitmq_channel.exchange_declare(
                exchange=Config.EXCHANGE,
                exchange_type=Config.EXCHANGE_TYPE,
                durable=True
            )
            rabbitmq_channel.queue_declare(queue=Config.QUEUE, durable=True)
            logger.info("RabbitMQ channel restored")
            return

        # Full reconnect needed
        logger.info("Establishing new RabbitMQ connection...")
        params = pika.URLParameters(Config.RABBIT_URL)
        params.heartbeat = 120                  # Heartbeat > poll interval
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
        logger.info("✅ RabbitMQ connection established (heartbeat=120s)")

    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        _alert_notifier.notify_service_down(
            "RabbitMQ", str(e), context="setup_rabbitmq_connection"
        )
        rabbitmq_connection = None
        rabbitmq_channel = None


def keep_rabbitmq_alive():
    """
    Flush pending heartbeat frames to keep the connection alive between cycles.
    Call once at the start of each poll cycle.
    """
    global rabbitmq_connection, rabbitmq_channel
    try:
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            rabbitmq_connection.process_data_events(time_limit=0)  # Non-blocking
    except Exception as e:
        logger.warning(f"RabbitMQ heartbeat flush failed: {e}. Will reconnect on next publish.")
        rabbitmq_connection = None
        rabbitmq_channel = None


# ---------------------------------------------------------------------------
# OpenSearch REST query helper
# ---------------------------------------------------------------------------

def build_opensearch_query(since_dt: datetime) -> Dict[str, Any]:
    """
    Build an OpenSearch bool query that fetches ERROR-level logs
    received after `since_dt`.

    Args:
        since_dt: UTC datetime lower bound for es_time filter.

    Returns:
        dict payload ready to POST to OpenSearch _search endpoint.
    """
    since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    query: Dict[str, Any] = {
        "query": {
            "bool": {
                "filter": [
                    # Only ERROR level logs
                    {"term": {"loglevel": Config.OPENSEARCH_LOG_LEVEL_FILTER}},
                    # Time window — only new logs since last poll
                    {"range": {"es_time": {"gte": since_str}}}
                ]
            }
        },
        "size": Config.OPENSEARCH_BATCH_SIZE,
        "sort": [{"es_time": {"order": "asc"}}]
    }
    return query


def fetch_opensearch_logs(since_dt: datetime) -> List[Dict[str, Any]]:
    """
    Execute a REST POST to OpenSearch and return the list of hit documents.

    Supports three auth modes (configured via env):
      1. API Key  (OPENSEARCH_APIKEY is set)
      2. Basic Auth (OPENSEARCH_USERNAME + OPENSEARCH_PASSWORD are set)
      3. No auth (local unauthenticated cluster)

    Args:
        since_dt: Lower bound for es_time range filter.

    Returns:
        List of raw OpenSearch hit dicts (each has _id, _source, etc.)
    """
    url = f"{Config.OPENSEARCH_URL}/{Config.OPENSEARCH_INDEX}/_search"
    headers = {"Content-Type": "application/json"}
    auth = None

    # Auth selection
    if Config.OPENSEARCH_APIKEY:
        headers["Authorization"] = f"ApiKey {Config.OPENSEARCH_APIKEY}"
        logger.debug("OpenSearch auth: API Key")
    elif Config.OPENSEARCH_USERNAME and Config.OPENSEARCH_PASSWORD:
        auth = (Config.OPENSEARCH_USERNAME, Config.OPENSEARCH_PASSWORD)
        logger.debug("OpenSearch auth: Basic Auth")
    else:
        logger.debug("OpenSearch auth: None (open cluster)")

    query_body = build_opensearch_query(since_dt)

    try:
        resp = requests.post(
            url,
            headers=headers,
            json=query_body,
            auth=auth,
            timeout=Config.OPENSEARCH_TIMEOUT,
            verify=Config.OPENSEARCH_VERIFY_SSL
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {}).get("value", 0)
        logger.info(
            f"OpenSearch query returned {len(hits)}/{total} hits "
            f"(timed_out={data.get('timed_out', False)})"
        )
        return hits

    except requests.exceptions.ConnectionError as e:
        logger.error(f"OpenSearch connection error — is the cluster running? {e}")
        _alert_notifier.notify_service_down(
            "OpenSearch", str(e), context="fetch_opensearch_logs:ConnectionError"
        )
        return []
    except requests.exceptions.Timeout:
        msg = f"OpenSearch query timed out after {Config.OPENSEARCH_TIMEOUT}s"
        logger.error(msg)
        _alert_notifier.notify_service_down(
            "OpenSearch", msg, context="fetch_opensearch_logs:Timeout"
        )
        return []
    except requests.exceptions.HTTPError as e:
        err_text = f"OpenSearch HTTP error: {e.response.status_code} — {e.response.text[:300]}"
        logger.error(err_text)
        _alert_notifier.notify_service_down(
            "OpenSearch", err_text, context="fetch_opensearch_logs:HTTPError"
        )
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching OpenSearch logs: {e}")
        _alert_notifier.notify_service_down(
            "OpenSearch", str(e), context="fetch_opensearch_logs:unexpected"
        )
        return []


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------

def extract_error_code_from_log(log_line: str) -> str:
    """
    Extract a meaningful error code from the raw log string.

    Strategy (in priority order):
      1.  Custom regex from OPENSEARCH_ERROR_CODE_REGEX env var (if set).
      2.  Pattern: "ERROR - <ClassName> " → use ClassName as the code.
          e.g. "ERROR - DataEndpointConnectionWorker Error while..."
               → "DataEndpointConnectionWorker"
      3.  Pattern: [ClassName] bracket notation.
      4.  Fallback → "UNKNOWN_ERROR"
    """
    if not log_line:
        return "UNKNOWN_ERROR"

    # Priority 1: user-supplied custom regex
    custom_regex = Config.OPENSEARCH_ERROR_CODE_REGEX
    if custom_regex:
        m = re.search(custom_regex, log_line)
        if m:
            return m.group(1) if m.lastindex else m.group(0)

    # Priority 2: "ERROR - <Word> " pattern (class/component name after the dash)
    m = re.search(r'ERROR\s+-\s+([A-Za-z][A-Za-z0-9_$]+)', log_line)
    if m:
        return m.group(1)

    # Priority 3: [ClassName] bracket notation
    m = re.search(r'\[([A-Za-z][A-Za-z0-9_$.]+)\]', log_line)
    if m:
        return m.group(1)

    return "UNKNOWN_ERROR"


def parse_opensearch_log(hit: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a single OpenSearch hit document into the standard error payload
    format consumed by the RAG pipeline.

    Expected _source structure (from the sample):
    {
        "kubernetes": {
            "container_name": "wso2am-gateway",
            "pod_id": "8834675d-...",
            "namespace_name": "wso2am4x-uat",
            "pod_name": "wso2am-gateway-internal-deployment-...",
            "host": "usadlskub020"
        },
        "log": "[2026-02-25 14:18:58,397] ERROR - DataEndpointConnectionWorker ...",
        "loglevel": "ERROR",
        "time": "2026-02-25T14:18:58.398882923Z",
        "es_time": "2026-02-25T14:18:58.398Z",
        "datacenter": "usdc-east",
        "stream": "stdout"
    }

    Returns:
        Standardised payload dict, or None if parsing fails or log is not an error.
    """
    try:
        doc_id = hit.get("_id", "UNKNOWN")
        source = hit.get("_source", {})

        # Guard: only process ERROR-level logs (belt-and-suspenders — query already filters)
        if source.get("loglevel", "").upper() != "ERROR":
            return None

        # --- Application name ---
        # Prefer namespace_name as it's the logical grouping; fall back to container_name
        k8s = source.get("kubernetes", {})
        app_name = (
            k8s.get("namespace_name")
            or k8s.get("container_name")
            or "UNKNOWN_APP"
        )

        # --- Correlation ID ---
        # pod_id is a stable, meaningful correlation handle
        correlation_id = k8s.get("pod_id") or k8s.get("pod_name") or doc_id

        # --- Error code ---
        log_line = source.get("log", "")
        error_code = extract_error_code_from_log(log_line)

        # --- Description — full log line ---
        description = log_line.strip() or "No description"

        # Optionally enrich description with k8s context
        if Config.OPENSEARCH_ENRICH_DESCRIPTION:
            extras = []
            if k8s.get("host"):
                extras.append(f"Host: {k8s['host']}")
            if k8s.get("container_image"):
                extras.append(f"Image: {k8s['container_image']}")
            if k8s.get("datacenter") or source.get("datacenter"):
                extras.append(f"Datacenter: {k8s.get('datacenter') or source.get('datacenter')}")
            if extras:
                description = description + " | " + " | ".join(extras)

        # --- Timestamp ---
        raw_ts = source.get("es_time") or source.get("time", "")
        try:
            # Handle nanosecond precision: "2026-02-25T14:18:58.398882923Z"
            # Trim to microseconds so fromisoformat can parse it
            ts_clean = re.sub(r'(\.\d{6})\d+(Z)$', r'\1\2', raw_ts)
            ts_clean = ts_clean.replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts_clean)
            timestamp = dt.timestamp()
        except Exception:
            logger.warning(f"Could not parse timestamp '{raw_ts}' for doc {doc_id}; using now()")
            timestamp = datetime.now(timezone.utc).timestamp()

        return {
            "applicationName": app_name,
            "correlationId": correlation_id,
            "code": error_code,
            "description": description,
            "timestamp": timestamp,
            "source": "opensearch",
            "_doc_id": doc_id          # Internal field — stripped before publishing
        }

    except Exception as e:
        logger.error(f"Failed to parse OpenSearch hit {hit.get('_id', '?')}: {e}")
        return None


# ---------------------------------------------------------------------------
# Seen-ID tracker (replaces "mark email as read")
# ---------------------------------------------------------------------------

def is_seen_doc(doc_id: str) -> bool:
    """Return True if this document has already been published this window."""
    return doc_id in _seen_doc_ids


def mark_doc_seen(doc_id: str):
    """Record that this document has been published."""
    _seen_doc_ids[doc_id] = datetime.now(timezone.utc)


def evict_expired_seen_ids():
    """
    Remove stale entries from _seen_doc_ids.
    Call once per cycle to prevent unbounded memory growth.
    TTL = DB_DUPLICATE_WINDOW_MINUTES (same as the DB dedup window).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=Config.DB_DUPLICATE_WINDOW_MINUTES)
    expired = [k for k, v in _seen_doc_ids.items() if v < cutoff]
    for k in expired:
        del _seen_doc_ids[k]
    if expired:
        logger.debug(f"Evicted {len(expired)} expired doc IDs from seen-ID cache")


# ---------------------------------------------------------------------------
# Deduplication — DB layer (shared logic with email extractor)
# ---------------------------------------------------------------------------

def check_occurrence_count(app_name: str, code: str, desc: str, timestamp: datetime) -> int:
    """
    Atomic UPDATE...RETURNING to increment occurrence_count if a matching record
    exists within DB_DUPLICATE_WINDOW_MINUTES.

    Returns:
        new occurrence_count if record found (>= 1), or 0 if brand-new error.
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
            logger.debug(f"No existing DB record for {app_name}/{code} — new error")
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
        _db_conn = None   # Force reconnect next call
        return 0          # Fail-open: treat as new error


def batch_fetch_occurrence_counts(
    error_keys: List[Tuple[str, str, str]],
    local_timestamps: List[datetime]
) -> Dict[Tuple[str, str, str], int]:
    """
    Pre-fetch occurrence counts for ALL logs in ONE query.
    Returns { (app_name, code, description) -> current occurrence_count }

    Reduces N per-log DB queries to a single batch SELECT at cycle start.
    """
    if not error_keys:
        return {}
    global _db_conn
    try:
        conn = get_persistent_db()
        min_ts = min(local_timestamps)

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

        result: Dict[Tuple[str, str, str], int] = {}
        for row in rows:
            key = (row['application_name'], row['error_code'], row['error_description'])
            if key not in result:   # Already ordered DESC, first hit is most recent
                result[key] = int(row['occurrence_count'] or 1)

        logger.debug(f"Batch pre-fetch: found {len(result)}/{len(error_keys)} errors in DB")
        return result

    except Exception as e:
        logger.error(f"Batch fetch occurrence counts failed: {e}")
        _db_conn = None
        return {}   # Fail-open: treat all as new


# ---------------------------------------------------------------------------
# High-priority escalation (identical to email-extract-app)
# ---------------------------------------------------------------------------

def send_high_priority_alert(
    app_name: str, code: str, description: str, count: int, timestamp: datetime
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
            </table>
            <p style="margin-top:16px;color:#666;font-size:0.85em;">Sent by Opssolver Engine</p>
        </div>
        </body></html>
        """

        msg = EmailMessage()
        msg["Subject"] = (
            f"HIGH PRIORITY: {code} occurred {count}x "
            f"in {Config.DB_DUPLICATE_WINDOW_MINUTES}min [{app_name}] (OpenSearch)"
        )
        msg["From"] = Config.SMTP_USERNAME
        msg["To"] = to_email
        msg.set_content(
            f"HIGH PRIORITY: {code} for {app_name} occurred {count} times. "
            f"Check HTML version."
        )
        msg.add_alternative(html_body, subtype="html")

        with smtplib.SMTP(Config.SMTP_HOST, Config.SMTP_PORT, timeout=Config.SMTP_TIMEOUT) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(Config.SMTP_USERNAME, Config.SMTP_PASSWORD)
            s.send_message(msg)

        logger.warning(
            f"HIGH-PRIORITY ALERT SENT (OpenSearch): "
            f"{app_name}/{code} ({count}x) → {to_email}"
        )

    except Exception as e:
        logger.error(f"Failed to send high-priority alert: {e}")


# ---------------------------------------------------------------------------
# Main poll cycle
# ---------------------------------------------------------------------------

def process_opensearch_cycle():
    """
    Main polling loop executed every POLL_INTERVAL_SECONDS by APScheduler.

    Steps:
      1. Flush RabbitMQ heartbeat frames.
      2. Calculate the time window lower bound (now - POLL_INTERVAL_SECONDS).
      3. Evict stale entries from the seen-doc-ID cache.
      4. Fetch ERROR-level hits from OpenSearch.
      5. Parse each hit into the standard error payload.
      6. Batch-fetch occurrence counts from PostgreSQL in one query.
      7. Per-doc 4-layer deduplication:
           Layer 0 — doc-ID cache
           Layer 1 — DB atomic UPDATE...RETURNING
           Layer 2 — in-memory _published_cache (async-gap bridge)
           Layer 3 — within-cycle counter
      8. Publish new errors to RabbitMQ; escalate high-occurrence errors.
      9. Log cycle metrics.
    """
    cycle_start = datetime.now()
    logger.info("▶️  Starting OpenSearch Poll Cycle...")

    # Step 1: Keep RabbitMQ alive between cycles
    keep_rabbitmq_alive()

    # Step 2: Build time window lower bound
    since_dt = datetime.now(timezone.utc) - timedelta(seconds=Config.POLL_INTERVAL_SECONDS)

    # Step 3: Evict expired doc IDs from seen cache
    evict_expired_seen_ids()

    # Step 4: Fetch hits from OpenSearch
    hits = fetch_opensearch_logs(since_dt)
    if not hits:
        logger.info("No ERROR-level logs returned from OpenSearch this cycle.")
        duration = (datetime.now() - cycle_start).total_seconds()
        logger.info(f"Cycle completed in {duration:.2f}s: 0 hits.")
        return

    # Cycle metrics — initialised here so all sub-loops can safely increment them
    processed = 0
    published = 0
    skipped_duplicate = 0
    skipped_invalid = 0

    # Step 5: Parse all hits into standard payloads (skip Layer-0 doc-ID dupes here)
    parsed_batch: List[Tuple[str, Dict[str, Any], datetime]] = []
    for hit in hits:
        doc_id = hit.get("_id", "UNKNOWN")

        # Layer 0: doc-ID dedup
        if is_seen_doc(doc_id):
            logger.debug(f"⏭️ Already seen doc_id={doc_id}; skipping.")
            continue

        payload = parse_opensearch_log(hit)
        if payload is None:
            continue

        # Strip internal _doc_id before publishing
        payload.pop("_doc_id", None)

        # Fix 1: Guard — skip docs with missing required fields (mirrors email-extract-app)
        if not all([payload.get("applicationName"), payload.get("code"), payload.get("description")]):
            logger.warning(f"⚠️ Skipping doc {doc_id} with missing required fields")
            skipped_invalid += 1
            continue

        try:
            error_timestamp = datetime.fromtimestamp(payload["timestamp"], tz=timezone.utc)
        except (ValueError, OSError, TypeError) as e:
            logger.error(f"❌ Invalid timestamp for doc {doc_id}: {e}")
            # Fix 2: Increment skipped_invalid on timestamp failure (mirrors email-extract-app)
            skipped_invalid += 1
            continue

        parsed_batch.append((doc_id, payload, error_timestamp))

    if not parsed_batch:
        logger.info("No new (unseen) valid ERROR docs after Layer-0 filter.")
        duration = (datetime.now() - cycle_start).total_seconds()
        logger.info(f"Cycle completed in {duration:.2f}s: {len(hits)} hits, 0 new.")
        return

    # Pre-count occurrences of each (app, code, desc) key in this batch.
    # Used to publish a single message with the accumulated batch count for
    # brand-new errors, so the consumer inserts with the correct occurrence_count
    # instead of always hardcoding 1.
    batch_occurrence_counts: Dict[tuple, int] = {}
    for _, p, _ in parsed_batch:
        k = (p["applicationName"], p["code"], p["description"])
        batch_occurrence_counts[k] = batch_occurrence_counts.get(k, 0) + 1

    # Step 6: Batch-fetch DB occurrence counts in ONE query
    all_keys = [
        (p["applicationName"], p["code"], p["description"])
        for _, p, _ in parsed_batch
    ]
    all_ts = [
        ts.astimezone().replace(tzinfo=None) if ts.tzinfo else ts
        for _, _, ts in parsed_batch
    ]
    prefetched_counts = batch_fetch_occurrence_counts(all_keys, all_ts)
    logger.info(
        f"Batch pre-fetch: {len(prefetched_counts)} known errors, "
        f"{len(set(all_keys)) - len(prefetched_counts)} new"
    )

    # Step 7 & 8: Per-doc processing
    setup_rabbitmq_connection()

    # Within-cycle tracker — catches same-batch duplicate log lines
    within_cycle_counts: Dict[tuple, int] = {}
    # Records the DB count at first-occurrence of each key this cycle.
    # 0 = brand-new error (batch count was encoded in published payload).
    # >0 = existing error (individual DB increments still needed).
    within_cycle_first_db_count: Dict[tuple, int] = {}

    for doc_id, payload, error_timestamp in parsed_batch:
        try:
            cycle_key = (payload["applicationName"], payload["code"], payload["description"])

            # Layer 3: Within-cycle deduplication
            if cycle_key in within_cycle_counts:
                within_cycle_counts[cycle_key] += 1
                cycle_count = within_cycle_counts[cycle_key]
                logger.info(
                    f"⏭️ Same-cycle duplicate: {payload['code']} "
                    f"(#{cycle_count} this cycle)"
                )

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
                        payload["applicationName"], payload["code"],
                        payload["description"], error_timestamp
                    )
                    now_utc = datetime.now(timezone.utc)
                    # Bug 2 fix carried forward: check escalation for existing errors
                    if new_db_count >= Config.HIGH_PRIORITY_THRESHOLD:
                        error_key = (payload["applicationName"], payload["code"])
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
                                app_name=payload["applicationName"],
                                code=payload["code"],
                                description=payload["description"],
                                count=new_db_count,
                                timestamp=error_timestamp
                            )
                            escalation_cooldown[error_key] = now_utc
                        else:
                            minutes_since = (now_utc - last_alert).total_seconds() / 60
                            logger.info(
                                f"Escalation cooldown active for {payload['code']} "
                                f"({minutes_since:.1f}min ago, "
                                f"cooldown={Config.ESCALATION_COOLDOWN_MINUTES}min)"
                            )

                skipped_duplicate += 1
                continue





            # Layer 1: DB atomic UPDATE...RETURNING  f
            count = check_occurrence_count(
                payload["applicationName"], payload["code"],
                payload["description"], error_timestamp
            )

            now_utc = datetime.now(timezone.utc)
            cache_ttl = timedelta(minutes=Config.DB_DUPLICATE_WINDOW_MINUTES)

            if count > 0:
                # DB already has the record — consumer has processed it
                # Clean up in-memory cache (no longer needed as async bridge)
                _published_cache.pop(cycle_key, None)

            elif count == 0:
                # DB has no record yet — could be brand-new OR an async gap

                # Layer 2: In-memory published-cache check
                cache_entry = _published_cache.get(cycle_key)
                if cache_entry:
                    pub_time, mem_count = cache_entry
                    age = now_utc - pub_time

                    if age > cache_ttl:
                        # Cache expired — treat as fresh new error
                        logger.info(
                            f"Cache expired for {payload['code']} "
                            f"({age.total_seconds() / 60:.1f}min > "
                            f"{Config.DB_DUPLICATE_WINDOW_MINUTES}min TTL). "
                            f"Treating as new."
                        )
                        _published_cache.pop(cycle_key, None)
                        # count stays 0 → will publish below
                    else:
                        # Async gap duplicate — consumer hasn't inserted yet
                        mem_count += 1
                        _published_cache[cycle_key] = (pub_time, mem_count)
                        count = mem_count
                        logger.info(
                            f"Async gap duplicate: {payload['code']} "
                            f"(in-memory count={mem_count}, DB not yet updated)"
                        )

            # Register in cycle tracker + first-occurrence DB count
            within_cycle_counts[cycle_key] = max(count, 1)
            within_cycle_first_db_count[cycle_key] = count

            # ── Act on final count ────────────────────────────────────────────
            if count == 0:
                # Brand-new error — publish ONE message with the total batch
                # occurrence count so the consumer inserts the correct value.
                batch_count = batch_occurrence_counts.get(cycle_key, 1)
                publish_payload = {**payload, "occurrence_count": batch_count}
                logger.info(
                    f"✅ New error: {payload['applicationName']}/{payload['code']} "
                    f"(source=opensearch, doc_id={doc_id}, batch_count={batch_count})"
                )
                if rabbitmq_channel:
                    rabbitmq_channel.basic_publish(
                        exchange=Config.EXCHANGE,
                        routing_key=Config.ROUTING_KEY,
                        body=json.dumps(publish_payload),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type="application/json"
                        )
                    )
                    mark_doc_seen(doc_id)
                    _published_cache[cycle_key] = (now_utc, batch_count)
                    published += 1
                    processed += 1
                    logger.info(
                        f"✅ Published: {payload['code']} | batch_count={batch_count} | "
                        f"doc_id cached for dedup, payload cached for async gap"
                    )
                    # Escalate immediately if batch itself crosses the threshold
                    if batch_count >= Config.HIGH_PRIORITY_THRESHOLD:
                        error_key = (payload["applicationName"], payload["code"])
                        last_alert = escalation_cooldown.get(error_key)
                        cooldown_delta = timedelta(minutes=Config.ESCALATION_COOLDOWN_MINUTES)
                        if last_alert is None or (now_utc - last_alert) >= cooldown_delta:
                            logger.warning(
                                f"ESCALATION TRIGGERED (batch count={batch_count}): "
                                f"{payload['applicationName']}/{payload['code']} "
                                f"(threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                            )
                            send_high_priority_alert(
                                app_name=payload["applicationName"],
                                code=payload["code"],
                                description=payload["description"],
                                count=batch_count,
                                timestamp=error_timestamp
                            )
                            # ITSM: open/escalate ServiceNow incident for high-priority burst
                            _itsm_key = f"{payload['applicationName']}_{payload['code']}"
                            _incident_manager.handle_high_priority(
                                error_key=_itsm_key,
                                app_name=payload["applicationName"],
                                error_code=payload["code"],
                                description=payload["description"],
                                count=batch_count,
                            )
                            escalation_cooldown[error_key] = now_utc
                else:
                    logger.error("❌ RabbitMQ channel unavailable. Will retry next cycle.")

            elif count < Config.HIGH_PRIORITY_THRESHOLD:
                # Known duplicate — below threshold, skip email/queue publish
                logger.info(
                    f"Duplicate: {payload['applicationName']}/{payload['code']} "
                    f"(seen {count}x, threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                )
                # ITSM: update the existing ticket with recurrence note
                # (no email, no queue publish — just keep the ITSM ticket fresh)
                _itsm_key = f"{payload['applicationName']}_{payload['code']}"
                _incident_manager.handle_error(
                    error_key=_itsm_key,
                    app_name=payload["applicationName"],
                    error_code=payload["code"],
                    description=payload["description"],
                    count=count,
                )
                skipped_duplicate += 1

            else:
                # count >= HIGH_PRIORITY_THRESHOLD — escalate!
                error_key = (payload["applicationName"], payload["code"])
                last_alert = escalation_cooldown.get(error_key)
                cooldown_delta = timedelta(minutes=Config.ESCALATION_COOLDOWN_MINUTES)

                # ITSM: always update to high-priority (has its own suppression logic)
                _itsm_key = f"{payload['applicationName']}_{payload['code']}"
                _incident_manager.handle_high_priority(
                    error_key=_itsm_key,
                    app_name=payload["applicationName"],
                    error_code=payload["code"],
                    description=payload["description"],
                    count=count,
                )

                if last_alert is None or (now_utc - last_alert) >= cooldown_delta:
                    logger.warning(
                        f"ESCALATION TRIGGERED: "
                        f"{payload['applicationName']}/{payload['code']} "
                        f"occurred {count}x (threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                    )
                    send_high_priority_alert(
                        app_name=payload["applicationName"],
                        code=payload["code"],
                        description=payload["description"],
                        count=count,
                        timestamp=error_timestamp
                    )
                    escalation_cooldown[error_key] = now_utc
                else:
                    minutes_since = (now_utc - last_alert).total_seconds() / 60
                    logger.info(
                        f"Escalation email cooldown active for {payload['code']} "
                        f"({minutes_since:.1f}min ago, "
                        f"cooldown={Config.ESCALATION_COOLDOWN_MINUTES}min)"
                    )

                skipped_duplicate += 1

        except Exception as e:
            logger.error(f"Failed to process doc {doc_id}: {e}")

    # Step 9: Cycle metrics (aligned with email-extract-app.py format)
    duration = (datetime.now() - cycle_start).total_seconds()
    logger.info(
        f"Cycle completed in {duration:.2f}s: "
        f"total_hits={len(hits)}, parsed={len(parsed_batch)}, "
        f"processed={processed}, published={published}, "
        f"skipped_duplicate={skipped_duplicate}, skipped_invalid={skipped_invalid}"
    )


# ---------------------------------------------------------------------------
# Graceful shutdown helpers
# ---------------------------------------------------------------------------

def cleanup_and_exit():
    """Gracefully shut down the scheduler and RabbitMQ connection before exiting."""
    logger.info("🛑 Shutting down OpenSearch extractor...")
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        try:
            rabbitmq_connection.close()
        except Exception:
            pass
    sys.exit(0)


def signal_handler(signum, frame):
    """Handle SIGTERM and SIGINT for graceful shutdown."""
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    cleanup_and_exit()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Startup validation
    if not Config.OPENSEARCH_URL:
        logger.error("OPENSEARCH_URL is not configured. Cannot start.")
        sys.exit(1)

    if not Config.OPENSEARCH_INDEX:
        logger.error("OPENSEARCH_INDEX is not configured. Cannot start.")
        sys.exit(1)

    logger.info(
        f"Starting OpenSearch Extractor | "
        f"index={Config.OPENSEARCH_INDEX} | "
        f"poll_interval={Config.POLL_INTERVAL_SECONDS}s | "
        f"dedup_window={Config.DB_DUPLICATE_WINDOW_MINUTES}min | "
        f"escalation_threshold={Config.HIGH_PRIORITY_THRESHOLD}"
    )

    # Initial connection setup at startup (fail-fast rather than silent)
    try:
        setup_rabbitmq_connection()
        logger.info("✅ Initial RabbitMQ connection ready")
    except Exception as e:
        logger.warning(f"⚠️ Initial RabbitMQ connection failed: {e}. Will retry per cycle.")

    try:
        get_persistent_db()
        logger.info("✅ Initial DB connection ready")
    except Exception as e:
        logger.warning(f"⚠️ Initial DB connection failed: {e}. Will retry per cycle.")

    scheduler = BlockingScheduler()
    scheduler.add_job(
        process_opensearch_cycle,
        "interval",
        seconds=Config.POLL_INTERVAL_SECONDS,
        max_instances=1,        # Prevent overlapping cycles if one runs long
        coalesce=True           # Collapse missed executions into a single run
    )

    logger.info(
        f"⏱️  Scheduler configured — running every {Config.POLL_INTERVAL_SECONDS}s. "
        f"Press CTRL+C to stop."
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        cleanup_and_exit()