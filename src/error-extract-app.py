import os
import json
import logging
import signal
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import pika

from src.config import Config
from src.service_alert import ServiceAlertNotifier

# Setup logging
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

rabbitmq_connection: Optional[pika.BlockingConnection] = None
rabbitmq_channel: Optional[pika.channel.Channel] = None
scheduler: Optional[BlockingScheduler] = None
http_session: Optional[requests.Session] = None

# Persistent PostgreSQL connection — reused across all poll cycles
_db_conn: Optional[psycopg2.extensions.connection] = None

# In-memory escalation cooldown tracker: { (app_name, error_code) -> last_alert_datetime }
escalation_cooldown: Dict[tuple, datetime] = {}

# Cross-cycle async-gap cache: { (app_name, code, desc) -> (first_published_at UTC, count) }
# Bridges the window between "published to RabbitMQ" and "consumer inserts DB record".
_published_cache: Dict[tuple, Tuple[datetime, int]] = {}

# Layer 0: Rolling set of ELK document IDs already published this window.
# Keyed by doc _id, value is the UTC datetime when we first saw it.
# Entries expire after DB_DUPLICATE_WINDOW_MINUTES.
_seen_elk_ids: Dict[str, datetime] = {}

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
# RabbitMQ connection helpers
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
            rabbitmq_channel.queue_bind(Config.QUEUE, Config.EXCHANGE, Config.ROUTING_KEY)
            logger.info("RabbitMQ channel restored")
            return

        # Full reconnect needed
        logger.info("Establishing new RabbitMQ connection...")
        params = pika.URLParameters(Config.RABBIT_URL)
        params.heartbeat = 120
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
        rabbitmq_channel.queue_bind(Config.QUEUE, Config.EXCHANGE, Config.ROUTING_KEY)
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
# HTTP session (for ELK REST calls)
# ---------------------------------------------------------------------------

def setup_http_session() -> requests.Session:
    """Create HTTP session with retry logic"""
    global http_session
    if http_session is None:
        session = requests.Session()
        retry_strategy = Retry(
            total=Config.HTTP_RETRIES,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=Config.HTTP_POOL_SIZE,
            pool_maxsize=Config.HTTP_POOL_SIZE
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        http_session = session
        logger.info("HTTP session created with retry logic")
    return http_session


# ---------------------------------------------------------------------------
# ELK query helpers
# ---------------------------------------------------------------------------

def build_elk_query(since_dt: datetime) -> Dict[str, Any]:
    """Build ELK query for ERROR logs received after `since_dt`."""
    since_epoch = int(since_dt.timestamp())
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    return {
        "query": {
            "bool": {
                "must": [
                    {"match": {"level": "ERROR"}},
                    {
                        "range": {
                            "instant.epochSecond": {
                                "gte": since_epoch,
                                "lte": now_epoch,
                            }
                        }
                    },
                ]
            }
        },
        "size": 1000,
        "sort": [{"instant.epochSecond": "asc"}]
    }


def fetch_elk_logs(since_dt: datetime) -> List[Dict[str, Any]]:
    """
    Execute a REST POST to ELK and return the list of hit documents.
    Returns list of raw ELK hit dicts (each has _id, _source, etc.)
    """
    session = setup_http_session()
    headers = {
        "Authorization": Config.ELK_APIKEY,
        "Content-Type": "application/json"
    }
    query_body = build_elk_query(since_dt)

    try:
        resp = session.post(
            Config.ELK_SEARCH_URL,
            headers=headers,
            json=query_body,
            timeout=Config.ELK_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {}).get("value", 0)
        logger.info(
            f"ELK query returned {len(hits)}/{total} hits "
            f"(timed_out={data.get('timed_out', False)})"
        )
        return hits

    except requests.exceptions.ConnectionError as e:
        logger.error(f"ELK connection error — is the cluster running? {e}")
        _alert_notifier.notify_service_down(
            "ELK/Elasticsearch", str(e), context="fetch_elk_logs:ConnectionError"
        )
        return []
    except requests.exceptions.Timeout:
        msg = f"ELK query timed out after {Config.ELK_TIMEOUT}s"
        logger.error(msg)
        _alert_notifier.notify_service_down(
            "ELK/Elasticsearch", msg, context="fetch_elk_logs:Timeout"
        )
        return []
    except requests.exceptions.HTTPError as e:
        err_text = f"ELK HTTP error: {e.response.status_code} — {e.response.text[:300]}"
        logger.error(err_text)
        _alert_notifier.notify_service_down(
            "ELK/Elasticsearch", err_text, context="fetch_elk_logs:HTTPError"
        )
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching ELK logs: {e}")
        _alert_notifier.notify_service_down(
            "ELK/Elasticsearch", str(e), context="fetch_elk_logs:unexpected"
        )
        return []


def parse_elk_hit(hit: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a single ELK hit document into the standard error payload format.

    Expected _source structure:
    {
        "_id": "abc123",
        "_source": {
            "level": "ERROR",
            "message": "{\"applicationName\":\"...\",\"correlationId\":\"...\",
                         \"code\":\"...\",\"description\":\"...\"}",
            "instant": {"epochSecond": 1234567890}
        }
    }

    Returns standardised payload dict, or None if parsing fails.
    """
    try:
        doc_id = hit.get("_id", "UNKNOWN")
        source = hit.get("_source", {})

        # Guard: only process ERROR-level logs
        if source.get("level", "").upper() != "ERROR":
            return None

        msg = source.get("message", "")
        # ELK message field may be JSON-encoded
        try:
            parsed_msg = json.loads(msg)
        except (json.JSONDecodeError, TypeError):
            parsed_msg = {"rawMessage": msg}

        app_name = parsed_msg.get("applicationName") or "UNKNOWN_APP"
        correlation_id = parsed_msg.get("correlationId") or doc_id
        error_code = parsed_msg.get("code") or "UNKNOWN_ERROR"
        description = (
            parsed_msg.get("description")
            or parsed_msg.get("rawMessage")
            or "No description"
        )

        # Timestamp from epochSecond
        epoch = source.get("instant", {}).get("epochSecond")
        try:
            timestamp = float(epoch) if epoch is not None else datetime.now(timezone.utc).timestamp()
        except (TypeError, ValueError):
            logger.warning(f"Could not parse timestamp '{epoch}' for doc {doc_id}; using now()")
            timestamp = datetime.now(timezone.utc).timestamp()

        return {
            "applicationName": app_name,
            "correlationId": correlation_id,
            "code": error_code,
            "description": description,
            "timestamp": timestamp,
            "source": "elk",
            "_doc_id": doc_id   # Internal — stripped before publishing
        }

    except Exception as e:
        logger.error(f"Failed to parse ELK hit {hit.get('_id', '?')}: {e}")
        return None


# ---------------------------------------------------------------------------
# Seen-ID tracker (Layer 0 dedup)
# ---------------------------------------------------------------------------

def is_seen_elk_doc(doc_id: str) -> bool:
    """Return True if this document has already been published this window."""
    return doc_id in _seen_elk_ids


def mark_elk_doc_seen(doc_id: str):
    """Record that this document has been published."""
    _seen_elk_ids[doc_id] = datetime.now(timezone.utc)


def evict_expired_elk_ids():
    """
    Remove stale entries from _seen_elk_ids.
    Call once per cycle to prevent unbounded memory growth.
    TTL = DB_DUPLICATE_WINDOW_MINUTES.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=Config.DB_DUPLICATE_WINDOW_MINUTES)
    expired = [k for k, v in _seen_elk_ids.items() if v < cutoff]
    for k in expired:
        del _seen_elk_ids[k]
    if expired:
        logger.debug(f"Evicted {len(expired)} expired ELK doc IDs from seen-ID cache")


# ---------------------------------------------------------------------------
# Occurrence count helpers (Layer 1 — DB atomic UPDATE...RETURNING)
# ---------------------------------------------------------------------------

def check_occurrence_count(app_name: str, code: str, desc: str, timestamp: datetime) -> int:
    """
    Single atomic UPDATE...RETURNING.
    - DB record NOT found → returns 0 (new error)
    - DB record FOUND → atomically increments occurrence_count and returns new value
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
        logger.info(f"📈 {app_name}/{code}: occurrence_count → {new_count}")
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
    Pre-fetch occurrence counts for ALL errors in ONE query.
    Returns { (app_name, code, description) -> current occurrence_count }
    Reduces N per-error DB queries to a single batch SELECT at cycle start.
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
            if key not in result:
                result[key] = int(row['occurrence_count'] or 1)

        logger.debug(f"Batch pre-fetch: found {len(result)}/{len(error_keys)} errors in DB")
        return result

    except Exception as e:
        logger.error(f"Batch fetch occurrence counts failed: {e}")
        _db_conn = None
        return {}  # Fail open — all errors treated as new


# ---------------------------------------------------------------------------
# High-priority alert
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
            <h2 style="margin:0;">🚨 HIGH-PRIORITY ERROR ALERT</h2>
        </div>
        <div style="border:2px solid #b0002a;border-top:none;padding:20px;border-radius:0 0 6px 6px;">
            <p><strong>This error has occurred <span style="color:#b0002a;font-size:1.4em;">{count}x</span>
            in the last {Config.DB_DUPLICATE_WINDOW_MINUTES} minutes and requires immediate attention.</strong></p>
            <table style="width:100%;border-collapse:collapse;margin-top:12px;">
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;width:30%;">Application</td>
                <td style="padding:8px;border:1px solid #ddd;">{app_name}</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Error Code</td>
                <td style="padding:8px;border:1px solid #ddd;">{code}</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Source</td>
                <td style="padding:8px;border:1px solid #ddd;">ELK/Elasticsearch</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Occurrences</td>
                <td style="padding:8px;border:1px solid #ddd;color:#b0002a;font-weight:bold;">{count} times</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Time Window</td>
                <td style="padding:8px;border:1px solid #ddd;">Last {Config.DB_DUPLICATE_WINDOW_MINUTES} minutes</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Last Seen</td>
                <td style="padding:8px;border:1px solid #ddd;">{timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</td></tr>
            <tr><td style="background:#f4f4f4;font-weight:bold;padding:8px;border:1px solid #ddd;">Description</td>
                <td style="padding:8px;border:1px solid #ddd;">{description[:500]}</td></tr>
            </table>
            <p style="margin-top:16px;color:#666;font-size:0.85em;">Sent by RAG Error Handling Framework — Escalation Engine</p>
        </div>
        </body></html>
        """

        msg = EmailMessage()
        msg["Subject"] = f"🚨 HIGH PRIORITY: {code} occurred {count}x in {Config.DB_DUPLICATE_WINDOW_MINUTES}min [{app_name}]"
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

        logger.warning(f"🚨 HIGH-PRIORITY ALERT SENT: {app_name}/{code} ({count}x) → {to_email}")

    except Exception as e:
        logger.error(f"Failed to send high-priority alert: {e}")


# ---------------------------------------------------------------------------
# Main processing cycle
# ---------------------------------------------------------------------------

def process_cycle():
    """Main ELK polling cycle with 4-layer deduplication and occurrence tracking."""
    cycle_start = datetime.now()
    logger.info("▶️  Starting ELK Poll Cycle...")

    # Step 1: Keep RabbitMQ alive between cycles
    keep_rabbitmq_alive()

    # Step 2: Evict stale doc IDs from Layer-0 dedup cache
    evict_expired_elk_ids()

    # Step 3: Reconnect if needed
    setup_rabbitmq_connection()

    # Step 4: Fetch logs from ELK
    since_dt = datetime.now(timezone.utc) - timedelta(seconds=Config.POLL_INTERVAL_SECONDS)
    hits = fetch_elk_logs(since_dt)

    processed = 0
    published = 0
    skipped_duplicate = 0
    skipped_invalid = 0

    if not hits:
        logger.info("No ELK hits in this cycle.")
        duration = (datetime.now() - cycle_start).total_seconds()
        logger.info(f"📊 Cycle completed in {duration:.2f}s: no hits")
        return

    # Step 5: Parse all hits, apply Layer-0 dedup
    parsed_batch: List[Tuple[Dict, Dict, datetime]] = []
    for hit in hits:
        doc_id = hit.get("_id", "UNKNOWN")

        # Layer 0: doc-ID dedup (prevents reprocessing same ELK doc across poll windows)
        if is_seen_elk_doc(doc_id):
            logger.debug(f"⏭️ Already seen doc_id={doc_id}; skipping.")
            skipped_duplicate += 1
            continue

        payload = parse_elk_hit(hit)
        if payload is None:
            skipped_invalid += 1
            continue

        if not all([payload.get('applicationName'), payload.get('code'), payload.get('description')]):
            logger.warning(f"⚠️ Skipping ELK hit with missing fields: doc_id={doc_id}")
            skipped_invalid += 1
            continue

        try:
            error_timestamp = datetime.fromtimestamp(payload['timestamp'], tz=timezone.utc)
        except (ValueError, OSError, TypeError) as e:
            logger.error(f"❌ Invalid timestamp {payload['timestamp']} for doc {doc_id}: {e}")
            skipped_invalid += 1
            continue

        parsed_batch.append((hit, payload, error_timestamp))

    if not parsed_batch:
        logger.info("No valid ELK hits after parsing and Layer-0 dedup.")
        duration = (datetime.now() - cycle_start).total_seconds()
        logger.info(
            f"📊 Cycle completed in {duration:.2f}s: total_hits={len(hits)}, "
            f"parsed=0, skipped_invalid={skipped_invalid}, skipped_duplicate={skipped_duplicate}"
        )
        return

    # Step 6: Pre-count occurrences within this batch
    # For brand-new errors: publish ONE message with the total batch count
    batch_occurrence_counts: Dict[tuple, int] = {}
    for _, p, _ in parsed_batch:
        k = (p['applicationName'], p['code'], p['description'])
        batch_occurrence_counts[k] = batch_occurrence_counts.get(k, 0) + 1

    # Step 7: ONE batch DB query for all error keys (Optimization: N queries → 1)
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

    # Step 8: Within-cycle tracker
    within_cycle_counts: Dict[tuple, int] = {}
    within_cycle_first_db_count: Dict[tuple, int] = {}

    for hit, payload, error_timestamp in parsed_batch:
        doc_id = payload.pop("_doc_id", hit.get("_id", "UNKNOWN"))
        try:
            cycle_key = (payload['applicationName'], payload['code'], payload['description'])

            # ── Layer 3: Within-cycle deduplication ─────────────────────────────
            if cycle_key in within_cycle_counts:
                within_cycle_counts[cycle_key] += 1
                cycle_count = within_cycle_counts[cycle_key]
                logger.info(f"⏭️ Same-cycle duplicate: {payload['code']} (#{cycle_count} this cycle)")

                first_db_count = within_cycle_first_db_count.get(cycle_key, -1)

                if first_db_count == 0:
                    # Brand-new: batch count already encoded in the single published message
                    logger.debug(
                        f"Batch-counted dup for new error {payload['code']} "
                        f"— no individual DB update needed"
                    )
                else:
                    # Existing error: individual DB increment still required
                    new_db_count = check_occurrence_count(
                        payload['applicationName'], payload['code'],
                        payload['description'], error_timestamp
                    )
                    now_utc = datetime.now(timezone.utc)
                    if new_db_count >= Config.HIGH_PRIORITY_THRESHOLD:
                        error_key = (payload['applicationName'], payload['code'])
                        last_alert = escalation_cooldown.get(error_key)
                        cooldown_delta = timedelta(minutes=Config.ESCALATION_COOLDOWN_MINUTES)
                        if last_alert is None or (now_utc - last_alert) >= cooldown_delta:
                            logger.warning(
                                f"🚨 ESCALATION TRIGGERED (within-cycle): "
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
                                f"⏸️ Escalation cooldown active for {payload['code']} "
                                f"({minutes_since:.1f}min ago, cooldown={Config.ESCALATION_COOLDOWN_MINUTES}min)"
                            )

                skipped_duplicate += 1
                continue

            # ── Layer 1: DB atomic UPDATE...RETURNING ───────────────────────────
            count = check_occurrence_count(
                payload['applicationName'], payload['code'],
                payload['description'], error_timestamp
            )

            now_utc = datetime.now(timezone.utc)
            cache_ttl = timedelta(minutes=Config.DB_DUPLICATE_WINDOW_MINUTES)

            if count > 0:
                # DB already has the record — clean up async-gap cache
                _published_cache.pop(cycle_key, None)

            elif count == 0:
                # ── Layer 2: In-memory published-cache check ─────────────────────
                cache_entry = _published_cache.get(cycle_key)
                if cache_entry:
                    pub_time, mem_count = cache_entry
                    age = now_utc - pub_time

                    if age > cache_ttl:
                        # Cache expired — treat as fresh new error
                        logger.info(
                            f"🔄 Cache expired for {payload['code']} "
                            f"({age.total_seconds() / 60:.1f}min > "
                            f"{Config.DB_DUPLICATE_WINDOW_MINUTES}min TTL). "
                            f"Treating as new."
                        )
                        _published_cache.pop(cycle_key, None)
                    else:
                        # Async gap duplicate — consumer hasn't inserted yet
                        mem_count += 1
                        _published_cache[cycle_key] = (pub_time, mem_count)
                        count = mem_count
                        logger.info(
                            f"⏳ Async gap duplicate: {payload['code']} "
                            f"(in-memory count={mem_count}, DB not yet updated)"
                        )

            # Register in cycle tracker
            within_cycle_counts[cycle_key] = max(count, 1)
            within_cycle_first_db_count[cycle_key] = count

            # ── Act on final count ───────────────────────────────────────────────
            if count == 0:
                # Brand-new error — publish ONE message with total batch count
                batch_count = batch_occurrence_counts.get(cycle_key, 1)
                publish_payload = {**payload, "occurrence_count": batch_count}
                logger.info(
                    f"✅ New error: {payload['applicationName']}/{payload['code']} "
                    f"(source=elk, doc_id={doc_id}, batch_count={batch_count})"
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
                    mark_elk_doc_seen(doc_id)
                    _published_cache[cycle_key] = (now_utc, batch_count)
                    published += 1
                    processed += 1
                    logger.info(
                        f"✅ Published: {payload['code']} | batch_count={batch_count} | "
                        f"doc_id cached for dedup, payload cached for async gap"
                    )
                    # Escalate immediately if batch itself crosses the threshold
                    if batch_count >= Config.HIGH_PRIORITY_THRESHOLD:
                        error_key = (payload['applicationName'], payload['code'])
                        last_alert = escalation_cooldown.get(error_key)
                        cooldown_delta = timedelta(minutes=Config.ESCALATION_COOLDOWN_MINUTES)
                        if last_alert is None or (now_utc - last_alert) >= cooldown_delta:
                            logger.warning(
                                f"🚨 ESCALATION TRIGGERED (batch count={batch_count}): "
                                f"{payload['applicationName']}/{payload['code']} "
                                f"(threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                            )
                            send_high_priority_alert(
                                app_name=payload['applicationName'],
                                code=payload['code'],
                                description=payload['description'],
                                count=batch_count,
                                timestamp=error_timestamp
                            )
                            escalation_cooldown[error_key] = now_utc
                else:
                    logger.error("❌ RabbitMQ channel unavailable. Will retry next cycle.")

            elif count < Config.HIGH_PRIORITY_THRESHOLD:
                # Known duplicate — skip silently
                logger.info(
                    f"⏭️ Duplicate: {payload['applicationName']}/{payload['code']} "
                    f"(seen {count}x, threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                )
                skipped_duplicate += 1

            else:
                # count >= HIGH_PRIORITY_THRESHOLD — escalate!
                error_key = (payload['applicationName'], payload['code'])
                last_alert = escalation_cooldown.get(error_key)
                cooldown_delta = timedelta(minutes=Config.ESCALATION_COOLDOWN_MINUTES)

                if last_alert is None or (now_utc - last_alert) >= cooldown_delta:
                    logger.warning(
                        f"🚨 ESCALATION TRIGGERED: "
                        f"{payload['applicationName']}/{payload['code']} "
                        f"occurred {count}x (threshold={Config.HIGH_PRIORITY_THRESHOLD})"
                    )
                    send_high_priority_alert(
                        app_name=payload['applicationName'],
                        code=payload['code'],
                        description=payload['description'],
                        count=count,
                        timestamp=error_timestamp
                    )
                    escalation_cooldown[error_key] = now_utc
                else:
                    minutes_since = (now_utc - last_alert).total_seconds() / 60
                    logger.info(
                        f"⏸️ Escalation cooldown active for {payload['code']} "
                        f"({minutes_since:.1f}min ago, "
                        f"cooldown={Config.ESCALATION_COOLDOWN_MINUTES}min)"
                    )

                skipped_duplicate += 1

        except Exception as e:
            logger.error(f"Failed to process doc {doc_id}: {e}")

    # Step 9: Cycle metrics
    duration = (datetime.now() - cycle_start).total_seconds()
    logger.info(
        f"📊 Cycle completed in {duration:.2f}s: "
        f"total_hits={len(hits)}, parsed={len(parsed_batch)}, "
        f"processed={processed}, published={published}, "
        f"skipped_duplicate={skipped_duplicate}, skipped_invalid={skipped_invalid}"
    )


# ---------------------------------------------------------------------------
# Graceful shutdown helpers
# ---------------------------------------------------------------------------

def cleanup_and_exit():
    """Gracefully shut down the scheduler and connections before exiting."""
    logger.info("🛑 Shutting down ELK extractor...")
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        try:
            rabbitmq_connection.close()
        except Exception:
            pass
    if http_session:
        try:
            http_session.close()
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
    if not getattr(Config, 'ELK_SEARCH_URL', None):
        logger.error("ELK_SEARCH_URL is not configured. Cannot start.")
        sys.exit(1)

    logger.info(
        f"🚀 Starting ELK Extractor | "
        f"poll_interval={Config.POLL_INTERVAL_SECONDS}s | "
        f"dedup_window={Config.DB_DUPLICATE_WINDOW_MINUTES}min | "
        f"escalation_threshold={Config.HIGH_PRIORITY_THRESHOLD}"
    )

    # Initial connection setup at startup (fail-fast rather than silent)
    try:
        setup_http_session()
        logger.info("✅ HTTP session ready")
    except Exception as e:
        logger.warning(f"⚠️ HTTP session setup failed: {e}.")

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
        process_cycle,
        "interval",
        seconds=Config.POLL_INTERVAL_SECONDS,
        max_instances=1,    # Prevent overlapping cycles if one runs long
        coalesce=True       # Collapse missed executions into a single run
    )

    logger.info(
        f"⏱️  Scheduler configured — running every {Config.POLL_INTERVAL_SECONDS}s. "
        f"Press CTRL+C to stop."
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        cleanup_and_exit()
    except Exception as e:
        logger.error(f"❌ Scheduler error: {e}", exc_info=True)
        cleanup_and_exit()
