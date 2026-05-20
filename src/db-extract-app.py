import json
import logging
import hashlib
import signal
import sys
import pika
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from apscheduler.schedulers.blocking import BlockingScheduler

from src.config import Config
from src.service_alert import ServiceAlertNotifier
from src.logger import setup_logging

# ── Logging ──────────────────────────────────────────────────────────────────
setup_logging(service_name="db-extractor", level=Config.LOG_LEVEL)
logger = logging.getLogger(__name__)

# ── Global State ──────────────────────────────────────────────────────────────
rabbitmq_connection: Optional[pika.BlockingConnection] = None
rabbitmq_channel: Optional[pika.channel.Channel] = None
scheduler: Optional[BlockingScheduler] = None

# Persistent DB connection — reused across all cycles (same DB as OpsResolver)
_db_conn: Optional[psycopg2.extensions.connection] = None

# Cross-cycle async-gap cache: { (app_name, code, desc) → (first_published_at UTC, count) }
# Bridges the window between "published to RabbitMQ" and "consumer inserts DB record"
_published_cache: Dict[tuple, Tuple[datetime, int]] = {}

# Rolling set of record hashes already published this window.
# Keyed by hash string, value is the UTC datetime when first published.
# Entries expire after DB_DUPLICATE_WINDOW_MINUTES. Prevents overlap-window duplicates.
_seen_record_hashes: Dict[str, datetime] = {}

# Module-level service alert notifier (shared, cooldown-aware)
_alert_notifier: ServiceAlertNotifier = ServiceAlertNotifier()


# ── Persistent DB Connection ──────────────────────────────────────────────────
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


# ── RabbitMQ Connection Management ───────────────────────────────────────────
def setup_rabbitmq_connection():
    """
    Setup persistent RabbitMQ connection with heartbeat.
    - Reuses existing connection/channel if both are healthy
    - Reconnects if connection dropped
    - Recreates channel if connection alive but channel closed
    """
    global rabbitmq_connection, rabbitmq_channel
    try:
        conn_alive = rabbitmq_connection is not None and not rabbitmq_connection.is_closed
        channel_alive = conn_alive and rabbitmq_channel is not None and rabbitmq_channel.is_open

        if channel_alive:
            return

        if conn_alive and not channel_alive:
            logger.debug("RabbitMQ: connection alive, recreating channel...")
            rabbitmq_channel = rabbitmq_connection.channel()
            rabbitmq_channel.exchange_declare(
                exchange=Config.EXCHANGE, exchange_type=Config.EXCHANGE_TYPE, durable=True
            )
            rabbitmq_channel.queue_declare(queue=Config.QUEUE, durable=True)
            logger.debug("RabbitMQ channel restored")
            return

        logger.debug("Establishing new RabbitMQ connection...")
        params = pika.URLParameters(Config.RABBIT_URL)
        params.heartbeat = 120
        params.blocked_connection_timeout = 30
        params.socket_timeout = 10
        params.connection_attempts = 3
        params.retry_delay = 2

        rabbitmq_connection = pika.BlockingConnection(params)
        rabbitmq_channel = rabbitmq_connection.channel()
        rabbitmq_channel.exchange_declare(
            exchange=Config.EXCHANGE, exchange_type=Config.EXCHANGE_TYPE, durable=True
        )
        rabbitmq_channel.queue_declare(queue=Config.QUEUE, durable=True)
        logger.info("RabbitMQ connection established")

    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        _alert_notifier.notify_service_down(
            "RabbitMQ", str(e), context="setup_rabbitmq_connection"
        )
        rabbitmq_connection = None
        rabbitmq_channel = None


def keep_rabbitmq_alive():
    """Process pending heartbeat frames to keep connection alive between cycles."""
    global rabbitmq_connection, rabbitmq_channel
    try:
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            rabbitmq_connection.process_data_events(time_limit=0)
    except Exception as e:
        logger.warning(f"RabbitMQ heartbeat flush failed: {e}. Will reconnect on next publish.")
        rabbitmq_connection = None
        rabbitmq_channel = None


# ── Record Hash Tracker (replaces email-ID tracker from email-extract-app.py) ─
def generate_record_hash(app_name: str, error_code: str, error_timestamp) -> str:
    """
    Generate a unique fingerprint for a DB row.
    Combines app_name + error_code + error_timestamp string to uniquely identify
    each individual error occurrence. Stored in _seen_record_hashes to prevent
    the same row being published again in a subsequent overlapping cycle.
    """
    raw = f"{app_name}|{error_code}|{str(error_timestamp)}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def is_seen_record(record_hash: str) -> bool:
    """Return True if this record hash has already been published this window."""
    return record_hash in _seen_record_hashes


def mark_record_seen(record_hash: str):
    """Record that this DB row has been published."""
    _seen_record_hashes[record_hash] = datetime.now(timezone.utc)


def evict_expired_seen_records():
    """
    Remove stale entries from _seen_record_hashes.
    Called once per cycle. TTL = DB_DUPLICATE_WINDOW_MINUTES.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=Config.DB_DUPLICATE_WINDOW_MINUTES)
    expired = [k for k, v in _seen_record_hashes.items() if v < cutoff]
    for k in expired:
        del _seen_record_hashes[k]
    if expired:
        logger.debug(f"Evicted {len(expired)} expired record hashes from seen-hash cache")


# ── Row → Payload Mapper (replaces parse_tibco_email from email-extract-app.py) ─
def map_db_row_to_payload(row: dict) -> Dict[str, Any]:
    """
    Map a tibco_error_events row directly to the standard RabbitMQ payload format.
    No HTML parsing needed — columns map directly to fields.
    """
    error_ts = row.get("error_timestamp")
    ts_float = error_ts.timestamp() if isinstance(error_ts, datetime) else datetime.now(timezone.utc).timestamp()
    error_category = (row.get("error_category") or "").strip()
    return {
        "applicationName": row.get("application_name", ""),
        "correlationId": row.get("correlation_id") or "UNKNOWN",
        "code": row.get("error_code", "UNKNOWN_ERROR"),
        "description": row.get("error_description", ""),
        "timestamp": ts_float,
        "error_type": error_category.lower() if error_category else None,
        "source": "database",
    }


# ── Occurrence Count Helpers (identical to email-extract-app.py) ──────────────
def check_occurrence_count(app_name: str, code: str, desc: str, timestamp: datetime) -> int:
    """
    Atomic UPDATE...RETURNING on errorsolutiontable.
    Returns 0 if no existing record (new error), >0 if duplicate found.
    """
    global _db_conn
    try:
        conn = get_persistent_db()
        local_ts = timestamp.astimezone().replace(tzinfo=None) if (
            hasattr(timestamp, "tzinfo") and timestamp.tzinfo
        ) else timestamp

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
            cur.execute(sql, (app_name, code, desc, local_ts, Config.DB_DUPLICATE_WINDOW_MINUTES, local_ts))
            rows = cur.fetchall()
        conn.commit()

        if not rows:
            logger.debug(f"No existing record for {app_name}/{code} — new error")
            return 0
        new_count = max(int(r["occurrence_count"]) for r in rows)
        logger.debug(f"{app_name}/{code}: occurrence_count={new_count}")
        return new_count

    except Exception as e:
        logger.error(f"DB occurrence count check failed: {e}")
        try:
            if _db_conn:
                _db_conn.rollback()
        except Exception:
            pass
        _db_conn = None
        return 0  # Fail open — treat as new error


def batch_fetch_occurrence_counts(
    error_keys: List[Tuple[str, str, str]],
    local_timestamps: List[datetime],
) -> Dict[Tuple[str, str, str], int]:
    """
    Pre-fetch occurrence counts for ALL records in ONE query (Optimization).
    Reduces N per-record DB queries to a single batch SELECT at cycle start.
    Includes one reconnect-retry for stale SSL connections (Neon cloud DB).
    """
    if not error_keys:
        return {}
    global _db_conn

    def _run_batch(conn):
        min_ts = min(local_timestamps)
        unique_pairs = list({(k[0], k[1]) for k in error_keys})
        placeholders = ",".join(["(%s,%s)"] * len(unique_pairs))
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
            key = (row["application_name"], row["error_code"], row["error_description"])
            if key not in result:
                result[key] = int(row["occurrence_count"] or 1)
        return result

    try:
        result = _run_batch(get_persistent_db())
        logger.debug(f"Batch pre-fetch: found {len(result)}/{len(error_keys)} errors in DB")
        return result
    except Exception as e:
        logger.warning(f"Batch fetch failed (likely stale connection): {e}. Reconnecting and retrying...")
        _db_conn = None
        try:
            result = _run_batch(get_persistent_db())
            logger.info(f"Batch pre-fetch retry succeeded: found {len(result)}/{len(error_keys)} errors in DB")
            return result
        except Exception as retry_e:
            logger.error(f"Batch fetch retry also failed: {retry_e}. Failing open.")
            _db_conn = None
            return {}


# ── Main Poll Cycle ───────────────────────────────────────────────────────────
def process_db_cycle():
    """Main DB polling logic — mirrors process_email_cycle() from email-extract-app.py."""
    cycle_start = datetime.now()
    now_utc = datetime.now(timezone.utc)

    # Step 1: Flush RabbitMQ heartbeat frames
    keep_rabbitmq_alive()

    # Step 2: Evict expired record hashes (memory hygiene)
    evict_expired_seen_records()

    # Metrics
    total = 0
    filtered = 0
    processed = 0
    published = 0
    skipped_duplicate = 0
    skipped_invalid = 0
    filtered_records: List[dict] = []
    rows: List[dict] = []

    try:
        # Step 3: Query source table with overlap timestamp window
        since_dt = now_utc - timedelta(seconds=Config.SOURCE_DB_OVERLAP_SECONDS)
        logger.info(
            f"Polling {Config.SOURCE_DB_TABLE} since "
            f"{since_dt.strftime('%Y-%m-%dT%H:%M:%SZ')} "
            f"(overlap={Config.SOURCE_DB_OVERLAP_SECONDS}s)"
        )

        conn = get_persistent_db()
        # Table name from config (not user input) — f-string safe here
        poll_sql = f"""
            SELECT id, application_name, error_code, error_description,
                   error_timestamp, correlation_id, error_category
              FROM {Config.SOURCE_DB_TABLE}
             WHERE error_timestamp >= %s
             ORDER BY error_timestamp ASC
             LIMIT 500
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(poll_sql, (since_dt,))
            rows = [dict(r) for r in cur.fetchall()]
        conn.commit()

        total = len(rows)
        if not rows:
            logger.info("No new records in poll window.")
            duration = (datetime.now() - cycle_start).total_seconds()
            logger.info(f"📊 Cycle completed in {duration:.2f}s: total=0, filtered=0, processed=0, published=0, skipped_duplicate=0, skipped_invalid=0")
            return

        logger.info(f"Fetched {total} records from {Config.SOURCE_DB_TABLE}")

        # Step 4: Layer 0 — Record hash dedup (replaces email-ID dedup)
        # Skips rows already published in a previous overlapping cycle
        for row in rows:
            record_hash = generate_record_hash(
                row.get("application_name", ""),
                row.get("error_code", ""),
                row.get("error_timestamp"),
            )
            if is_seen_record(record_hash):
                logger.debug(f"⏭️ Already seen record hash={record_hash[:8]}; skipping.")
                continue
            row["_record_hash"] = record_hash
            filtered_records.append(row)

        filtered = len(filtered_records)
        if not filtered_records:
            logger.info(f"All {total} records already seen (overlap dedup). Nothing to publish.")
            duration = (datetime.now() - cycle_start).total_seconds()
            logger.info(f"📊 Cycle completed in {duration:.2f}s: total={total}, filtered=0, processed=0, published=0, skipped_duplicate={total}, skipped_invalid=0")
            return

        logger.info(f"After hash dedup: {filtered}/{total} records are new this cycle")

        # Step 5: Ensure RabbitMQ is connected
        setup_rabbitmq_connection()

        # Step 6: Map DB rows to payloads (direct column mapping — no HTML parsing)
        parsed_batch: List[Tuple[dict, Dict[str, Any], datetime]] = []
        for row in filtered_records:
            payload = map_db_row_to_payload(row)

            if not all([payload.get("applicationName"), payload.get("code"), payload.get("description")]):
                logger.warning(
                    f"⚠️ Skipping record id={row.get('id')} — missing required fields: "
                    f"app={payload.get('applicationName')} code={payload.get('code')}"
                )
                skipped_invalid += 1
                continue

            raw_ts = row.get("error_timestamp")
            if isinstance(raw_ts, datetime):
                error_timestamp = raw_ts if raw_ts.tzinfo else raw_ts.replace(tzinfo=timezone.utc)
            else:
                logger.error(f"❌ Invalid or missing error_timestamp in record id={row.get('id')}")
                skipped_invalid += 1
                continue

            parsed_batch.append((row, payload, error_timestamp))

        if not parsed_batch:
            logger.info("No valid records after field validation.")
            duration = (datetime.now() - cycle_start).total_seconds()
            logger.info(f"📊 Cycle completed in {duration:.2f}s: total={total}, filtered={filtered}, processed=0, published=0, skipped_duplicate={skipped_duplicate}, skipped_invalid={skipped_invalid}")
            return

        # Step 7: Count how many times each (app, code, desc) key appears in this batch
        batch_occurrence_counts: Dict[tuple, int] = {}
        for _, p, _ in parsed_batch:
            k = (p["applicationName"], p["code"], p["description"])
            batch_occurrence_counts[k] = batch_occurrence_counts.get(k, 0) + 1

        # Step 8: Batch pre-fetch existing occurrence counts from OpsResolver DB (ONE query)
        all_keys = [(p["applicationName"], p["code"], p["description"]) for _, p, _ in parsed_batch]
        all_ts = [
            ts.astimezone().replace(tzinfo=None) if ts.tzinfo else ts
            for _, _, ts in parsed_batch
        ]
        prefetched_counts = batch_fetch_occurrence_counts(all_keys, all_ts)
        logger.debug(
            f"Batch pre-fetch: {len(prefetched_counts)} known, "
            f"{len(set(all_keys)) - len(prefetched_counts)} new"
        )

        # Step 9: Within-cycle dedup tracker (catches same error appearing twice in one batch)
        within_cycle_counts: Dict[tuple, int] = {}
        within_cycle_first_db_count: Dict[tuple, int] = {}

        # Step 10: Per-record 3-layer decision loop
        for row, payload, error_timestamp in parsed_batch:
            try:
                cycle_key = (payload["applicationName"], payload["code"], payload["description"])

                # ── Within-cycle duplicate check ──────────────────────────────
                if cycle_key in within_cycle_counts:
                    within_cycle_counts[cycle_key] += 1
                    cycle_count = within_cycle_counts[cycle_key]
                    logger.info(f"⏭️ Same-cycle duplicate: {payload['code']} (#{cycle_count} this cycle)")

                    first_db_count = within_cycle_first_db_count.get(cycle_key, -1)
                    if first_db_count == 0:
                        # Brand-new error: batch count already encoded in published message
                        logger.debug(
                            f"Batch-counted dup for new error {payload['code']} "
                            f"— no individual DB update needed"
                        )
                    else:
                        # Existing error: individual DB increment still required
                        check_occurrence_count(
                            payload["applicationName"], payload["code"],
                            payload["description"], error_timestamp
                        )
                    skipped_duplicate += 1
                    continue

                # ── Layer 1: DB check — atomic UPDATE RETURNING ───────────────
                count = check_occurrence_count(
                    payload["applicationName"], payload["code"],
                    payload["description"], error_timestamp
                )

                now_utc = datetime.now(timezone.utc)
                cache_ttl = timedelta(minutes=Config.DB_DUPLICATE_WINDOW_MINUTES)

                if count > 0:
                    # DB has the record — source of truth, clean up cache
                    _published_cache.pop(cycle_key, None)

                elif count == 0:
                    # ── Layer 2: Async gap cache check ───────────────────────
                    cache_entry = _published_cache.get(cycle_key)
                    if cache_entry:
                        pub_time, mem_count = cache_entry
                        age = now_utc - pub_time

                        if age > cache_ttl:
                            # Cache entry expired — treat as fresh new error
                            logger.info(
                                f"🔄 Cache expired for {payload['code']} "
                                f"({age.total_seconds()/60:.1f}min > {Config.DB_DUPLICATE_WINDOW_MINUTES}min TTL). "
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
                                f"(in-memory count={mem_count}, DB not yet updated by consumer)"
                            )

                # Register in cycle tracker
                within_cycle_counts[cycle_key] = max(count, 1)
                within_cycle_first_db_count[cycle_key] = count

                # ── Act on final count ────────────────────────────────────────
                if count == 0:
                    # Brand-new error — publish ONE message with total batch count
                    batch_count = batch_occurrence_counts.get(cycle_key, 1)
                    publish_payload = {**payload, "occurrence_count": batch_count}
                    logger.info(
                        f"✅ New error: {payload['applicationName']}/{payload['code']} "
                        f"(batch_count={batch_count})"
                    )
                    if rabbitmq_channel:
                        rabbitmq_channel.basic_publish(
                            exchange=Config.EXCHANGE,
                            routing_key=Config.ROUTING_KEY,
                            body=json.dumps(publish_payload),
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                                content_type="application/json",
                            ),
                        )
                        mark_record_seen(row["_record_hash"])
                        _published_cache[cycle_key] = (now_utc, batch_count)
                        published += 1
                        processed += 1
                        logger.info(
                            f"✅ Published: {payload['code']} | batch_count={batch_count} | "
                            f"record hash cached for dedup, payload cached for async gap"
                        )
                    else:
                        logger.error("❌ RabbitMQ channel unavailable. Will retry next cycle.")

                else:
                    # Known duplicate (DB or in-memory) — skip
                    logger.debug(
                        f"Duplicate: {payload['applicationName']}/{payload['code']} (seen {count}x)"
                    )
                    skipped_duplicate += 1

            except Exception as e:
                logger.error(f"Failed to process record id={row.get('id')}: {e}")

    except Exception as e:
        logger.error(f"Poll cycle failed: {e}")
        _alert_notifier.notify_service_down(
            "PostgreSQL/DB", str(e), context="process_db_cycle:poll_query"
        )

    # Step 11: Cycle metrics
    duration = (datetime.now() - cycle_start).total_seconds()
    logger.info(
        f"📊 Cycle completed in {duration:.2f}s: "
        f"total={total}, filtered={filtered}, processed={processed}, "
        f"published={published}, skipped_duplicate={skipped_duplicate}, "
        f"skipped_invalid={skipped_invalid}"
    )


# ── Graceful Shutdown ─────────────────────────────────────────────────────────
def cleanup_and_exit():
    logger.info("🛑 Shutting down DB Extractor...")
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        try:
            rabbitmq_connection.close()
        except Exception:
            pass
    sys.exit(0)


def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    cleanup_and_exit()


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Startup validation
    if not Config.DB_URL:
        logger.error("Missing DB_URL in config. Cannot start DB extractor.")
        sys.exit(1)

    if not Config.SOURCE_DB_TABLE:
        logger.error("Missing SOURCE_DB_TABLE in config. Cannot start DB extractor.")
        sys.exit(1)

    logger.info(
        f"🚀 Starting DB Extractor | "
        f"table={Config.SOURCE_DB_TABLE} | "
        f"poll_interval={Config.SOURCE_DB_POLL_INTERVAL}s | "
        f"overlap={Config.SOURCE_DB_OVERLAP_SECONDS}s | "
        f"dedup_window={Config.DB_DUPLICATE_WINDOW_MINUTES}min"
    )

    # Pre-startup connection check (warn only — don't crash, retry per cycle)
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
        process_db_cycle,
        "interval",
        seconds=Config.SOURCE_DB_POLL_INTERVAL,
        max_instances=1,   # Prevent overlapping cycles if one runs long
        coalesce=True,     # Collapse missed executions into a single run
    )

    logger.info(
        f"⏱️  Scheduler configured — running every {Config.SOURCE_DB_POLL_INTERVAL}s. "
        f"Press CTRL+C to stop."
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        cleanup_and_exit()
