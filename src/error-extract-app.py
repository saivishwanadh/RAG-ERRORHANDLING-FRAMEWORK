import os
import json
import logging
import signal
import sys
from typing import Dict, Any, List, Optional
import requests
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler
import pika
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from structuraldb import DB
from config import Config

# Setup logging
logger = logging.getLogger(__name__)

# Global variables for resource management
http_session: Optional[requests.Session] = None
rabbitmq_connection: Optional[pika.BlockingConnection] = None
rabbitmq_channel: Optional[pika.channel.Channel] = None
scheduler: Optional[BlockingScheduler] = None
# We don't need a global DB connection anymore as we use context managers or short-lived connections
# But the original code had a persistent one. With my refactor of DB, it handles connection internally.

def setup_http_session() -> requests.Session:
    """Create HTTP session with retry logic"""
    global http_session
    if http_session is None:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        http_session = session
        logger.info("HTTP session created with retry logic")
    return http_session


def setup_rabbitmq_connection():
    """Setup persistent RabbitMQ connection and channel"""
    global rabbitmq_connection, rabbitmq_channel
    
    try:
        if rabbitmq_connection is None or rabbitmq_connection.is_closed:
            params = pika.URLParameters(Config.RABBIT_URL)
            params.connection_attempts = 3
            params.retry_delay = 2
            params.socket_timeout = 10
            
            rabbitmq_connection = pika.BlockingConnection(params)
            rabbitmq_channel = rabbitmq_connection.channel()
            
            # Declare exchange
            try:
                rabbitmq_channel.exchange_declare(
                    exchange=Config.EXCHANGE,
                    passive=True
                )
                logger.info(f"Using existing exchange '{Config.EXCHANGE}'")
            except pika.exceptions.ChannelClosedByBroker:
                # Exchange doesn't exist, create new connection and declare it
                rabbitmq_connection.close()
                rabbitmq_connection = pika.BlockingConnection(params)
                rabbitmq_channel = rabbitmq_connection.channel()
                
                rabbitmq_channel.exchange_declare(
                    exchange=Config.EXCHANGE,
                    exchange_type=Config.EXCHANGE_TYPE,
                    durable=True
                )
                logger.info(f"Created new exchange '{Config.EXCHANGE}' as {Config.EXCHANGE_TYPE}")
            
            # Declare queue
            try:
                rabbitmq_channel.queue_declare(queue=Config.QUEUE, passive=True)
                logger.info(f"Using existing queue '{Config.QUEUE}'")
            except pika.exceptions.ChannelClosedByBroker:
                rabbitmq_connection.close()
                rabbitmq_connection = pika.BlockingConnection(params)
                rabbitmq_channel = rabbitmq_connection.channel()
                
                # Re-check exchange
                try:
                    rabbitmq_channel.exchange_declare(exchange=Config.EXCHANGE, passive=True)
                except:
                    rabbitmq_channel.exchange_declare(
                        exchange=Config.EXCHANGE,
                        exchange_type=Config.EXCHANGE_TYPE,
                        durable=True
                    )
                
                rabbitmq_channel.queue_declare(queue=Config.QUEUE, durable=True)
                logger.info(f"Created new queue '{Config.QUEUE}'")
            
            # Bind queue to exchange
            rabbitmq_channel.queue_bind(Config.QUEUE, Config.EXCHANGE, Config.ROUTING_KEY)
            
            logger.info("RabbitMQ connection established")
    except Exception as e:
        logger.error(f"Failed to setup RabbitMQ connection: {e}", exc_info=True)
        rabbitmq_connection = None
        rabbitmq_channel = None
        raise


def is_duplicate_in_last_window(
    app_name: str,
    code: str,
    desc: str,
    current_timestamp: datetime
) -> bool:
    """
    Check if exact same error exists in database within configured window.
    """
    try: 
        # Use context manager for safe DB access
        with DB() as db:
            # Convert UTC timestamp to LOCAL time (matching database format)
            # Assuming database is in Indian Standard Time (IST = UTC+5:30) or system local time
            if current_timestamp.tzinfo is not None:
                local_timestamp = current_timestamp.astimezone().replace(tzinfo=None)
            else:
                local_timestamp = current_timestamp
            
            logger.debug(f"🕐 Timestamp conversion: UTC={current_timestamp} → Local={local_timestamp}")
            
            query = """
                SELECT 
                    id,
                    error_timestamp,
                    EXTRACT(EPOCH FROM (error_timestamp - %s)) / 60 AS minutes_diff_db,
                    EXTRACT(EPOCH FROM (%s - error_timestamp)) / 60 AS minutes_ago
                FROM errorsolutiontable
                WHERE application_name = %s
                  AND error_code = %s
                  AND error_description = %s
                  AND error_timestamp >= %s - INTERVAL '%s minutes'
                  AND error_timestamp <= %s + INTERVAL '1 minute'
                ORDER BY error_timestamp DESC
                LIMIT 1
            """
            
            result = db.execute(
                query,
                params=(
                    local_timestamp, local_timestamp, app_name, code, desc,
                    local_timestamp, Config.DB_DUPLICATE_WINDOW_MINUTES, local_timestamp
                ),
                fetch=True
            )
            
            if not result:
                logger.info(f"✅ New error: {app_name}/{code}")
                return False
            
            record = result[0]
            # RealDictCursor returns dict
            record_id = record["id"]
            existing_timestamp = record["error_timestamp"]
            minutes_ago = float(record["minutes_ago"])
            
            logger.info(
                f"📝 Found existing: ID={record_id}, "
                f"Timestamp={existing_timestamp}, "
                f"Age={abs(minutes_ago):.2f}min"
            )
            
            if abs(minutes_ago) <= Config.DB_DUPLICATE_WINDOW_MINUTES:
                logger.warning(f"⏭️  DUPLICATE: {app_name}/{code} (occurred {abs(minutes_ago):.1f}min ago)")
                return True
            else:
                logger.info(f"✅ Old error (>{abs(minutes_ago):.1f}min ago): {app_name}/{code}")
                return False
    
    except Exception as e:
        logger.error(f"❌ DB duplicate check failed: {e}", exc_info=True)
        return False  # Fail open


def build_elk_query(window_seconds: int = 60) -> Dict[str, Any]:
    """Build ELK query for error logs in time window"""
    now_sec = int(datetime.now(timezone.utc).timestamp())
    return {
        "query": {
            "bool": {
                "must": [
                    {"match": {"level": "ERROR"}},
                    {
                        "range": {
                            "instant.epochSecond": {
                                "gte": now_sec - window_seconds,
                                "lte": now_sec,
                            }
                        }
                    },
                ]
            }
        },
        "size": 1000,
        "sort": [{"instant.epochSecond": "desc"}]
    }


def call_elk(query: Dict[str, Any]) -> Dict[str, Any]:
    """Call ELK search API with retry logic"""
    session = setup_http_session()
    headers = {
        "Authorization": Config.ELK_APIKEY,
        "Content-Type": "application/json"
    }
    
    try:
        resp = session.post(
            Config.ELK_SEARCH_URL,
            headers=headers,
            json=query,
            timeout=Config.ELK_TIMEOUT
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        logger.error(f"ELK query timeout after {Config.ELK_TIMEOUT} seconds")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"ELK query failed: {e}")
        raise


def parse_hits(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse and deduplicate ELK hits"""
    results = []
    seen = set()
    
    for item in hits:
        try:
            src = item.get("_source", {})
            msg = src.get("message", "")

            try:
                parsed_msg = json.loads(msg)
            except json.JSONDecodeError:
                parsed_msg = {"rawMessage": msg}

            record = {
                "applicationName": parsed_msg.get("applicationName"),
                "correlationId": parsed_msg.get("correlationId"),
                "code": parsed_msg.get("code"),
                "description": parsed_msg.get("description") or parsed_msg.get("rawMessage"),
                "timestamp": src.get("instant", {}).get("epochSecond")
            }

            unique_key = (
                record["applicationName"],
                record["code"],
                record["description"]
            )

            if unique_key not in seen:
                seen.add(unique_key)
                results.append(record)
            else:
                logger.debug(f"⏭️  Filtered duplicate in batch: {record['applicationName']}/{record['code']}")
        
        except Exception as e:
            logger.warning(f"Failed to parse ELK hit: {e}")
            continue

    if len(hits) != len(results):
        logger.info(f"📊 Batch deduplication: {len(hits)} raw → {len(results)} unique")
    
    return results


def publish_to_rabbitmq(message: dict) -> bool:
    """Publish message to RabbitMQ with persistent connection"""
    global rabbitmq_connection, rabbitmq_channel
    
    try:
        setup_rabbitmq_connection()
        
        if rabbitmq_channel is None:
            logger.error("RabbitMQ channel not available")
            return False
        
        rabbitmq_channel.basic_publish(
            exchange=Config.EXCHANGE,
            routing_key=Config.ROUTING_KEY,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json'
            )
        )
        return True
    
    except Exception as e:
        logger.error(f"Failed to publish to RabbitMQ: {e}", exc_info=True)
        cleanup_rabbitmq()
        return False


def cleanup_rabbitmq():
    """Close RabbitMQ connection safely"""
    global rabbitmq_connection, rabbitmq_channel
    
    try:
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            rabbitmq_connection.close()
            logger.info("RabbitMQ connection closed")
    except Exception as e:
        logger.warning(f"Error closing RabbitMQ connection: {e}")
    finally:
        rabbitmq_connection = None
        rabbitmq_channel = None


def process_cycle():
    """Main processing cycle - poll ELK and publish errors"""
    cycle_start = datetime.now()
    
    query = build_elk_query(Config.POLL_INTERVAL_SECONDS)
    try:
        resp = call_elk(query)
    except Exception as e:
        logger.exception("Error calling ELK: %s", e)
        return

    hits = resp.get("hits", {}).get("hits", [])
    if len(hits) == 0:
        logger.info("no error records found from elk, No further action needed")
        return

    logger.info("error records found from elk, The number of messages=%d", len(hits))
    records = parse_hits(hits)

    processed = 0
    published = 0
    skipped_invalid = 0
    skipped_duplicate = 0

    for rec in records:
        app_name = rec.get("applicationName")
        error_code = rec.get("code")
        error_desc = rec.get("description")
        timestamp_epoch = rec.get("timestamp")
        
        if not all([app_name, error_code, error_desc, timestamp_epoch]):
            logger.warning(f"⚠️  Skipping record with missing fields: {rec}")
            skipped_invalid += 1
            continue
        
        try:
            error_timestamp = datetime.fromtimestamp(timestamp_epoch, tz=timezone.utc)
        except (ValueError, OSError, TypeError) as e:
            logger.error(f"❌ Invalid timestamp {timestamp_epoch}: {e}")
            skipped_invalid += 1
            continue
        
        if is_duplicate_in_last_window(app_name, error_code, error_desc, error_timestamp):
            skipped_duplicate += 1
            continue
        
        if publish_to_rabbitmq(rec):
            published += 1
            logger.info(
                f"✅ Published new error: {app_name}/{error_code} "
                f"(correlationId: {rec.get('correlationId')})"
            )
        
        processed += 1
    
    duration = (datetime.now() - cycle_start).total_seconds()
    logger.info(
        f"📊 Cycle completed in {duration:.2f}s: "
        f"total={len(hits)}, unique={len(records)}, processed={processed}, "
        f"published={published}, skipped_invalid={skipped_invalid}, "
        f"skipped_duplicate={skipped_duplicate}"
    )


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"⚠️  Received signal {signum} - initiating graceful shutdown")
    cleanup_and_exit()


def cleanup_and_exit():
    """Cleanup resources and exit"""
    global scheduler, http_session
    
    logger.info("🛑 Stopping scheduler...")
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=True)
    
    logger.info("🔌 Closing connections...")
    cleanup_rabbitmq()
    
    if http_session:
        http_session.close()
    
    logger.info("✅ Shutdown complete")
    sys.exit(0)


if __name__ == "__main__":
    Config.validate()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info(f"▶ Starting ELK Poller — runs every {Config.POLL_INTERVAL_SECONDS} seconds")
    logger.info(f"📋 Configuration: ELK_TIMEOUT={Config.ELK_TIMEOUT}s, "
                f"DB_DUPLICATE_WINDOW={Config.DB_DUPLICATE_WINDOW_MINUTES}min, "
                f"LOG_LEVEL={Config.LOG_LEVEL}")
    
    try:
        setup_http_session()
        setup_rabbitmq_connection()
        # DB connection is lazy initialized
        logger.info("✅ Initial connections established successfully")
    except Exception as e:
        logger.error(f"❌ Failed to establish initial connections: {e}")
        sys.exit(1)
    
    scheduler = BlockingScheduler()
    scheduler.add_job(
        process_cycle,
        "interval",
        seconds=Config.POLL_INTERVAL_SECONDS,
        max_instances=1,
        coalesce=True
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("⛔ Scheduler stopped manually")
        cleanup_and_exit()
    except Exception as e:
        logger.error(f"❌ Scheduler error: {e}", exc_info=True)
        cleanup_and_exit()
