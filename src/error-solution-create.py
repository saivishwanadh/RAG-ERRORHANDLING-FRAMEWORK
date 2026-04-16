
import json
import uuid
import logging
import signal
import sys
import time
import datetime
import re
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable, Tuple

import pika
import requests
from qdrant_client import models

from src.vectordb import QdrantStore
from src.embeddingmodel import EmbeddingGenerator
from src.structuraldb import DB
from src.geminicall import GeminiClient
from src.maskdata import LogSanitizer
from src.service_alert import ServiceAlertNotifier
from src.config import Config
from src.logger import setup_logging, set_session_id, clear_session_id

# ---- Logging ----
setup_logging(service_name="consumer", level=Config.LOG_LEVEL)
logger = logging.getLogger(__name__)

# ---- Constants ----
PREFETCH_COUNT = int(getattr(Config, "PREFETCH_COUNT", 1) or 1)
MAX_RETRIES_PER_MESSAGE = int(getattr(Config, "MAX_RETRIES_PER_MESSAGE", 2) or 2)
RATE_LIMIT_DELAY = int(getattr(Config, "RATE_LIMIT_DELAY", 60) or 60)
EXP_BACKOFF_BASE = 1
DLX_EXCHANGE = getattr(Config, "DLX_EXCHANGE", None)
DLQ_ROUTING_KEY = getattr(Config, "DLQ_ROUTING_KEY", None)
DLQ_ENABLED = bool(DLX_EXCHANGE and DLQ_ROUTING_KEY)

# ---- Utility: retry decorator ----

def retry(
    exceptions: Tuple[Exception, ...] = (Exception,),
    max_attempts: int = 3,
    backoff_base: int = 2,
    backoff_jitter: float = 0.1,
    allowed_status_for_retry: Tuple[int, ...] = (429,)
) -> Callable:
    """Generic retry decorator with exponential backoff and small jitter."""
    def _decorator(fn: Callable):
        def _wrapped(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    status = None
                    try:
                        status = e.response.status_code
                    except Exception:
                        status = None

                    # Only retry for allowed_status_for_retry if present
                    if status and status in allowed_status_for_retry and attempt < max_attempts - 1:
                        delay = (backoff_base ** attempt) + (backoff_jitter * attempt)
                        logger.warning(f"Retryable HTTP error {status} on {fn.__name__}, sleeping {delay}s (attempt {attempt+1})")
                        time.sleep(delay)
                        attempt += 1
                        continue
                    raise

                except exceptions as e:
                    if attempt >= max_attempts - 1:
                        raise
                    delay = (backoff_base ** attempt) + (backoff_jitter * attempt)
                    logger.warning(f"Retrying {fn.__name__} after {delay}s due to {type(e).__name__}: {e} (attempt {attempt+1})")
                    time.sleep(delay)
                    attempt += 1
        return _wrapped
    return _decorator


# ---- Simple circuit breaker ----
class CircuitBreaker:
    def __init__(self, fail_threshold: int = 5, reset_timeout_sec: int = 60):
        self.fail_threshold = fail_threshold
        self.reset_timeout = reset_timeout_sec
        self.fail_count = 0
        self.last_fail_ts: Optional[float] = None
        self.opened_until: Optional[float] = None

    def record_success(self):
        self.fail_count = 0
        self.last_fail_ts = None
        self.opened_until = None

    def record_failure(self):
        self.fail_count += 1
        self.last_fail_ts = time.time()
        if self.fail_count >= self.fail_threshold:
            self.opened_until = time.time() + self.reset_timeout
            logger.error(f"Circuit breaker OPEN for {self.reset_timeout}s after {self.fail_count} failures")

    def is_open(self) -> bool:
        if self.opened_until and time.time() < self.opened_until:
            return True
        if self.opened_until and time.time() >= self.opened_until:
            # reset after timeout
            self.fail_count = 0
            self.opened_until = None
            return False
        return False


@dataclass
class MessageContext:
    """Holds all per-message state so the shared ServiceContainer is never mutated.

    Each message processed by the RabbitMQ callback gets its own MessageContext,
    eliminating the risk of message N's state leaking into message N+1.
    """
    incoming_payload: Dict[str, Any] = field(default_factory=dict)
    sessionid: str = ""
    masked_errordescription: str = ""
    error_ts_str: str = ""


# ---- Service container ----
class ServiceContainer:
    def __init__(self):
        self.store: Optional[QdrantStore] = None
        self.client: Optional[GeminiClient] = None
        self.embed_gen: Optional[EmbeddingGenerator] = None
        self.sanitizer: Optional[LogSanitizer] = None
        self._db = None

        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None

        # circuit-breakers per dependency
        self.cb_db = CircuitBreaker(fail_threshold=3, reset_timeout_sec=30)
        self.cb_llm = CircuitBreaker(fail_threshold=3, reset_timeout_sec=30)
        self.cb_qdrant = CircuitBreaker(fail_threshold=3, reset_timeout_sec=30)

        # Service health alert notifier (shared, cooldown-aware)
        self.alert = ServiceAlertNotifier()

    # ---- initialization ----
    def initialize(self):
        logger.info("Initializing services...")
        Config.validate()

        try:
            self.embed_gen = EmbeddingGenerator(api_key=Config.GEMINI_APIKEY)
            logger.debug("Embedding generator initialized")
        except Exception as e:
            logger.exception("Failed to init Embedding generator")
            self.alert.notify_service_down(
                "Gemini/LLM", str(e), context="initialize:EmbeddingGenerator"
            )
            raise

        # Qdrant
        try:
            self.store = QdrantStore(embedding_model=self.embed_gen.embeddings)
            logger.debug("Qdrant initialized")
        except Exception as e:
            logger.exception("Failed to init Qdrant")
            self.alert.notify_service_down(
                "Qdrant/VectorDB", str(e), context="initialize:QdrantStore"
            )
            raise

        # Gemini
        try:
            self.client = GeminiClient(api_key=Config.GEMINI_APIKEY)
            logger.debug("Gemini initialized")
        except Exception as e:
            logger.exception("Failed to init Gemini")
            self.alert.notify_service_down(
                "Gemini/LLM", str(e), context="initialize:GeminiClient"
            )
            raise

        # Sanitizer
        try:
            self.sanitizer = LogSanitizer()
            logger.debug("Sanitizer initialized")
        except Exception as e:
            logger.exception("Failed to init sanitizer")
            raise

        # DB test
        try:
            with DB() as db:
                db.execute("SELECT 1", fetch=True)
            logger.debug("DB reachable")
        except Exception as e:
            logger.exception("DB init failure")
            self.alert.notify_service_down(
                "PostgreSQL/DB", str(e), context="initialize:DB"
            )
            raise

    def initialize_rabbitmq(self):
        params = pika.URLParameters(Config.RABBIT_URL)
        params.connection_attempts = 3
        params.retry_delay = 2
        params.socket_timeout = 10
        params.heartbeat = 600
        params.blocked_connection_timeout = 300

        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        # declare passive ensures queue exists
        self.channel.queue_declare(queue=Config.QUEUE, passive=True)
        logger.debug("RabbitMQ connected")

    # DB execute with retry and circuit breaker
    @retry(exceptions=(Exception,), max_attempts=3)
    def db_execute(self, sql: str, params: tuple = (), fetch: bool = False):
        if self.cb_db.is_open():
            raise Exception("DB circuit open")
        try:
            with DB() as db:
                result = db.execute(sql, params, fetch=fetch)
            self.cb_db.record_success()
            return result
        except Exception as e:
            self.cb_db.record_failure()
            logger.exception("DB operation failed")
            self.alert.notify_service_down(
                "PostgreSQL/DB", str(e), context="db_execute"
            )
            raise

    # qdrant search wrapper
    @retry(exceptions=(Exception,), max_attempts=3)
    def qdrant_search(self, collection: str, vector, limit: int = 3, query_filter=None):
        if self.cb_qdrant.is_open():
            raise Exception("Qdrant circuit open")
        try:
            res = self.store.search(collection=collection, vector=vector, limit=limit, query_filter=query_filter)
            self.cb_qdrant.record_success()
            return res
        except Exception as e:
            self.cb_qdrant.record_failure()
            logger.exception("Qdrant search failed")
            self.alert.notify_service_down(
                "Qdrant/VectorDB", str(e), context="qdrant_search"
            )
            raise

    @retry(exceptions=(Exception,), max_attempts=3, allowed_status_for_retry=(429,))
    def call_llm(self, error_code: str, description: str, context: str = ""):
        if self.cb_llm.is_open():
            raise Exception("LLM circuit open")
        try:
            res = self.client.analyze_error(error_code, description, context=context)
            self.cb_llm.record_success()
            return res
        except requests.exceptions.HTTPError as e:
            self.cb_llm.record_failure()
            self.alert.notify_service_down(
                "Gemini/LLM", str(e), context="call_llm:HTTPError"
            )
            raise
        except Exception as e:
            self.cb_llm.record_failure()
            logger.exception("LLM call failed")
            self.alert.notify_service_down(
                "Gemini/LLM", str(e), context="call_llm"
            )
            raise

    @retry(exceptions=(Exception,), max_attempts=3)
    def qdrant_upsert(self, collection: str, vector_id: int, vector, payload: dict):
        if self.cb_qdrant.is_open():
            raise Exception("Qdrant circuit open")

        try:
            self.store.upsert_vector(
                collection=collection,
                vector_id=vector_id,
                vector=vector,
                payload=payload
            )
            self.cb_qdrant.record_success()
            return True

        except Exception as e:
            self.cb_qdrant.record_failure()
            logger.exception("Qdrant upsert failed")
            self.alert.notify_service_down(
                "Qdrant/VectorDB", str(e), context="qdrant_upsert"
            )
            raise


# ---- core pipeline functions (use service wrappers) ----

services = ServiceContainer()


def store_incoming_payload_and_set_uuid(payload: Dict[str, Any]) -> MessageContext:
    """Create a fresh MessageContext for this message — no shared state mutation."""
    ctx = MessageContext()
    ctx.incoming_payload = payload
    if services.sanitizer is None:
        # Sanitizer failed to initialize — raise now rather than processing unmasked PII
        raise RuntimeError("LogSanitizer not initialized — cannot process message safely")
    ctx.masked_errordescription = services.sanitizer.sanitize(payload.get('description', ''))
    logger.debug(f"PII masking applied, desc_len={len(ctx.masked_errordescription)}")
    ctx.sessionid = str(uuid.uuid4())
    logger.info(f"Processing: app={payload.get('applicationName')} code={payload.get('code')} session={ctx.sessionid}")
    return ctx


def clean_error_description(text: str) -> dict:
    if not text:
        return {"cleanText": ""}
    s = text.replace("\\n", " ").replace("\n", " ")
    s = s.replace('\"', "").replace("&quot;", "")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[\(\)\[\]\{\}]", "", s)
    s = re.sub(r"[\'\":;^|\-,]", "", s)
    return {"cleanText": s.strip().strip('"').strip("'")}


# safe db insert uses services.db_execute

def db_insert(llmresponse: dict, ctx: MessageContext) -> Optional[int]:
    """Insert a new error+solution row; uses ctx for per-message state."""
    cleanErr = clean_error_description(ctx.masked_errordescription)
    insert_sql = """
        INSERT INTO errorsolutiontable (
            application_name, error_code, error_description, sessionID,
            llm_solution, error_timestamp, sessionid_status, occurrence_count, error_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
    """
    params = (
        ctx.incoming_payload.get('applicationName'),
        ctx.incoming_payload.get('code'),
        ctx.masked_errordescription,          # ← masked, not raw
        ctx.sessionid,
        json.dumps(llmresponse),
        ctx.error_ts_str,
        'active',
        ctx.incoming_payload.get('occurrence_count', 1),
        ctx.incoming_payload.get('error_type')  # 'technical', 'business', or None
    )

    inserted_rows = services.db_execute(insert_sql, params, fetch=True)
    logger.debug("DB row inserted")
    if inserted_rows and len(inserted_rows) > 0:
        new_id = inserted_rows[0].get('id') if isinstance(inserted_rows[0], dict) else inserted_rows[0][0]
    else:
        new_id = None
    return new_id



# extract solutions

def extract_solutions_from_points(points):
    final_output = ''
    count = 1
    for p in points:
        payload = getattr(p, 'payload', {})
        sol = payload.get('solution')
        if not sol:
            continue
        final_output += f"Solution {count}:\n{json.dumps(sol)}\n\n"
        count += 1
    return final_output.strip()


# main processing flow with guarded calls

def main(ctx: MessageContext):
    """Core RAG pipeline. All per-message state is read from ctx, not from services."""
    # Null guard — should never happen if callback is correct, but belt-and-suspenders
    if not ctx.incoming_payload:
        raise ValueError("main() called with empty MessageContext — this is a bug in callback()")

    logger.debug("Checking structural DB for existing solution")
    sql = """
        SELECT id, ops_solution, llm_solution
        FROM errorsolutiontable
        WHERE error_description = %s
        AND application_name = %s
        LIMIT 1;
    """
    # Use masked description for DB lookup to match how records are stored
    params = (ctx.masked_errordescription, ctx.incoming_payload.get('applicationName'))

    rows = services.db_execute(sql, params, fetch=True)
    if rows and len(rows) > 0:
        solutions = rows[0].get('ops_solution')
        if solutions:
            logger.info("Found verified solution in structural DB")
            llm_str = rows[0].get('llm_solution')
            llmresponse = json.loads(llm_str) if llm_str else {}
            new_id = db_insert(llmresponse, ctx)
            logger.info(f"Stored verified-solution path result in DB (id={new_id})")
            return
        else:
             logger.info("Found record in structural DB but NO verified solution - falling through to Vector DB")

    # vector path
    logger.debug("Checking vector DB")
    try:
        if services.embed_gen is None:
            raise RuntimeError("EmbeddingGenerator not initialized — skipping vector path")
        cleanErr = clean_error_description(ctx.masked_errordescription)
        embed_input = f"Error:{ctx.incoming_payload.get('code','')} Description:{cleanErr.get('cleanText','')}"
        raw_embedding = services.embed_gen.get_embedding(embed_input)

        qfilter = models.Filter(must=[models.FieldCondition(key='error_code', match=models.MatchValue(value=ctx.incoming_payload.get('code')))])
        result = services.qdrant_search(collection='error_solutions', vector=raw_embedding, limit=3, query_filter=qfilter)
        points = [r for r in result if getattr(r, 'score', 0) >= 0.85]

        if len(points) > 0:
            logger.info(f"Found {len(points)} matching vectors")
            context_text = extract_solutions_from_points(points)
            logger.debug(f"Injecting {len(context_text)} chars of vector context into LLM")
            llmresponse = services.call_llm(
                ctx.incoming_payload.get('code',''),
                ctx.masked_errordescription,
                context=context_text
            )
            new_id = db_insert(llmresponse, ctx)
            logger.info(f"Stored vector-context path result in DB (id={new_id})")
            return

    except Exception:
        logger.exception('Vector DB path failed - falling back to LLM only')

    # LLM only path
    logger.info('Using LLM only')
    llmresponse = services.call_llm(ctx.incoming_payload.get('code',''), ctx.masked_errordescription)
    new_id = db_insert(llmresponse, ctx)
    logger.info(f"Stored LLM-only path result in DB (id={new_id})")


# ---- DLQ helper ----

def publish_to_dlx(ch: pika.channel.Channel, body: bytes, headers: dict):
    try:
        if DLQ_ENABLED:
            props = pika.BasicProperties(headers=headers, delivery_mode=2)
            ch.basic_publish(exchange=DLX_EXCHANGE, routing_key=DLQ_ROUTING_KEY, body=body, properties=props)
            logger.info(f"Published to DLX {DLX_EXCHANGE}:{DLQ_ROUTING_KEY}")
        else:
            logger.warning("DLQ not configured; discarding message")
    except Exception:
        logger.exception("Failed to publish to DLX")


# ---- retry / failure handling for messages ----

def handle_retry(ch, method, properties, body: bytes, retry_count: int, error: Exception):
    # If retry_count exceeded, send to DLQ
    if retry_count >= MAX_RETRIES_PER_MESSAGE:
        logger.error(f"Max retries reached for message; moving to DLQ: {error}")
        headers = (properties.headers or {}).copy() if properties else {}
        headers.update({'x-retry-count': retry_count, 'x-error': str(type(error).__name__)})

        # Prefer publishing to DLX (explicit) so DLX metadata is present
        publish_to_dlx(ch, body, headers)

        # ACK original so it doesn't remain in queue
        try:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            logger.exception('Failed to ack after DLQ publish')
        return

    # otherwise republish with increased retry and exponential backoff
    new_retry = retry_count + 1
    backoff = 10
    logger.warning(f"Retrying message {new_retry}/{MAX_RETRIES_PER_MESSAGE} after sleep {backoff}s")
    time.sleep(backoff)

    props = pika.BasicProperties(headers={'x-retry-count': new_retry}, delivery_mode=2)
    ch.basic_publish(exchange='', routing_key=Config.QUEUE, body=body, properties=props)

    try:
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        logger.exception('Failed to ack after republish')


# ---- RabbitMQ callback ----

def callback(ch, method, properties, body):
    delivery_tag = method.delivery_tag
    logger.debug(f"Message received tag={delivery_tag}")

    retry_count = 0
    if properties and getattr(properties, 'headers', None):
        retry_count = properties.headers.get('x-retry-count', 0)

    try:
        payload = json.loads(body.decode('utf-8'))
    except Exception:
        logger.exception('Invalid JSON - acking and dropping')
        ch.basic_ack(delivery_tag=delivery_tag)
        return

    # Basic validation
    required = ['applicationName', 'code', 'description', 'timestamp']
    missing = [f for f in required if f not in payload]
    if missing:
        logger.error(f"Missing fields {missing} - acking")
        ch.basic_ack(delivery_tag=delivery_tag)
        return

    try:
        # Build a fresh per-message context — no shared mutable state on services
        ctx = store_incoming_payload_and_set_uuid(payload)
        epoch = payload.get('timestamp')
        ctx.error_ts_str = (
            datetime.datetime.utcfromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S') if epoch
            else datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        )

        # Inject session ID into logging context FIRST so every log in this message has session_id
        _log_token = set_session_id(ctx.sessionid)
        try:
            logger.info(f"Processing: app={ctx.incoming_payload.get('applicationName')} code={ctx.incoming_payload.get('code')} session={ctx.sessionid}")

            # If any circuit is open, fail-fast
            if services.cb_db.is_open() or services.cb_llm.is_open() or services.cb_qdrant.is_open():
                logger.warning('One or more circuits open; performing retry/backoff')
                raise Exception('Downstream service circuit open')

            main(ctx)

            ch.basic_ack(delivery_tag=delivery_tag)
            logger.info(f"Message acked | app={ctx.incoming_payload.get('applicationName')} code={ctx.incoming_payload.get('code')}")
        finally:
            clear_session_id(_log_token)

    except Exception as e:
        logger.exception('Processing failed')
        handle_retry(ch, method, properties, body, retry_count, e)


# ---- graceful shutdown ----

def signal_handler(signum, frame):
    logger.info(f"Signal {signum} received - shutting down")
    try:
        if services.channel and not services.channel.is_closed:
            services.channel.stop_consuming()
    except Exception:
        logger.exception('Error during shutdown')
    try:
        if services.connection and not services.connection.is_closed:
            services.connection.close()
    except Exception:
        pass
    sys.exit(0)


# ---- entrypoint ----
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        services.initialize()
        services.initialize_rabbitmq()

        services.channel.basic_consume(queue=Config.QUEUE, on_message_callback=callback, auto_ack=False)

        logger.info(f"Listening on {Config.QUEUE}; prefetch={PREFETCH_COUNT}; DLQ={DLQ_ENABLED}")
        services.channel.start_consuming()

    except Exception as e:
        logger.exception('Fatal error in consumer')
        try:
            if services.connection and not services.connection.is_closed:
                services.connection.close()
        except Exception:
            pass
        sys.exit(1)