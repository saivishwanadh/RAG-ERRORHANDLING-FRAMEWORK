
import json
import uuid
import logging
import signal
import sys
import time
import datetime
import re
from typing import Dict, Any, Optional, Callable, Tuple

import pika
import requests
from qdrant_client import models

from src.vectordb import QdrantStore
from src.embeddingmodel import EmbeddingGenerator
from src.structuraldb import DB
from src.geminicall import GeminiClient
from src.sendemail import EmailService
from src.maskdata import LogSanitizer
from src.service_alert import ServiceAlertNotifier
from src.incident_manager import IncidentManager
from src.config import Config

# ---- Logging ----
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
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
        self.cb_email = CircuitBreaker(fail_threshold=3, reset_timeout_sec=30)

        # Service health alert notifier (shared, cooldown-aware)
        self.alert = ServiceAlertNotifier()

        # ITSM incident manager (loosely coupled — disabled when ITSM_PROVIDER=none)
        self.incidents = IncidentManager()

    # ---- initialization ----
    def initialize(self):
        logger.info("Initializing services...")
        Config.validate()

        # Embedding
        try:
            self.embed_gen = EmbeddingGenerator(api_key=Config.GEMINI_APIKEY)
            logger.info("Embedding generator initialized")
        except Exception as e:
            logger.exception("Failed to init Embedding generator")
            self.alert.notify_service_down(
                "Gemini/LLM", str(e), context="initialize:EmbeddingGenerator"
            )
            raise

        # Qdrant
        try:
            self.store = QdrantStore(embedding_model=self.embed_gen.embeddings)
            logger.info("Qdrant initialized")
        except Exception as e:
            logger.exception("Failed to init Qdrant")
            self.alert.notify_service_down(
                "Qdrant/VectorDB", str(e), context="initialize:QdrantStore"
            )
            raise

        # Gemini
        try:
            self.client = GeminiClient(api_key=Config.GEMINI_APIKEY)
            logger.info("Gemini initialized")
        except Exception as e:
            logger.exception("Failed to init Gemini")
            self.alert.notify_service_down(
                "Gemini/LLM", str(e), context="initialize:GeminiClient"
            )
            raise

        # Sanitizer
        try:
            self.sanitizer = LogSanitizer()
            logger.info("Sanitizer initialized")
        except Exception as e:
            logger.exception("Failed to init sanitizer")
            raise

        # DB test
        try:
            with DB() as db:
                db.execute("SELECT 1", fetch=True)
            logger.info("DB reachable")
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
        logger.info("RabbitMQ connected")

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
    def send_email(self, template_name: str, payload: Dict[str, Any]):
        if self.cb_email.is_open():
            raise Exception("Email circuit open")
        try:
            svc = EmailService(template_name)
            if template_name == "databasesol-main-ui.html":
                html = svc.populate_template_db(payload)
            else:
                html = svc.populate_template_llm(payload)

            subject = f"Error Notification: {payload.get('errorType')} in {payload.get('serviceName')}"
            svc.send_email(html, subject, Config.TO_EMAIL)
            self.cb_email.record_success()
            return True
        except Exception:
            self.cb_email.record_failure()
            logger.exception("Send email failed")
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
# ---- helpers for formatting ----

def format_solution_text(solution_text: str) -> str:
    if not solution_text:
        return ""
    text = solution_text.replace("\\n", "\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    html_parts = []
    in_list = False
    for line in lines:
        if re.match(r'^\d+\.\s+', line):
            if not in_list:
                html_parts.append("<ol style='margin-left: 20px; margin-top: 10px;'>")
                in_list = True
            content = re.sub(r'^\d+\.\s+', '', line)
            html_parts.append(f"<li style='margin-bottom: 8px;'>{content}</li>")
        else:
            if in_list:
                html_parts.append("</ol>")
                in_list = False
            html_parts.append(f"<p style='margin-bottom: 10px;'>{line}</p>")
    if in_list:
        html_parts.append("</ol>")
    return "\n".join(html_parts)


def format_confirmed_solutions(solutions_text: str) -> str:
    if not solutions_text:
        return "<p>No confirmed solutions available.</p>"
    solution_blocks = re.split(r'Solution \d+:', solutions_text)
    solution_blocks = [block.strip() for block in solution_blocks if block.strip()]
    if not solution_blocks:
        return "<p>No confirmed solutions available.</p>"
    html_parts = []
    for i, block in enumerate(solution_blocks, 1):
        html_parts.append(f"<div style='margin-bottom: 15px; padding: 10px; background-color: #f0f8ff; border-left: 4px solid #007bff;'>")
        html_parts.append(f"<h4 style='color: #007bff; margin-top: 0;'>Confirmed Solution {i}</h4>")
        html_parts.append(format_solution_text(block))
        html_parts.append("</div>")
    return "\n".join(html_parts)


# ---- core pipeline functions (use service wrappers) ----

services = ServiceContainer()


def store_incoming_payload_and_set_uuid(payload: Dict[str, Any]):
    services.incoming_payload = payload
    services.sanitizer = services.sanitizer or LogSanitizer()
    services.masked_errordescription = services.sanitizer.sanitize(payload.get('description', ''))
    logger.info(f"Masked Data: {services.masked_errordescription}")
    services.sessionid = str(uuid.uuid4())
    logger.info(f"Processing: App={payload.get('applicationName')} Code={payload.get('code')} Session={services.sessionid}")


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

def db_insert(llmresponse: dict):
    cleanErr = clean_error_description(services.masked_errordescription)
    insert_sql = """
        INSERT INTO errorsolutiontable (
            application_name, error_code, error_description, sessionID,
            llm_solution, error_timestamp, sessionid_status, occurrence_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
    """
    params = (
        services.incoming_payload.get('applicationName'),
        services.incoming_payload.get('code'),
        services.incoming_payload.get('description', ''),
        services.sessionid,
        json.dumps(llmresponse),
        services.error_ts_str,
        'active',
        services.incoming_payload.get('occurrence_count', 1)  # Use batch-counted value from extractor
    )

    inserted_rows = services.db_execute(insert_sql, params, fetch=True)
    logger.info("Inserted structural DB row")
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


# send formatted email wrapper

def send_formatted_email(email_payload: Dict[str, Any], template_name: str):
    try:
        services.send_email(template_name, email_payload)
        logger.info("Email sent")
    except Exception:
        logger.exception("Failed to send formatted email")


# main processing flow with guarded calls

def main():
    # structural DB check
    sql = """
        SELECT id, ops_solution, llm_solution
        FROM errorsolutiontable
        WHERE error_description = %s
        AND application_name = %s
        LIMIT 1;
    """
    params = (services.incoming_payload.get('description'), services.incoming_payload.get('applicationName'))

    rows = services.db_execute(sql, params, fetch=True)
    if rows and len(rows) > 0:
        solutions = rows[0].get('ops_solution')
        if solutions:
            logger.info("Found verified solution in structural DB")
            # If we have a verified solution, use it.
            # We don't need to re-insert into DB or call LLM.
            # Just send the email.
            
            # Note: We need to load the ORIGINAL LLM response to populate the template fully,
            # or we can pass empty LLM fields if we only care about the confirmed solution.
            # The original code re-inserted a new row for every occurrence, which is good for tracking freq.
            
            llm_str = rows[0].get('llm_solution')
            llmresponse = json.loads(llm_str) if llm_str else {}
            
            new_id = db_insert(llmresponse)
            
            email_payload = {
                'serviceName': services.incoming_payload.get('applicationName'),
                'environment': 'Non Prod',
                'timestamp': services.error_ts_str,
                'errorType': services.incoming_payload.get('code'),
                'errorMessage': services.incoming_payload.get('description'),
                'errorId': str(new_id),
                'sessionId': services.sessionid,
                'rootCause': llmresponse.get('rootCause','N/A'),
                'solution1': {'instructions': llmresponse.get('solution1',{}).get('instructions','')},
                'solution2': {'instructions': llmresponse.get('solution2',{}).get('instructions','')},
                'solution3': {'instructions': llmresponse.get('solution3',{}).get('instructions','')},
                'confirmedSolutions': solutions
            }
            send_formatted_email(email_payload, 'databasesol-main-ui.html')

            # ITSM: create / update incident for this error (DB-verified solution path)
            _err_key = f"{services.incoming_payload.get('applicationName')}_{services.incoming_payload.get('code')}"
            services.incidents.handle_error(
                error_key=_err_key,
                app_name=services.incoming_payload.get('applicationName', ''),
                error_code=services.incoming_payload.get('code', ''),
                description=services.incoming_payload.get('description', ''),
                count=services.incoming_payload.get('occurrence_count', 1),
                llm_summary=llmresponse.get('rootCause', '') or '',
            )
            return
        else:
             logger.info("Found record in structural DB but NO verified solution - falling through to Vector DB")

    # vector path
    logger.info("Checking vector DB")
    try:
        cleanErr = clean_error_description(services.masked_errordescription)
        embed_input = f"Error:{services.incoming_payload.get('code','')} Description:{cleanErr.get('cleanText','')}"
        raw_embedding = services.embed_gen.get_embedding(embed_input)

        qfilter = models.Filter(must=[models.FieldCondition(key='error_code', match=models.MatchValue(value=services.incoming_payload.get('code')))])
        result = services.qdrant_search(collection='error_solutions', vector=raw_embedding, limit=3, query_filter=qfilter)
        points = [r for r in result if getattr(r, 'score', 0) >= 0.85]

        if len(points) > 0:
            logger.info(f"Found {len(points)} matching vectors")
            
            # Restoration: Extract context and pass to LLM
            context_text = extract_solutions_from_points(points)
            logger.info(f"Injecting context (len={len(context_text)}) into LLM prompt")
            
            llmresponse = services.call_llm(
                services.incoming_payload.get('code',''), 
                services.masked_errordescription,
                context=context_text
            )
            new_id = db_insert(llmresponse)
            solutions = extract_solutions_from_points(points)
            email_payload = {
                'serviceName': services.incoming_payload.get('applicationName'),
                'environment': 'Non Prod',
                'timestamp': services.error_ts_str,
                'errorType': services.incoming_payload.get('code'),
                'errorMessage': services.incoming_payload.get('description'),
                'errorId': str(new_id),
                'sessionId': services.sessionid,
                'rootCause': llmresponse.get('rootCause','N/A'),
                'solution1': {'instructions': llmresponse.get('solution1',{}).get('instructions','')},
                'solution2': {'instructions': llmresponse.get('solution2',{}).get('instructions','')},
                'solution3': {'instructions': llmresponse.get('solution3',{}).get('instructions','')},
                'confirmedSolutions': solutions
            }
            send_formatted_email(email_payload, 'databasesol-main-ui.html')

            # ITSM: create / update incident for this error (VectorDB path)
            error_key = f"{services.incoming_payload.get('applicationName')}_{services.incoming_payload.get('code')}"
            llm_summary = llmresponse.get('rootCause', '') or ''
            services.incidents.handle_error(
                error_key=error_key,
                app_name=services.incoming_payload.get('applicationName', ''),
                error_code=services.incoming_payload.get('code', ''),
                description=services.incoming_payload.get('description', ''),
                count=services.incoming_payload.get('occurrence_count', 1),
                llm_summary=llm_summary,
            )
            return

    except Exception:
        logger.exception('Vector DB path failed - falling back to LLM only')

    # LLM only path
    logger.info('Using LLM only')
    llmresponse = services.call_llm(services.incoming_payload.get('code',''), services.masked_errordescription)
    new_id = db_insert(llmresponse)
    email_payload = {
        'serviceName': services.incoming_payload.get('applicationName'),
        'environment': 'Non Prod',
        'timestamp': services.error_ts_str,
        'errorType': services.incoming_payload.get('code'),
        'errorMessage': services.incoming_payload.get('description'),
        'errorId': str(new_id),
        'sessionId': services.sessionid,
        'rootCause': llmresponse.get('rootCause','N/A'),
        'solution1': {'instructions': llmresponse.get('solution1',{}).get('instructions','')},
        'solution2': {'instructions': llmresponse.get('solution2',{}).get('instructions','')},
        'solution3': {'instructions': llmresponse.get('solution3',{}).get('instructions','')}
    }
    send_formatted_email(email_payload, 'email-main-ui.html')

    # ITSM: create / update incident for this error (LLM-only path)
    error_key = f"{services.incoming_payload.get('applicationName')}_{services.incoming_payload.get('code')}"
    llm_summary = llmresponse.get('rootCause', '') or ''
    services.incidents.handle_error(
        error_key=error_key,
        app_name=services.incoming_payload.get('applicationName', ''),
        error_code=services.incoming_payload.get('code', ''),
        description=services.incoming_payload.get('description', ''),
        count=services.incoming_payload.get('occurrence_count', 1),
        llm_summary=llm_summary,
    )


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
    logger.info(f"Message received tag={delivery_tag}")

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
        store_incoming_payload_and_set_uuid(payload)
        epoch = payload.get('timestamp')
        services.error_ts_str = str(datetime.datetime.fromtimestamp(epoch)) if epoch else str(datetime.datetime.now())

        # If any circuit is open, fail-fast: republish with retry increment to slow things down
        if services.cb_db.is_open() or services.cb_llm.is_open() or services.cb_qdrant.is_open():
            logger.warning('One or more circuits open; performing retry/backoff')
            raise Exception('Downstream service circuit open')

        # main pipeline
        main()

        ch.basic_ack(delivery_tag=delivery_tag)
        logger.info('Message processed and acknowledged')

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