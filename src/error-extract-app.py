# ...existing code...
import os
from dotenv import load_dotenv, dotenv_values 
import schedule
import time
import json
import logging
from typing import Dict, Any, List
import requests
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler
import pika
from pathlib import Path

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)
# Configuration - adjust for your environment
POLL_INTERVAL_SECONDS = 60
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RABBIT_URL = str(os.getenv("RABBIT_URL"))

EXCHANGE = str(os.getenv("EXCHANGE"))
QUEUE = str(os.getenv("QUEUE"))
ROUTING_KEY = str(os.getenv("ROUTING_KEY"))



def build_elk_query(window_seconds: int = 60) -> Dict[str, Any]:
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
        }
    }


def call_elk(query: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"Authorization": os.getenv("ELK_APIKEY"), "Content-Type": "application/json"}
    resp = requests.post(os.getenv('ELK_SEARCH_URL'), headers=headers, json=query, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_hits(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results=[]
    for item in hits:
        src = item.get("_source", {})
        msg = src.get("message", "")

        # Parse the message JSON string
        try:
            parsed_msg = json.loads(msg)
        except json.JSONDecodeError:
            parsed_msg = {"rawMessage": msg}

        results.append(
            {
                "applicationName": parsed_msg.get("applicationName"),
                "correlationId": parsed_msg.get("correlationId"),
                "code": parsed_msg.get("code"),
                "description": parsed_msg.get("description") or parsed_msg.get("rawMessage"),
                "timestamp": src.get("instant", {}).get("epochSecond")
            }
        )
    return results


def publish_to_rabbitmq(message: dict):
    params = pika.URLParameters(RABBIT_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(EXCHANGE)
    channel.queue_declare(queue=QUEUE)
    channel.queue_bind(QUEUE, EXCHANGE, ROUTING_KEY)

    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=ROUTING_KEY,
        body=json.dumps(message)
    )
    connection.close()


def process_cycle():
    query = build_elk_query(POLL_INTERVAL_SECONDS)
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

    for rec in records:
        print(rec)
        publish_to_rabbitmq(rec)
        
        



if __name__ == "__main__":
    logger.info(f"▶ Starting ELK Poller — runs every {POLL_INTERVAL_SECONDS} seconds")
    scheduler = BlockingScheduler()
    scheduler.add_job(process_cycle, "interval", seconds=POLL_INTERVAL_SECONDS)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("⛔ Scheduler stopped manually")
