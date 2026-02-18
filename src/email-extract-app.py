import os
import json
import logging
import time
import signal
import sys
import msal
import requests
import pika
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from apscheduler.schedulers.blocking import BlockingScheduler

from src.config import Config
from src.structuraldb import DB

# Setup logging
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global variables
rabbitmq_connection: Optional[pika.BlockingConnection] = None
rabbitmq_channel: Optional[pika.channel.Channel] = None
scheduler: Optional[BlockingScheduler] = None
msal_app: Optional[msal.ConfidentialClientApplication] = None

# Graph API Configuration
# For Client Credentials, we usually use the /.default scope
SCOPES = ["https://graph.microsoft.com/.default"]

def setup_rabbitmq_connection():
    """Setup persistent RabbitMQ connection"""
    global rabbitmq_connection, rabbitmq_channel
    
    try:
        if rabbitmq_connection is None or rabbitmq_connection.is_closed:
            params = pika.URLParameters(Config.RABBIT_URL)
            params.connection_attempts = 3
            rabbitmq_connection = pika.BlockingConnection(params)
            rabbitmq_channel = rabbitmq_connection.channel()
            
            rabbitmq_channel.exchange_declare(
                exchange=Config.EXCHANGE,
                exchange_type=Config.EXCHANGE_TYPE,
                durable=True
            )
            rabbitmq_channel.queue_declare(queue=Config.QUEUE, durable=True)
            logger.info("RabbitMQ connection established")
            
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        rabbitmq_connection = None

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

    logger.info(
        f"Parsed email: app={app_name}, correlationId={correlation_id}, "
        f"code={error_code}, desc_len={len(description)}"
    )

    return {
        "applicationName": app_name,
        "correlationId": correlation_id,
        "code": error_code,
        "description": description,
        "timestamp": datetime.now(timezone.utc).timestamp(),
        "source": "email"
    }

def is_duplicate(app_name, code, desc, timestamp):
    """Check duplicate using DB with description matching and configurable time window"""
    try:
        with DB() as db:
            # Convert UTC to local if needed
            if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is not None:
                local_timestamp = timestamp.astimezone().replace(tzinfo=None)
            else:
                local_timestamp = timestamp
            
            query = """
                SELECT id, error_timestamp,
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
                    local_timestamp, app_name, code, desc,
                    local_timestamp, Config.DB_DUPLICATE_WINDOW_MINUTES, local_timestamp
                ),
                fetch=True
            )
            
            if result:
                record = result[0]
                minutes_ago = float(record["minutes_ago"])
                logger.warning(f"⏭️ DUPLICATE: {app_name}/{code} (occurred {abs(minutes_ago):.1f}min ago)")
                return True
            
            logger.info(f"✅ New error: {app_name}/{code}")
            return False
    except Exception as e:
        logger.error(f"DB Check failed: {e}")
        return False  # Fail open

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
        logger.info(f"📊 Batch deduplication: {len(emails)} raw → {len(unique)} unique")
    
    return unique

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
    
    token = get_graph_token()
    if not token:
        logger.error("Could not obtain Graph Token. Skipping cycle.")
        return

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    # URL for accessing specific user's mail via Application Permissions
    url = f"https://graph.microsoft.com/v1.0/users/{Config.AZURE_TARGET_EMAIL}/messages"
    
    # Build time filter: only emails from the last POLL_INTERVAL_SECONDS seconds
    since_dt = datetime.now(timezone.utc).replace(microsecond=0)
    from datetime import timedelta
    since_dt = since_dt - timedelta(seconds=Config.EMAIL_POLL_INTERVAL)
    since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO 8601 UTC for OData
    
    # Filter: unread + received in last poll window
    # Note: Filtering by 'from' + 'receivedDateTime' + 'isRead' causes InefficientFilter error
    # So we filter by time & status here, and filter by sender in Python
    odata_filter = (
        f"isRead eq false "
        f"and receivedDateTime ge {since_str}"
    )
    
    params = {
        '$filter': odata_filter,
        '$top': 50,
        '$select': 'id,subject,body,receivedDateTime,from',
        '$orderby': 'receivedDateTime desc'
    }
    logger.info(f"Polling unread emails since {since_str}")
    
    # Metrics
    processed = 0
    published = 0
    skipped_duplicate = 0
    skipped_invalid = 0
    
    try:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            logger.error(f"Graph API Error: {resp.status_code} - {resp.text}")
            return
            
        emails = resp.json().get('value', [])
        if not emails:
            logger.info("No unread emails.")
            return

        # 1. Deduplicate batch
        unique_emails = deduplicate_emails(emails)
        
        # 2. Filter by Sender (Python side)
        target_sender = Config.EMAIL_SENDER_FILTER.lower()
        filtered_emails = []
        
        for email in unique_emails:
            sender = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
            if sender == target_sender:
                filtered_emails.append(email)
            else:
                logger.debug(f"Skipping email from '{sender}' (not {target_sender})")
        
        if not filtered_emails:
            logger.info(f"Found {len(emails)} unread, but NONE from {target_sender}.")
            return

        logger.info(f"Found {len(filtered_emails)} matching emails from {target_sender}")
        
        setup_rabbitmq_connection()

        for email in filtered_emails:
            try:
                # Extract body
                body_content = email.get('body', {}).get('content', '')
                subject = email.get('subject', '')
                
                payload = parse_tibco_email(body_content, subject)
                
                # Validate required fields
                if not all([payload.get('applicationName'), payload.get('code'), payload.get('description')]):
                    logger.warning(f"⚠️ Skipping email with missing fields: {email.get('id')}")
                    skipped_invalid += 1
                    mark_email_read(email['id'], "", token)
                    continue
                
                # Convert timestamp
                try:
                    error_timestamp = datetime.fromtimestamp(payload['timestamp'], tz=timezone.utc)
                except (ValueError, OSError, TypeError) as e:
                    logger.error(f"❌ Invalid timestamp {payload['timestamp']}: {e}")
                    skipped_invalid += 1
                    mark_email_read(email['id'], "", token)
                    continue
                
                # Check Deduplicate
                if is_duplicate(payload['applicationName'], payload['code'], payload['description'], error_timestamp):
                    skipped_duplicate += 1
                    mark_email_read(email['id'], "", token)
                    continue

                # Publish
                if rabbitmq_channel:
                    logger.info(f"Publishing: {payload['applicationName']}, {payload['code']}, {payload['description']}, {error_timestamp}")
                    rabbitmq_channel.basic_publish(
                        exchange=Config.EXCHANGE,
                        routing_key=Config.ROUTING_KEY,
                        body=json.dumps(payload),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type='application/json'
                        )
                    )
                    published += 1
                    logger.info(f"✅ Published: {payload['code']}")
                
                # Mark as read
                mark_email_read(email['id'], "", token)
                processed += 1
                
            except Exception as e:
                logger.error(f"Failed to process email {email.get('id')}: {e}")

    except Exception as e:
        logger.error(f"Poll cycle failed: {e}")
    
    # Cycle metrics
    duration = (datetime.now() - cycle_start).total_seconds()
    logger.info(
        f"📊 Cycle completed in {duration:.2f}s: "
        f"total={len(emails) if 'emails' in locals() else 0}, "
        f"unique={len(unique_emails) if 'unique_emails' in locals() else 0}, "
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

    logger.info(f"🚀 Starting Email Extractor (Service Principal) for {Config.AZURE_TARGET_EMAIL}")
    
    scheduler = BlockingScheduler()
    # Runs every X seconds
    scheduler.add_job(process_email_cycle, 'interval', seconds=Config.EMAIL_POLL_INTERVAL)
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        cleanup_and_exit()
