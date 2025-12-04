"""
Updated main file with solution text formatting integrated
"""

import os
import re
import json
import uuid
import logging
import datetime
from typing import Dict, Any, Optional
from pathlib import Path

import pika
from dotenv import load_dotenv
from qdrant_client import models

from vectordb import QdrantStore
from embeddingmodel import EmbeddingGenerator
from structuraldb import DB
from geminicall import GeminiClient
from sendemail import EmailService

# Load environment variables
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

# Configuration
MQ_URL = str(os.getenv("RABBIT_URL"))
QUEUE = str(os.getenv("QUEUE"))
API_KEY = os.getenv("HUGGINGFACE_APIKEY")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

# Global variables
incoming_payload: Optional[Dict[str, Any]] = None
sessionid: Optional[str] = None
error_ts_str: Optional[str] = None


# ============================================================================
# Solution Formatting Functions
# ============================================================================

def format_solution_text(solution_text: str) -> str:
    """Convert plain text solution with numbered steps to formatted HTML."""
    if not solution_text:
        return ""
    
    # Replace escaped newlines with actual newlines
    text = solution_text.replace("\\n", "\n")
    
    # Split into lines
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    
    html_parts = []
    in_list = False
    
    for line in lines:
        # Check if line starts with a number (e.g., "1.", "2.", "3.")
        if re.match(r'^\d+\.\s+', line):
            if not in_list:
                html_parts.append("<ol style='margin-left: 20px; margin-top: 10px;'>")
                in_list = True
            # Remove the number and add as list item
            content = re.sub(r'^\d+\.\s+', '', line)
            html_parts.append(f"<li style='margin-bottom: 8px;'>{content}</li>")
        else:
            # Close list if we were in one
            if in_list:
                html_parts.append("</ol>")
                in_list = False
            # Add as paragraph
            html_parts.append(f"<p style='margin-bottom: 10px;'>{line}</p>")
    
    # Close list if still open
    if in_list:
        html_parts.append("</ol>")
    
    return "\n".join(html_parts)


def format_confirmed_solutions(solutions_text: str) -> str:
    """Format confirmed solutions from vector database."""
    if not solutions_text:
        return "<p>No confirmed solutions available.</p>"
    
    # Split by "Solution N:" pattern
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


# ============================================================================
# Core Functions
# ============================================================================

def store_incoming_payload_and_set_uuid(payload: Dict[str, Any]) -> None:
    """Store incoming JSON payload and set a new UUID."""
    global incoming_payload, sessionid
    incoming_payload = payload
    sessionid = str(uuid.uuid4())


def clean_error_description(text: str) -> dict:
    """Normalize/clean incoming description."""
    if not text:
        return {"cleanText": ""}
    
    s = text
    s = s.replace("\\n", " ").replace("\n", " ")
    s = s.replace('\\"', "").replace("&quot;", "")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[\(\)\[\]\{\}]", "", s)
    s = re.sub(r"[\'\":;^|\-,]", "", s)
    s = s.strip().strip('"').strip("'")
    return {"cleanText": s}


def llm_response():
    """Call LLM to get solution."""
    error_code = incoming_payload.get("code", "")
    error_description = incoming_payload.get("description", "")
    result = client.analyze_error(error_code, error_description)
    return result


def db_insert(llmresponse: dict):
    """Insert error and solution into databases."""
    cleanErrorDescription = clean_error_description(incoming_payload.get("description", ""))
    
    insert_sql = """
        INSERT INTO errorsolutiontable (
            application_name,
            error_code,
            error_description,
            sessionID,
            llm_solution,
            error_timestamp,
            sessionid_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    
    params = (
        incoming_payload.get("applicationName"),
        incoming_payload.get("code"),
        incoming_payload.get("description"),
        sessionid,
        json.dumps(llmresponse),
        error_ts_str,
        "active",
    )
    
    inserted_rows = db.execute(insert_sql, params, fetch=True)
    logging.info("Inserted data into structural database")
    
    if inserted_rows and len(inserted_rows) > 0:
        new_id = inserted_rows[0].get("id") if isinstance(inserted_rows[0], dict) else inserted_rows[0]["id"]
    else:
        new_id = None
    
    logging.info(f"Inserted ID: {new_id}")
    
    # Generate embedding and insert into vector database
    embed_input = f"Error:{incoming_payload.get('code', '')} Description:{cleanErrorDescription.get('cleanText', '')}"
    raw_embedding = embed_gen.get_embedding(embed_input)
    
    store.upsert_vector(
        collection="error_solutions",
        vector_id=new_id,
        vector=raw_embedding,
        payload={
            "error_code": incoming_payload.get("code", ""),
            "error_description": cleanErrorDescription.get("cleanText", ""),
            "solution": llmresponse
        }
    )
    
    return new_id


def extract_solutions_from_points(points):
    """Extract solutions from vector search results."""
    final_output = ""
    solution_counter = 1
    
    for point in points:
        payload = getattr(point, "payload", {})
        solution_text = payload.get("solution")
        
        if not solution_text:
            continue
        
        final_output += f"Solution {solution_counter}:\n{json.dumps(solution_text)}\n\n"
        solution_counter += 1
    
    return final_output.strip()


def send_formatted_email(email_payload: Dict[str, Any], template_name: str):
    """Send formatted email with proper solution formatting."""
    try:
        email_service = EmailService(template_name)
        
        if template_name == "databasesol-main-ui.html":
            html_content = email_service.populate_template_db(email_payload)
        else:
            html_content = email_service.populate_template_llm(email_payload)
        
        subject = f"Error Notification: {email_payload['errorType']} in {email_payload['serviceName']}"
        to_address = "saivishwanadh.veerlapati@prowesssoft.com"
        
        email_service.send_email(html_content, subject, to_address)
        logging.info(f"Email sent successfully using template: {template_name}")
        
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        raise


# ============================================================================
# Main Processing Logic
# ============================================================================

def main():
    """Main processing function."""
    
    # Check structural database first
    sql = """
        SELECT id, ops_solution, llm_solution
        FROM errorsolutiontable
        WHERE error_description = %s
        AND application_name = %s;
    """
    params = (
        incoming_payload.get("description"),
        incoming_payload.get("applicationName")
    )
    
    rows = db.execute(sql, params, fetch=True)
    
    # ========================================================================
    # Case 1: Found in Structural Database
    # ========================================================================
    if len(rows) > 0:
        logging.info("Incoming error details matched in Structural Database")
        
        llmresponse = json.loads(rows[0].get("llm_solution"))
        new_id = db_insert(llmresponse)
        solutions = rows[0].get("ops_solution")
        
        email_payload = {
            "serviceName": incoming_payload.get("applicationName"),
            "environment": "Non Prod",
            "timestamp": error_ts_str,
            "errorType": incoming_payload.get("code"),
            "errorMessage": incoming_payload.get("description"),
            "errorId": str(new_id),
            "sessionId": sessionid,
            "rootCause": llmresponse.get("rootCause"),
            "solution1": {"instructions": llmresponse.get("solution1", {}).get("instructions")},
            "solution2": {"instructions": llmresponse.get("solution2", {}).get("instructions")},
            "solution3": {"instructions": llmresponse.get("solution3", {}).get("instructions")},
            "confirmedSolutions": solutions
        }
        
        send_formatted_email(email_payload, "databasesol-main-ui.html")
        logging.info("Email sent with solutions from Structural DB and LLM")
        
        return
    
    # ========================================================================
    # Case 2: Check Vector Database
    # ========================================================================
    logging.info("Checking vector database")
    
    cleanErrorDescription = clean_error_description(incoming_payload.get("description", ""))
    embed_input = f"Error:{incoming_payload.get('code', '')} Description:{cleanErrorDescription.get('cleanText', '')}"
    raw_embedding = embed_gen.get_embedding(embed_input)
    
    # Vector search
    result = store.search(
        collection="error_solutions",
        vector=raw_embedding,
        limit=3,
        query_filter=models.Filter(
            must=[
                models.FieldCondition(
                    key="error_code",
                    match=models.MatchValue(value=incoming_payload.get("code"))
                )
            ]
        )
    )
    
    points = [r for r in result if r.score >= 0.85]
    
    # ========================================================================
    # Case 2a: Found in Vector Database
    # ========================================================================
    if len(points) > 0:
        logging.info(f"Found {len(points)} matching points in vector database")
        
        llmresponse = llm_response()
        new_id = db_insert(llmresponse)
        solutions = extract_solutions_from_points(points)
        
        email_payload = {
            "serviceName": incoming_payload.get("applicationName"),
            "environment": "Non Prod",
            "timestamp": error_ts_str,
            "errorType": incoming_payload.get("code"),
            "errorMessage": incoming_payload.get("description"),
            "errorId": str(new_id),
            "sessionId": sessionid,
            "rootCause": llmresponse.get("rootCause"),
            "solution1": {"instructions": llmresponse.get("solution1", {}).get("instructions")},
            "solution2": {"instructions": llmresponse.get("solution2", {}).get("instructions")},
            "solution3": {"instructions": llmresponse.get("solution3", {}).get("instructions")},
            "confirmedSolutions": solutions
        }
        
        send_formatted_email(email_payload, "databasesol-main-ui.html")
        logging.info("Email sent with solutions from Vector DB and LLM")
        
        return
    
    # ========================================================================
    # Case 3: Not found - Use LLM only
    # ========================================================================
    logging.info("No matching points in databases, calling LLM for solution")
    
    llmresponse = llm_response()
    new_id = db_insert(llmresponse)
    
    email_payload = {
        "serviceName": incoming_payload.get("applicationName"),
        "environment": "Non Prod",
        "timestamp": error_ts_str,
        "errorType": incoming_payload.get("code"),
        "errorMessage": incoming_payload.get("description"),
        "errorId": str(new_id),
        "sessionId": sessionid,
        "rootCause": llmresponse.get("rootCause"),
        "solution1": {"instructions": llmresponse.get("solution1", {}).get("instructions")},
        "solution2": {"instructions": llmresponse.get("solution2", {}).get("instructions")},
        "solution3": {"instructions": llmresponse.get("solution3", {}).get("instructions")},
    }
    
    send_formatted_email(email_payload, "email-main-ui.html")
    logging.info("Email sent with LLM generated solutions")


# ============================================================================
# RabbitMQ Consumer
# ============================================================================

def callback(ch, method, properties, body):
    """RabbitMQ message callback."""
    global error_ts_str
    
    logging.info(f"📥 Incoming message from MQ")
    
    try:
        msg_str = body.decode("utf-8")
        payload = json.loads(msg_str)
    except Exception as e:
        logging.error(f"❌ Invalid JSON message: {e}")
        return
    
    store_incoming_payload_and_set_uuid(payload)
    
    # Convert timestamp
    epoch = payload.get("timestamp")
    if epoch:
        error_ts_str = str(datetime.datetime.fromtimestamp(epoch))
    else:
        error_ts_str = str(datetime.datetime.now())
    
    try:
        main()
        logging.info("✔ Finished processing successfully")
    except Exception as e:
        logging.error(f"❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    logging.info("Starting error-solution-create process")
    
    # Initialize services
    db = DB()
    store = QdrantStore()
    client = GeminiClient(api_key=os.getenv("GEMINI_APIKEY"))
    embed_gen = EmbeddingGenerator(api_key=API_KEY)
    
    # Start RabbitMQ consumer
    try:
        connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
        channel = connection.channel()
        channel.basic_consume(queue=QUEUE, on_message_callback=callback, auto_ack=True)
        
        logging.info("🐇 Waiting for messages from RabbitMQ...")
        channel.start_consuming()
        
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise