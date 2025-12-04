from apscheduler.schedulers.blocking import BlockingScheduler
from structuraldb import DB
from sendemail import EmailService
import json
import logging
from dotenv import load_dotenv, dotenv_values 
from pathlib import Path
import os

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)


def run_scheduler():
    db = DB()
    email_service = EmailService("email-main-ui.html")
    rows = db.execute("""
        SELECT *
        FROM errorsolutiontable
        WHERE (ops_solution IS NULL OR ops_solution = '')
        AND retry_count < 3;
    """, fetch=True)
    if not rows:
        logging.info("All users responded ✓")
        return
    for incoming in rows:
        llmresponse = json.loads(incoming["llm_solution"])
        
        email_payload = {
                "serviceName": incoming["application_name"],
                "environment": "Non Prod (This value need to change )",
                "timestamp": incoming["error_timestamp"],
                "errorType": incoming["error_code"],
                "errorMessage": incoming["error_description"],
                "errorId": str(incoming["id"]),
                "sessionId": incoming["sessionid"],
                "rootCause": llmresponse.get("rootCause"),
                "solution1": {"instructions": llmresponse.get("solution1", {}).get("instructions")},
                "solution2": {"instructions": llmresponse.get("solution2", {}).get("instructions")},
                "solution3": {"instructions": llmresponse.get("solution3", {}).get("instructions")},
        }
        html_content = email_service.populate_template_llm(email_payload)
        subject = f"Error Notification: {email_payload['errorType']} in {email_payload['serviceName']}"
        to_address = str(os.getenv('TO_EMAIL'))
        email_service.send_email(html_content, subject, to_address)
        
        
        update_retry_sql = """
            UPDATE errorsolutiontable
            SET retry_count = retry_count + 1
            WHERE id = %s;
        """
        db.execute(update_retry_sql, (incoming["id"],))
        
scheduler = BlockingScheduler()
scheduler.add_job(run_scheduler, trigger="interval", seconds=1000)

if __name__ == "__main__":
    logging.info("📌 Scheduler started...")
    scheduler.start()