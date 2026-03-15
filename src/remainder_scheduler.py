"""
Production-Ready Reminder Scheduler
- Persistent DB connection with health checks
- Comprehensive error handling
- Graceful shutdown
- Proper logging
- Resource cleanup
"""

import json
import logging
import signal
import sys
from datetime import datetime
from typing import Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from src.structuraldb import DB
from src.sendemail import EmailService
from src.config import Config

# Logging setup
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================================
# Service Container for Resource Management
# ============================================================================

class SchedulerService:
    """Manages database and email service connections."""
    
    def __init__(self):
        self.email_service: Optional[EmailService] = None
        self._is_shutting_down = False
        # DB connection is managed via DB class
    
    def initialize(self):
        """Initialize all services with error handling."""
        try:
            logger.info("=" * 70)
            logger.info("Initializing Reminder Scheduler services...")
            logger.info("=" * 70)
            
            Config.validate()
            
            # Initialize Database
            try:
                with DB() as db:
                    db.execute("SELECT 1", fetch=True)
                logger.info("✅ Database connection initialized")
            except Exception as e:
                logger.error(f"❌ Failed to initialize Database: {e}", exc_info=True)
                raise
            
            # Initialize Email Service
            try:
                self.email_service = EmailService("email-main-ui.html")
                logger.info("✅ Email service initialized")
            except Exception as e:
                logger.error(f"❌ Failed to initialize Email service: {e}", exc_info=True)
                raise
            
            logger.info("✅ All services initialized successfully")
            logger.info(f"Configuration:")
            logger.info(f"   - Reminder interval: {Config.REMINDER_INTERVAL_HOURS} hours")
            logger.info(f"   - Max retries: {Config.MAX_RETRY_COUNT}")
            logger.info(f"   - Environment: {Config.ENVIRONMENT}")
            
        except Exception as e:
            logger.error(f"❌ Service initialization failed: {e}", exc_info=True)
            raise
    
    def cleanup(self):
        """Cleanup resources before shutdown."""
        if self._is_shutting_down:
            return
        
        self._is_shutting_down = True
        logger.info("=" * 70)
        logger.info("🧹 Cleaning up resources...")
        logger.info("=" * 70)
        
        # Database connection closes automatically via context manager when used
        
        logger.info("=" * 70)
        logger.info("✅ Cleanup complete")
        logger.info("=" * 70)


# Global service instance
service = SchedulerService()


# ============================================================================
# Core Reminder Logic
# ============================================================================

def send_reminder(record: dict) -> bool:
    """
    Send reminder email for a single record.
    
    Returns:
        True if email sent successfully, False otherwise
    """
    try:
        # Parse LLM solution
        try:
            llmresponse = json.loads(record["llm_solution"])
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in llm_solution for ID {record['id']}: {e}")
            return False
        
        # Build email payload
        email_payload = {
            "serviceName": record["application_name"],
            "environment": Config.ENVIRONMENT,
            "timestamp": str(record["error_timestamp"]),
            "errorType": record["error_code"],
            "errorMessage": record["error_description"],
            "errorId": str(record["id"]),
            "sessionId": record["sessionid"],
            "rootCause": llmresponse.get("rootCause", "N/A"),
            "solution1": {"instructions": llmresponse.get("solution1", {}).get("instructions", "")},
            "solution2": {"instructions": llmresponse.get("solution2", {}).get("instructions", "")},
            "solution3": {"instructions": llmresponse.get("solution3", {}).get("instructions", "")},
        }
        
        # Generate email content
        html_content = service.email_service.populate_template_llm(email_payload)
        
        # Create subject with reminder count
        retry_number = record["retry_count"] + 1
        subject = (
            f"Error Notification (Remainder {retry_number}/{Config.MAX_RETRY_COUNT}): "
            f"{email_payload['errorType']} in {email_payload['serviceName']}"
        )
        
        # Send email
        to_address = Config.TO_EMAIL
        if not to_address:
            logger.error("❌ TO_EMAIL environment variable not set")
            return False
        
        service.email_service.send_email(html_content, subject, to_address)
        
        logger.info(
            f"✅ Reminder sent for ID {record['id']} "
            f"({retry_number}/{Config.MAX_RETRY_COUNT}) to {to_address}"
        )
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send reminder for ID {record.get('id', 'unknown')}: {e}", exc_info=True)
        return False


def update_retry_count(record_id: int) -> bool:
    """
    Update retry count for a record.
    
    Returns:
        True if updated successfully, False otherwise
    """
    try:
        update_sql = """
            UPDATE errorsolutiontable
            SET retry_count = retry_count + 1
            WHERE id = %s;
        """
        with DB() as db:
            db.execute(update_sql, (record_id,))
        logger.info(f"✅ Updated retry count for ID {record_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to update retry count for ID {record_id}: {e}", exc_info=True)
        return False


def run_scheduler():
    """
    Main scheduler function - sends reminders for pending errors.
    Runs with comprehensive error handling to prevent scheduler crashes.
    """
    try:
        logger.info("=" * 70)
        logger.info(f"Starting reminder check at {datetime.now()}")
        logger.info("=" * 70)
        
        # Query for records needing reminders
        query_sql = """
            SELECT *
            FROM errorsolutiontable
            WHERE (ops_solution IS NULL OR ops_solution = '')
            AND retry_count < %s
            AND sessionid_status = 'active'
            ORDER BY error_timestamp ASC;
        """
        
        with DB() as db:
            rows = db.execute(query_sql, (Config.MAX_RETRY_COUNT,), fetch=True)
        
        if not rows or len(rows) == 0:
            logger.info("✅ No pending reminders - all errors have responses")
            return
        
        logger.info(f"📧 Found {len(rows)} errors requiring reminders")
        
        # Process each record
        success_count = 0
        failure_count = 0
        
        for record in rows:
            record_id = record.get("id", "unknown")
            retry_count = record.get("retry_count", 0)
            
            logger.info(
                f"📝 Processing ID {record_id}: "
                f"App={record.get('application_name')}, "
                f"Code={record.get('error_code')}, "
                f"Retry={retry_count}/{Config.MAX_RETRY_COUNT}"
            )
            
            # Send reminder
            email_sent = send_reminder(record)
            
            if email_sent:
                # Update retry count only if email sent successfully
                count_updated = update_retry_count(record_id)
                
                if count_updated:
                    success_count += 1
                else:
                    failure_count += 1
                    logger.warning(f"⚠️  Email sent but failed to update retry count for ID {record_id}")
            else:
                failure_count += 1
        
        # Summary
        logger.info("=" * 70)
        logger.info(f"Reminder run complete:")
        logger.info(f"   ✅ Successful: {success_count}")
        logger.info(f"   ❌ Failed: {failure_count}")
        logger.info(f"   📧 Total processed: {len(rows)}")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"❌ Scheduler run failed: {e}", exc_info=True)
        # Don't raise - let scheduler continue running


# ============================================================================
# Graceful Shutdown Handler
# ============================================================================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"⚠️  Signal {signum} received - shutting down gracefully")
    service.cleanup()
    sys.exit(0)


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("Starting Reminder Scheduler Service")
    logger.info("=" * 70)
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Initialize services
        service.initialize()
        
        # Create scheduler
        scheduler = BlockingScheduler()
        
        # Add job with interval
        scheduler.add_job(
            run_scheduler,
            trigger="interval",
            hours=Config.REMINDER_INTERVAL_HOURS,
            max_instances=1,  # Prevent overlapping runs
            next_run_time=datetime.now()  # Run immediately on startup
        )
        
        logger.info("=" * 70)
        logger.info(f"⏰ Scheduler configured:")
        logger.info(f"   - Interval: Every {Config.REMINDER_INTERVAL_HOURS} hour(s)")
        logger.info(f"   - Next run: Immediately")
        logger.info(f"   - Max concurrent runs: 1")
        logger.info("=" * 70)
        
        # Start scheduler (blocks here)
        logger.info("✅ Scheduler started - press Ctrl+C to stop")
        scheduler.start()
        
    except KeyboardInterrupt:
        logger.info("⚠️  Keyboard interrupt received")
        signal_handler(signal.SIGINT, None)
    
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        service.cleanup()
        sys.exit(1)
