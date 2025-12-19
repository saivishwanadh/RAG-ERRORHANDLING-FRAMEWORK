import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables once
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

class Config:
    # App Settings
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
    ENVIRONMENT = os.getenv("ENVIRONMENT", "Non Prod")
    
    # Scheduler Settings
    REMINDER_INTERVAL_HOURS = int(os.getenv("REMINDER_INTERVAL_HOURS", "24"))
    MAX_RETRY_COUNT = int(os.getenv("MAX_RETRY_COUNT", "3"))
    PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "1"))
    
    # Database
    DB_URL = os.getenv("DB_URL")
    DB_DUPLICATE_WINDOW_MINUTES = int(os.getenv("DB_DUPLICATE_WINDOW_MINUTES", "10"))
    
    # RabbitMQ
    RABBIT_URL = os.getenv("RABBIT_URL")
    EXCHANGE = os.getenv("EXCHANGE")
    QUEUE = os.getenv("QUEUE")
    ROUTING_KEY = os.getenv("ROUTING_KEY")
    EXCHANGE_TYPE = os.getenv("EXCHANGE_TYPE", "topic")
    DLQ_ENABLED=os.getenv("DLQ_ENABLED")
    DLX_EXCHANGE=os.getenv("DLX_EXCHANGE")
    DLQ_ROUTING_KEY=os.getenv("DLQ_ROUTING_KEY")
    # ELK
    ELK_SEARCH_URL = os.getenv("ELK_SEARCH_URL")
    ELK_APIKEY = os.getenv("ELK_APIKEY")
    ELK_TIMEOUT = int(os.getenv("ELK_TIMEOUT_SECONDS", "30"))
    
    # Qdrant / Vector DB
    QDRANT_URL = os.getenv("QDRANT_URL")
    QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
    QDRANT_DEFAULT_COLLECTION = os.getenv("QDRANT_DEFAULT_COLLECTION", "error_solutions")
    # LLM / Embedding
    HUGGINGFACE_APIKEY = os.getenv("HUGGINGFACE_APIKEY")
    GEMINI_APIKEY = os.getenv("GEMINI_APIKEY")
    GEMINI_URL = os.getenv("GEMINI_URL")
    GEMINI_MODEL = os.getenv("GEMINI_MODEL")
    
    # Email / SMTP
    TO_EMAIL = os.getenv("TO_EMAIL")
    SMTP_HOST = os.getenv("SMTP_HOST")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    
    @classmethod
    def validate(cls):
        """Validate critical configuration is present"""
        required = [
            'DB_URL', 'RABBIT_URL', 'EXCHANGE', 'QUEUE', 'ROUTING_KEY',
            'ELK_SEARCH_URL', 'ELK_APIKEY'
        ]
        missing = [key for key in required if not getattr(cls, key)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

# Setup central logging configuration
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)
