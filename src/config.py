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
    # ELK (legacy — kept for backward compatibility)
    ELK_SEARCH_URL = os.getenv("ELK_SEARCH_URL")
    ELK_APIKEY = os.getenv("ELK_APIKEY")
    ELK_TIMEOUT = int(os.getenv("ELK_TIMEOUT_SECONDS", "30"))

    # OpenSearch / Elasticsearch
    OPENSEARCH_URL      = os.getenv("OPENSEARCH_URL", "http://localhost:9200")
    OPENSEARCH_INDEX    = os.getenv("OPENSEARCH_INDEX", "ceva-logs")
    OPENSEARCH_APIKEY   = os.getenv("OPENSEARCH_APIKEY", "")          # API Key auth (optional)
    OPENSEARCH_USERNAME = os.getenv("OPENSEARCH_USERNAME", "")        # Basic auth (optional)
    OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")        # Basic auth (optional)
    OPENSEARCH_VERIFY_SSL           = os.getenv("OPENSEARCH_VERIFY_SSL", "false").lower() == "true"
    OPENSEARCH_TIMEOUT              = int(os.getenv("OPENSEARCH_TIMEOUT", "30"))
    OPENSEARCH_BATCH_SIZE           = int(os.getenv("OPENSEARCH_BATCH_SIZE", "100"))
    OPENSEARCH_LOG_LEVEL_FILTER     = os.getenv("OPENSEARCH_LOG_LEVEL_FILTER", "ERROR")
    OPENSEARCH_ERROR_CODE_REGEX     = os.getenv("OPENSEARCH_ERROR_CODE_REGEX", "")
    OPENSEARCH_ENRICH_DESCRIPTION   = os.getenv("OPENSEARCH_ENRICH_DESCRIPTION", "false").lower() == "true"
    
    # Qdrant / Vector DB
    QDRANT_URL = os.getenv("QDRANT_URL")
    QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
    QDRANT_DEFAULT_COLLECTION = os.getenv("QDRANT_DEFAULT_COLLECTION", "error_solutions")
    # LLM / Embedding
    GEMINI_APIKEY = os.getenv("GEMINI_APIKEY")
    GEMINI_URL = os.getenv("GEMINI_URL")
    GEMINI_MODEL = os.getenv("GEMINI_MODEL")

    # Platform / Application Context for LLM Prompts
    APP_PLATFORM_NAME = os.getenv("APP_PLATFORM_NAME", "Enterprise Application")
    APP_PLATFORM_DOCS_URL = os.getenv("APP_PLATFORM_DOCS_URL", "https://docs.example.com")
    APP_PLATFORM_TERMS = os.getenv("APP_PLATFORM_TERMS", "Components, Services, APIs")
    APP_PLATFORM_TONE = os.getenv("APP_PLATFORM_TONE", "Technical expert, Actionable, Context-Aware, support engineer")
    
    # Email / SMTP
    TO_EMAIL = os.getenv("TO_EMAIL")
    SMTP_HOST = os.getenv("SMTP_HOST")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    SMTP_TIMEOUT = int(os.getenv("SMTP_TIMEOUT", "30"))

    # Email / Graph API Configuration
    AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
    AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "")
    AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", "your-tenant-id")
    AZURE_TARGET_EMAIL = os.getenv("AZURE_TARGET_EMAIL", "")
    
    EMAIL_FOLDER = os.getenv("EMAIL_FOLDER", "Inbox")
    EMAIL_POLL_INTERVAL = int(os.getenv("EMAIL_POLL_INTERVAL", "60"))
    EMAIL_SUBJECT_FILTER = os.getenv("EMAIL_SUBJECT_FILTER", "Exception")

    # Configurable Limits
    GEMINI_EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "models/embedding-001")
    PRESIDIO_SCORE_THRESHOLD = float(os.getenv("PRESIDIO_SCORE_THRESHOLD", "0.8"))
    HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
    HTTP_POOL_SIZE = int(os.getenv("HTTP_POOL_SIZE", "10"))
    RABBIT_RETRIES = int(os.getenv("RABBIT_RETRIES", "3"))
    RABBIT_RETRY_DELAY = int(os.getenv("RABBIT_RETRY_DELAY", "2"))
    RABBIT_CONNECTION_TIMEOUT = int(os.getenv("RABBIT_CONNECTION_TIMEOUT", "10"))
    MAX_RETRIES_PER_MESSAGE = int(os.getenv("MAX_RETRIES_PER_MESSAGE", "2"))
    RATE_LIMIT_DELAY = int(os.getenv("RATE_LIMIT_DELAY", "60"))
    
    # Email sender filter - only process emails from this address
    EMAIL_SENDER_FILTER = os.getenv("EMAIL_SENDER_FILTER", "veerlapatisaivishwanadh@prowesssoft.com")

    # High-Priority Escalation
    HIGH_PRIORITY_THRESHOLD = int(os.getenv("HIGH_PRIORITY_THRESHOLD", "5"))  # occurrences before escalation
    ESCALATION_COOLDOWN_MINUTES = int(os.getenv("ESCALATION_COOLDOWN_MINUTES", "60"))  # min gap between repeat alerts
    HIGH_PRIORITY_TO_EMAIL = os.getenv("HIGH_PRIORITY_TO_EMAIL", "")

    # Service Health Alerts (VectorDB / DB / Gemini / OpenSearch down)
    ALERT_TO_EMAIL = os.getenv("ALERT_TO_EMAIL", "")                          # Recipient for service-down alerts
    SERVICE_ALERT_COOLDOWN_MINUTES = int(os.getenv("SERVICE_ALERT_COOLDOWN_MINUTES", "30"))  # Min gap between repeat alerts per service

    # ITSM / Incident Management integration (loosely coupled)
    # Set ITSM_PROVIDER to 'none' (or leave blank) to disable completely.
    # Set to 'servicenow' to enable ServiceNow integration.
    # Future: 'jira', 'pagerduty', etc.
    ITSM_PROVIDER              = os.getenv("ITSM_PROVIDER", "none")
    SERVICENOW_INSTANCE_URL    = os.getenv("SERVICENOW_INSTANCE_URL", "")
    SERVICENOW_USERNAME        = os.getenv("SERVICENOW_USERNAME", "")
    SERVICENOW_PASSWORD        = os.getenv("SERVICENOW_PASSWORD", "")

    
    @classmethod
    def validate(cls):
        """Validate critical configuration is present"""
        required = [
            'DB_URL', 'RABBIT_URL', 'EXCHANGE', 'QUEUE', 'ROUTING_KEY'
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