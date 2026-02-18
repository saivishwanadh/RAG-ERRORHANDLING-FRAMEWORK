from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from src.config import Config

class LogSanitizer:
    def __init__(self):
        # Initialize Presidio Engines
        self.analyzer = AnalyzerEngine()
        self.anonymizer = AnonymizerEngine()
        
        #  Add Custom Recognizers
        custom_patterns = [
            # Cloud & API Keys
            ("AWS_ACCESS_KEY", r"AKIA[0-9A-Z]{16}", 0.95),
            ("AWS_SECRET_KEY", r"(?i)aws_secret.*[=:]\s*['\"]?([A-Za-z0-9/+=]{40})['\"]?", 0.9),
            ("AZURE_KEY", r"(?i)(azure|az).*key.*[=:]\s*['\"]?([A-Za-z0-9+/=]{44})['\"]?", 0.9),
            ("GCP_KEY", r"(?i)(gcp|google).*key.*[=:]\s*['\"]?([A-Za-z0-9_\-]{39})['\"]?", 0.9),
            
            # Tokens
            ("JWT_TOKEN", r"eyJ[a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+", 0.95),
            ("BEARER_TOKEN", r"Bearer\s+[A-Za-z0-9.\-_]{20,}", 0.9),
            ("API_KEY", r"(?i)api[_\-]?key['\"]?\s*[:=]\s*['\"]?([A-Za-z0-9_\-]{16,})['\"]?", 0.85),
            
            # Credentials
            ("PASSWORD", r"(?i)(password|passwd|pwd|secret)['\"]?\s*[:=]\s*['\"]([^'\"]{6,})['\"]", 0.95),
            ("SECRET_KEY", r"(?i)(secret[_\-]?key|client[_\-]?secret)['\"]?\s*[:=]\s*['\"]?([A-Za-z0-9_\-+/=]{16,})['\"]?", 0.9),
            
            # Salesforce
            ("SALESFORCE_SESSION", r"00D[A-Za-z0-9]{15,18}", 0.9),
            ("SALESFORCE_TOKEN", r"(?i)sfdc.*token.*[=:]\s*['\"]?([A-Za-z0-9!_\-.]{15,})['\"]?", 0.85),
            
            # Database
            ("DB_CONNECTION", r"(?i)(mongodb|mysql|postgresql|sqlserver|oracle):\/\/[^\s;]+", 0.9),
            ("DB_PASSWORD", r"(?i)(database|db)[_\-]?(password|pwd)[=:]['\"]?([^'\";\s]+)['\"]?", 0.9),
            
            # Private Keys
            ("PRIVATE_KEY", r"-----BEGIN\s+(?:RSA\s+)?PRIVATE\s+KEY-----[\s\S]+?-----END\s+(?:RSA\s+)?PRIVATE\s+KEY-----", 0.99),
            ("SSH_KEY", r"ssh-rsa\s+[A-Za-z0-9+/=]{200,}", 0.95),
            
            # Certificates
            ("CERTIFICATE", r"-----BEGIN\s+CERTIFICATE-----[\s\S]+?-----END\s+CERTIFICATE-----", 0.95),
            
            # IP Addresses (private + public ranges - Presidio default misses private IPs)
            ("IP_ADDRESS", r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(?::\d{1,5})?\b", 0.95),
        ]

        for entity, regex, score in custom_patterns:
            self.analyzer.registry.add_recognizer(
                PatternRecognizer(
                    supported_entity=entity,
                    patterns=[Pattern(name=entity.lower(), regex=regex, score=score)]
                )
            )

      
        # Define all entities (built-in + custom)
        # Note: IN_* entities removed - Presidio has no English recognizers for them
        self.ALL_ENTITIES = [
            # Built-in / Global
            "PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "IP_ADDRESS", "LOCATION", "URL",
            "CREDIT_CARD", "US_SSN", "US_BANK_NUMBER",
            "US_DRIVER_LICENSE", "US_PASSPORT", "US_ITIN",

            # Custom
            "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AZURE_KEY", "GCP_KEY",
            "JWT_TOKEN", "BEARER_TOKEN", "API_KEY",
            "PASSWORD", "SECRET_KEY",
            "SALESFORCE_SESSION", "SALESFORCE_TOKEN",
            "DB_CONNECTION", "DB_PASSWORD",
            "PRIVATE_KEY", "SSH_KEY", "CERTIFICATE"
        ]
        

        # Define Operators
        self.operators = {
            entity: OperatorConfig("replace", {"new_value": f"<{entity}>"} )
            for entity in self.ALL_ENTITIES
        }


    #Method to sanitize text
    def sanitize(self, text: str) -> str:
        results = self.analyzer.analyze(
            text=text,
            language="en",
            entities=self.ALL_ENTITIES,
            score_threshold=Config.PRESIDIO_SCORE_THRESHOLD
        )
        anonymized_output = self.anonymizer.anonymize(
            text=text,
            analyzer_results=results,
            operators=self.operators
        )
        return anonymized_output.text
