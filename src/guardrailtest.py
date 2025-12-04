import re
from guardrails import Guard
from guardrails.hub import DetectPII

class CompliancePIIMasker:

    def __init__(self):
        # Guardrails for advanced entity detection
        self.guard = Guard().use(
            DetectPII(
                pii_entities=[
                    "EMAIL_ADDRESS", "PERSON", "LOCATION", "PHONE_NUMBER",
                    "IP_ADDRESS", "CREDIT_CARD", "IBAN_CODE", 
                    "SSN", "US_PASSPORT", "US_DRIVER_LICENSE"
                ],
                on_fail="fix"
            )
        )

        # Redact categories (NEVER allowed to be visible)
        self.full_redact_patterns = {
            "PASSWORD": r"(password|pwd|pass)=\S+",
            "API_KEY": r"(api[-_ ]?key|apikey|key)=\S+",
            "JWT_TOKEN": r"eyJ[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{10,}\.\S+",
            "TOKEN": r"(token|bearer)\s+[a-zA-Z0-9-_]+",
            "AWS_SECRET": r"(AKIA[0-9A-Z]{16})"
        }

        # PCI and GDPR masking
        self.mask_patterns = {
            "EMAIL": r"\b[A-Za-z0-9._%+-]+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})\b",
            "PHONE": r"\b\+?\d[\d -]{8,}\d\b",
            "IP": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
            "CREDIT_CARD": r"\b(?:\d[ -]?){13,19}\b",
            "NAME": r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b"  # GDPR strict
        }

    def redact_sensitive(self, text):
        """Fully redact secrets/token/passwords."""
        for _, pattern in self.full_redact_patterns.items():
            text = re.sub(pattern, "***REDACTED***", text, flags=re.IGNORECASE)
        return text

    def mask_gdpr(self, match):
        """Mask name but keep initials."""
        name = match.group(0)
        parts = name.split()
        return f"{parts[0][0]}**** {parts[1][0]}****"

    def mask_card(self, match):
        """Show last 4 digits → PCI rule."""
        number = re.sub(r"\D", "", match.group(0))
        if len(number) >= 12:
            return "**** **** **** " + number[-4:]
        return "***REDACTED***"

    def apply_regex_masking(self, text):
        """Mask based on GDPR + PCI rules."""

        # EMAIL
        text = re.sub(self.mask_patterns["EMAIL"], lambda m: f"****@{m.group(1)}", text)

        # PHONE
        text = re.sub(self.mask_patterns["PHONE"], lambda m: "*" * len(m.group(0)), text)

        # IP addresses
        text = re.sub(self.mask_patterns["IP"], "***.***.***.***", text)

        # Names (GDPR strict)
        text = re.sub(self.mask_patterns["NAME"], self.mask_gdpr, text)

        # Credit Card (PCI)
        text = re.sub(self.mask_patterns["CREDIT_CARD"], self.mask_card, text)

        return text

    def mask(self, text):
        """Main masking pipeline"""
        if not text:
            return text

        # 1️⃣ First, remove HIGH-RISK data
        text = self.redact_sensitive(text)

        # 2️⃣ Run DetectPII
        try:
            result = self.guard.validate(text)
            text = result.validated_output
        except:
            pass

        # 3️⃣ Regex improvements
        return self.apply_regex_masking(text)


# ===================== APPLY ONLY TO ERROR DESCRIPTION =====================

def sanitize_error_log(log_record: dict):
    """
    Only masks `error_description` field.
    """
    masker = CompliancePIIMasker()

    if "error_description" in log_record and isinstance(log_record["error_description"], str):
        log_record["error_description"] = masker.mask(log_record["error_description"])

    return log_record
log = {
    "error_id": "123",
    "error_description": "api key awieio32323iifjo3232ddd is not valid"
}

print(sanitize_error_log(log))
