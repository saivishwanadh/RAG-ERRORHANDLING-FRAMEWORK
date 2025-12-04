class PromptBuilder:
    def __init__(self):
        # Platform configurations
        self.PLATFORM_CONFIGS = {
            "mulesoft": {
                "PLATFORM_NAME": "MuleSoft",
                "PLATFORM_DOCS_URL": "https://docs.mulesoft.com/",
                "PLATFORM_TERMS": "Anypoint Platform, DataWeave, Connectors, Runtimes",
                "TONE": (
                    "Technical expert, Actionable, Production-Safe, Concise, "
                    "Context-Aware, Human Understandable, professional, "
                    "mulesoft mentor/trainer/architect, trouble shooter, "
                    "mulesoft support engineer, mulesoft strategic engineer"
                )
            }
        }

        # Base prompt template
        self.PROMPT_TEMPLATE = (
            "You are a certified {PLATFORM_NAME} integration expert with deep knowledge of its runtime, "
            "connectors, adapters, transformation language, platform services, and error-handling mechanisms.\n\n"
            "Analyze the provided Error Code: {ERROR_CODE}, Error Description: {ERROR_DESCRIPTION} to identify the "
            "exact root cause with platform-specific accuracy.\n\n"
            "Follow all CRITICAL RULES and return the response strictly in the JSON schema containing three sections "
            "— Solution 1 (Quick Fix), Solution 2 (Root Cause Fix), and Solution 3 (Preventive Actions).\n\n"
            "OUTPUT FORMAT (STRICT JSON ONLY, NO MARKDOWN):\n"
            "{{\n"
            '  "rootCause": "",\n'
            '  "solution1": {{ "instructions": "" }},\n'
            '  "solution2": {{ "instructions": "" }},\n'
            '  "solution3": {{ "instructions": "" }}\n'
            "}}\n\n"
            "CRITICAL RULES:\n"
            "1. Ensure every recommendation is verified from {PLATFORM_DOCS_URL} or standard patterns\n"
            "2. No generic advice — responses must be solution-specific\n"
            '3. Use imperative language ("Do this", "Add this config")\n'
            "4. Never omit critical safety/config details (credentials, retries, etc.)\n"
            "5. Prioritize root cause clarity over quick fixes\n"
            "6. No hallucination: all API or feature mentions must exist in {PLATFORM_NAME}\n"
            "7. Stay compliant with {PLATFORM_TERMS} platform governance and org policies\n"
            "8. Always verify version compatibility (e.g., runtime versions, component variations)\n"
            "9. Never suggest deleting configs or endpoints blindly\n"
            "10. Never propose anti-patterns (e.g., business logic inside flows/pipelines)\n"
            "11. Avoid oversimplification that hides technical details\n"
            "12. Stay technically correct\n"
            "13. Max 3–5 steps per response\n"
            "14. Never modify governance policies\n"
            "15. Never expose credentials or secrets in any response\n"
            "16. Return ONLY valid JSON — no markdown backticks, no code blocks, no extra text\n"
            "17. All text fields must be plain text — no special characters that break JSON parsing\n"
            "18. Each solution must be completely independent and implementable without the others\n"
            "19. Include realistic timelines, proper namespaces, and full context in all solutions\n"
            "20. Every solution must have validation steps and rollback procedures\n\n"
            "{TONE}"
        )

    def get_prompt(self, platform, error_code, error_description):
        """
        Generate platform-specific prompt dynamically.

        Args:
            platform (str): Platform name (mulesoft, etc.)
            error_code (str): Error code
            error_description (str): Error description text

        Returns:
            str: Fully formatted prompt
        """

        platform_key = platform.lower()

        if platform_key not in self.PLATFORM_CONFIGS:
            raise ValueError(
                f"Platform '{platform}' not supported. Available: {list(self.PLATFORM_CONFIGS.keys())}"
            )

        config = self.PLATFORM_CONFIGS[platform_key]

        return self.PROMPT_TEMPLATE.format(
            PLATFORM_NAME=config["PLATFORM_NAME"],
            PLATFORM_DOCS_URL=config["PLATFORM_DOCS_URL"],
            PLATFORM_TERMS=config["PLATFORM_TERMS"],
            TONE=config["TONE"],
            ERROR_CODE=error_code,
            ERROR_DESCRIPTION=error_description,
        )

 
# Example usage
'''if __name__ == "__main__":
    builder = PromptBuilder()

    prompt = builder.get_prompt(
        "mulesoft",
        "MULE:EXPRESSION",
        "You called the function '+' with null value"
    )

    print(prompt)'''
