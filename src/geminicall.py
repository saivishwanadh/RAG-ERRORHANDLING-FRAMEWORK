import json
import re
import requests
import logging
from typing import Dict, Any
from prompt import PromptBuilder   # ⬅️ import your class
from dotenv import load_dotenv, dotenv_values 
from pathlib import Path
import os

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)


class GeminiClient:
    """Minimal Gemini client for MuleSoft error analysis - JSON output only"""

    BASE_URL = os.getenv("GEMINI_URL")
    DEFAULT_MODEL = os.getenv("GEMINI_MODEL")

    def __init__(self, api_key: str, model: str = DEFAULT_MODEL):
        self.api_key = api_key
        self.model = model
        self.url = f"{self.BASE_URL}/{model}:generateContent?key={api_key}"
        self.prompt_builder = PromptBuilder()     # ⬅️ instantiate your class
        logging.info(f"Initialized GeminiClient with model: {model}")

    def _extract_json(self, text: str) -> Dict[str, Any]:
        """Extract JSON from response"""

        # JSON inside {}
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0).strip())
            except json.JSONDecodeError:
                pass

        raise ValueError("Could not extract JSON from response")

    def analyze_error(self, error_code: str, error_description: str) -> Dict[str, Any]:
        """
        Build system prompt dynamically and analyze error
        """

        # ✔ Dynamically build the full SYSTEM PROMPT
        dynamic_system_prompt = self.prompt_builder.get_prompt(
            platform="mulesoft",
            error_code=error_code,
            error_description=error_description
        )

        payload = {
            "systemInstruction": {"parts": [{"text": dynamic_system_prompt}]},
            "contents": [{"role": "user", "parts": [{"text": error_description}]}],
            "generationConfig": {
                "temperature": 0.0,
                "topP": 0.95,
                "topK": 40,
                "maxOutputTokens": 8192
            }
        }

        response = requests.post(self.url, json=payload, timeout=60)
        response.raise_for_status()

        data = response.json()
        text = data['candidates'][0]['content']['parts'][0]['text']
        logging.info(f"Received response text from Gemini")

        return self._extract_json(text)

'''def main():
    client = GeminiClient(api_key="AIzaSyDouQUBHOV4XIumezWSs0c0v_GESFSQxxI")

    error_code = "DB:CONNECTION"
    error_desc = "Cannot get connection for PostgreSQL database"

    llmresponse = client.analyze_error(error_code, error_desc)

    payload={
                "rootCause": llmresponse.get("rootCause"),
                "solution1": {"instructions": llmresponse.get("solution1", {}).get("instructions")},
                "solution2": {"instructions": llmresponse.get("solution2", {}).get("instructions")},
                "solution3": {"instructions": llmresponse.get("solution3", {}).get("instructions")},
        }
    print(payload)
    #print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()'''