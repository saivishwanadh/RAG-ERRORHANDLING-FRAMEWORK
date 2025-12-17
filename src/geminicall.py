import json
import re
import requests
import logging
from typing import Dict, Any, Optional
from src.prompt import PromptBuilder
from src.config import Config

logger = logging.getLogger(__name__)

class GeminiClient:
    """Minimal Gemini client for MuleSoft error analysis - JSON output only"""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or Config.GEMINI_APIKEY
        self.model = model or Config.GEMINI_MODEL
        self.base_url = Config.GEMINI_URL
        
        if not self.api_key or not self.model or not self.base_url:
            logger.warning("Gemini configuration missing (API Key, Model, or URL)")
        
        self.url = f"{self.base_url}/{self.model}:generateContent?key={self.api_key}"
        self.prompt_builder = PromptBuilder()
        logger.info(f"Initialized GeminiClient with model: {self.model}")

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
        if not self.api_key:
             raise ValueError("Gemini API Key is not configured")

        # Dynamically build the full SYSTEM PROMPT
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

        try:
            response = requests.post(self.url, json=payload, timeout=60)
            response.raise_for_status()

            data = response.json()
            # Safety check for response structure
            try:
                text = data['candidates'][0]['content']['parts'][0]['text']
            except (KeyError, IndexError) as e:
                logger.error(f"Unexpected response structure from Gemini: {data}")
                raise ValueError("Unexpected response structure from Gemini") from e
                
            logger.info(f"Received response text from Gemini")

            return self._extract_json(text)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Gemini API request failed: {e}")
            raise
