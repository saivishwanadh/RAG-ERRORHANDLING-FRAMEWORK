import json
import logging
import re
from typing import Dict, Any, Optional
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import SystemMessagePromptTemplate, HumanMessagePromptTemplate, ChatPromptTemplate
from src.prompt import PromptBuilder
from src.config import Config

logger = logging.getLogger(__name__)

class GeminiClient:
    """Gemini client using LangChain"""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or Config.GEMINI_APIKEY
        self.model = model or Config.GEMINI_MODEL
        
        if not self.api_key:
            logger.warning("Gemini API Key missing")
        
        self.llm = ChatGoogleGenerativeAI(
            model=self.model,
            google_api_key=self.api_key,
            temperature=0.0,
            top_p=0.95,
            top_k=40,
            max_output_tokens=8192,
            convert_system_message_to_human=True, # Sometimes needed for certain Gemini versions/LangChain adaptors
            model_kwargs={"response_mime_type": "application/json"}
        )
        self.prompt_builder = PromptBuilder()
        logger.info(f"Initialized GeminiClient (LangChain) with model: {self.model}")

    def _extract_json(self, text: str) -> Dict[str, Any]:
        """Extract JSON from response, handling potential trailing commas"""
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            candidate = match.group(0).strip()
            # Remove trailing commas like ", }" or ", ]"
            candidate = re.sub(r",\s*([\]}])", r"\1", candidate)
            try:
                return json.loads(candidate)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
        
        # If we reach here, extraction failed
        logger.error(f"Failed to extract JSON from response. Raw content:\n{text}")
        raise ValueError("Could not extract JSON from response")

    def analyze_error(self, error_code: str, error_description: str, context: str = "") -> Dict[str, Any]:
        """
        Analyze error using LangChain invocation
        """
        # Get ChatPromptTemplate from builder (pre-filled with platform context)
        prompt_template = self.prompt_builder.get_prompt_template(platform="mulesoft")

        # Create chain using LCEL (LangChain Expression Language) style or simple piping
        # chain = prompt | llm
        chain = prompt_template | self.llm
        
        try:
            logger.info("Invoking Gemini via LangChain chain...")
            
            # Context handling: If None or empty, provide a fallback "None" string so template parses
            context_val = context if context else "None"
            
            response_msg = chain.invoke({
                "ERROR_CODE": error_code, 
                "ERROR_DESCRIPTION": error_description,
                "CONTEXT": context_val
            })
            
            content = response_msg.content
            logger.info("Received response from Gemini (LangChain)")
            return self._extract_json(content)
            
        except Exception as e:
            logger.error(f"Gemini/LangChain request failed: {e}")
            raise
