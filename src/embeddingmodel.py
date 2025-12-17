import requests
import logging
from typing import List, Optional
from google import genai
from google.genai import types
from src.config import Config

logger = logging.getLogger(__name__)

class EmbeddingGenerator:

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or Config.HUGGINGFACE_APIKEY # Fallback or main key
        # If using Google GenAI, we might want a different key variable or reuse the same one depending on intent.
        # Original code used `HUGGINGFACE_APIKEY` env var passed as `api_key` to this class, 
        # but then used it for Google GenAI. This is confusing naming in the original code.
        # I will assume the user intends to use the key passed or configured for their chosen provider.
        
        self.client = None
        if self.api_key:
            try:
                self.client = genai.Client(api_key=self.api_key)
            except Exception as e:
                logger.warning(f"Failed to initialize GenAI client: {e}")

    def get_embedding(self, text: str, model: str = "models/text-embedding-004", dimensions: int = 768) -> List[float]:
        """Generate embedding using Gemini Python SDK"""
        if not self.client:
             # Try to init again or fail
             if self.api_key:
                 self.client = genai.Client(api_key=self.api_key)
             else:
                 raise ValueError("API Key not provided for EmbeddingGenerator")

        logger.info(f"[SDK] Generating embedding using Gemini SDK model: {model}")

        try:
            resp = self.client.models.embed_content(
                model=model,
                contents=text,
                config=types.EmbedContentConfig(output_dimensionality=dimensions)
            )
            return resp.embeddings[0].values
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise

    def get_embedding_rest(self, text: str, model: str = "gemini-embedding-001", dimensions: int = 768) -> List[float]:
        """
        Generates embedding vector using Gemini REST API.
        """
        logger.info(f"[REST] Generating embedding using Gemini model: {model}")

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:embedContent"

        headers = {
            "x-goog-api-key": self.api_key,
            "Content-Type": "application/json"
        }

        payload = {
            "content": {
                "parts": [{"text": text}]
            },
            "output_dimensionality": dimensions
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            
            resp_json = response.json()
            embedding = resp_json.get("embedding", {}).get("values", [])
            
            logger.info(f"[REST] Embedding generated. Vector length: {len(embedding)}")
            return embedding
            
        except requests.exceptions.RequestException as e:
            logger.error(f"REST API Error: {e}")
            raise Exception(f"Embedding API failed: {e}")
