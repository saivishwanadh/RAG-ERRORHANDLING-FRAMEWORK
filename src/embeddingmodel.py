import logging
from typing import List, Optional
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from src.config import Config

logger = logging.getLogger(__name__)

class EmbeddingGenerator:

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or Config.GEMINI_APIKEY
        if not self.api_key:
             # Legacy fallback if GEMINI_APIKEY not set but HUGGINGFACE_APIKEY was intended for this?
             # But the plan says standardize on GEMINI_APIKEY.
             logger.warning("GEMINI_APIKEY not found for EmbeddingGenerator")

        self.embeddings = GoogleGenerativeAIEmbeddings(
            google_api_key=self.api_key,
            model="models/text-embedding-004"
        )
        logger.info("Initialized EmbeddingGenerator with LangChain GoogleGenerativeAIEmbeddings")

    def get_embedding(self, text: str) -> List[float]:
        """Generate embedding using LangChain"""
        try:
            return self.embeddings.embed_query(text)
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise
