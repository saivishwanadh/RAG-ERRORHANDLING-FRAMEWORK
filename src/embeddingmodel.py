import logging
from typing import List, Optional
from langchain_core.embeddings import Embeddings
from google import genai
from google.genai import types
from src.config import Config

logger = logging.getLogger(__name__)

class GoogleGenAIEmbeddings(Embeddings):
    """
    Custom Embeddings class using the new `google.genai` SDK
    to support `output_dimensionality`.
    """
    def __init__(self, api_key: str, model: str, output_dimensionality: int = 768):
        self.client = genai.Client(api_key=api_key)
        self.model = model
        self.output_dimensionality = output_dimensionality

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        # Batch embedding not straightforward in simple client usage without loop or batch calls
        # For simplicity and correctness with the new SDK:
        embeddings = []
        for text in texts:
            result = self.client.models.embed_content(
                model=self.model,
                contents=text,
                config=types.EmbedContentConfig(output_dimensionality=self.output_dimensionality)
            )
            embeddings.append(result.embeddings[0].values)
        return embeddings

    def embed_query(self, text: str) -> List[float]:
        result = self.client.models.embed_content(
            model=self.model,
            contents=text,
            config=types.EmbedContentConfig(output_dimensionality=self.output_dimensionality)
        )
        return result.embeddings[0].values

class EmbeddingGenerator:

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or Config.GEMINI_APIKEY
        if not self.api_key:
             logger.warning("GEMINI_APIKEY not found for EmbeddingGenerator")
        
        # Use our custom class that supports 768 dimensions
        logger.info(f"Initializing Custom GoogleGenAIEmbeddings with model={Config.GEMINI_EMBEDDING_MODEL} and dim=768")
        self.embeddings = GoogleGenAIEmbeddings(
            api_key=self.api_key,
            model=Config.GEMINI_EMBEDDING_MODEL,
            output_dimensionality=768 # Force 768 as requested
        )

    def get_embedding(self, text: str) -> List[float]:
        """Generate embedding using LangChain"""
        try:
            return self.embeddings.embed_query(text)
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise