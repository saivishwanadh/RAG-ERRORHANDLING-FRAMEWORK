import logging
from typing import List, Dict, Any, Optional
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient, models
from src.config import Config

logger = logging.getLogger(__name__)

class QdrantStore:
    """
    Qdrant adapter using LangChain QdrantVectorStore.
    """

    def __init__(
        self,
        embedding_model, # LangChain Embeddings object
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        collection_name: str = "error_solutions",
        prefer_grpc: bool = False,
    ):
        self.url = url or Config.QDRANT_URL
        self.api_key = api_key or Config.QDRANT_API_KEY
        self.collection_name = collection_name or Config.QDRANT_DEFAULT_COLLECTION
        self.embedding_model = embedding_model
        
        if not self.url:
            logger.warning("Qdrant URL not provided. Vector DB operations may fail.")
            
        self.client = QdrantClient(url=self.url, api_key=self.api_key, prefer_grpc=prefer_grpc)
        
        # Initialize LangChain VectorStore
        self.vector_store = QdrantVectorStore(
            client=self.client,
            collection_name=self.collection_name,
            embedding=self.embedding_model
        )
        logger.info("✅ Connected to Vector DB via LangChain")

    def list_collections(self):
        """Return list of collections."""
        return self.client.get_collections()

    def upsert_vector(
        self,
        collection: str, 
        vector_id: int,
        vector: List[float], 
        payload: Dict[str, Any],
        text_content: Optional[str] = None 
    ):
        """
        Insert/update using Hybrid Approach:
        - LangChain for Embeddings (Integration)
        - Qdrant Client for Storage (Schema Control)
        """
        # 1. Generate Embedding via LangChain (if text provided) or use raw vector
        if text_content:
            generated_vector = self.embedding_model.embed_query(text_content)
        elif vector:
            generated_vector = vector
        else:
             logging.error("Either text_content or vector must be provided for upsert.")
             raise ValueError("text_content or vector must be provided.")

        # 2. Insert via Qdrant Client
        # This preserves the "Flat Schema" requirement (Image 1)
        # We assume the user wants the text content to be part of the payload or just metadata?
        # Image 1 shows 'error_code', 'error_description', 'solution' at root.
        # It does NOT show 'page_content'.
        
        # We pass the payload AS IS, which gives the user full control over the schema.
        point = models.PointStruct(
            id=vector_id,
            vector=generated_vector,
            payload=payload
        )
        
        return self.client.upsert(collection_name=collection, points=[point], wait=True)

    def search(
        self,
        collection: str,
        vector: List[float], 
        limit: int,
        query_filter: Optional[models.Filter] = None,
        score_threshold: float = 0.85,
        text_query: Optional[str] = None 
    ):
        """Search using Hybrid Approach (LangChain Vector + Qdrant Client Data)"""
        
        query_vector = None
        if text_query:
            # Use LangChain to generate the query vector
            query_vector = self.embedding_model.embed_query(text_query)
        elif vector:
            query_vector = vector
        else:
             raise ValueError("Either text_query or vector must be provided.")

        # Use Qdrant Client to search (Preserves schema exactness)
        results = self.client.query_points(
            collection_name=collection,
            query=query_vector,
            limit=limit,
            query_filter=query_filter,
            with_payload=True
        )
        
        # Format results
        formatted_results = []
        for point in results.points:
            if point.score >= score_threshold:
                 # Wrap it to mimic the old interface expected by the app
                 # Point object has .payload and .score
                 formatted_results.append(point)
        
        return formatted_results

    @staticmethod
    def extract_solutions(results) -> str:
        solutions = []
        for index, point in enumerate(results, start=1):
            solution_text = point.payload.get("solution", "")
            solutions.append(f"solution{index}: {solution_text}")

        return "\n".join(solutions)
