import logging
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient, models
from src.config import Config

logger = logging.getLogger(__name__)

class QdrantStore:
    """
    Qdrant adapter with methods to list collections, upsert points and perform searches.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        default_collection: Optional[str] = None,
        prefer_grpc: bool = False,
    ):
        self.url = url or Config.QDRANT_URL
        self.api_key = api_key or Config.QDRANT_API_KEY
        self.default_collection = default_collection
        
        if not self.url:
            logger.warning("Qdrant URL not provided. Vector DB operations may fail.")
            
        self.client = QdrantClient(url=self.url, api_key=self.api_key, prefer_grpc=prefer_grpc)
        logger.info("✅ Connected to Vector DB")

    def list_collections(self):
        """Return list of collections."""
        return self.client.get_collections()

    def upsert_vector(
        self,
        collection: str,
        vector_id: int,
        vector: List[float],
        payload: Dict[str, Any]
    ):
        """Insert/update a single vector."""
        point = models.PointStruct(
            id=vector_id,
            vector=vector,
            payload=payload
        )
        return self.client.upsert(collection_name=collection, points=[point])

    def search(
        self,
        collection: str,
        vector: List[float],
        limit: int,
        query_filter: Optional[models.Filter] = None,
        score_threshold: float = 0.85
    ):
        """Search using the high-level search API."""
        results = self.client.query_points(
            collection_name=collection,
            query=vector,
            limit=limit,
            query_filter=query_filter,
            with_payload=True
        )
        # Filter by score manually if needed, though Qdrant usually handles this better via params
        # Note: query_points might not have score_threshold param in all versions, 
        # but we can filter the result list.
        filtered_points = [p for p in results.points if p.score >= score_threshold]
        return filtered_points

    @staticmethod
    def extract_solutions(results) -> str:
        solutions = []
        for index, point in enumerate(results, start=1):
            solution_text = point.payload.get("solution", "")
            solutions.append(f"solution{index}: {solution_text}")

        return "\n".join(solutions)
