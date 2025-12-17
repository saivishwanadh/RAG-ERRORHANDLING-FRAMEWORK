import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Optional, List, Any, Tuple, Union
from src.config import Config

logger = logging.getLogger(__name__)

class DB:
    def __init__(self):
        self.conn_string = Config.DB_URL
        self.conn = None
        # Connect immediately
        self.connect()

    def connect(self):
        """Establish database connection"""
        try:
            if self.conn is None or self.conn.closed:
                self.conn = psycopg2.connect(self.conn_string)
                logger.info("✅ PostgreSQL connection established")
        except Exception as e:
            logger.error(f"❌ DB connection failed: {e}")
            raise e

    def execute(self, query: str, params: Optional[Tuple] = None, fetch: bool = False) -> Optional[List[dict]]:
        """
        Execute a query safely.
        
        Args:
            query: SQL string
            params: tuple of parameters
            fetch: set True for SELECT queries
            
        Returns:
            List of dicts if fetch=True, else None
        """
        try:
            # Reconnect if connection closed
            if self.conn is None or self.conn.closed:
                self.connect()

            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params or ())
                
                if fetch:
                    rows = cur.fetchall()
                    self.conn.commit()
                    return rows
                
                self.conn.commit()
                return None

        except psycopg2.Error as e:
            logger.error(f"❌ SQL ERROR: {str(e)} | QUERY: {query}")
            if self.conn:
                try:
                    self.conn.rollback()
                except psycopg2.InterfaceError:
                    # Connection likely closed
                    pass
            raise e
        except Exception as e:
            logger.error(f"❌ Unexpected DB Error: {e}")
            raise e

    def close(self):
        """Close database connection"""
        try:
            if self.conn and not self.conn.closed:
                self.conn.close()
                logger.info("❎ DB connection closed")
        except Exception as e:
            logger.error(f"Error closing DB: {e}")
        finally:
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
