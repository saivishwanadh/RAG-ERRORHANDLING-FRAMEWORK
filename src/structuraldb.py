import psycopg2
from psycopg2.extras import RealDictCursor
import logging

class DB:

    def __init__(self):
        self.conn_string = (
            "postgresql://neondb_owner:npg_tquG3E7eagTP"
            "@ep-summer-breeze-advcvth1-pooler.c-2.us-east-1.aws.neon.tech"
            ":5432/neondb?sslmode=require"
        )

        self.conn = psycopg2.connect(self.conn_string, cursor_factory=RealDictCursor)
        self.cur = self.conn.cursor()
        logging.info("✅ Connected to DB")

    # ---------------------------------------------------------
    # EXECUTE ANY QUERY (INSERT / UPDATE / DELETE / SELECT)
    # ---------------------------------------------------------
    def execute(self, query: str, params: tuple = None, fetch: bool = False):
        """
        query: SQL string
        params: tuple of parameters (optional)
        fetch: if True → returns results (for SELECT queries)
        """
        self.cur.execute(query, params or ())

        if fetch:
            rows = self.cur.fetchall()
                # Commit modifications (INSERT/UPDATE/DELETE with RETURNING need commit)
            self.conn.commit()
            return rows


        self.conn.commit()
        return None

    def close(self):
        self.cur.close()
        self.conn.close()
        print("❎ Connection closed")
        

#if __name__ == "__main__":
    #db = DB()

    '''rows = db.execute(
        "SELECT * FROM errorsolutiontable WHERE id = %s",
        ("102",),
        fetch=True
    )
    print(rows)'''
    '''insert_sql = """
                INSERT INTO errorsolutiontable (
                    application_name,
                    error_code,
                    error_description,
                    sessionID,
                    llm_solution,
                    error_timestamp,
                    sessionid_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """
            
    

    
    params = (
        "applicationName",
        "code",
        "description",
        "e99e898r9r",
        "hello",
        "errorhello",
        "active"
    )
    inserted_rows = db.execute(insert_sql, params, fetch=True)

    print(inserted_rows)
    if inserted_rows and len(inserted_rows) > 0:
        new_id = inserted_rows[0].get("id") if isinstance(inserted_rows[0], dict) else inserted_rows[0]["id"]
    else:
        new_id = None

            # example usage: log or use new_id for qdrant upsert
    print("inserted id:", new_id)
    db.close()'''
