from typing import List, Optional, Dict
from database import with_duckdb_connection
from datetime import datetime

class LogImportRepository:

    @staticmethod
    def get_all_logs() -> List[Dict]:
        def _query(conn):
            conn.execute("""
                CREATE TABLE IF NOT EXISTS log_import (
                    timestamp TIMESTAMP,
                    last_id_before INTEGER,
                    inserted_rows INTEGER
                )
            """)
            rows = conn.execute("SELECT * FROM log_import ORDER BY timestamp DESC").fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in rows]
        return with_duckdb_connection(_query)

    @staticmethod
    def get_last_log() -> Optional[Dict]:
        def _query(conn):
            conn.execute("""
                CREATE TABLE IF NOT EXISTS log_import (
                    timestamp TIMESTAMP,
                    last_id_before INTEGER,
                    inserted_rows INTEGER
                )
            """)
            result = conn.execute("""
                SELECT * FROM log_import ORDER BY timestamp DESC LIMIT 1
            """).fetchone()
            if result:
                columns = [desc[0] for desc in conn.description]
                return dict(zip(columns, result))
            return None
        return with_duckdb_connection(_query)

    @staticmethod
    def insert_log(last_id_before: int, inserted_rows: int):
        def _insert(conn):
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS log_import (
                    timestamp TIMESTAMP,
                    last_id_before INTEGER,
                    inserted_rows INTEGER
                )
            """)
            conn.execute("""
                INSERT INTO log_import (timestamp, last_id_before, inserted_rows)
                VALUES (?, ?, ?)
            """, (now, last_id_before, inserted_rows))
        with_duckdb_connection(_insert)
