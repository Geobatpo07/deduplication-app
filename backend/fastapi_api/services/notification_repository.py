from typing import List, Optional, Dict
from database import with_duckdb_connection
import json
import pandas as pd

class NotificationRepository:

    @staticmethod
    def get_all_notifications() -> List[Dict]:
        def _query(conn):
            rows = conn.execute("SELECT * FROM notification").fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in rows]
        return with_duckdb_connection(_query)

    @staticmethod
    def get_notification_by_id(notif_id: int) -> Optional[Dict]:
        def _query(conn):
            result = conn.execute(
                "SELECT * FROM notification WHERE id = ?", [notif_id]
            ).fetchone()
            if result:
                columns = [desc[0] for desc in conn.description]
                return dict(zip(columns, result))
            return None
        return with_duckdb_connection(_query)

    @staticmethod
    def get_last_id() -> int:
        def _query(conn):
            result = conn.execute("SELECT MAX(id) FROM notification").fetchone()
            return result[0] if result[0] is not None else 0
        return with_duckdb_connection(_query)

    @staticmethod
    def insert_notifications(df: pd.DataFrame):
        def _insert(conn):
            conn.register("df_to_insert", df)
            conn.execute("INSERT INTO notification SELECT * FROM df_to_insert")
            conn.unregister("df_to_insert")
        with_duckdb_connection(_insert)

    @staticmethod
    def get_suspected_duplicates() -> List[Dict]:
        def _query(conn):
            rows = conn.execute("""
                SELECT *
                FROM notification
                WHERE mpi_ref IN (
                    SELECT mpi_ref
                    FROM notification
                    GROUP BY mpi_ref
                    HAVING COUNT(*) > 1
                )
            """).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in rows]
        return with_duckdb_connection(_query)
