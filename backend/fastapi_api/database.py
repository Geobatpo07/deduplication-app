import duckdb
from config import DUCKDB_PATH

def get_duckdb_connection():
    return duckdb.connect(DUCKDB_PATH)

def with_duckdb_connection(fn):
    """
    Utilitaire pour encapsuler les connexions DuckDB en mode context manager.
    Usage :
        with_duckdb_connection(lambda conn: conn.execute(...))
    """
    conn = get_duckdb_connection()
    try:
        return fn(conn)
    finally:
        conn.close()
