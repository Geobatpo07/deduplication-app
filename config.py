import os
from utils.path import DUCKDB_FILE, TRAINING_FILE, SETTINGS_FILE

# Recommandé : utiliser des variables d’environnement pour cacher les infos sensibles
DB_SERVER = os.getenv("DB_SERVER", "66.148.112.213")
DB_NAME = os.getenv("DB_NAME", "SALVH")
DB_USER = os.getenv("DB_USER", "gblaguerre")
DB_PASSWORD = os.getenv("DB_PASSWORD", "n)j9{V(M/G")

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# Chemin vers la base de données DuckDB (fichier .duckdb)
DUCKDB_PATH = str(DUCKDB_FILE)

# Fichiers de déduplication
TRAINING_FILE = str(TRAINING_FILE)
SETTINGS_FILE = str(SETTINGS_FILE)

def get_db_connection_string():
    from urllib.parse import quote_plus

    connection_string = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD}"
    )

    return f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}"

def get_pyodbc_connection_string():
    return (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD}"
    )
