import os
import pandas as pd
import pyodbc
from datetime import datetime
from config import get_pyodbc_connection_string
from database import with_duckdb_connection

def get_last_id_from_duckdb():
    def _get_last(conn):
        try:
            result = conn.execute("SELECT MAX(id) FROM notification").fetchone()
            return result[0] if result[0] is not None else 0
        except:
            return 0
    return with_duckdb_connection(_get_last)

def load_new_notifications_from_sqlserver(last_id=0):
    conn_str = get_pyodbc_connection_string()
    conn = pyodbc.connect(conn_str)

    if last_id > 0:
        query = "SELECT * FROM notification WHERE id > ?"
        df = pd.read_sql(query, conn, params=[last_id])
    else:
        df = pd.read_sql("SELECT * FROM notification", conn)

    conn.close()
    return df

def insert_into_duckdb(df, last_id_before):
    def _insert(conn):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS notification AS 
            SELECT * FROM df LIMIT 0
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS log_import (
                timestamp TIMESTAMP,
                last_id_before INTEGER,
                inserted_rows INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS validation_doublons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id INTEGER,
                record_id INTEGER,
                action TEXT, -- 'validé' ou 'rejeté'
                user TEXT,
                date_validation TIMESTAMP
            )
        """)
        conn.register("df", df)
        conn.execute("INSERT INTO notification SELECT * FROM df")
        conn.unregister("df")
        df["statut_validation"] = "en attente"
        df["date_validation"] = None

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""
            INSERT INTO log_import (timestamp, last_id_before, inserted_rows)
            VALUES (?, ?, ?)
        """, (now, last_id_before, len(df)))
    with_duckdb_connection(_insert)

def main():
    print("🔍 Vérification du dernier ID dans DuckDB...")
    last_id = get_last_id_from_duckdb()
    print(f"📌 Dernier ID trouvé : {last_id}")

    print("🔄 Chargement des nouvelles données depuis SQL Server...")
    df = load_new_notifications_from_sqlserver(last_id)

    if df.empty:
        print("✅ Aucune nouvelle donnée à insérer.")
    else:
        print(f"📥 {len(df)} nouvelles lignes à insérer.")
        insert_into_duckdb(df, last_id)
        print("✅ Insertion réussie dans DuckDB.")
