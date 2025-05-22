import sys
import os
import dedupe
import pandas as pd
sys.path.append("..")
from database import get_duckdb_connection, with_duckdb_connection
from utils.path import TRAINING_FILE, SETTINGS_FILE

def fetch_notification_data() -> pd.DataFrame:
    conn = get_duckdb_connection()
    df = conn.execute("SELECT id, nom, prenom, prenommere, sexe, date_naissance, mpi_ref, codeVCT FROM notification").df()
    conn.close()
    return df

def dataframe_to_dedupe_format(df: pd.DataFrame) -> dict:
    data = {}
    for index, row in df.iterrows():
        clean_row = row.fillna("").to_dict()
        record_id = int(clean_row.pop("id"))
        data[record_id] = clean_row
    return data

def train_deduper(data_d: dict) -> dedupe.Dedupe:
    fields = [
        dedupe.variables.String("Nom"),
        dedupe.variables.String("Prenom"),
        dedupe.variables.String("Prenommere"),
        dedupe.variables.String("altphone"),
    ]
    deduper = dedupe.Dedupe(fields)
    with open(TRAINING_FILE, "r") as f:
        deduper.prepare_training(data_d, training_file=f)
    deduper.train()
    with open(SETTINGS_FILE, "wb") as f:
        deduper.write_settings(f)
    return deduper

def store_deduplication_results(results):
    if not results:
        print("Aucun doublon détecté.")
        return

    df = pd.DataFrame(results)

    with get_duckdb_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS deduplicated_patients")
        conn.register("df", df)
        conn.execute("CREATE TABLE deduplicated_patients AS SELECT * FROM df")
        conn.unregister("df")

    print(f"✅ {len(df)} doublons enregistrés dans 'deduplicated_patients'.")


def run_deduplication():
    df = fetch_notification_data()
    data_d = dataframe_to_dedupe_format(df)

    if SETTINGS_FILE.exists():
        with open(SETTINGS_FILE, "rb") as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        deduper = train_deduper(data_d)

    threshold = deduper.threshold(data_d, recall_weight=1.5)
    clustered_dupes = deduper.match(data_d, threshold)

    results = []
    for cluster_id, (record_ids, score) in enumerate(clustered_dupes):
        for record_id in record_ids:
            record = data_d[record_id]
            record["cluster_id"] = cluster_id
            record["score"] = score
            record["id"] = record_id
            results.append(record)

    store_deduplication_results(results)
    return results

def update_doublon_statut(cluster_id: int, record_id: int, action: str):
    def _update(conn):
        conn.execute("""
            UPDATE deduplicated_patients
            SET statut_validation = ?
            WHERE cluster_id = ? AND id = ?
        """, (action, cluster_id, record_id))
    with_duckdb_connection(_update)