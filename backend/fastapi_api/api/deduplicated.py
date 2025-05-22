import sys
from fastapi import APIRouter, Depends
from typing import List
sys.path.append("..")
from utils.security import authenticate
from services.deduplication_service import run_deduplication
from models.deduplicated import DeduplicatedPatient
from database import with_duckdb_connection

router = APIRouter(
    prefix="/api",
    tags=["Déduplication"],
    dependencies=[Depends(authenticate)]
)

@router.get("/doublons", response_model=List[DeduplicatedPatient])
def get_deduplicated_patients():
    def _query(conn):
        rows = conn.execute("SELECT * FROM deduplicated_patients ORDER BY cluster_id").fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row)) for row in rows]

    return with_duckdb_connection(_query)

@router.post("/doublons/run")
def trigger_deduplication():
    results = run_deduplication()
    return {
        "message": "Déduplication terminée avec succès.",
        "clusters_detectés": len(set([r["cluster_id"] for r in results])),
        "doublons_total": len(results)
    }
