import sys
from fastapi import APIRouter, Depends
sys.path.append("..")
from utils.security import authenticate
from models.validation import ValidationDoublon
from database import with_duckdb_connection
from services.deduplication_service import update_doublon_statut

router = APIRouter(
    prefix="/api",
    tags=["Déduplication"],
    dependencies=[Depends(authenticate)]
)

@router.post("/doublons/valider")
@router.post("/doublons/valider")
def valider_doublon(validation: ValidationDoublon):
    def _insert(conn):
        conn.execute("""
            INSERT INTO validation_doublons (cluster_id, record_id, action, user, date_validation)
            VALUES (?, ?, ?, ?, ?)
        """, (
            validation.cluster_id,
            validation.record_id,
            validation.action,
            validation.user,
            validation.date_validation.isoformat()
        ))
    with_duckdb_connection(_insert)

    update_doublon_statut(validation.cluster_id, validation.record_id, validation.action)

    return {"message": "Validation enregistrée et doublon mis à jour."}
