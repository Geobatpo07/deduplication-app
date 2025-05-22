import sys
from fastapi import APIRouter, Depends
from utils.security import authenticate
from typing import List
sys.path.append("..")
from services.notification_repository import NotificationRepository
from models.notification import Notification

router = APIRouter(
    prefix="/api",
    tags=["Déduplication"],
    dependencies=[Depends(authenticate)]  # Appliqué à tous les endpoints du fichier
)
@router.get("/notifications", response_model=List[Notification])
def list_notifications():
    return NotificationRepository.get_all_notifications()
