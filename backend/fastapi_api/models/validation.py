from pydantic import BaseModel
from typing import Literal
from datetime import datetime

class ValidationDoublon(BaseModel):
    cluster_id: int
    record_id: int
    action: Literal["validé", "rejeté"]
    user: str
    date_validation: datetime
