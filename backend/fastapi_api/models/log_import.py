from pydantic import BaseModel
from datetime import datetime

class LogImport(BaseModel):
    timestamp: datetime
    last_id_before: int
    inserted_rows: int
