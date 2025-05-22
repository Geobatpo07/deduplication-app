from pydantic import BaseModel
from typing import Optional

class Notification(BaseModel):
    id: int
    nom: Optional[str]
    prenom: Optional[str]
    prenommere: Optional[str]
    sexe: Optional[str]
    date_naissance: Optional[str]
    mpi_ref: Optional[str]
    codeVCT: Optional[str]

    class Config:
        orm_mode = True
