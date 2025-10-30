from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.api.deps import get_db_session
from backend.db.models import Entity

router = APIRouter(prefix="/api", tags=["core"])

@router.get("/ping")
def ping():
    return {"message": "pong"}

@router.get("/entities")
def list_entities(db: Session = Depends(get_db_session)):
    rows = db.query(Entity).order_by(Entity.id).all()
    return [
        {
            "id": r.id,
            "name": r.name,
            "cage": r.cage,
            "uei": r.uei,
            "parent": r.parent,
            "type": r.type,
            "sponsor": r.sponsor,
            "sites": r.sites_json,
        }
        for r in rows
    ]
