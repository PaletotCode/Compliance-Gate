from typing import Generator
from sqlalchemy.orm import Session
from compliance_gate.infra.db.session import SessionLocal

def get_db_session() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class PaginationDep:
    def __init__(self, page: int = 1, size: int = 50):
        self.page = page
        self.size = size
