"""Database engine and session configuration."""

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

DB_PATH = Path(settings.output_dir).parent / "docvision.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=settings.app_debug,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
    """Create all tables if they don't exist."""
    from app.database.models import Base
    Base.metadata.create_all(bind=engine)
    print(f"📦 Database ready: {DB_PATH}")


def get_db():
    """FastAPI dependency – yields a DB session per request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
