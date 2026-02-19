"""Database engine and session configuration.

Uses PostgreSQL if DATABASE_URL is set (production/Railway),
otherwise falls back to local SQLite for development.
"""

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

if settings.database_url:
    # Production: PostgreSQL
    DATABASE_URL = settings.database_url
    engine = create_engine(DATABASE_URL, echo=settings.app_debug, pool_pre_ping=True)
    print(f"🐘 Using PostgreSQL: {DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else '(configured)'}")
else:
    # Local development: SQLite
    DB_PATH = Path(settings.output_dir).parent / "docvision.db"
    DATABASE_URL = f"sqlite:///{DB_PATH}"
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=settings.app_debug,
    )
    print(f"📦 Using SQLite: {DB_PATH}")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
    """Create all tables if they don't exist."""
    from app.database.models import Base
    Base.metadata.create_all(bind=engine)
    print("✅ Database tables ready")


def get_db():
    """FastAPI dependency — yields a DB session per request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
