"""Kvittoanalys — Smart utgiftsanalys."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api.routes import router as analysis_router
from app.api.auth_routes import router as auth_router
from app.config import settings

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Setup and teardown logic."""
    # Startup: ensure directories exist
    settings.upload_path.mkdir(parents=True, exist_ok=True)
    settings.output_path.mkdir(parents=True, exist_ok=True)

    # Ensure new tables exist (User, CategorySuggestion)
    from app.database.database import engine
    from app.database.models import Base
    Base.metadata.create_all(bind=engine)

    # Run safe migrations for new columns on existing tables
    from app.database.database import get_db
    db = next(get_db())
    try:
        from app.database.crud import _safe_migrate
        _safe_migrate(db, "SELECT ica_store_ids FROM users LIMIT 1",
                      "ALTER TABLE users ADD COLUMN ica_store_ids TEXT")
    except Exception as e:
        print(f"⚠️ Migration check: {e}")
    finally:
        db.close()

    print(f"🚀 Kvittoanalys API starting on http://{settings.app_host}:{settings.app_port}")
    print(f"📖 Docs: http://localhost:{settings.app_port}/docs")
    yield
    # Shutdown
    print("👋 Kvittoanalys API shutting down")


app = FastAPI(
    title="Kvittoanalys API",
    description=(
        "Smart utgiftsanalys — skanna kvitton, spåra utgifter, hitta erbjudanden.\n\n"
        "Ladda upp kvitton och fakturor för att:\n"
        "- Automatiskt extrahera produkter och priser\n"
        "- Kategorisera utgifter\n"
        "- Jämföra priser mellan butiker\n"
        "- Hitta kampanjer i din ort"
    ),
    version="0.2.0",
    lifespan=lifespan,
)

# CORS — allow all origins in dev, restrict in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(analysis_router)
app.include_router(auth_router)


@app.get("/")
async def root():
    """Serve the frontend."""
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {
        "app": "Kvittoanalys",
        "version": "0.2.0",
        "docs": "/docs",
    }


@app.get("/verify")
async def verify_page():
    """Serve frontend for email verification — JS handles the token."""
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/reset-password")
async def reset_page():
    """Serve frontend for password reset — JS handles the token."""
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/health")
async def health_check():
    return {"status": "healthy", "model": settings.claude_model}
