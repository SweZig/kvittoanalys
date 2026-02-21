"""Auth API routes — registration, login, verification, user management."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.config import settings
from app.database.database import get_db
from app.database.models import User, CategorySuggestion, Document
from app.services.auth_service import (
    hash_password, verify_password,
    create_token, decode_token,
    create_verification_token, create_reset_token,
    send_verification_email, send_reset_email,
)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


# ── Helpers ──────────────────────────────────────────────────────────

def _get_current_user(
    db: Session = Depends(get_db),
    authorization: str | None = Header(None),
) -> User:
    """Extract and validate user from JWT token."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Ej inloggad")
    token = authorization.split(" ", 1)[1]
    payload = decode_token(token, settings.jwt_secret)
    if not payload:
        raise HTTPException(status_code=401, detail="Ogiltig eller utgången token")
    user = db.query(User).filter(User.id == payload.get("user_id")).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="Användaren finns inte eller är inaktiverad")
    return user


def _maybe_impersonate(
    real_user: User, db: Session, request: Request,
) -> User:
    """If admin sends X-Impersonate-User-Id header, return that user instead."""
    imp_id = request.headers.get("x-impersonate-user-id")
    if not imp_id or real_user.role != "admin":
        return real_user
    try:
        target = db.query(User).filter(User.id == int(imp_id)).first()
        if target:
            return target
    except (ValueError, TypeError):
        pass
    return real_user


def get_current_user(
    db: Session = Depends(get_db),
    authorization: str | None = Header(None),
    request: Request = None,
) -> User:
    """Public dependency for use in other routers. Supports admin impersonation."""
    user = _get_current_user(db, authorization)
    if request:
        return _maybe_impersonate(user, db, request)
    return user


def get_optional_user(
    db: Session = Depends(get_db),
    authorization: str | None = Header(None),
    request: Request = None,
) -> User | None:
    """Returns user if logged in, None otherwise (for gradual migration)."""
    if not authorization or not authorization.startswith("Bearer "):
        return None
    try:
        user = _get_current_user(db, authorization)
        if request:
            return _maybe_impersonate(user, db, request)
        return user
    except HTTPException:
        return None


def require_role(*roles: str):
    """Dependency factory: require user to have one of the given roles."""
    def check(user: User = Depends(get_current_user)):
        if user.role not in roles:
            raise HTTPException(status_code=403, detail="Otillräcklig behörighet")
        return user
    return check


def _email_configured() -> bool:
    return bool(settings.resend_api_key or (settings.smtp_host and settings.smtp_user and settings.smtp_password))

# Legacy alias
def _smtp_configured() -> bool:
    return _email_configured()


def _smtp_settings() -> dict:
    return {
        "smtp_host": settings.smtp_host,
        "smtp_port": settings.smtp_port,
        "smtp_user": settings.smtp_user,
        "smtp_password": settings.smtp_password,
        "from_addr": settings.smtp_from or settings.smtp_user,
    }


def _email_kwargs() -> dict:
    """Return kwargs for send_verification_email / send_reset_email."""
    if settings.resend_api_key:
        return {"resend_api_key": settings.resend_api_key, "from_addr": settings.email_from}
    return {"smtp_settings": _smtp_settings()}


def _user_dict(u: User) -> dict[str, Any]:
    return {
        "id": u.id,
        "email": u.email,
        "display_name": u.display_name,
        "role": u.role,
        "city": u.city,
        "is_verified": u.is_verified,
        "is_approved": u.is_approved,
        "is_active": u.is_active,
        "created_at": u.created_at.isoformat() if u.created_at else None,
    }


# ── Schemas ──────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: str
    password: str
    display_name: str | None = None
    city: str | None = None

class LoginRequest(BaseModel):
    email: str
    password: str

class PasswordResetRequest(BaseModel):
    email: str

class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str

class UpdateUserRole(BaseModel):
    role: str  # admin, superuser, user

class UpdateProfile(BaseModel):
    display_name: str | None = None
    city: str | None = None

class CategorySuggestionRequest(BaseModel):
    description: str
    current_category: str | None = None
    suggested_category: str
    reason: str | None = None

class SuggestionAction(BaseModel):
    status: str  # approved, rejected


# ── Registration & Login ─────────────────────────────────────────────

@router.post("/register")
async def register(data: RegisterRequest, db: Session = Depends(get_db)):
    """Register a new user account."""
    if len(data.password) < 6:
        raise HTTPException(status_code=400, detail="Lösenordet måste vara minst 6 tecken")

    existing = db.query(User).filter(User.email == data.email.lower().strip()).first()
    if existing:
        raise HTTPException(status_code=409, detail="E-postadressen är redan registrerad")

    # First user ever → auto-admin, auto-verified, auto-approved
    user_count = db.query(User).count()
    is_first = user_count == 0

    user = User(
        email=data.email.lower().strip(),
        password_hash=hash_password(data.password),
        display_name=data.display_name or data.email.split("@")[0],
        city=data.city,
        role="admin" if is_first else "user",
        is_verified=is_first,
        is_approved=is_first,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    # Send verification email if email provider configured
    email_sent = False
    if not is_first and _email_configured():
        token = create_verification_token(user.email, settings.jwt_secret)
        base = settings.app_base_url or f"http://localhost:{settings.app_port}"
        email_sent = send_verification_email(user.email, token, base, **_email_kwargs())

    result = {
        "status": "success",
        "message": "Konto skapat!" if is_first else (
            "Konto skapat! Kolla din e-post för att verifiera kontot." if email_sent
            else "Konto skapat — väntar på godkännande"
        ),
        "user": _user_dict(user),
        "is_first_user": is_first,
        "email_sent": email_sent,
    }

    # Auto-login first user
    if is_first:
        result["token"] = create_token(
            {"user_id": user.id, "email": user.email, "role": user.role},
            settings.jwt_secret,
        )

    return result


@router.post("/login")
async def login(data: LoginRequest, db: Session = Depends(get_db)):
    """Login with email and password."""
    user = db.query(User).filter(User.email == data.email.lower().strip()).first()
    if not user or not verify_password(data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Fel e-post eller lösenord")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Kontot är inaktiverat")
    if not user.is_verified:
        raise HTTPException(status_code=403, detail="E-postadressen är inte verifierad. Kolla din inkorg eller klicka på 'Skicka verifieringsmail igen'.")
    if not user.is_approved:
        raise HTTPException(status_code=403, detail="Kontot väntar på godkännande av admin")

    token = create_token(
        {"user_id": user.id, "email": user.email, "role": user.role},
        settings.jwt_secret,
    )
    return {"status": "success", "token": token, "user": _user_dict(user)}


@router.get("/me")
async def get_me(user: User = Depends(get_current_user)):
    """Get current user profile."""
    return {"status": "success", "user": _user_dict(user)}


@router.put("/me")
async def update_me(
    data: UpdateProfile, user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update own profile (name, city)."""
    if data.display_name is not None:
        user.display_name = data.display_name
    if data.city is not None:
        user.city = data.city
        # Auto-add city to campaign cities if not present
        _ensure_campaign_city(data.city)
    db.commit()
    return {"status": "success", "user": _user_dict(user)}


def _ensure_campaign_city(city: str):
    """Add city to campaign service if not already present."""
    try:
        from app.services.campaign_service import SWEDISH_CITIES
        city_key = city.lower().strip()
        if city_key not in SWEDISH_CITIES:
            # Try to geocode using a heuristic — user can adjust later
            print(f"📍 Ny ort '{city}' — läggs till i kampanjsökning vid nästa API-uppdatering")
    except Exception:
        pass


# ── Email verification ───────────────────────────────────────────────

@router.get("/verify")
async def verify_email(token: str = Query(...), db: Session = Depends(get_db)):
    """Verify email from link."""
    payload = decode_token(token, settings.jwt_secret)
    if not payload or payload.get("purpose") != "verify":
        raise HTTPException(status_code=400, detail="Ogiltig eller utgången verifieringslänk")
    user = db.query(User).filter(User.email == payload["email"]).first()
    if not user:
        raise HTTPException(status_code=404, detail="Användaren hittades inte")
    user.is_verified = True
    db.commit()
    return {"status": "success", "message": "E-post verifierad!"}


@router.post("/resend-verification")
async def resend_verification(data: PasswordResetRequest, db: Session = Depends(get_db)):
    """Resend verification email."""
    user = db.query(User).filter(User.email == data.email.lower().strip()).first()
    if not user:
        # Don't reveal if email exists
        return {"status": "success", "message": "Om kontot finns skickas ett nytt verifieringsmail"}
    if user.is_verified:
        return {"status": "success", "message": "Kontot är redan verifierat — du kan logga in"}
    if not _email_configured():
        raise HTTPException(status_code=500, detail="E-posttjänsten är inte konfigurerad")

    token = create_verification_token(user.email, settings.jwt_secret)
    base = settings.app_base_url or f"http://localhost:{settings.app_port}"
    sent = send_verification_email(user.email, token, base, **_email_kwargs())
    if not sent:
        raise HTTPException(status_code=500, detail="Kunde inte skicka verifieringsmail")
    return {"status": "success", "message": "Verifieringsmail skickat — kolla din inkorg"}


# ── Password reset ───────────────────────────────────────────────────

@router.post("/forgot-password")
async def forgot_password(data: PasswordResetRequest, db: Session = Depends(get_db)):
    """Request password reset email."""
    user = db.query(User).filter(User.email == data.email.lower().strip()).first()
    # Always return success to avoid email enumeration
    if user and _email_configured():
        token = create_reset_token(user.email, settings.jwt_secret)
        base = settings.app_base_url or f"http://localhost:{settings.app_port}"
        send_reset_email(user.email, token, base, **_email_kwargs())
    return {"status": "success", "message": "Om kontot finns skickas ett mejl med återställningslänk"}


@router.post("/reset-password")
async def reset_password(data: PasswordResetConfirm, db: Session = Depends(get_db)):
    """Reset password using token from email."""
    payload = decode_token(data.token, settings.jwt_secret)
    if not payload or payload.get("purpose") != "reset":
        raise HTTPException(status_code=400, detail="Ogiltig eller utgången återställningslänk")
    if len(data.new_password) < 6:
        raise HTTPException(status_code=400, detail="Lösenordet måste vara minst 6 tecken")
    user = db.query(User).filter(User.email == payload["email"]).first()
    if not user:
        raise HTTPException(status_code=404, detail="Användaren hittades inte")
    user.password_hash = hash_password(data.new_password)
    db.commit()
    return {"status": "success", "message": "Lösenordet har återställts"}


# ── Admin: User management ───────────────────────────────────────────

@router.get("/users")
async def list_users(
    user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    """List all users (admin only)."""
    users = db.query(User).order_by(User.created_at.desc()).all()
    return {"status": "success", "users": [_user_dict(u) for u in users]}


@router.put("/users/{user_id}/role")
async def update_user_role(
    user_id: int, data: UpdateUserRole,
    user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    """Change a user's role (admin only)."""
    if data.role not in ("admin", "superuser", "user"):
        raise HTTPException(status_code=400, detail="Ogiltig roll")
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Användaren hittades inte")
    if target.id == user.id and data.role != "admin":
        raise HTTPException(status_code=400, detail="Du kan inte degradera dig själv")
    target.role = data.role
    db.commit()
    return {"status": "success", "user": _user_dict(target)}


@router.put("/users/{user_id}/approve")
async def approve_user(
    user_id: int,
    user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    """Approve a pending user (admin only)."""
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Användaren hittades inte")
    target.is_approved = True
    target.is_verified = True  # also verify
    db.commit()
    return {"status": "success", "user": _user_dict(target)}


@router.put("/users/{user_id}/toggle-active")
async def toggle_user_active(
    user_id: int,
    user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    """Activate/deactivate a user (admin only)."""
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Användaren hittades inte")
    if target.id == user.id:
        raise HTTPException(status_code=400, detail="Du kan inte inaktivera dig själv")
    target.is_active = not target.is_active
    db.commit()
    return {"status": "success", "user": _user_dict(target)}


# ── Category suggestions (non-admin users) ───────────────────────────

@router.post("/suggestions")
async def create_suggestion(
    data: CategorySuggestionRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Suggest a category change (for non-admin users)."""
    suggestion = CategorySuggestion(
        user_id=user.id,
        description=data.description,
        current_category=data.current_category,
        suggested_category=data.suggested_category,
        reason=data.reason,
    )
    db.add(suggestion)
    db.commit()
    return {"status": "success", "message": "Förslag skickat till admin", "id": suggestion.id}


@router.get("/suggestions")
async def list_suggestions(
    status: str = Query("pending"),
    user: User = Depends(require_role("admin", "superuser")),
    db: Session = Depends(get_db),
):
    """List category suggestions (admin/superuser)."""
    q = db.query(CategorySuggestion)
    if status:
        q = q.filter(CategorySuggestion.status == status)
    items = q.order_by(CategorySuggestion.created_at.desc()).all()
    return {
        "status": "success",
        "suggestions": [
            {
                "id": s.id,
                "user_email": s.user.email if s.user else "?",
                "description": s.description,
                "current_category": s.current_category,
                "suggested_category": s.suggested_category,
                "reason": s.reason,
                "status": s.status,
                "created_at": s.created_at.isoformat() if s.created_at else None,
            }
            for s in items
        ],
    }


@router.put("/suggestions/{suggestion_id}")
async def review_suggestion(
    suggestion_id: int, data: SuggestionAction,
    user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    """Approve or reject a category suggestion (admin only)."""
    s = db.query(CategorySuggestion).filter(CategorySuggestion.id == suggestion_id).first()
    if not s:
        raise HTTPException(status_code=404, detail="Förslaget hittades inte")
    s.status = data.status
    s.reviewed_by = user.id
    s.reviewed_at = datetime.now(timezone.utc)
    db.commit()

    # If approved, apply the category change
    result = {"status": "success", "suggestion_status": s.status}
    if data.status == "approved":
        from app.database import crud
        r = crud.update_product_category(
            db, description=s.description,
            category=s.suggested_category, should_create_rule=True,
        )
        result["items_updated"] = r.get("items_updated", 0)

    return result
