"""Authentication service — JWT tokens, password hashing, email verification."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, timezone
from typing import Any


# ── Password hashing (bcrypt-like using hashlib + salt) ──────────────

def hash_password(password: str) -> str:
    """Hash a password with a random salt."""
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return f"{salt}${h.hex()}"


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against its hash."""
    try:
        salt, h = hashed.split("$", 1)
        expected = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
        return hmac.compare_digest(expected.hex(), h)
    except Exception:
        return False


# ── JWT tokens (minimal, no external dependency) ─────────────────────

def _b64e(data: bytes) -> str:
    return urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64d(data: str) -> bytes:
    padding = 4 - len(data) % 4
    return urlsafe_b64decode(data + "=" * padding)


def create_token(payload: dict[str, Any], secret: str, expires_hours: int = 72) -> str:
    """Create a simple JWT-like token."""
    header = _b64e(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload["exp"] = int(time.time()) + expires_hours * 3600
    payload["iat"] = int(time.time())
    body = _b64e(json.dumps(payload).encode())
    sig = hmac.new(secret.encode(), f"{header}.{body}".encode(), hashlib.sha256).hexdigest()
    return f"{header}.{body}.{sig}"


def decode_token(token: str, secret: str) -> dict[str, Any] | None:
    """Decode and verify a token. Returns payload or None if invalid."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header, body, sig = parts
        expected = hmac.new(secret.encode(), f"{header}.{body}".encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        payload = json.loads(_b64d(body))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None


# ── Email sending (optional SMTP) ────────────────────────────────────

def send_email(
    to: str,
    subject: str,
    body_html: str,
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    from_addr: str | None = None,
) -> bool:
    """Send an email via SMTP. Returns True on success."""
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr or smtp_user
        msg["To"] = to
        msg.attach(MIMEText(body_html, "html", "utf-8"))

        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(msg["From"], [to], msg.as_string())

        print(f"✅ Email sent to {to}")
        return True
    except Exception as e:
        print(f"⚠️ Email failed to {to}: {e}")
        return False


def send_verification_email(
    email: str, token: str, base_url: str, smtp_settings: dict
) -> bool:
    """Send account verification email."""
    link = f"{base_url}/verify?token={token}"
    html = f"""
    <h2>Välkommen till Kvittoanalys!</h2>
    <p>Verifiera din e-postadress genom att klicka på länken nedan:</p>
    <p><a href="{link}" style="display:inline-block;background:#22c55e;color:#fff;
       padding:10px 24px;border-radius:6px;text-decoration:none;font-weight:600;">
       Verifiera konto</a></p>
    <p style="color:#888;font-size:0.85em;">Länken är giltig i 24 timmar.</p>
    """
    return send_email(email, "Verifiera ditt Kvittoanalys-konto", html, **smtp_settings)


def send_reset_email(
    email: str, token: str, base_url: str, smtp_settings: dict
) -> bool:
    """Send password reset email."""
    link = f"{base_url}/reset-password?token={token}"
    html = f"""
    <h2>Återställ lösenord</h2>
    <p>Klicka på länken nedan för att välja ett nytt lösenord:</p>
    <p><a href="{link}" style="display:inline-block;background:#4a9eff;color:#fff;
       padding:10px 24px;border-radius:6px;text-decoration:none;font-weight:600;">
       Återställ lösenord</a></p>
    <p style="color:#888;font-size:0.85em;">Länken är giltig i 1 timme.</p>
    """
    return send_email(email, "Återställ lösenord — Kvittoanalys", html, **smtp_settings)


# ── Verification tokens ──────────────────────────────────────────────

def create_verification_token(email: str, secret: str) -> str:
    return create_token({"email": email, "purpose": "verify"}, secret, expires_hours=24)


def create_reset_token(email: str, secret: str) -> str:
    return create_token({"email": email, "purpose": "reset"}, secret, expires_hours=1)
