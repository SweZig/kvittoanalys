"""Application configuration loaded from environment variables."""

from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # API Keys
    anthropic_api_key: str = ""

    # App
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_debug: bool = True
    app_base_url: str = ""  # e.g. http://localhost:8000, auto-detected if empty

    # File handling
    max_file_size_mb: int = 20
    upload_dir: str = "uploads"
    output_dir: str = "outputs"

    # AI Model
    claude_model: str = "claude-sonnet-4-5-20250929"
    claude_max_tokens: int = 4096

    # Auth / JWT
    jwt_secret: str = "kvittoanalys-dev-secret-change-in-production"  # CHANGE IN PROD

    # SMTP (optional — if not set, manual approval is used)
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = ""  # defaults to smtp_user if empty

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def max_file_size_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024

    @property
    def upload_path(self) -> Path:
        path = Path(self.upload_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def output_path(self) -> Path:
        path = Path(self.output_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def supported_image_types(self) -> set[str]:
        return {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff"}

    @property
    def supported_document_types(self) -> set[str]:
        return {".pdf", ".docx", ".doc"}


settings = Settings()
