"""Database models for Kvittoanalys."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, deferred, relationship


class Base(DeclarativeBase):
    pass


def _new_uuid() -> str:
    return str(uuid.uuid4())


class User(Base):
    """Application user with role-based access."""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password_hash = Column(String(255), nullable=False)
    display_name = Column(String(100), nullable=True)
    role = Column(String(20), nullable=False, default="user")  # admin, superuser, user
    city = Column(String(100), nullable=True)  # onboarding city for campaigns
    is_verified = Column(Boolean, default=False)    # email verified
    is_approved = Column(Boolean, default=False)    # admin approved (if no SMTP)
    is_active = Column(Boolean, default=True)       # can be deactivated
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    documents = relationship("Document", back_populates="owner")
    suggestions = relationship("CategorySuggestion", back_populates="user", foreign_keys="[CategorySuggestion.user_id]")


class CategorySuggestion(Base):
    """Category change suggestion from non-admin users."""
    __tablename__ = "category_suggestions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    description = Column(Text, nullable=False)          # product description
    current_category = Column(String(100), nullable=True)
    suggested_category = Column(String(100), nullable=False)
    reason = Column(Text, nullable=True)
    status = Column(String(20), default="pending")      # pending, approved, rejected
    reviewed_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", foreign_keys=[user_id], back_populates="suggestions")
    reviewer = relationship("User", foreign_keys=[reviewed_by])


class Vendor(Base):
    """Store/vendor with chain, format and city metadata."""
    __tablename__ = "vendors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)  # canonical vendor name
    chain = Column(String(100), nullable=True)   # ICA, Coop, Hemköp, Willys, Lidl…
    format = Column(String(100), nullable=True)  # Maxi Stormarknad, Kvantum, Supermarket, Nära…
    city = Column(String(100), nullable=True)     # Lindhagen, Liljeholmen, Hornstull…
    auto_detected = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    documents = relationship("Document", back_populates="vendor_ref", passive_deletes=True)


class Document(Base):
    __tablename__ = "documents"

    id = Column(String(36), primary_key=True, default=_new_uuid)
    filename = Column(String(255), nullable=False)
    file_extension = Column(String(10), nullable=False)
    file_size_bytes = Column(Integer, nullable=True)
    file_hash = Column(String(64), nullable=True, index=True)  # SHA-256 hex digest
    document_type = Column(String(50), nullable=True)
    analysis_type = Column(String(50), nullable=False)
    language = Column(String(20), default="swedish")

    vendor = Column(String(255), nullable=True)
    vendor_id = Column(Integer, ForeignKey("vendors.id"), nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)  # document owner
    total_amount = Column(Float, nullable=True)
    vat_amount = Column(Float, nullable=True)
    currency = Column(String(10), nullable=True)
    invoice_number = Column(String(100), nullable=True)
    ocr_number = Column(String(100), nullable=True)
    invoice_date = Column(String(50), nullable=True)
    due_date = Column(String(50), nullable=True)
    discount = Column(String(100), nullable=True)

    raw_analysis = Column(Text, nullable=True)
    query_text = Column(Text, nullable=True)

    file_preview = deferred(Column(LargeBinary, nullable=True))      # compressed JPEG preview
    file_preview_type = Column(String(20), nullable=True)   # e.g. "image/jpeg"

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    extracted_fields = relationship(
        "ExtractedField", back_populates="document", cascade="all, delete-orphan"
    )
    line_items = relationship(
        "LineItem", back_populates="document", cascade="all, delete-orphan"
    )
    vendor_ref = relationship("Vendor", back_populates="documents")
    owner = relationship("User", back_populates="documents")

    def __repr__(self) -> str:
        return f"<Document {self.id[:8]}… {self.filename}>"


class ExtractedField(Base):
    __tablename__ = "extracted_fields"

    id = Column(Integer, primary_key=True, autoincrement=True)
    document_id = Column(String(36), ForeignKey("documents.id"), nullable=False)
    field_name = Column(String(100), nullable=False)
    field_value = Column(Text, nullable=True)
    confidence = Column(Float, nullable=True)

    document = relationship("Document", back_populates="extracted_fields")


class LineItem(Base):
    __tablename__ = "line_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    document_id = Column(String(36), ForeignKey("documents.id"), nullable=False)
    description = Column(Text, nullable=True)
    quantity = Column(Float, nullable=True)
    unit = Column(String(50), nullable=True)
    unit_price = Column(Float, nullable=True)
    total_price = Column(Float, nullable=True)
    vat_rate = Column(Float, nullable=True)
    discount = Column(String(100), nullable=True)
    weight = Column(String(100), nullable=True)
    packaging = Column(String(100), nullable=True)
    category = Column(String(100), nullable=True)  # product category set by rules

    document = relationship("Document", back_populates="line_items")


class ExtractionRule(Base):
    """Editable rules for post-processing extracted data at document or line-item level."""

    __tablename__ = "extraction_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)

    # Scope: "document" or "line_item"
    scope = Column(String(20), nullable=False, default="document")

    rule_type = Column(String(50), nullable=False)
    # "vendor_normalize", "field_correction", "field_default",
    # "validation", "product_normalize", "category_assign", "unit_correction"

    # Condition: when to apply
    condition_field = Column(String(100), nullable=True)
    condition_operator = Column(String(50), nullable=True)
    condition_value = Column(Text, nullable=True)

    # Action: what to do
    target_field = Column(String(100), nullable=True)
    action = Column(String(50), nullable=True)
    action_value = Column(Text, nullable=True)

    auto_generated = Column(Boolean, default=False)
    active = Column(Boolean, default=True)
    times_applied = Column(Integer, default=0)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
