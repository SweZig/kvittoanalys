"""API routes for document and image analysis with database persistence and rules."""

import asyncio
import hashlib
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, Response, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.config import settings
from app.database.database import get_db
from app.database import crud
from app.database.models import User, Document
from app.services.document_loader import DocumentLoader
from app.services.image_analyzer import ImageAnalyzer
from app.services.structured_extractor import StructuredExtractor
from app.api.auth_routes import get_current_user, get_optional_user, require_role

router = APIRouter(prefix="/api/v1", tags=["analysis"])

loader = DocumentLoader()
analyzer = ImageAnalyzer()
extractor = StructuredExtractor()


# ── Analysis endpoints ──────────────────────────────────────────────

@router.post("/analyze")
async def analyze_document(
    file: UploadFile = File(...), prompt: str | None = Form(None),
    language: str = Form("swedish"), db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    file_path, file_size, file_hash = await _save_upload(file)
    try:
        _check_duplicate(db, file_hash, file.filename)
        content_blocks = loader.load_file(file_path)
        result, structured_data = await asyncio.gather(
            asyncio.to_thread(lambda: analyzer.analyze(content_blocks, prompt=prompt, language=language)),
            asyncio.to_thread(lambda: extractor.extract(content_blocks, language=language)),
        )
        doc = crud.save_document(
            db, filename=file.filename, file_extension=Path(file.filename).suffix.lower(),
            file_size_bytes=file_size, file_hash=file_hash, analysis_type="analyze", language=language,
            raw_analysis=result if isinstance(result, str) else str(result),
            structured_data=structured_data,
            user_id=user.id if user else None,
        )
        _save_preview(db, doc, file_path)
        return {"status": "success", "document_id": doc.id, "filename": file.filename,
                "result": result, "structured_data": structured_data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        file_path.unlink(missing_ok=True)


@router.post("/extract-text")
async def extract_text(file: UploadFile = File(...), db: Session = Depends(get_db),
                       user: User | None = Depends(get_optional_user)):
    file_path, file_size, file_hash = await _save_upload(file)
    try:
        _check_duplicate(db, file_hash, file.filename)
        content_blocks = loader.load_file(file_path)
        result = await asyncio.to_thread(analyzer.extract_text, content_blocks)
        doc = crud.save_document(
            db, filename=file.filename, file_extension=Path(file.filename).suffix.lower(),
            file_size_bytes=file_size, file_hash=file_hash, analysis_type="extract-text",
            raw_analysis=result if isinstance(result, str) else str(result),
            user_id=user.id if user else None,
        )
        _save_preview(db, doc, file_path)
        return {"status": "success", "document_id": doc.id, "filename": file.filename, "result": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        file_path.unlink(missing_ok=True)


@router.post("/describe")
async def describe_image(
    file: UploadFile = File(...), language: str = Form("swedish"), db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    file_path, file_size, file_hash = await _save_upload(file)
    try:
        _check_duplicate(db, file_hash, file.filename)
        content_blocks = loader.load_file(file_path)
        result = await asyncio.to_thread(lambda: analyzer.describe_image(content_blocks, language=language))
        doc = crud.save_document(
            db, filename=file.filename, file_extension=Path(file.filename).suffix.lower(),
            file_size_bytes=file_size, file_hash=file_hash, analysis_type="describe", language=language,
            raw_analysis=result if isinstance(result, str) else str(result),
            user_id=user.id if user else None,
        )
        _save_preview(db, doc, file_path)
        return {"status": "success", "document_id": doc.id, "filename": file.filename, "result": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        file_path.unlink(missing_ok=True)


@router.post("/query")
async def custom_query(
    file: UploadFile = File(...), query: str = Form(...),
    language: str = Form("swedish"), db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    file_path, file_size, file_hash = await _save_upload(file)
    try:
        _check_duplicate(db, file_hash, file.filename)
        content_blocks = loader.load_file(file_path)
        result = analyzer.custom_query(content_blocks, query=query, language=language)
        doc = crud.save_document(
            db, filename=file.filename, file_extension=Path(file.filename).suffix.lower(),
            file_size_bytes=file_size, file_hash=file_hash, analysis_type="query", language=language,
            raw_analysis=result if isinstance(result, str) else str(result), query_text=query,
            user_id=user.id if user else None,
        )
        _save_preview(db, doc, file_path)
        return {"status": "success", "document_id": doc.id, "filename": file.filename, "query": query, "result": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        file_path.unlink(missing_ok=True)


@router.post("/extract-structured")
async def extract_structured(
    file: UploadFile = File(...), language: str = Form("swedish"), db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    file_path, file_size, file_hash = await _save_upload(file)
    try:
        _check_duplicate(db, file_hash, file.filename)
        content_blocks = loader.load_file(file_path)
        structured_data = await asyncio.to_thread(lambda: extractor.extract(content_blocks, language=language))
        doc = crud.save_document(
            db, filename=file.filename, file_extension=Path(file.filename).suffix.lower(),
            file_size_bytes=file_size, file_hash=file_hash, analysis_type="extract-structured", language=language,
            raw_analysis=str(structured_data), structured_data=structured_data,
            user_id=user.id if user else None,
        )
        _save_preview(db, doc, file_path)
        return {"status": "success", "document_id": doc.id, "filename": file.filename, "structured_data": structured_data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        file_path.unlink(missing_ok=True)


# ── Database CRUD ───────────────────────────────────────────────────

@router.get("/documents", tags=["database"])
async def list_documents(
    skip: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200),
    document_type: str | None = Query(None), vendor: str | None = Query(None),
    search: str | None = Query(None), filter_user_id: int | None = Query(None),
    db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    # Non-admin users only see their own documents
    user_id_filter = None
    if user and user.role != "admin":
        user_id_filter = user.id
    elif user and user.role == "admin" and filter_user_id:
        user_id_filter = filter_user_id
    docs = crud.list_documents(db, skip=skip, limit=limit, document_type=document_type,
                               vendor=vendor, search=search, user_id=user_id_filter)
    total = crud.count_documents(db, document_type=document_type, vendor=vendor,
                                 search=search, user_id=user_id_filter)
    return {"status": "success", "total": total, "skip": skip, "limit": limit, "documents": [_doc_summary(d) for d in docs]}


@router.get("/documents/stats", tags=["database"])
async def document_stats(db: Session = Depends(get_db),
                         user: User | None = Depends(get_optional_user)):
    user_id_filter = user.id if user and user.role != "admin" else None
    return {"status": "success", "stats": crud.get_stats(db, user_id=user_id_filter)}


@router.get("/documents/categories", tags=["analytics"])
async def category_stats(
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    user_id_filter = user.id if user and user.role != "admin" else None
    categories = crud.get_category_stats(db, date_from=date_from, date_to=date_to, user_id=user_id_filter)
    return {"status": "success", "categories": categories}


@router.get("/documents/categories/timeline", tags=["analytics"])
async def category_timeline(
    period: str = Query("month", regex="^(week|month|quarter|year)$"),
    db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    user_id_filter = user.id if user and user.role != "admin" else None
    data = crud.get_category_timeline(db, period=period, user_id=user_id_filter)
    return {"status": "success", "periods": data}


@router.get("/documents/products", tags=["analytics"])
async def product_list(
    category: str | None = Query(None),
    vendor: str | None = Query(None),
    search: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(0, ge=0, le=5000),
    db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    user_id_filter = user.id if user and user.role != "admin" else None
    result = crud.get_products(db, category=category, vendor=vendor, search=search,
                               skip=skip, limit=limit, user_id=user_id_filter,
                               date_from=date_from, date_to=date_to)
    return {"status": "success", **result}


@router.get("/documents/products/price-history", tags=["analytics"])
async def product_price_history(
    description: str = Query(...),
    db: Session = Depends(get_db),
):
    history = crud.get_product_price_history(db, description)
    return {"status": "success", "description": description, "history": history}


@router.get("/documents/products/documents", tags=["analytics"])
async def product_documents(
    description: str = Query(...),
    db: Session = Depends(get_db),
):
    docs = crud.get_product_documents(db, description)
    return {"status": "success", "description": description, "documents": docs}


@router.get("/documents/products/vendor-compare", tags=["analytics"])
async def vendor_price_comparison(
    search: str | None = Query(None),
    category: str | None = Query(None),
    vendor: str | None = Query(None),
    min_vendors: int = Query(2, ge=2),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    user_id_filter = user.id if user and user.role != "admin" else None
    result = crud.get_vendor_price_comparison(
        db, search=search, category=category, vendor=vendor,
        user_id=user_id_filter,
        min_vendors=min_vendors, skip=skip, limit=limit,
    )
    return {"status": "success", **result}


@router.get("/documents/products/price-trends", tags=["analytics"])
async def price_trends(
    search: str | None = Query(None),
    category: str | None = Query(None),
    vendor: str | None = Query(None),
    top_n: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    user_id_filter = user.id if user and user.role != "admin" else None
    result = crud.get_price_trends(db, search=search, category=category, vendor=vendor,
                                    user_id=user_id_filter, top_n=top_n)
    return {"status": "success", **result}


@router.get("/documents/user-counts", tags=["database"])
async def get_document_user_counts(
    db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Get document count per user. Admin only."""
    counts = crud.get_user_document_counts(db)
    return {"status": "success", "counts": counts}


@router.delete("/documents", tags=["database"])
async def delete_documents_bulk(
    user_id: int | None = Query(None, description="Delete for specific user, or all if omitted"),
    db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Delete all documents for a user (or all documents). Admin only."""
    count = crud.delete_documents_by_user(db, user_id=user_id)
    return {"status": "success", "deleted": count}


@router.get("/documents/{document_id}", tags=["database"])
async def get_document(document_id: str, db: Session = Depends(get_db)):
    doc = crud.get_document(db, document_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return {"status": "success", "document": _doc_detail(doc)}


@router.get("/documents/{document_id}/preview", tags=["database"])
async def get_document_preview(document_id: str, db: Session = Depends(get_db)):
    """Return the stored preview image for a document."""
    doc = crud.get_document(db, document_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    if not doc.file_preview:
        raise HTTPException(status_code=404, detail="No preview available")
    return Response(
        content=doc.file_preview,
        media_type=doc.file_preview_type or "image/jpeg",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@router.delete("/documents/{document_id}", tags=["database"])
async def delete_document(document_id: str, db: Session = Depends(get_db)):
    if not crud.delete_document(db, document_id):
        raise HTTPException(status_code=404, detail="Document not found")
    return {"status": "success", "message": "Document deleted"}


@router.post("/documents/manual", tags=["database"])
async def manual_save(
    file: UploadFile = File(...), document_type: str = Form("other"),
    vendor: str | None = Form(None), notes: str | None = Form(None),
    language: str = Form("swedish"), extract: bool = Form(True),
    db: Session = Depends(get_db),
):
    file_path, file_size, file_hash = await _save_upload(file)
    try:
        _check_duplicate(db, file_hash, file.filename)
        structured_data: dict[str, Any] | None = None
        if extract:
            content_blocks = loader.load_file(file_path)
            structured_data = await asyncio.to_thread(lambda: extractor.extract(content_blocks, language=language))
        if structured_data is None:
            structured_data = {}
        if vendor: structured_data["vendor"] = vendor
        if document_type: structured_data["document_type"] = document_type
        if notes: structured_data["free_text"] = notes
        doc = crud.save_document(
            db, filename=file.filename, file_extension=Path(file.filename).suffix.lower(),
            file_size_bytes=file_size, file_hash=file_hash, analysis_type="manual", language=language,
            structured_data=structured_data,
        )
        _save_preview(db, doc, file_path)
        return {"status": "success", "document_id": doc.id, "filename": file.filename, "structured_data": structured_data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        file_path.unlink(missing_ok=True)


# ── Line item / product category updates ────────────────────────────

class LineItemCategoryUpdate(BaseModel):
    category: str
    create_rule: bool = True


class LineItemUpdate(BaseModel):
    description: str | None = None
    quantity: float | None = None
    unit: str | None = None
    unit_price: float | None = None
    total_price: float | None = None
    vat_rate: float | None = None
    discount: str | None = None
    weight: float | None = None
    packaging: str | None = None
    category: str | None = None


class DocumentFieldsUpdate(BaseModel):
    vendor: str | None = None
    total_amount: float | None = None
    vat_amount: float | None = None
    currency: str | None = None
    invoice_number: str | None = None
    ocr_number: str | None = None
    invoice_date: str | None = None
    due_date: str | None = None
    document_type: str | None = None
    discount: str | None = None


class ProductCategoryUpdate(BaseModel):
    description: str
    category: str
    create_rule: bool = True


@router.put("/line-items/{line_item_id}/category", tags=["database"])
async def update_line_item_category(
    line_item_id: int, data: LineItemCategoryUpdate, db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Update a single line item's category and optionally create a rule. Admin only."""
    result = crud.update_line_item_category(
        db, line_item_id=line_item_id,
        category=data.category, should_create_rule=data.create_rule,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Line item not found")
    return {"status": "success", **result}


@router.put("/products/category", tags=["database"])
async def update_product_category(
    data: ProductCategoryUpdate, db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Update category for ALL line items matching a description + create rule. Admin only."""
    result = crud.update_product_category(
        db, description=data.description,
        category=data.category, should_create_rule=data.create_rule,
    )
    return {"status": "success", **result}


@router.post("/categories/migrate", tags=["database"])
async def migrate_categories(
    db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Migrate all categories to the new structure and recategorize. Admin only."""
    result = crud.migrate_categories(db)
    return {"status": "success", **result}


@router.put("/line-items/{line_item_id}", tags=["database"])
async def update_line_item(
    line_item_id: int, data: LineItemUpdate, db: Session = Depends(get_db),
    user: User = Depends(require_role("admin", "superuser")),
):
    """Update any editable fields on a line item."""
    updates = data.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="Inga fält att uppdatera")
    result = crud.update_line_item(db, line_item_id=line_item_id, updates=updates)
    if not result:
        raise HTTPException(status_code=404, detail="Raden hittades inte")
    return {"status": "success", **result}


@router.put("/documents/{document_id}/fields", tags=["database"])
async def update_document_fields(
    document_id: str, data: DocumentFieldsUpdate, db: Session = Depends(get_db),
    user: User = Depends(require_role("admin", "superuser")),
):
    """Update editable document-level fields (vendor, amounts, dates, etc.)."""
    updates = data.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="Inga fält att uppdatera")
    result = crud.update_document_fields(db, document_id=document_id, updates=updates)
    if not result:
        raise HTTPException(status_code=404, detail="Dokumentet hittades inte")
    return {"status": "success", **result}


@router.post("/line-items/cleanup-discounts", tags=["database"])
async def cleanup_discounts(
    db: Session = Depends(get_db),
    user: User = Depends(require_role("admin", "superuser")),
):
    """Retroactively link orphan discount rows to their products."""
    result = crud.cleanup_discount_rows(db)
    return {"status": "success", **result}


class ProductMerge(BaseModel):
    source_descriptions: list[str]
    target_description: str
    target_category: str | None = None


@router.put("/products/merge", tags=["database"])
async def merge_products(
    data: ProductMerge, db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Merge multiple product descriptions into one canonical name. Admin only."""
    result = crud.merge_products(
        db, source_descriptions=data.source_descriptions,
        target_description=data.target_description,
        target_category=data.target_category,
    )
    return {"status": "success", **result}


class DiscountLink(BaseModel):
    discount_description: str
    product_description: str


@router.put("/products/link-discount", tags=["database"])
async def link_discount(
    data: DiscountLink, db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Link a discount row to a product row. Admin only."""
    result = crud.link_discount_to_product(
        db, discount_description=data.discount_description,
        product_description=data.product_description,
    )
    return {"status": "success", **result}


# ── Product groups ───────────────────────────────────────────────────

@router.get("/products/groups", tags=["analytics"])
async def get_product_groups(
    user_id: int | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    """Get product group summaries."""
    uid = user_id if user_id is not None else (user.id if user and user.role != "admin" else None)
    return crud.get_product_groups_summary(db, user_id=uid, date_from=date_from, date_to=date_to)


@router.post("/products/groups/auto-detect", tags=["database"])
async def auto_detect_groups(
    db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Auto-detect product groups based on common prefixes. Returns suggestions."""
    groups = crud.auto_detect_product_groups(db)
    return {"groups": {k: v for k, v in sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)}}


@router.post("/products/groups/apply", tags=["database"])
async def apply_groups(
    data: dict, db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Apply product group assignments. Body: {"groups": {"Oxfilé": ["Oxfilé skivad", ...], ...}}"""
    groups = data.get("groups", {})
    result = crud.apply_product_groups(db, groups)
    return {"status": "success", **result}


class ProductGroupSet(BaseModel):
    description: str
    group_name: str | None = None


@router.put("/products/groups/set", tags=["database"])
async def set_group(
    data: ProductGroupSet, db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Set product_group for a specific product."""
    count = crud.set_product_group(db, data.description, data.group_name)
    return {"status": "success", "line_items_updated": count}


class LineItemSplit(BaseModel):
    new_description: str
    new_quantity: float | None = None
    new_total_price: float | None = None


@router.post("/line-items/{line_item_id}/split", tags=["database"])
async def split_line_item(
    line_item_id: int, data: LineItemSplit, db: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
):
    """Split a line item into two: original keeps its description, a new row
    is created with the new description. Quantities/prices are adjusted."""
    result = crud.split_line_item(
        db, line_item_id=line_item_id,
        new_description=data.new_description,
        new_quantity=data.new_quantity,
        new_total_price=data.new_total_price,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Line item not found")
    return {"status": "success", **result}


# ── Rules ───────────────────────────────────────────────────────────

class RuleCreate(BaseModel):
    name: str
    description: str | None = None
    scope: str = "document"
    rule_type: str = "field_correction"
    condition_field: str | None = None
    condition_operator: str = "contains"
    condition_value: str | None = None
    target_field: str | None = None
    action: str = "set"
    action_value: str | None = None
    active: bool = True


class RuleUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    scope: str | None = None
    rule_type: str | None = None
    condition_field: str | None = None
    condition_operator: str | None = None
    condition_value: str | None = None
    target_field: str | None = None
    action: str | None = None
    action_value: str | None = None
    active: bool | None = None


@router.get("/rules", tags=["rules"])
async def list_rules(
    active_only: bool = Query(False),
    scope: str | None = Query(None),
    db: Session = Depends(get_db),
    user: User = Depends(require_role("admin", "superuser")),
):
    rules = crud.list_rules(db, active_only=active_only, scope=scope)
    return {"status": "success", "total": len(rules), "rules": [_rule_dict(r) for r in rules]}


@router.get("/rules/{rule_id}", tags=["rules"])
async def get_rule(rule_id: int, db: Session = Depends(get_db),
                   user: User = Depends(require_role("admin", "superuser"))):
    rule = crud.get_rule(db, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"status": "success", "rule": _rule_dict(rule)}


@router.post("/rules", tags=["rules"])
async def create_rule(rule_data: RuleCreate, db: Session = Depends(get_db),
                      user: User = Depends(require_role("admin"))):
    rule = crud.create_rule(db, **rule_data.model_dump())
    return {"status": "success", "rule": _rule_dict(rule)}


@router.put("/rules/{rule_id}", tags=["rules"])
async def update_rule(rule_id: int, rule_data: RuleUpdate, db: Session = Depends(get_db),
                      user: User = Depends(require_role("admin"))):
    updates = {k: v for k, v in rule_data.model_dump().items() if v is not None}
    rule = crud.update_rule(db, rule_id, updates)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"status": "success", "rule": _rule_dict(rule)}


@router.delete("/rules/{rule_id}", tags=["rules"])
async def delete_rule(rule_id: int, db: Session = Depends(get_db),
                      user: User = Depends(require_role("admin"))):
    if not crud.delete_rule(db, rule_id):
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"status": "success", "message": "Rule deleted"}


@router.post("/rules/apply-all", tags=["rules"])
async def apply_rules_to_all(db: Session = Depends(get_db)):
    result = crud.apply_rules_to_all_documents(db)
    return {"status": "success", **result}


# ── Vendors ─────────────────────────────────────────────────────────

@router.get("/vendors", tags=["vendors"])
async def list_vendors(
    db: Session = Depends(get_db),
    user: User | None = Depends(get_optional_user),
):
    user_id_filter = user.id if user and user.role != "admin" else None
    vendors = crud.list_vendors(db, user_id=user_id_filter)
    return {"status": "success", "vendors": vendors}


class VendorMerge(BaseModel):
    source_ids: list[int]
    target_id: int


@router.put("/vendors/merge", tags=["vendors"])
async def merge_vendors(data: VendorMerge, db: Session = Depends(get_db)):
    result = crud.merge_vendors(db, source_ids=data.source_ids, target_id=data.target_id)
    return {"status": "success", **result}


class VendorUpdate(BaseModel):
    name: str | None = None
    chain: str | None = None
    format: str | None = None
    city: str | None = None


@router.put("/vendors/{vendor_id}", tags=["vendors"])
async def update_vendor(vendor_id: int, data: VendorUpdate, db: Session = Depends(get_db)):
    # Only pass fields that were explicitly included in the request body
    updates = {k: v for k, v in data.model_dump().items() if k in data.model_fields_set}
    vendor = crud.update_vendor(db, vendor_id, **updates)
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    return {"status": "success", "vendor": {
        "id": vendor.id, "name": vendor.name,
        "chain": vendor.chain, "format": vendor.format, "city": vendor.city,
    }}


@router.get("/vendors/{vendor_id}/detect", tags=["vendors"])
async def detect_vendor_info(vendor_id: int, db: Session = Depends(get_db)):
    """Re-detect chain/format/city from vendor name."""
    from app.database.models import Vendor
    vendor = db.query(Vendor).filter(Vendor.id == vendor_id).first()
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    info = crud.detect_vendor_info(vendor.name)
    return {"status": "success", "detected": info}


# ── Categorizer ─────────────────────────────────────────────────────

@router.post("/categorizer/download", tags=["categorizer"])
async def download_food_database():
    """Download/update the Livsmedelsverket food database cache (fast, ~30 sec)."""
    from app.services.categorizer import _download_food_database, _cache_path
    try:
        foods = _download_food_database()
        return {
            "status": "success",
            "foods_count": len(foods),
            "cache_path": str(_cache_path),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/categorizer/enrich", tags=["categorizer"])
async def enrich_food_database():
    """Enrich food database with Livsmedelsverket Huvudgrupp (slow, ~20 min).
    Replaces name-based group estimates with official categories."""
    from app.services.categorizer import enrich_cache_with_groups
    try:
        count = enrich_cache_with_groups()
        return {"status": "success", "enriched_count": count}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categorizer/status", tags=["categorizer"])
async def categorizer_status():
    """Check status of the food database cache."""
    from app.services.categorizer import _cache_path, _food_cache
    import time
    cached_on_disk = _cache_path.exists()
    cache_age_days = None
    foods_count = 0
    if cached_on_disk:
        cache_age_days = round((time.time() - _cache_path.stat().st_mtime) / 86400, 1)
        try:
            import json
            with open(_cache_path, "r") as f:
                foods_count = len(json.load(f))
        except Exception:
            pass
    return {
        "status": "success",
        "cached_on_disk": cached_on_disk,
        "in_memory": _food_cache is not None,
        "foods_count": foods_count,
        "cache_age_days": cache_age_days,
        "cache_path": str(_cache_path),
    }


# ── Helpers ─────────────────────────────────────────────────────────

async def _save_upload(file: UploadFile) -> tuple[Path, int, str]:
    """Save upload and return (path, size, sha256_hash)."""
    suffix = Path(file.filename).suffix.lower()
    allowed = settings.supported_image_types | settings.supported_document_types
    if suffix not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {suffix}")
    content = await file.read()
    file_size = len(content)
    if file_size > settings.max_file_size_bytes:
        raise HTTPException(status_code=400, detail=f"File too large. Max: {settings.max_file_size_mb} MB")
    file_hash = hashlib.sha256(content).hexdigest()
    file_path = settings.upload_path / f"{uuid.uuid4()}{suffix}"
    file_path.write_bytes(content)
    return file_path, file_size, file_hash


def _check_duplicate(db: Session, file_hash: str, filename: str):
    """Raise 409 if document already exists."""
    existing = crud.check_duplicate(db, file_hash)
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Dokumentet '{filename}' finns redan (uppladdad som '{existing.filename}')",
        )


_PREVIEW_MAX_DIM = 1200
_PREVIEW_QUALITY = 75


def _generate_preview(file_path: Path) -> tuple[bytes, str] | None:
    """Generate a compressed JPEG preview from an image or PDF file.
    Returns (jpeg_bytes, mime_type) or None if unsupported."""
    suffix = file_path.suffix.lower()
    try:
        from PIL import Image, ImageEnhance
        import io

        img = None

        if suffix in (".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tiff", ".tif"):
            img = Image.open(file_path)

        elif suffix == ".pdf":
            try:
                import fitz  # PyMuPDF
                pdf = fitz.open(str(file_path))
                page = pdf[0]
                # Render at 150 DPI for good quality without huge size
                mat = fitz.Matrix(150 / 72, 150 / 72)
                pix = page.get_pixmap(matrix=mat)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                pdf.close()
            except ImportError:
                return None  # PyMuPDF not installed

        if img is None:
            return None

        # Convert to RGB
        if img.mode in ("RGBA", "P", "LA"):
            bg = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode in ("RGBA", "LA"):
                bg.paste(img, mask=img.split()[-1])
            else:
                bg.paste(img)
            img = bg
        elif img.mode != "RGB":
            img = img.convert("RGB")

        # Resize if needed
        w, h = img.size
        if w > _PREVIEW_MAX_DIM or h > _PREVIEW_MAX_DIM:
            ratio = min(_PREVIEW_MAX_DIM / w, _PREVIEW_MAX_DIM / h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)

        # Compress to JPEG
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=_PREVIEW_QUALITY, optimize=True)
        return buf.getvalue(), "image/jpeg"

    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("Preview generation failed: %s", e)
        return None


def _save_preview(db: Session, doc: Document, file_path: Path) -> None:
    """Generate and save a preview image on the document."""
    preview = _generate_preview(file_path)
    if preview:
        data, mime = preview
        doc.file_preview = data
        doc.file_preview_type = mime
        db.commit()


def _doc_summary(doc) -> dict[str, Any]:
    return {
        "id": doc.id, "filename": doc.filename, "document_type": doc.document_type,
        "vendor": doc.vendor, "total_amount": doc.total_amount, "currency": doc.currency,
        "invoice_number": doc.invoice_number, "invoice_date": doc.invoice_date,
        "analysis_type": doc.analysis_type,
        "created_at": doc.created_at.isoformat() if doc.created_at else None,
        "user_id": doc.user_id,
        "uploaded_by": doc.owner.display_name if doc.owner else None,
    }

def _doc_detail(doc) -> dict[str, Any]:
    return {
        **_doc_summary(doc),
        "file_extension": doc.file_extension, "file_size_bytes": doc.file_size_bytes,
        "language": doc.language, "ocr_number": doc.ocr_number, "due_date": doc.due_date,
        "vat_amount": doc.vat_amount, "discount": doc.discount,
        "raw_analysis": doc.raw_analysis, "query_text": doc.query_text,
        "has_preview": doc.file_preview_type is not None,
        "updated_at": doc.updated_at.isoformat() if doc.updated_at else None,
        "extracted_fields": [
            {"field_name": ef.field_name, "field_value": ef.field_value, "confidence": ef.confidence}
            for ef in doc.extracted_fields
        ],
        "line_items": [
            {"id": li.id, "description": li.description, "quantity": li.quantity, "unit": li.unit,
             "unit_price": li.unit_price, "total_price": li.total_price, "vat_rate": li.vat_rate,
             "discount": li.discount, "weight": li.weight, "packaging": li.packaging,
             "category": li.category}
            for li in doc.line_items
        ],
    }


def _rule_dict(rule) -> dict[str, Any]:
    return {
        "id": rule.id, "name": rule.name, "description": rule.description,
        "scope": rule.scope, "rule_type": rule.rule_type,
        "condition_field": rule.condition_field, "condition_operator": rule.condition_operator,
        "condition_value": rule.condition_value,
        "target_field": rule.target_field, "action": rule.action, "action_value": rule.action_value,
        "auto_generated": rule.auto_generated, "active": rule.active,
        "times_applied": rule.times_applied,
        "created_at": rule.created_at.isoformat() if rule.created_at else None,
        "updated_at": rule.updated_at.isoformat() if rule.updated_at else None,
    }


# ── Campaigns (integrated matpriskollen) ─────────────────────────────

from app.services.campaign_service import (
    fetch_campaigns as _fetch_campaigns,
    get_cities as _get_cities,
    resolve_coordinates as _resolve_coords,
)
from app.services.ica_campaign_service import (
    fetch_ica_campaigns as _fetch_ica_campaigns,
    discover_ica_stores as _discover_ica_stores,
    check_ica_health as _check_ica_health,
)
import asyncio as _asyncio
import time as _time


@router.get("/campaigns/status", tags=["campaigns"])
async def campaign_status(
    city: str | None = Query(None, description="City to check (default: stockholm)"),
    user: User | None = Depends(get_optional_user),
):
    """Check health of campaign sources (matpriskollen + ICA direct)."""
    import httpx
    city = city or (user.city if user else None) or "stockholm"
    coords = _resolve_coords(city, None, None)
    result = {"city": city, "matpriskollen": {"status": "unknown"}, "ica_direct": {"status": "unknown"}}

    # Check matpriskollen
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get("https://matpriskollen.se/api/v1/stores",
                                 params={"lat": coords[0], "lon": coords[1]} if coords else {})
            r.raise_for_status()
            stores = r.json()
            result["matpriskollen"] = {
                "status": "green",
                "stores_found": len(stores),
                "response_ms": int(r.elapsed.total_seconds() * 1000),
            }
    except Exception as e:
        result["matpriskollen"] = {"status": "red", "error": str(e)[:100]}

    # Check ICA direct
    ica_store_id = None
    if user and user.ica_store_ids:
        import json
        try:
            saved = json.loads(user.ica_store_ids)
            ica_store_id = next((s["id"] for s in saved if s.get("id")), None)
        except Exception:
            pass

    if ica_store_id:
        try:
            health = await _check_ica_health(ica_store_id)
            status = health.get("status", "unknown")
            result["ica_direct"] = {
                "status": "green" if status == "ok" else ("amber" if status == "degraded" else "red"),
                "store_id": ica_store_id,
                "categories_found": health.get("categories_found", 0),
            }
        except Exception as e:
            result["ica_direct"] = {"status": "red", "store_id": ica_store_id, "error": str(e)[:100]}
    else:
        result["ica_direct"] = {"status": "amber", "reason": "Inget ICA butiks-ID sparat"}

    return result


@router.get("/campaigns/ica-stores", tags=["campaigns"])
async def discover_ica_stores(
    city: str | None = Query(None),
    lat: float | None = Query(None),
    lon: float | None = Query(None),
    max_distance_km: float = Query(10.0),
    save: bool = Query(True, description="Save discovered stores to user profile"),
    user: User = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    """Discover ICA stores near a city/coordinates."""
    coords = _resolve_coords(city, lat, lon)
    if not coords:
        raise HTTPException(status_code=400, detail="Ange city eller lat+lon.")

    stores = await _discover_ica_stores(coords[0], coords[1], max_distance_km, city=city)

    # Auto-save to user profile
    if save and user and stores:
        import json
        user.ica_store_ids = json.dumps(stores, ensure_ascii=False)
        if city and not user.city:
            user.city = city
        db.commit()

    return {"stores": stores, "saved": save and user is not None}


@router.get("/campaigns", tags=["campaigns"])
async def get_campaigns(
    city: str | None = Query(None),
    lat: float | None = Query(None),
    lon: float | None = Query(None),
    max_distance_km: float = Query(10.0),
    max_stores: int = Query(30),
    chain: str | None = Query(None, description="Filter by chain name"),
    match_products: bool = Query(False, description="Cross-reference with purchased products"),
    ica_store_id: str | None = Query(None, description="ICA store ID for direct scraping"),
    user: User | None = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    """Fetch current campaigns with ICA direct scraping + matpriskollen fallback."""
    t0 = _time.monotonic()
    coords = _resolve_coords(city, lat, lon)
    if not coords:
        raise HTTPException(status_code=400, detail="Ange city eller lat+lon. Orten hittades inte.")

    resolved_lat, resolved_lon = coords

    # ── Resolve ICA store IDs: explicit param > user profile (city match) > auto-discover ──
    ica_ids_to_try: list[str] = []
    if ica_store_id:
        ica_ids_to_try = [ica_store_id]
    elif user:
        import json as _json
        request_city = (city or "").lower().strip()
        user_city = (user.city or "").lower().strip()

        # Only use saved stores if they match the requested city
        if user.ica_store_ids and request_city and request_city == user_city:
            try:
                saved = _json.loads(user.ica_store_ids)
                ica_ids_to_try = [s["id"] for s in saved if s.get("id")]
            except Exception:
                pass

        # If no match — auto-discover for this city inline (fast: ~1 request to ica.se)
        if not ica_ids_to_try and request_city:
            try:
                stores = await _discover_ica_stores(
                    resolved_lat, resolved_lon, max_distance_km, city=city,
                )
                ica_ids_to_try = [s["id"] for s in stores if s.get("id")]
            except Exception:
                pass

    # ── PARALLEL: Matpriskollen + ICA Direct ──
    async def _do_matpriskollen():
        return await _fetch_campaigns(resolved_lat, resolved_lon, max_distance_km, max_stores)

    async def _do_ica_direct():
        if not ica_ids_to_try:
            return None
        for sid in ica_ids_to_try[:2]:  # Max 2 attempts
            try:
                result = await _fetch_ica_campaigns(
                    store_id=sid,
                    lat=resolved_lat,
                    lon=resolved_lon,
                    max_distance_km=max_distance_km,
                    fallback_enabled=False,  # Skip redundant matpriskollen fallback
                )
                if result.get("source") == "ica_direct" and result.get("offers"):
                    result["_store_id"] = sid
                    return result
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning("ICA direct failed for %s: %s", sid, e)
        return None

    try:
        mpk_data, ica_data = await _asyncio.gather(
            _do_matpriskollen(),
            _do_ica_direct(),
            return_exceptions=False,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Kunde inte hämta kampanjer: {e}")

    data = mpk_data
    data["city"] = city.capitalize() if city else f"{resolved_lat},{resolved_lon}"

    # ── Merge ICA direct data if available ──
    if ica_data and ica_data.get("offers"):
        non_ica_chains = [c for c in data.get("chains", []) if "ica" not in c["chain"].lower()]
        ica_chain = {
            "chain": "ICA",
            "stores": [f"ICA (butik {ica_data.get('_store_id', '?')})"],
            "total_offers": len(ica_data["offers"]),
            "offers": ica_data["offers"],
            "source": "ica_direct",
        }
        data["chains"] = [ica_chain] + non_ica_chains
        data["total_offers"] = sum(c.get("total_offers", len(c.get("offers", []))) for c in data["chains"])
        data["ica_source"] = "ica_direct"
        data["ica_store_id"] = ica_data.get("_store_id")
    elif ica_ids_to_try:
        data["ica_source"] = "matpriskollen"
    

    # Filter by chain if requested
    if chain:
        chain_lower = chain.lower()
        data["chains"] = [c for c in data.get("chains", []) if chain_lower in c["chain"].lower()]
        data["total_offers"] = sum(c["total_offers"] for c in data["chains"])

    # Cross-reference with purchased products
    if match_products:
        from app.database.models import LineItem
        from sqlalchemy import func as sqlfunc
        import statistics

        # Get user context for filtering
        user = None
        try:
            from app.api.auth_routes import get_optional_user
            # Try to get user from request state if available
            pass
        except Exception:
            pass

        base_q = db.query(LineItem).filter(LineItem.description.isnot(None))

        purchased = {
            row[0].lower()
            for row in base_q.with_entities(LineItem.description).distinct().all()
        }

        # Build median price lookup: description_lower -> median unit_price
        price_rows = (
            base_q
            .filter(LineItem.unit_price.isnot(None), LineItem.unit_price > 0)
            .with_entities(LineItem.description, LineItem.unit_price)
            .all()
        )
        from collections import defaultdict
        _price_map: dict[str, list[float]] = defaultdict(list)
        for desc, price in price_rows:
            _price_map[desc.lower()].append(float(price))
        median_prices = {k: statistics.median(v) for k, v in _price_map.items()}

        for ch in data.get("chains", []):
            for offer in ch.get("offers", []):
                product_name = (offer.get("product") or {}).get("name", "").lower()
                brand = (offer.get("product") or {}).get("brand", "").lower()
                matched_desc = None
                for desc in purchased:
                    if product_name and (product_name in desc or desc in product_name or
                                          (brand and brand in desc and _word_overlap(product_name, desc))):
                        matched_desc = desc
                        break
                offer["matches_purchased"] = matched_desc is not None
                # Add user's median price for matched products
                if matched_desc and matched_desc in median_prices:
                    offer["user_median_price"] = round(median_prices[matched_desc], 2)

    data["timing_ms"] = int((_time.monotonic() - t0) * 1000)
    return data


@router.get("/campaigns/cities", tags=["campaigns"])
async def get_campaign_cities():
    """Return all available cities with coordinates."""
    return _get_cities()


def _word_overlap(a: str, b: str) -> bool:
    """Check if significant words overlap between two strings."""
    stop = {"och", "med", "för", "den", "det", "av", "på", "st", "kg", "g", "ml", "l", "cl"}
    words_a = {w for w in a.lower().split() if len(w) > 2 and w not in stop}
    words_b = {w for w in b.lower().split() if len(w) > 2 and w not in stop}
    if not words_a or not words_b:
        return False
    return len(words_a & words_b) >= 1


# ── Inbound email (receive receipts via email) ──────────────────────

import json
import re
import urllib.request

def _resend_api_get(path: str) -> dict | None:
    """Call Resend API GET endpoint."""
    try:
        req = urllib.request.Request(
            f"https://api.resend.com{path}",
            headers={
                "Authorization": f"Bearer {settings.resend_api_key}",
                "User-Agent": "Kvittoanalys/1.0",
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"⚠️ Resend API GET {path} failed: {e}")
        return None


def _download_url(url: str) -> bytes | None:
    """Download file from URL, return bytes."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Kvittoanalys/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except Exception as e:
        print(f"⚠️ Download failed {url}: {e}")
        return None


def _parse_email_address(from_field: str) -> str:
    """Extract email from 'Name <email>' or plain email format."""
    match = re.search(r'<([^>]+)>', from_field)
    if match:
        return match.group(1).lower().strip()
    return from_field.lower().strip()


_INBOUND_SUPPORTED_TYPES = {
    "application/pdf": ".pdf",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
}


@router.post("/inbound-email")
async def inbound_email(request: Request, db: Session = Depends(get_db)):
    """Receive inbound email webhook from Resend."""
    try:
        event = await request.json()
    except Exception:
        return {"status": "ok"}

    if event.get("type") != "email.received":
        return {"status": "ok", "message": "ignored event type"}

    data = event.get("data", {})
    email_id = data.get("email_id")
    from_raw = data.get("from", "")
    subject = data.get("subject", "")
    attachment_meta = data.get("attachments", [])

    sender_email = _parse_email_address(from_raw)
    print(f"📨 Inbound email from {sender_email}, subject: {subject}, attachments: {len(attachment_meta)}")

    # Look up user by sender email
    user = db.query(User).filter(User.email == sender_email).first()
    if not user:
        print(f"⚠️ Inbound email from unknown sender: {sender_email} — ignoring")
        return {"status": "ok", "message": "unknown sender"}

    if not user.is_verified or not user.is_active:
        print(f"⚠️ Inbound email from unverified/inactive user: {sender_email} — ignoring")
        return {"status": "ok", "message": "user not active"}

    processed = 0
    errors = []

    # Process attachments (PDF / images)
    if attachment_meta:
        # Fetch attachment download URLs from Resend API
        att_list = _resend_api_get(f"/emails/receiving/{email_id}/attachments")
        attachments = att_list.get("data", []) if att_list else []

        for att in attachments:
            content_type = att.get("content_type", "")
            filename = att.get("filename", "attachment")
            download_url = att.get("download_url")

            if not download_url:
                continue

            # Check if supported file type
            ext = _INBOUND_SUPPORTED_TYPES.get(content_type)
            if not ext:
                print(f"  ⏭️ Skipping unsupported attachment: {filename} ({content_type})")
                continue

            # Download the file
            file_bytes = _download_url(download_url)
            if not file_bytes:
                errors.append(f"Kunde inte ladda ned {filename}")
                continue

            # Save to temp file and process
            try:
                file_hash = hashlib.sha256(file_bytes).hexdigest()
                existing = crud.check_duplicate(db, file_hash)
                if existing:
                    print(f"  ⏭️ Duplicate: {filename} (already uploaded as {existing.filename})")
                    continue

                file_path = settings.upload_path / f"{uuid.uuid4()}{ext}"
                file_path.write_bytes(file_bytes)

                content_blocks = loader.load_file(file_path)
                result, structured_data = await asyncio.gather(
                    asyncio.to_thread(lambda: analyzer.analyze(content_blocks, language="swedish")),
                    asyncio.to_thread(lambda: extractor.extract(content_blocks, language="swedish")),
                )
                doc = crud.save_document(
                    db, filename=filename, file_extension=ext,
                    file_size_bytes=len(file_bytes), file_hash=file_hash,
                    analysis_type="analyze", language="swedish",
                    raw_analysis=result if isinstance(result, str) else str(result),
                    structured_data=structured_data,
                    user_id=user.id,
                )
                _save_preview(db, doc, file_path)
                file_path.unlink(missing_ok=True)
                processed += 1
                print(f"  ✅ Processed: {filename} → document {doc.id}")
            except Exception as e:
                errors.append(f"Fel vid bearbetning av {filename}: {e}")
                print(f"  ❌ Error processing {filename}: {e}")
                if 'file_path' in dir() and file_path.exists():
                    file_path.unlink(missing_ok=True)

    # If no supported attachments, check for text receipt in email body
    if processed == 0 and not attachment_meta:
        email_data = _resend_api_get(f"/emails/receiving/{email_id}")
        if email_data:
            body_text = email_data.get("text") or ""
            body_html = email_data.get("html") or ""

            # Use text body, or strip HTML tags as fallback
            receipt_text = body_text.strip()
            if not receipt_text and body_html:
                receipt_text = re.sub(r'<[^>]+>', '', body_html).strip()

            if receipt_text and len(receipt_text) > 20:
                # Create a text-based document from the email body
                try:
                    text_bytes = receipt_text.encode("utf-8")
                    file_hash = hashlib.sha256(text_bytes).hexdigest()
                    existing = crud.check_duplicate(db, file_hash)
                    if not existing:
                        # Use structured extractor on the text content
                        content_blocks = [{"type": "text", "text": receipt_text}]
                        result, structured_data = await asyncio.gather(
                            asyncio.to_thread(lambda: analyzer.analyze(content_blocks, language="swedish")),
                            asyncio.to_thread(lambda: extractor.extract(content_blocks, language="swedish")),
                        )
                        doc = crud.save_document(
                            db, filename=f"email-kvitto-{email_id[:8]}.txt",
                            file_extension=".txt",
                            file_size_bytes=len(text_bytes), file_hash=file_hash,
                            analysis_type="analyze", language="swedish",
                            raw_analysis=result if isinstance(result, str) else str(result),
                            structured_data=structured_data,
                            user_id=user.id,
                        )
                        processed += 1
                        print(f"  ✅ Processed email body as text receipt → document {doc.id}")
                    else:
                        print(f"  ⏭️ Duplicate text receipt")
                except Exception as e:
                    errors.append(f"Fel vid bearbetning av mailtext: {e}")
                    print(f"  ❌ Error processing email body: {e}")
            else:
                print(f"  ⚠️ Email body too short or empty, nothing to process")

    print(f"📨 Inbound result: {processed} processed, {len(errors)} errors")
    return {"status": "ok", "processed": processed, "errors": errors}
