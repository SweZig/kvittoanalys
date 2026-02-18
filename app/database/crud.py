"""CRUD operations for the Kvittoanalys database."""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from pathlib import Path
from typing import Any

from sqlalchemy import func, or_, text
from sqlalchemy.orm import Session, joinedload

from app.database.models import Document, ExtractedField, ExtractionRule, LineItem, Vendor

log = logging.getLogger(__name__)

# Rules backup file — persists outside the database
RULES_BACKUP_PATH = Path("data/rules_backup.json")

# Pant is excluded from analytics views (normalized form from _fmt)
_PANT_DESCRIPTIONS = {"Pant", "Pant+"}

# One-time normalization flag
_data_normalized = False


def _fmt(text: str | None) -> str | None:
    """Capitalize first letter, lowercase rest. 'BANAN KLASS 1' → 'Banan klass 1'."""
    if not text:
        return text
    return text[0].upper() + text[1:].lower() if len(text) > 1 else text.upper()


def _ensure_normalized(db: Session) -> None:
    """Auto-normalize existing data on first access. Idempotent & fast if already done."""
    global _data_normalized
    if _data_normalized:
        return
    _data_normalized = True

    # Safe migration: add columns/tables if missing
    _safe_migrate(db, "SELECT file_hash FROM documents LIMIT 1",
                  "ALTER TABLE documents ADD COLUMN file_hash VARCHAR(64)")
    _safe_migrate(db, "SELECT 1 FROM vendors LIMIT 1",
                  """CREATE TABLE IF NOT EXISTS vendors (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      name VARCHAR(255) NOT NULL UNIQUE,
                      chain VARCHAR(100), format VARCHAR(100), city VARCHAR(100),
                      auto_detected BOOLEAN DEFAULT 1,
                      created_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")
    _safe_migrate(db, "SELECT vendor_id FROM documents LIMIT 1",
                  "ALTER TABLE documents ADD COLUMN vendor_id INTEGER REFERENCES vendors(id)")

    result = normalize_existing_data(db)
    if result["descriptions_normalized"] or result["categories_normalized"]:
        log.info("Auto-normalized existing data: %s", result)

    # Backfill vendor records for existing documents
    bf = backfill_vendors(db)
    if bf["documents_linked"]:
        log.info("Backfilled vendors: %s", bf)


def _safe_migrate(db: Session, check_sql: str, migrate_sql: str) -> None:
    """Run a migration SQL if the check SQL fails."""
    try:
        db.execute(text(check_sql))
    except Exception:
        db.rollback()
        try:
            db.execute(text(migrate_sql))
            db.commit()
        except Exception:
            db.rollback()


def _find_rule(db: Session, *, scope: str, rule_type: str, condition_field: str,
               condition_value: str, target_field: str | None = None) -> ExtractionRule | None:
    """Find a rule using Python-side case-insensitive matching (SQLite lower() can't handle Unicode)."""
    query = db.query(ExtractionRule).filter(
        ExtractionRule.scope == scope,
        ExtractionRule.rule_type == rule_type,
        ExtractionRule.condition_field == condition_field,
    )
    if target_field:
        query = query.filter(ExtractionRule.target_field == target_field)
    cv_lower = condition_value.strip().lower()
    for rule in query.all():
        if (rule.condition_value or "").strip().lower() == cv_lower:
            return rule
    return None


# ── Document CRUD ───────────────────────────────────────────────────

def check_duplicate(db: Session, file_hash: str) -> Document | None:
    """Check if a document with this file hash already exists."""
    try:
        return db.query(Document).filter(Document.file_hash == file_hash).first()
    except Exception:
        db.rollback()
        return None


def save_document(
    db: Session,
    *,
    filename: str,
    file_extension: str,
    file_size_bytes: int | None = None,
    file_hash: str | None = None,
    analysis_type: str,
    language: str = "swedish",
    raw_analysis: str | None = None,
    query_text: str | None = None,
    structured_data: dict[str, Any] | None = None,
    user_id: int | None = None,
) -> Document:
    _ensure_normalized(db)
    doc = Document(
        filename=filename,
        file_extension=file_extension,
        file_size_bytes=file_size_bytes,
        file_hash=file_hash,
        analysis_type=analysis_type,
        language=language,
        raw_analysis=raw_analysis,
        query_text=query_text,
        user_id=user_id,
    )

    if structured_data:
        _apply_structured_data(doc, structured_data, db)

    # Direct categorization (always runs, independent of rules)
    _auto_categorize_line_items(doc)

    # Apply existing active rules
    _apply_all_rules_to_document(doc, db)

    # Auto-create/link vendor
    if doc.vendor and not doc.vendor_id:
        vendor = get_or_create_vendor(db, doc.vendor)
        if vendor:
            doc.vendor_id = vendor.id

    db.add(doc)
    db.commit()
    db.refresh(doc)

    # Auto-generate rules from patterns (for future documents)
    _auto_generate_rules(doc, db)

    return doc


def _apply_structured_data(doc: Document, data: dict[str, Any], db: Session) -> None:
    direct_fields = {
        "vendor", "total_amount", "vat_amount", "currency",
        "invoice_number", "ocr_number", "invoice_date", "due_date",
        "document_type", "discount",
    }

    for field in direct_fields:
        value = data.get(field)
        if value is not None:
            setattr(doc, field, value)

    for item_data in data.get("line_items", []):
        line = LineItem(
            description=_fmt(item_data.get("description")),
            quantity=item_data.get("quantity"),
            unit=item_data.get("unit"),
            unit_price=item_data.get("unit_price"),
            total_price=item_data.get("total_price"),
            vat_rate=item_data.get("vat_rate"),
            discount=item_data.get("discount"),
            weight=item_data.get("weight"),
            packaging=item_data.get("packaging"),
            category=_fmt(item_data.get("category")),
        )
        doc.line_items.append(line)

    skip_keys = direct_fields | {"line_items"}
    for key, value in data.items():
        if key not in skip_keys and value is not None:
            ef = ExtractedField(field_name=key, field_value=str(value))
            doc.extracted_fields.append(ef)


# Categorization is handled by app.services.categorizer
# which uses Livsmedelsverket API + fuzzy matching + Claude AI fallback


def _auto_categorize_line_items(doc: Document) -> None:
    """Assign categories to line items using the categorizer service.

    Strategy:
    1. Skip items with existing category (e.g. Pant → dryck from extractor).
    2. Batch-categorize remaining via Livsmedelsverket DB + fuzzy match.
    3. AI fallback for still-uncategorized items.
    """
    from app.services.categorizer import categorize_products_batch, ai_categorize_batch

    # Collect items that need categorization
    to_categorize: list[tuple[int, str]] = []  # (index_in_line_items, description)

    for i, line in enumerate(doc.line_items):
        if line.category:
            continue
        desc = (line.description or "").strip()
        if desc:
            to_categorize.append((i, desc))

    if not to_categorize:
        return

    # Batch categorize via Livsmedelsverket + keywords
    descriptions = [desc for _, desc in to_categorize]
    results = categorize_products_batch(descriptions)

    # Apply results, collect remaining uncategorized for AI
    ai_needed: list[tuple[int, str]] = []

    for (idx, desc), category in zip(to_categorize, results):
        if category:
            doc.line_items[idx].category = _fmt(category)
        else:
            ai_needed.append((idx, desc))

    # AI fallback for remaining
    if ai_needed:
        ai_results = ai_categorize_batch(ai_needed)
        for idx, category in ai_results.items():
            if 0 <= idx < len(doc.line_items):
                doc.line_items[idx].category = _fmt(category)


def get_document(db: Session, document_id: str) -> Document | None:
    return (
        db.query(Document)
        .options(joinedload(Document.extracted_fields), joinedload(Document.line_items))
        .filter(Document.id == document_id)
        .first()
    )


def update_line_item_category(
    db: Session, *, line_item_id: int, category: str, should_create_rule: bool = True,
) -> dict[str, Any] | None:
    """Update a line item's category and optionally create a rule for future matching."""
    line = db.query(LineItem).filter(LineItem.id == line_item_id).first()
    if not line:
        return None

    old_category = line.category
    line.category = _fmt(category)
    db.commit()

    result: dict[str, Any] = {
        "line_item_id": line_item_id,
        "description": line.description,
        "old_category": old_category,
        "new_category": _fmt(category),
        "rule_created": False,
    }

    if should_create_rule and line.description:
        desc = line.description.strip()
        norm_cat = _fmt(category)

        existing = _find_rule(db, scope="line_item", rule_type="category_assign",
                              condition_field="description", condition_value=desc,
                              target_field="category")

        if existing:
            existing.action_value = norm_cat
            existing.active = True
            db.commit()
            result["rule_created"] = False
            result["rule_updated"] = True
            result["rule_id"] = existing.id
        else:
            rule = create_rule(
                db,
                name=f"Kategori '{norm_cat}' för '{desc[:40]}'",
                description=f"Manuellt korrigerad: '{desc}' → {norm_cat}",
                scope="line_item",
                rule_type="category_assign",
                condition_field="description",
                condition_operator="contains",
                condition_value=desc,
                target_field="category",
                action="set",
                action_value=norm_cat,
                auto_generated=False,
                active=True,
            )
            result["rule_created"] = True
            result["rule_id"] = rule.id

        _backup_rules_to_file(db)

    return result


def update_product_category(
    db: Session, *, description: str, category: str, should_create_rule: bool = True,
) -> dict[str, Any]:
    """Update category for ALL line items matching a description + create rule."""
    norm_desc = _fmt(description)
    norm_cat = _fmt(category)
    lines = db.query(LineItem).filter(LineItem.description == norm_desc).all()
    for line in lines:
        line.category = norm_cat
    db.commit()

    result: dict[str, Any] = {
        "description": norm_desc,
        "new_category": norm_cat,
        "items_updated": len(lines),
        "total_matching": len(lines),
        "rule_created": False,
    }

    if should_create_rule and norm_desc:
        # Deactivate any conflicting auto-generated category rules for this product
        conflicting = db.query(ExtractionRule).filter(
            ExtractionRule.scope == "line_item",
            ExtractionRule.rule_type == "category_assign",
            ExtractionRule.target_field == "category",
            ExtractionRule.auto_generated == True,
            ExtractionRule.active == True,
        ).all()
        desc_lower = norm_desc.lower()
        for r in conflicting:
            cv = (r.condition_value or "").lower()
            if cv and cv in desc_lower:
                r.active = False

        # Find or create the manual rule
        existing = _find_rule(db, scope="line_item", rule_type="category_assign",
                              condition_field="description", condition_value=norm_desc,
                              target_field="category")

        if existing:
            existing.action_value = norm_cat
            existing.action = "set"  # Ensure it's "set", not "set_if_empty"
            existing.active = True
            existing.auto_generated = False  # Promote to manual rule
            db.commit()
            result["rule_updated"] = True
            result["rule_id"] = existing.id
        else:
            rule = create_rule(
                db,
                name=f"Kategori '{norm_cat}' för '{norm_desc[:40]}'",
                description=f"Manuellt korrigerad: '{norm_desc}' → {norm_cat}",
                scope="line_item",
                rule_type="category_assign",
                condition_field="description",
                condition_operator="contains",
                condition_value=norm_desc,
                target_field="category",
                action="set",
                action_value=norm_cat,
                auto_generated=False,
                active=True,
            )
            result["rule_created"] = True
            result["rule_id"] = rule.id

        _backup_rules_to_file(db)

    return result


def migrate_categories(db: Session) -> dict[str, Any]:
    """Migrate all categories to the new structure.

    1. Rename old categories (livsmedel→skafferi, godis→snacks & godis, snacks→snacks & godis)
    2. Clear categories that need re-splitting (mejeri, kött, hygien)
    3. Re-categorize all cleared items using updated categorizer
    4. Update rules to match new categories
    """
    from app.services.categorizer import (
        CATEGORY_MIGRATION, CATEGORIES_TO_RESPLIT,
        categorize_products_batch, ai_categorize_batch,
    )

    stats = {
        "renamed": 0,
        "cleared_for_resplit": 0,
        "recategorized": 0,
        "rules_updated": 0,
        "rules_deleted": 0,
    }

    # ── Step 1: Direct renames ──
    for old_cat, new_cat in CATEGORY_MIGRATION.items():
        count = db.query(LineItem).filter(
            LineItem.category == old_cat
        ).update({LineItem.category: new_cat}, synchronize_session="fetch")
        stats["renamed"] += count
        print(f"  ✅ {old_cat} → {new_cat}: {count} rader")

    db.commit()

    # ── Step 2: Clear categories that need re-evaluation ──
    for cat in CATEGORIES_TO_RESPLIT:
        count = db.query(LineItem).filter(
            LineItem.category == cat
        ).update({LineItem.category: None}, synchronize_session="fetch")
        stats["cleared_for_resplit"] += count
        print(f"  🔄 Nollställde {cat}: {count} rader för omklassificering")

    db.commit()

    # ── Step 3: Re-categorize all items without category ──
    uncategorized = (
        db.query(LineItem)
        .filter(
            LineItem.category.is_(None),
            LineItem.description.isnot(None),
        )
        .all()
    )

    if uncategorized:
        descs = [(i, (li.description or "").strip()) for i, li in enumerate(uncategorized)]
        desc_strs = [d for _, d in descs]

        # Batch categorize
        results = categorize_products_batch(desc_strs)

        ai_needed = []
        for (idx, desc), cat in zip(descs, results):
            if cat:
                uncategorized[idx].category = _fmt(cat)
                stats["recategorized"] += 1
            elif desc:
                ai_needed.append((idx, desc))

        # AI fallback
        if ai_needed:
            print(f"  🤖 AI-kategoriserar {len(ai_needed)} okategoriserade rader…")
            ai_results = ai_categorize_batch(ai_needed)
            for idx, cat in ai_results.items():
                if 0 <= idx < len(uncategorized):
                    uncategorized[idx].category = _fmt(cat)
                    stats["recategorized"] += 1

        db.commit()

    print(f"  ✅ Omkategoriserade {stats['recategorized']} rader")

    # ── Step 4: Update rules ──
    rules = db.query(ExtractionRule).filter(
        ExtractionRule.rule_type == "category_assign",
        ExtractionRule.target_field == "category",
    ).all()

    for rule in rules:
        old_val = (rule.action_value or "").strip().lower()

        # Direct rename
        if old_val in CATEGORY_MIGRATION:
            rule.action_value = CATEGORY_MIGRATION[old_val]
            stats["rules_updated"] += 1
            continue

        # Rules for resplit categories — deactivate, they'll be wrong
        if old_val in CATEGORIES_TO_RESPLIT:
            if rule.auto_generated:
                db.delete(rule)
                stats["rules_deleted"] += 1
            else:
                rule.active = False
                stats["rules_updated"] += 1

    db.commit()

    print(f"  ✅ Regler: {stats['rules_updated']} uppdaterade, {stats['rules_deleted']} borttagna")

    return stats


def cleanup_discount_rows(db: Session) -> dict[str, Any]:
    """Retroactively link orphan discount rows in existing data.

    Finds line items that look like discounts (Rabatt:X, Willys plus:X, etc.),
    matches them to the correct product in the same document, applies the discount,
    and removes the orphan row.
    """
    import re as _re

    discount_pattern = _re.compile(
        r"^(rabatt|willys\s*plus|hemköp\s*plus|ica\s*bonus|coop\s*rabatt|"
        r"kupong|bonus|avdrag|erbjudande|kampanj|prisnedsättning|"
        r"nedsatt|kort[\s-]*rabatt|medlems[\s-]*rabatt)",
        _re.IGNORECASE,
    )
    hint_pattern = _re.compile(
        r"^(?:rabatt|willys\s*plus|hemköp\s*plus|ica\s*bonus|coop\s*rabatt|"
        r"kupong|bonus|avdrag|erbjudande|kampanj)[:\s]+(.+)",
        _re.IGNORECASE,
    )

    stats = {"linked": 0, "deleted": 0, "unmatched": 0}

    # Find all potential discount rows
    all_lines = (
        db.query(LineItem)
        .filter(LineItem.total_price < 0)
        .order_by(LineItem.document_id, LineItem.id)
        .all()
    )

    discount_rows = []
    for line in all_lines:
        desc = (line.description or "").strip()
        if discount_pattern.search(desc):
            discount_rows.append(line)

    if not discount_rows:
        return stats

    # Group by document
    from collections import defaultdict
    by_doc: dict[str, list[LineItem]] = defaultdict(list)
    for line in discount_rows:
        by_doc[line.document_id].append(line)

    for doc_id, discounts in by_doc.items():
        # Get all line items for this document
        doc_lines = (
            db.query(LineItem)
            .filter(LineItem.document_id == doc_id)
            .order_by(LineItem.id)
            .all()
        )

        for disc in discounts:
            desc = (disc.description or "").strip()
            amount = disc.total_price
            if not isinstance(amount, (int, float)) or amount >= 0:
                continue

            # Try to extract product hint
            hint_match = hint_pattern.match(desc)
            target = None

            if hint_match:
                hint = hint_match.group(1).strip().lower()
                if len(hint) >= 3:
                    # Find best matching product in same document
                    best_score = 0.0
                    for li in doc_lines:
                        if li.id == disc.id or (li.total_price or 0) < 0:
                            continue
                        li_desc = (li.description or "").lower()
                        if not li_desc:
                            continue
                        if hint in li_desc or li_desc in hint:
                            score = len(hint) / max(len(li_desc), 1) + 0.5
                        else:
                            hint_words = set(hint.split())
                            desc_words = set(li_desc.split())
                            overlap = hint_words & desc_words
                            score = len(overlap) / max(len(hint_words), 1) if overlap else 0
                        if score > best_score:
                            best_score = score
                            target = li
                    if best_score < 0.3:
                        target = None

            # Fallback: preceding item
            if target is None:
                disc_idx = next((i for i, li in enumerate(doc_lines) if li.id == disc.id), -1)
                for j in range(disc_idx - 1, -1, -1):
                    li = doc_lines[j]
                    if (li.total_price or 0) >= 0 and not discount_pattern.search(li.description or ""):
                        target = li
                        break

            if target:
                # Apply discount
                orig_price = target.total_price
                if isinstance(orig_price, (int, float)):
                    target.total_price = round(orig_price + amount, 2)

                existing = target.discount or ""
                new_disc = f"{desc} {amount:.2f} kr"
                target.discount = f"{existing}; {new_disc}".lstrip("; ") if existing else new_disc

                db.delete(disc)
                stats["linked"] += 1
                stats["deleted"] += 1
            else:
                stats["unmatched"] += 1

    db.commit()
    print(f"  ✅ Rabatter: {stats['linked']} kopplade, {stats['deleted']} borttagna, {stats['unmatched']} omatchade")
    return stats


def merge_products(
    db: Session, *,
    source_descriptions: list[str],
    target_description: str,
    target_category: str | None = None,
) -> dict[str, Any]:
    """Merge multiple product descriptions into one canonical name.
    Updates all existing line items + creates normalization rules for future."""
    _ensure_normalized(db)
    norm_target = _fmt(target_description)
    norm_cat = _fmt(target_category) if target_category else None
    updated = 0
    rules_created = 0

    for src in source_descriptions:
        norm_src = _fmt(src)
        # Find items matching this source (normalized data = direct match)
        items = db.query(LineItem).filter(LineItem.description == norm_src).all()
        for item in items:
            item.description = norm_target
            if norm_cat:
                item.category = norm_cat
            updated += 1

        # Create normalization rule (skip if source == target)
        if norm_src == norm_target:
            continue

        existing = _find_rule(db, scope="line_item", rule_type="product_normalize",
                              condition_field="description", condition_value=norm_src)
        if existing:
            existing.action_value = norm_target
            existing.active = True
        else:
            create_rule(
                db,
                name=f"Normalisera '{norm_src[:30]}' → '{norm_target[:30]}'",
                description=f"Sammanslagen produkt: '{norm_src}' → '{norm_target}'",
                scope="line_item",
                rule_type="product_normalize",
                condition_field="description",
                condition_operator="equals",
                condition_value=norm_src,
                target_field="description",
                action="set",
                action_value=norm_target,
                auto_generated=False,
                active=True,
            )
            rules_created += 1
    db.commit()
    _backup_rules_to_file(db)

    return {
        "target_description": norm_target,
        "items_updated": updated,
        "rules_created": rules_created,
    }


def list_documents(
    db: Session, *, skip: int = 0, limit: int = 50,
    document_type: str | None = None, vendor: str | None = None,
    search: str | None = None, user_id: int | None = None,
) -> list[Document]:
    query = db.query(Document)
    if user_id is not None:
        query = query.filter(Document.user_id == user_id)
    if document_type:
        query = query.filter(Document.document_type == document_type)
    if vendor:
        query = query.filter(Document.vendor.ilike(f"%{vendor}%"))
    if search:
        pattern = f"%{search}%"
        query = query.filter(or_(
            Document.filename.ilike(pattern),
            Document.vendor.ilike(pattern),
            Document.raw_analysis.ilike(pattern),
            Document.invoice_number.ilike(pattern),
        ))
    return query.order_by(Document.created_at.desc()).offset(skip).limit(limit).all()


def count_documents(
    db: Session, *, document_type: str | None = None,
    vendor: str | None = None, search: str | None = None,
    user_id: int | None = None,
) -> int:
    query = db.query(Document)
    if user_id is not None:
        query = query.filter(Document.user_id == user_id)
    if document_type:
        query = query.filter(Document.document_type == document_type)
    if vendor:
        query = query.filter(Document.vendor.ilike(f"%{vendor}%"))
    if search:
        pattern = f"%{search}%"
        query = query.filter(or_(
            Document.filename.ilike(pattern),
            Document.vendor.ilike(pattern),
            Document.raw_analysis.ilike(pattern),
            Document.invoice_number.ilike(pattern),
        ))
    return query.count()


def delete_document(db: Session, document_id: str) -> bool:
    doc = db.query(Document).filter(Document.id == document_id).first()
    if not doc:
        return False
    db.delete(doc)
    db.commit()
    return True


def get_stats(db: Session, user_id: int | None = None) -> dict[str, Any]:
    doc_q = db.query(Document)
    li_q = db.query(LineItem)
    if user_id is not None:
        doc_q = doc_q.filter(Document.user_id == user_id)
        li_q = li_q.join(Document).filter(Document.user_id == user_id)
    total = doc_q.count()
    total_amount = doc_q.with_entities(func.sum(Document.total_amount)).scalar() or 0.0
    by_type = (
        doc_q.with_entities(Document.document_type, func.count(Document.id))
        .group_by(Document.document_type).all()
    )
    by_vendor = (
        doc_q.with_entities(Document.vendor, func.count(Document.id))
        .filter(Document.vendor.isnot(None))
        .group_by(Document.vendor)
        .order_by(func.count(Document.id).desc())
        .limit(10).all()
    )
    total_line_items = li_q.count()
    by_category = (
        li_q.with_entities(LineItem.category, func.count(LineItem.id))
        .filter(LineItem.category.isnot(None))
        .group_by(LineItem.category)
        .order_by(func.count(LineItem.id).desc())
        .limit(10).all()
    )
    return {
        "total_documents": total,
        "total_amount": round(total_amount, 2),
        "total_line_items": total_line_items,
        "by_type": {t or "unknown": c for t, c in by_type},
        "top_vendors": {v: c for v, c in by_vendor},
        "top_categories": {cat: c for cat, c in by_category},
    }


# ── Vendor detection & CRUD ─────────────────────────────────────────

# Swedish grocery chain detection patterns
_CHAINS = [
    ("ICA", ["ica"]),
    ("Coop", ["coop"]),
    ("Hemköp", ["hemköp"]),
    ("Willys", ["willys"]),
    ("Lidl", ["lidl"]),
    ("City Gross", ["city gross", "citygross"]),
    ("Tempo", ["tempo"]),
    ("Netto", ["netto"]),
    ("Matöppet", ["matöppet"]),
]

_FORMATS: dict[str, list[tuple[str, list[str]]]] = {
    "ICA": [
        ("Maxi Stormarknad", ["maxi", "stormarknad"]),
        ("Kvantum", ["kvantum"]),
        ("Supermarket", ["supermarket"]),
        ("Nära", ["nära"]),
    ],
    "Coop": [
        ("Stora Coop", ["stora coop", "stora"]),
        ("Konsum", ["konsum"]),
    ],
    "Willys": [
        ("Willys Hemma", ["hemma"]),
    ],
    "City Gross": [
        ("City Gross", []),  # only one format
    ],
}

# Words to strip when extracting city from vendor name
_STRIP_WORDS = {
    "ica", "coop", "hemköp", "willys", "lidl", "city", "gross", "tempo",
    "netto", "maxi", "stormarknad", "kvantum", "supermarket", "nära",
    "stora", "konsum", "hemma", "matöppet", "ab", "butik", "butiken",
    "sverige", "kb", "filial", "nr",
}


def detect_vendor_info(vendor_name: str) -> dict[str, str | None]:
    """Auto-detect chain, format, and city from a Swedish vendor name."""
    if not vendor_name:
        return {"chain": None, "format": None, "city": None}

    name_lower = vendor_name.lower().strip()

    # Detect chain
    chain = None
    for chain_name, keywords in _CHAINS:
        if any(kw in name_lower for kw in keywords):
            chain = chain_name
            break

    # Detect format
    fmt = None
    if chain and chain in _FORMATS:
        for fmt_name, keywords in _FORMATS[chain]:
            if keywords and any(kw in name_lower for kw in keywords):
                fmt = fmt_name
                break
        if not fmt:
            # Default format per chain
            defaults = {"ICA": "Supermarket", "Coop": "Coop", "Willys": "Willys"}
            fmt = defaults.get(chain)
    elif chain:
        fmt = chain  # e.g. Lidl, Hemköp — format = chain name

    # Detect city: strip chain/format words, keep remainder
    words = re.split(r"[\s,]+", vendor_name.strip())
    city_words = [w for w in words if w.lower() not in _STRIP_WORDS and len(w) > 1]
    # City is typically the last word(s)
    city = " ".join(city_words[-2:]) if city_words else None
    # Clean up: don't return numbers or very short results
    if city and (len(city) < 2 or city.isdigit()):
        city = None

    return {"chain": chain, "format": fmt, "city": city}


def get_or_create_vendor(db: Session, vendor_name: str) -> Vendor | None:
    """Find existing vendor by name, or create with auto-detected attributes."""
    if not vendor_name or not vendor_name.strip():
        return None

    name = vendor_name.strip()
    existing = db.query(Vendor).filter(Vendor.name == name).first()
    if existing:
        return existing

    info = detect_vendor_info(name)
    vendor = Vendor(
        name=name,
        chain=info["chain"],
        format=info["format"],
        city=info["city"],
        auto_detected=True,
    )
    db.add(vendor)
    db.flush()  # Get ID without full commit
    return vendor


def list_vendors(db: Session) -> list[dict[str, Any]]:
    """Get all vendors with document count and total amount."""
    vendors = db.query(Vendor).all()
    result = []
    for v in vendors:
        stats = (
            db.query(
                func.count(Document.id),
                func.sum(Document.total_amount),
            )
            .filter(Document.vendor_id == v.id)
            .first()
        )
        doc_count = stats[0] if stats else 0
        total_amount = stats[1] if stats else 0
        result.append({
            "id": v.id,
            "name": v.name,
            "chain": v.chain,
            "format": v.format,
            "city": v.city,
            "auto_detected": v.auto_detected,
            "document_count": doc_count,
            "total_amount": round(total_amount or 0, 2),
        })
    result.sort(key=lambda x: x["total_amount"], reverse=True)
    return result


def update_vendor(db: Session, vendor_id: int, **kwargs) -> Vendor | None:
    """Update vendor attributes (name, chain, format, city)."""
    vendor = db.query(Vendor).filter(Vendor.id == vendor_id).first()
    if not vendor:
        return None

    # If name is changing, also update all linked documents' vendor text
    new_name = kwargs.get("name")
    if new_name and new_name != vendor.name:
        db.query(Document).filter(Document.vendor_id == vendor_id).update(
            {Document.vendor: new_name}, synchronize_session="fetch"
        )
        vendor.name = new_name

    for key in ("chain", "format", "city"):
        if key in kwargs:
            setattr(vendor, key, kwargs[key] or None)
    vendor.auto_detected = False  # Mark as manually edited
    db.commit()
    return vendor


def backfill_vendors(db: Session) -> dict[str, int]:
    """Create Vendor records for all existing documents missing vendor_id."""
    docs = db.query(Document).filter(
        Document.vendor.isnot(None),
        Document.vendor_id.is_(None),
    ).all()
    created = 0
    linked = 0
    for doc in docs:
        vendor = get_or_create_vendor(db, doc.vendor)
        if vendor:
            doc.vendor_id = vendor.id
            linked += 1
            if vendor.id and not db.query(Vendor).filter(Vendor.id == vendor.id).first():
                created += 1
    if linked:
        db.commit()
    return {"vendors_created": created, "documents_linked": linked}


def merge_vendors(
    db: Session, *, source_ids: list[int], target_id: int,
) -> dict[str, Any]:
    """Merge multiple vendors into one. Moves all documents to target, deletes sources."""
    target = db.query(Vendor).filter(Vendor.id == target_id).first()
    if not target:
        return {"error": "Target vendor not found", "documents_moved": 0, "vendors_deleted": 0}

    docs_moved = 0
    vendors_deleted = 0

    for src_id in source_ids:
        if src_id == target_id:
            continue
        src = db.query(Vendor).filter(Vendor.id == src_id).first()
        if not src:
            continue

        # Move all documents from source to target using direct UPDATE
        # (avoids SQLAlchemy relationship cleanup overwriting vendor_id)
        count = (
            db.query(Document)
            .filter(Document.vendor_id == src_id)
            .update({
                Document.vendor_id: target_id,
                Document.vendor: target.name,
            }, synchronize_session="fetch")
        )
        docs_moved += count

        # Now safe to delete — no documents reference this vendor anymore
        db.delete(src)
        vendors_deleted += 1

    db.commit()
    return {
        "target_name": target.name,
        "documents_moved": docs_moved,
        "vendors_deleted": vendors_deleted,
    }


# ── Product-level analytics ─────────────────────────────────────────

def get_category_stats(
    db: Session,
    date_from: str | None = None,
    date_to: str | None = None,
    user_id: int | None = None,
) -> list[dict[str, Any]]:
    """Get category breakdown with count, total amount, and avg price.
    Optional date_from/date_to filter (ISO format YYYY-MM-DD).
    Uses invoice_date (receipt date) with created_at as fallback."""
    _ensure_normalized(db)

    # Effective date: receipt date if available, else upload date
    # SQLite date() parses ISO strings; COALESCE falls back to created_at
    effective_date = func.coalesce(
        func.date(Document.invoice_date),
        func.date(Document.created_at),
    )

    base = (
        db.query(
            LineItem.category,
            func.count(LineItem.id),
            func.sum(LineItem.total_price),
            func.avg(LineItem.total_price),
        )
        .join(Document, LineItem.document_id == Document.id)
        .filter(LineItem.category.isnot(None))
        .filter(LineItem.description.notin_(_PANT_DESCRIPTIONS))
    )

    if user_id is not None:
        base = base.filter(Document.user_id == user_id)
    if date_from:
        base = base.filter(effective_date >= date_from)
    if date_to:
        base = base.filter(effective_date <= date_to)

    results = (
        base
        .group_by(LineItem.category)
        .order_by(func.sum(LineItem.total_price).desc())
        .all()
    )
    return [
        {
            "category": _fmt(cat) or "okategoriserad",
            "count": count,
            "total_amount": round(total or 0, 2),
            "avg_price": round(avg or 0, 2),
        }
        for cat, count, total, avg in results
    ]


def get_products(
    db: Session, *, category: str | None = None, vendor: str | None = None,
    search: str | None = None, skip: int = 0, limit: int = 0,
    user_id: int | None = None,
) -> dict[str, Any]:
    """Get product-level aggregation: grouped by normalized description.
    limit=0 means return all."""
    _ensure_normalized(db)
    base = (
        db.query(LineItem)
        .filter(LineItem.description.isnot(None))
        .filter(LineItem.description.notin_(_PANT_DESCRIPTIONS))
    )
    needs_join = vendor or user_id is not None
    if needs_join:
        base = base.join(Document, LineItem.document_id == Document.id)
        if vendor:
            base = base.filter(Document.vendor == vendor)
        if user_id is not None:
            base = base.filter(Document.user_id == user_id)
    if search:
        base = base.filter(LineItem.description.ilike(f"%{search}%"))
    if category:
        base = base.filter(LineItem.category == _fmt(category))

    total = base.with_entities(func.count(func.distinct(LineItem.description))).scalar() or 0

    q = (
        base.with_entities(
            LineItem.description,
            func.max(LineItem.category).label("category"),
            func.count(LineItem.id).label("purchase_count"),
            func.sum(LineItem.total_price).label("total_spent"),
            func.avg(LineItem.unit_price).label("avg_unit_price"),
            func.min(LineItem.unit_price).label("min_unit_price"),
            func.max(LineItem.unit_price).label("max_unit_price"),
            func.sum(LineItem.quantity).label("total_quantity"),
            func.max(LineItem.unit).label("unit"),
        )
        .group_by(LineItem.description)
        .order_by(func.sum(LineItem.total_price).desc())
        .offset(skip)
    )
    if limit > 0:
        q = q.limit(limit)
    rows = q.all()

    products = []
    for desc, cat, count, total_spent, avg_price, min_price, max_price, total_qty, unit in rows:
        products.append({
            "description": _fmt(desc),
            "category": _fmt(cat),
            "purchase_count": count,
            "total_spent": round(total_spent or 0, 2),
            "avg_unit_price": round(avg_price or 0, 2),
            "min_unit_price": round(min_price or 0, 2) if min_price else None,
            "max_unit_price": round(max_price or 0, 2) if max_price else None,
            "total_quantity": round(total_qty or 0, 2),
            "unit": unit,
        })

    return {"total": total, "products": products}


def get_product_price_history(db: Session, description: str) -> list[dict[str, Any]]:
    """Get price history for a specific product over time."""
    rows = (
        db.query(
            Document.invoice_date,
            Document.created_at,
            Document.vendor,
            LineItem.unit_price,
            LineItem.total_price,
            LineItem.quantity,
            LineItem.weight,
            LineItem.unit,
        )
        .join(Document, LineItem.document_id == Document.id)
        .filter(LineItem.description == _fmt(description))
        .order_by(Document.created_at.asc())
        .all()
    )

    return [
        {
            "date": str(inv_date or created.strftime("%Y-%m-%d") if created else None),
            "vendor": vendor,
            "unit_price": round(unit_price, 2) if unit_price else None,
            "total_price": round(total_price, 2) if total_price else None,
            "quantity": quantity,
            "weight": round(weight, 3) if weight else None,
            "unit": unit,
        }
        for inv_date, created, vendor, unit_price, total_price, quantity, weight, unit in rows
    ]


def get_product_documents(db: Session, description: str) -> list[dict[str, Any]]:
    """Get all documents that contain a specific product."""
    norm_desc = _fmt(description)
    rows = (
        db.query(
            Document.id,
            Document.filename,
            Document.vendor,
            Document.invoice_date,
            Document.created_at,
            Document.total_amount,
            Document.document_type,
            LineItem.quantity,
            LineItem.unit_price,
            LineItem.total_price,
            LineItem.unit,
        )
        .join(Document, LineItem.document_id == Document.id)
        .filter(LineItem.description == norm_desc)
        .order_by(Document.created_at.desc())
        .all()
    )
    return [
        {
            "document_id": doc_id,
            "filename": filename,
            "vendor": vendor,
            "invoice_date": str(inv_date) if inv_date else None,
            "created_at": created.isoformat() if created else None,
            "total_amount": round(total_amount or 0, 2),
            "document_type": doc_type,
            "quantity": qty,
            "unit_price": round(up or 0, 2) if up else None,
            "total_price": round(tp or 0, 2) if tp else None,
            "unit": unit,
        }
        for doc_id, filename, vendor, inv_date, created, total_amount, doc_type, qty, up, tp, unit in rows
    ]


def normalize_existing_data(db: Session) -> dict[str, int]:
    """Normalize all existing descriptions and categories to _fmt() form.
    Run once after upgrading to ensure consistent data."""
    items = db.query(LineItem).filter(LineItem.description.isnot(None)).all()
    desc_fixed = 0
    cat_fixed = 0
    for item in items:
        nd = _fmt(item.description)
        if nd != item.description:
            item.description = nd
            desc_fixed += 1
        if item.category:
            nc = _fmt(item.category)
            if nc != item.category:
                item.category = nc
                cat_fixed += 1
    if desc_fixed or cat_fixed:
        db.commit()
    return {"descriptions_normalized": desc_fixed, "categories_normalized": cat_fixed}


def get_vendor_price_comparison(
    db: Session, *, search: str | None = None, category: str | None = None,
    vendor: str | None = None,
    min_vendors: int = 2, skip: int = 0, limit: int = 50,
) -> dict[str, Any]:
    """Compare prices for same products across different vendors.
    Only includes products sold by at least min_vendors different vendors."""

    # Subquery: products with multiple vendors
    sub = (
        db.query(
            LineItem.description,
            func.count(func.distinct(Document.vendor)).label("vendor_count"),
        )
        .join(Document, LineItem.document_id == Document.id)
        .filter(LineItem.description.isnot(None), Document.vendor.isnot(None))
        .filter(LineItem.unit_price.isnot(None))
        .filter(LineItem.description.notin_(_PANT_DESCRIPTIONS))
    )
    if category:
        sub = sub.filter(LineItem.category == _fmt(category))
    sub = (
        sub.group_by(LineItem.description)
        .having(func.count(func.distinct(Document.vendor)) >= min_vendors)
        .subquery()
    )

    # Main query: per product per vendor
    query = (
        db.query(
            LineItem.description,
            LineItem.category,
            Document.vendor,
            func.avg(LineItem.unit_price).label("avg_price"),
            func.min(LineItem.unit_price).label("min_price"),
            func.max(LineItem.unit_price).label("max_price"),
            func.count(LineItem.id).label("count"),
            LineItem.unit,
        )
        .join(Document, LineItem.document_id == Document.id)
        .join(sub, LineItem.description == sub.c.description)
        .filter(Document.vendor.isnot(None), LineItem.unit_price.isnot(None))
    )

    if search:
        query = query.filter(LineItem.description.ilike(f"%{search}%"))
    if category:
        query = query.filter(LineItem.category == _fmt(category))
    if vendor:
        # Only show products that this vendor sells (but show all vendors for comparison)
        vendor_prods = (
            db.query(LineItem.description)
            .join(Document, LineItem.document_id == Document.id)
            .filter(Document.vendor == vendor)
            .distinct().subquery()
        )
        query = query.filter(LineItem.description.in_(db.query(vendor_prods.c.description)))

    rows = (
        query.group_by(LineItem.description, Document.vendor)
        .order_by(LineItem.description, func.avg(LineItem.unit_price).asc())
        .all()
    )

    # Group by product
    products: dict[str, dict] = {}
    for desc, cat, vendor, avg_p, min_p, max_p, cnt, unit in rows:
        if desc not in products:
            products[desc] = {
                "description": _fmt(desc), "category": _fmt(cat), "unit": unit,
                "vendors": [], "cheapest_vendor": None, "cheapest_price": None,
                "most_expensive_vendor": None, "most_expensive_price": None,
            }
        products[desc]["vendors"].append({
            "vendor": vendor,
            "avg_price": round(avg_p or 0, 2),
            "min_price": round(min_p or 0, 2),
            "max_price": round(max_p or 0, 2),
            "purchase_count": cnt,
        })

    # Compute cheapest/most expensive
    result_list = []
    for prod in products.values():
        vs = prod["vendors"]
        if vs:
            vs.sort(key=lambda v: v["avg_price"])
            prod["cheapest_vendor"] = vs[0]["vendor"]
            prod["cheapest_price"] = vs[0]["avg_price"]
            prod["most_expensive_vendor"] = vs[-1]["vendor"]
            prod["most_expensive_price"] = vs[-1]["avg_price"]
            if vs[-1]["avg_price"] > 0:
                prod["savings_pct"] = round(
                    (1 - vs[0]["avg_price"] / vs[-1]["avg_price"]) * 100, 1
                )
            else:
                prod["savings_pct"] = 0
        result_list.append(prod)

    # Sort by potential savings
    result_list.sort(key=lambda p: p.get("savings_pct", 0), reverse=True)
    total = len(result_list)
    paged = result_list[skip:skip + limit]

    return {"total": total, "comparisons": paged}


def get_price_trends(
    db: Session, *, search: str | None = None, category: str | None = None,
    vendor: str | None = None, top_n: int = 10,
) -> dict[str, Any]:
    """Get price trends for top products over time."""

    # Find top products by purchase frequency
    prod_q = (
        db.query(
            LineItem.description,
            LineItem.category,
            func.count(LineItem.id).label("cnt"),
        )
        .filter(LineItem.description.isnot(None), LineItem.unit_price.isnot(None))
        .filter(LineItem.description.notin_(_PANT_DESCRIPTIONS))
    )
    if vendor:
        prod_q = prod_q.join(Document, LineItem.document_id == Document.id).filter(Document.vendor == vendor)
    if search:
        prod_q = prod_q.filter(LineItem.description.ilike(f"%{search}%"))
    if category:
        prod_q = prod_q.filter(LineItem.category == _fmt(category))

    top_products = (
        prod_q.group_by(LineItem.description)
        .having(func.count(LineItem.id) >= 2)
        .order_by(func.count(LineItem.id).desc())
        .limit(top_n)
        .all()
    )

    trends = []
    for desc, cat, cnt in top_products:
        rows = (
            db.query(
                Document.invoice_date,
                Document.created_at,
                Document.vendor,
                LineItem.unit_price,
            )
            .join(Document, LineItem.document_id == Document.id)
            .filter(LineItem.description == desc, LineItem.unit_price.isnot(None))
            .order_by(Document.created_at.asc())
            .all()
        )

        points = []
        for inv_date, created, vendor, price in rows:
            d = str(inv_date or (created.strftime("%Y-%m-%d") if created else ""))
            points.append({"date": d, "price": round(price, 2), "vendor": vendor})

        if len(points) >= 2:
            first_price = points[0]["price"]
            last_price = points[-1]["price"]
            change_pct = round((last_price - first_price) / first_price * 100, 1) if first_price > 0 else 0
        else:
            change_pct = 0

        trends.append({
            "description": _fmt(desc), "category": _fmt(cat),
            "purchase_count": cnt, "data_points": points,
            "change_pct": change_pct,
        })

    # Sort by absolute price change
    trends.sort(key=lambda t: abs(t["change_pct"]), reverse=True)
    return {"trends": trends}


# ── Rule CRUD ───────────────────────────────────────────────────────

# ── Rules backup/restore ───────────────────────────────────────────

_RULE_FIELDS = [
    "name", "description", "scope", "rule_type", "active", "auto_generated",
    "condition_field", "condition_operator", "condition_value",
    "target_field", "action", "action_value", "times_applied",
]


def _backup_rules_to_file(db: Session) -> None:
    """Save all rules to a JSON file so they survive database resets."""
    try:
        rules = db.query(ExtractionRule).all()
        data = []
        for r in rules:
            data.append({f: getattr(r, f, None) for f in _RULE_FIELDS})
        RULES_BACKUP_PATH.parent.mkdir(parents=True, exist_ok=True)
        RULES_BACKUP_PATH.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8",
        )
    except Exception as e:
        print(f"[rules-backup] Warning: could not save rules: {e}")


def restore_rules_from_backup(db: Session) -> int:
    """Restore rules from backup file if the database has none.
    Returns the number of rules restored."""
    if not RULES_BACKUP_PATH.exists():
        return 0
    # Only restore into an empty rules table
    existing = db.query(ExtractionRule).count()
    if existing > 0:
        return 0
    try:
        data = json.loads(RULES_BACKUP_PATH.read_text(encoding="utf-8"))
        count = 0
        for entry in data:
            # Clean up: only keep known fields
            cleaned = {k: v for k, v in entry.items() if k in _RULE_FIELDS}
            rule = ExtractionRule(**cleaned)
            db.add(rule)
            count += 1
        db.commit()
        print(f"[rules-backup] Restored {count} rules from backup")
        return count
    except Exception as e:
        print(f"[rules-backup] Warning: could not restore rules: {e}")
        db.rollback()
        return 0


def list_rules(db: Session, *, active_only: bool = False, scope: str | None = None) -> list[ExtractionRule]:
    # Auto-restore from backup if db is empty
    if db.query(ExtractionRule).count() == 0:
        restore_rules_from_backup(db)
    query = db.query(ExtractionRule)
    if active_only:
        query = query.filter(ExtractionRule.active == True)
    if scope:
        query = query.filter(ExtractionRule.scope == scope)
    return query.order_by(ExtractionRule.scope, ExtractionRule.created_at.desc()).all()


def get_rule(db: Session, rule_id: int) -> ExtractionRule | None:
    return db.query(ExtractionRule).filter(ExtractionRule.id == rule_id).first()


def create_rule(db: Session, **kwargs) -> ExtractionRule:
    # Check for existing rule with same key fields → overwrite instead of duplicate
    scope = kwargs.get("scope")
    rule_type = kwargs.get("rule_type")
    cond_field = kwargs.get("condition_field")
    cond_value = kwargs.get("condition_value", "")
    target_field = kwargs.get("target_field")

    if scope and rule_type and cond_field and cond_value:
        existing = _find_rule(
            db, scope=scope, rule_type=rule_type,
            condition_field=cond_field, condition_value=cond_value,
            target_field=target_field,
        )
        if existing:
            # Overwrite all fields on the existing rule
            for key, value in kwargs.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
            db.commit()
            db.refresh(existing)
            _backup_rules_to_file(db)
            return existing

    rule = ExtractionRule(**kwargs)
    db.add(rule)
    db.commit()
    db.refresh(rule)
    _backup_rules_to_file(db)
    return rule


def update_rule(db: Session, rule_id: int, updates: dict[str, Any]) -> ExtractionRule | None:
    rule = db.query(ExtractionRule).filter(ExtractionRule.id == rule_id).first()
    if not rule:
        return None
    for key, value in updates.items():
        if hasattr(rule, key):
            setattr(rule, key, value)
    db.commit()
    db.refresh(rule)
    _backup_rules_to_file(db)
    return rule


def delete_rule(db: Session, rule_id: int) -> bool:
    rule = db.query(ExtractionRule).filter(ExtractionRule.id == rule_id).first()
    if not rule:
        return False
    db.delete(rule)
    db.commit()
    _backup_rules_to_file(db)
    return True


# ── Rule Engine ─────────────────────────────────────────────────────

# Fields that exist on Document vs LineItem
DOCUMENT_FIELDS = {
    "vendor", "total_amount", "vat_amount", "currency", "invoice_number",
    "ocr_number", "invoice_date", "due_date", "document_type", "discount", "filename",
}
LINE_ITEM_FIELDS = {
    "description", "quantity", "unit", "unit_price", "total_price",
    "vat_rate", "discount", "weight", "packaging", "category",
}


def _apply_all_rules_to_document(doc: Document, db: Session) -> None:
    """Apply all active rules to a document and its line items.
    Manual rules run AFTER auto-generated ones so 'set' overrides 'set_if_empty'."""
    # Auto-restore rules from backup if db is empty
    if db.query(ExtractionRule).count() == 0:
        restore_rules_from_backup(db)
    rules = db.query(ExtractionRule).filter(ExtractionRule.active == True).all()
    # Sort: auto-generated first, manual last (manual overrides auto)
    rules.sort(key=lambda r: (0 if r.auto_generated else 1, r.id or 0))

    doc_rules = [r for r in rules if r.scope == "document"]
    line_rules = [r for r in rules if r.scope == "line_item"]

    # Apply document-level rules
    for rule in doc_rules:
        if _condition_matches(rule, doc, DOCUMENT_FIELDS):
            _execute_action(rule, doc, DOCUMENT_FIELDS)
            rule.times_applied = (rule.times_applied or 0) + 1

    # Apply line-item-level rules
    for line in doc.line_items:
        for rule in line_rules:
            if _condition_matches(rule, line, LINE_ITEM_FIELDS):
                _execute_action(rule, line, LINE_ITEM_FIELDS)
                rule.times_applied = (rule.times_applied or 0) + 1


def _condition_matches(rule: ExtractionRule, obj: Any, valid_fields: set[str]) -> bool:
    """Check if a rule's condition matches an object (Document or LineItem)."""
    if rule.condition_operator == "always":
        return True

    if not rule.condition_field:
        return False

    # For line_item rules, also allow matching against parent document fields
    field_value = getattr(obj, rule.condition_field, None)

    # If field not found on line item, check parent document
    if field_value is None and hasattr(obj, "document") and rule.condition_field in DOCUMENT_FIELDS:
        field_value = getattr(obj.document, rule.condition_field, None) if obj.document else None

    if field_value is None:
        return False

    field_str = str(field_value).lower().strip()
    cond_str = (rule.condition_value or "").lower().strip()

    if rule.condition_operator == "equals":
        return field_str == cond_str
    elif rule.condition_operator == "contains":
        return cond_str in field_str
    elif rule.condition_operator == "starts_with":
        return field_str.startswith(cond_str)
    elif rule.condition_operator == "ends_with":
        return field_str.endswith(cond_str)
    elif rule.condition_operator == "regex":
        try:
            return bool(re.search(rule.condition_value or "", str(field_value), re.IGNORECASE))
        except re.error:
            return False
    elif rule.condition_operator == "greater_than":
        try:
            return float(field_value) > float(rule.condition_value or 0)
        except (ValueError, TypeError):
            return False
    elif rule.condition_operator == "less_than":
        try:
            return float(field_value) < float(rule.condition_value or 0)
        except (ValueError, TypeError):
            return False

    return False


def _execute_action(rule: ExtractionRule, obj: Any, valid_fields: set[str]) -> None:
    """Execute a rule's action on an object (Document or LineItem)."""
    if not rule.target_field or not rule.action:
        return

    # Only set fields that belong to this object type
    if rule.target_field not in valid_fields:
        return

    current_value = getattr(obj, rule.target_field, None)
    NUMERIC_FIELDS = {"total_amount", "vat_amount", "quantity", "unit_price", "total_price", "vat_rate"}
    TEXT_NORM_FIELDS = {"description", "category"}  # Normalize casing on write

    if rule.action == "set":
        value = rule.action_value
        if rule.target_field in NUMERIC_FIELDS:
            try:
                value = float(value)
            except (ValueError, TypeError):
                pass
        elif rule.target_field in TEXT_NORM_FIELDS:
            value = _fmt(value)
        setattr(obj, rule.target_field, value)

    elif rule.action == "set_if_empty":
        if not current_value:
            value = rule.action_value
            if rule.target_field in NUMERIC_FIELDS:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    pass
            elif rule.target_field in TEXT_NORM_FIELDS:
                value = _fmt(value)
            setattr(obj, rule.target_field, value)

    elif rule.action == "replace":
        if current_value and "|||" in (rule.action_value or ""):
            old, new = rule.action_value.split("|||", 1)
            setattr(obj, rule.target_field, str(current_value).replace(old, new))

    elif rule.action == "strip_chars":
        if current_value and rule.action_value:
            cleaned = str(current_value)
            for char in rule.action_value:
                cleaned = cleaned.replace(char, "")
            setattr(obj, rule.target_field, cleaned.strip())

    elif rule.action == "format_number":
        if current_value:
            cleaned = (
                str(current_value)
                .replace(" ", "").replace(",", ".")
                .replace("kr", "").replace("SEK", "").strip()
            )
            try:
                setattr(obj, rule.target_field, float(cleaned))
            except ValueError:
                pass

    elif rule.action == "multiply":
        if current_value and rule.action_value:
            try:
                setattr(obj, rule.target_field, float(current_value) * float(rule.action_value))
            except (ValueError, TypeError):
                pass

    elif rule.action == "recalculate":
        # Recalculate total_price = quantity * unit_price (for line items)
        if hasattr(obj, "quantity") and hasattr(obj, "unit_price"):
            q = obj.quantity
            p = obj.unit_price
            if q is not None and p is not None:
                setattr(obj, "total_price", round(q * p, 2))


def apply_rules_to_all_documents(db: Session) -> dict[str, int]:
    """Re-apply all active rules to all documents and line items."""
    rules = db.query(ExtractionRule).filter(ExtractionRule.active == True).all()
    if not rules:
        return {"documents_updated": 0, "line_items_updated": 0}
    # Sort: auto-generated first, manual last (manual overrides auto)
    rules.sort(key=lambda r: (0 if r.auto_generated else 1, r.id or 0))

    doc_rules = [r for r in rules if r.scope == "document"]
    line_rules = [r for r in rules if r.scope == "line_item"]

    docs = db.query(Document).options(joinedload(Document.line_items)).all()
    docs_updated = 0
    lines_updated = 0

    for doc in docs:
        doc_changed = False

        for rule in doc_rules:
            if _condition_matches(rule, doc, DOCUMENT_FIELDS):
                _execute_action(rule, doc, DOCUMENT_FIELDS)
                rule.times_applied = (rule.times_applied or 0) + 1
                doc_changed = True

        for line in doc.line_items:
            for rule in line_rules:
                if _condition_matches(rule, line, LINE_ITEM_FIELDS):
                    _execute_action(rule, line, LINE_ITEM_FIELDS)
                    rule.times_applied = (rule.times_applied or 0) + 1
                    lines_updated += 1
                    doc_changed = True

        if doc_changed:
            docs_updated += 1

    db.commit()
    return {"documents_updated": docs_updated, "line_items_updated": lines_updated}


# ── Auto-generate rules ─────────────────────────────────────────────

def _auto_generate_rules(doc: Document, db: Session) -> None:
    """Analyze extracted data and auto-generate rules from patterns."""

    # ── Document-level rules ──
    if doc.vendor:
        _maybe_create_vendor_normalize_rule(doc.vendor, db)

    if doc.vendor and doc.currency:
        _maybe_create_default_rule(
            db, name=f"Standardvaluta för {doc.vendor}",
            scope="document", condition_field="vendor",
            condition_operator="contains", condition_value=doc.vendor,
            target_field="currency", action="set_if_empty",
            action_value=doc.currency, rule_type="field_default",
        )

    if doc.vendor and doc.document_type:
        _maybe_create_default_rule(
            db, name=f"Dokumenttyp för {doc.vendor}",
            scope="document", condition_field="vendor",
            condition_operator="contains", condition_value=doc.vendor,
            target_field="document_type", action="set_if_empty",
            action_value=doc.document_type, rule_type="field_default",
        )

    # ── Line-item-level rules ──
    for line in doc.line_items:
        _auto_generate_line_item_rules(line, doc, db)

    db.commit()


def _auto_generate_line_item_rules(line: LineItem, doc: Document, db: Session) -> None:
    """Generate rules based on line item patterns."""

    desc = (line.description or "").strip()
    if not desc or len(desc) < 3:
        return

    # Rule: Detect missing unit and suggest default
    if line.quantity and not line.unit:
        _maybe_create_line_rule(
            db,
            name=f"Standardenhet för '{_truncate(desc, 40)}'",
            description=f"Sätt enhet till 'st' när enhet saknas för produkter som matchar '{_truncate(desc, 60)}'",
            rule_type="unit_correction",
            condition_field="description",
            condition_operator="contains",
            condition_value=desc[:50],
            target_field="unit",
            action="set_if_empty",
            action_value="st",
        )

    # Rule: Detect VAT rate patterns
    if line.vat_rate and line.description:
        # Common Swedish VAT categories
        vat = line.vat_rate
        keywords = _extract_category_keywords(desc)
        if keywords and vat in (6.0, 12.0, 25.0):
            for kw in keywords[:2]:  # max 2 keywords per item
                _maybe_create_line_rule(
                    db,
                    name=f"Moms {vat}% för '{kw}'",
                    description=f"Produkter med '{kw}' i beskrivningen har typiskt {vat}% moms",
                    rule_type="field_default",
                    condition_field="description",
                    condition_operator="contains",
                    condition_value=kw,
                    target_field="vat_rate",
                    action="set_if_empty",
                    action_value=str(vat),
                )

    # Rule: Product name normalization (detect variations)
    _detect_product_name_variations(desc, db)

    # Rule: Category assignment based on keywords
    _auto_assign_category_rule(desc, line, db)


def _detect_product_name_variations(desc: str, db: Session) -> None:
    """Find existing line items with similar descriptions and suggest normalization."""
    desc_lower = desc.lower().strip()

    # Get existing unique descriptions
    existing = (
        db.query(LineItem.description)
        .filter(LineItem.description.isnot(None))
        .distinct()
        .all()
    )

    for (existing_desc,) in existing:
        if not existing_desc:
            continue
        existing_lower = existing_desc.lower().strip()

        if existing_lower == desc_lower:
            continue

        # Check if one contains the other (likely same product, different format)
        if len(desc_lower) > 5 and len(existing_lower) > 5:
            if desc_lower in existing_lower or existing_lower in desc_lower:
                # Use longer form as canonical
                canonical = desc if len(desc) >= len(existing_desc) else existing_desc
                variant = existing_desc if canonical == desc else desc

                already = db.query(ExtractionRule).filter(
                    ExtractionRule.scope == "line_item",
                    ExtractionRule.rule_type == "product_normalize",
                    ExtractionRule.condition_value == variant,
                    ExtractionRule.auto_generated == True,
                ).first()

                if not already:
                    create_rule(
                        db,
                        name=f"Normalisera '{_truncate(variant, 30)}' → '{_truncate(canonical, 30)}'",
                        description=f"'{variant}' och '{canonical}' verkar vara samma produkt",
                        scope="line_item",
                        rule_type="product_normalize",
                        condition_field="description",
                        condition_operator="equals",
                        condition_value=variant,
                        target_field="description",
                        action="set",
                        action_value=canonical,
                        auto_generated=True,
                        active=False,
                    )
                break  # Only one normalization rule per item


def _auto_assign_category_rule(desc: str, line: LineItem, db: Session) -> None:
    """Auto-generate category assignment rules based on the categorized result.
    If the line already has a category (from categorizer), create a rule for it."""
    if not line.category:
        return

    # Use the first significant keyword from description as the rule trigger
    keywords = _extract_category_keywords(desc)
    if not keywords:
        return

    kw = keywords[0]

    already = db.query(ExtractionRule).filter(
        ExtractionRule.scope == "line_item",
        ExtractionRule.rule_type == "category_assign",
        ExtractionRule.condition_value == kw,
        ExtractionRule.auto_generated == True,
    ).first()

    if not already:
        create_rule(
            db,
            name=f"Kategori '{line.category}' för '{kw}'",
            description=f"Produkter med '{kw}' kategoriseras som '{line.category}'",
            scope="line_item",
            rule_type="category_assign",
            condition_field="description",
            condition_operator="contains",
            condition_value=kw,
            target_field="category",
            action="set_if_empty",
            action_value=line.category,
            auto_generated=True,
            active=False,
        )


def _extract_category_keywords(desc: str) -> list[str]:
    """Extract meaningful keywords from a product description."""
    stop_words = {"och", "med", "för", "den", "det", "ett", "en", "av", "till", "från", "som", "har", "var"}
    words = re.findall(r"[a-zåäö]{3,}", desc.lower())
    return [w for w in words if w not in stop_words][:3]


def _maybe_create_vendor_normalize_rule(vendor: str, db: Session) -> None:
    existing_vendors = (
        db.query(Document.vendor)
        .filter(Document.vendor.isnot(None))
        .distinct().all()
    )
    vendor_lower = vendor.lower().strip()

    for (existing,) in existing_vendors:
        if not existing or existing.lower().strip() == vendor_lower:
            continue
        existing_lower = existing.lower().strip()

        if len(vendor_lower) > 4 and len(existing_lower) > 4:
            if vendor_lower in existing_lower or existing_lower in vendor_lower:
                already = db.query(ExtractionRule).filter(
                    ExtractionRule.rule_type == "vendor_normalize",
                    ExtractionRule.condition_value.ilike(f"%{vendor}%"),
                    ExtractionRule.auto_generated == True,
                ).first()
                if not already:
                    canonical = vendor if len(vendor) >= len(existing) else existing
                    variant = existing if canonical == vendor else vendor
                    create_rule(
                        db, name=f"Normalisera '{variant}' → '{canonical}'",
                        description=f"'{variant}' och '{canonical}' verkar vara samma leverantör",
                        scope="document", rule_type="vendor_normalize",
                        condition_field="vendor", condition_operator="equals",
                        condition_value=variant, target_field="vendor",
                        action="set", action_value=canonical,
                        auto_generated=True, active=False,
                    )
                break


def _maybe_create_default_rule(db: Session, *, name: str, scope: str, **kwargs) -> None:
    existing = db.query(ExtractionRule).filter(
        ExtractionRule.condition_field == kwargs.get("condition_field"),
        ExtractionRule.condition_value == kwargs.get("condition_value"),
        ExtractionRule.target_field == kwargs.get("target_field"),
        ExtractionRule.scope == scope,
        ExtractionRule.auto_generated == True,
    ).first()

    if not existing:
        create_rule(
            db, name=name, scope=scope,
            description=f"Auto: sätt {kwargs.get('target_field')}='{kwargs.get('action_value')}' "
                        f"när {kwargs.get('condition_field')} matchar '{kwargs.get('condition_value')}'",
            auto_generated=True, active=False, **kwargs,
        )


def _maybe_create_line_rule(db: Session, *, name: str, **kwargs) -> None:
    existing = db.query(ExtractionRule).filter(
        ExtractionRule.scope == "line_item",
        ExtractionRule.condition_field == kwargs.get("condition_field"),
        ExtractionRule.condition_value == kwargs.get("condition_value"),
        ExtractionRule.target_field == kwargs.get("target_field"),
        ExtractionRule.auto_generated == True,
    ).first()

    if not existing:
        create_rule(
            db, name=name, scope="line_item",
            auto_generated=True, active=False, **kwargs,
        )


def _truncate(s: str, length: int) -> str:
    return s[:length] + "…" if len(s) > length else s
