"""Category learning service — harvests product→category mappings from external sources.

Sources:
- Matpriskollen campaign offers (product name + category + parent_category)
- ICA direct offers (product name + category from erbjudanden page)

The learned mappings are stored in category_references table and used to:
1. Improve categorization of new receipt line items (before fuzzy match)
2. Periodically re-categorize existing uncategorized/poorly-categorized items
3. Suggest updates to manual category rules
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database.models import CategoryReference, ExtractionRule, LineItem

logger = logging.getLogger("category_learning")

# ── Harmonization: external category names → our canonical categories ─────────
# Goal: map matpriskollen + ICA category names to our ~22 internal categories

_HARMONIZE_MAP: dict[str, str] = {
    # ── Matpriskollen categories (from parent_category + category) ──
    "mejeri": "mejeri",
    "mjölk": "mejeri",
    "yoghurt": "mejeri",
    "fil": "mejeri",
    "grädde": "mejeri",
    "smör": "mejeri",
    "margarin": "mejeri",
    "crème fraiche": "mejeri",
    "kvarg": "mejeri",
    "ägg": "skafferi",
    "ost": "ost",
    "hårdost": "ost",
    "mjukost": "ost",
    "färskost": "ost",
    "kött": "kött",
    "nötkött": "kött",
    "fläskkött": "kött",
    "lammkött": "kött",
    "viltkött": "kött",
    "färs": "kött",
    "fågel": "kött",
    "kyckling": "kött",
    "chark": "chark",
    "charkuteri": "chark",
    "korv": "chark",
    "skinka": "chark",
    "pålägg": "chark",
    "bacon": "chark",
    "fisk": "fisk",
    "fisk och skaldjur": "fisk",
    "skaldjur": "fisk",
    "frukt": "frukt",
    "frukt och bär": "frukt",
    "bär": "frukt",
    "grönsaker": "grönsaker",
    "grönt": "grönsaker",
    "potatis": "grönsaker",
    "sallad": "grönsaker",
    "rotfrukter": "grönsaker",
    "bröd": "bröd",
    "bröd och kakor": "bröd",
    "bageri": "bröd",
    "kakor": "bröd",
    "glass": "glass",
    "glass och sorbet": "glass",
    "godis": "snacks & godis",
    "choklad": "snacks & godis",
    "snacks": "snacks & godis",
    "chips": "snacks & godis",
    "konfekt": "snacks & godis",
    "kex": "snacks & godis",
    "dryck": "dryck",
    "drycker": "dryck",
    "läsk": "dryck",
    "juice": "dryck",
    "vatten": "dryck",
    "kaffe": "dryck",
    "te": "dryck",
    "öl": "dryck",
    "vin": "dryck",
    "alkohol": "dryck",
    "skafferi": "skafferi",
    "skafferivaror": "skafferi",
    "konserv": "skafferi",
    "pasta": "skafferi",
    "ris": "skafferi",
    "kryddor": "skafferi",
    "olja": "skafferi",
    "såser": "skafferi",
    "mjöl": "skafferi",
    "socker": "skafferi",
    "fryst": "färdigmat",
    "djupfryst": "färdigmat",
    "färdigmat": "färdigmat",
    "färdigrätter": "färdigmat",
    "vegetariskt": "skafferi",  # map to closest
    "vegan": "skafferi",

    # ── ICA erbjudanden page categories ──
    "frukt & grönt": "grönsaker",
    "kött, chark & fågel": "kött",
    "fisk & skaldjur": "fisk",
    "mejeri & ost": "mejeri",
    "bröd & kakor": "bröd",
    "glass, godis & snacks": "snacks & godis",
    "apotek, hälsa & skönhet": "hälsa",
    "städ, tvätt & papper": "hygien",
    "hem & fritid": "hushåll",

    # ── Matpriskollen parent categories ──
    "hälsa": "hälsa",
    "hygien": "hygien",
    "hushåll": "hushåll",
    "barn": "barnprodukter",
    "barnprodukter": "barnprodukter",
    "djur": "djur",
    "tobak": "tobak",
}


def _harmonize_category(
    source_category: str,
    parent_category: str = "",
    product_name: str = "",
) -> str | None:
    """Map external category to our canonical category.
    
    Tries multiple strategies:
    1. Exact match on source_category
    2. Exact match on parent_category
    3. Keyword matching on product name for ambiguous cases
    """
    if not source_category:
        return None

    cat_lower = source_category.lower().strip()
    parent_lower = parent_category.lower().strip() if parent_category else ""

    # Strategy 1: direct match on category
    if cat_lower in _HARMONIZE_MAP:
        result = _HARMONIZE_MAP[cat_lower]
        # Refine: "Kött, Chark & Fågel" → check product name for chark vs kött
        if result == "kött" and product_name:
            name_lower = product_name.lower()
            chark_words = ["korv", "skinka", "salami", "bacon", "pålägg", "leverpastej",
                          "falukorv", "prinskorv", "chorizo", "medwurst"]
            if any(w in name_lower for w in chark_words):
                return "chark"
        # Refine: "Mejeri & Ost" → check for ost
        if result == "mejeri" and product_name:
            name_lower = product_name.lower()
            ost_words = ["ost", "cheddar", "brie", "mozzarella", "parmesan", "halloumi",
                        "fetaost", "grevé", "prästost", "hushållsost", "edamer"]
            if any(w in name_lower for w in ost_words):
                return "ost"
        # Refine: "Frukt & Grönt" → check for frukt
        if result == "grönsaker" and product_name:
            name_lower = product_name.lower()
            frukt_words = ["äpple", "banan", "apelsin", "citron", "vindruv", "mango",
                          "avokado", "melon", "päron", "kiwi", "ananas", "jordgubb",
                          "blåbär", "hallon", "persika", "plommon", "lime", "nektarin"]
            if any(w in name_lower for w in frukt_words):
                return "frukt"
        return result

    # Strategy 2: parent category
    if parent_lower and parent_lower in _HARMONIZE_MAP:
        return _HARMONIZE_MAP[parent_lower]

    # Strategy 3: partial matching
    for key, value in _HARMONIZE_MAP.items():
        if key in cat_lower or cat_lower in key:
            return value

    return None


def _normalize_product_name(name: str) -> str:
    """Normalize product name for matching.
    
    Strips brand, weight, volume info to get core product name.
    "Arla Mjölk 3% 1L" → "mjölk"
    "ICA Basic Falukorv 800g" → "falukorv"
    """
    if not name:
        return ""
    
    name_lower = name.lower().strip()
    
    # Remove common volume/weight patterns
    name_lower = re.sub(r"\d+[\d,.]*\s*(?:g|kg|ml|cl|l|dl|st|p|pack|port)\b", "", name_lower)
    # Remove percentage patterns
    name_lower = re.sub(r"\d+[\d,.]*\s*%", "", name_lower)
    # Remove price patterns
    name_lower = re.sub(r"\d+[,:]\d+\s*kr", "", name_lower)
    # Remove common brand prefixes (keep product name)
    name_lower = re.sub(
        r"^(ica\s+(?:basic|selection|gott\s+liv|i\s+love\s+eco)?|"
        r"garant|eldorado|coop\s+(?:änglamark)?|willys|"
        r"arla|scan|atria|findus|felix|dafgårds?|"
        r"gevalia|zoégas|löfbergs|lavazza|"
        r"fazer|pågen|korvbrödsbagarn)\s*[.,]?\s*",
        "",
        name_lower,
    )
    
    # Clean up whitespace and dots
    name_lower = re.sub(r"[.,]+$", "", name_lower)
    name_lower = re.sub(r"\s+", " ", name_lower).strip()
    
    return name_lower


# ── Learning: harvest categories from campaign data ──────────────────────────

def learn_from_campaigns(db: Session, campaign_data: dict) -> dict[str, int]:
    """Extract product→category mappings from campaign response and store them.
    
    Args:
        db: database session
        campaign_data: full campaign response from the campaigns endpoint
        
    Returns:
        {"new": X, "updated": Y, "skipped": Z}
    """
    stats = {"new": 0, "updated": 0, "skipped": 0}
    now = datetime.now(timezone.utc)
    
    for chain in campaign_data.get("chains", []):
        chain_name = chain.get("chain", "").lower()
        source = "ica_direct" if "ica" in chain_name else "matpriskollen"
        
        for offer in chain.get("offers", []):
            product = offer.get("product", {})
            raw_name = product.get("name", "").strip()
            brand = product.get("brand", "").strip()
            source_cat = product.get("category", "").strip()
            parent_cat = product.get("parent_category", "").strip()
            
            if not raw_name or not source_cat:
                stats["skipped"] += 1
                continue
            
            # Harmonize to our category
            our_cat = _harmonize_category(source_cat, parent_cat, raw_name)
            if not our_cat:
                stats["skipped"] += 1
                continue
            
            # Normalize name for matching
            norm_name = _normalize_product_name(raw_name)
            if not norm_name or len(norm_name) < 2:
                # Use raw name if normalization stripped too much
                norm_name = raw_name.lower().strip()
            
            # Upsert into category_references
            existing = db.query(CategoryReference).filter(
                CategoryReference.product_name == norm_name,
                CategoryReference.source == source,
            ).first()
            
            if existing:
                existing.times_seen = (existing.times_seen or 0) + 1
                existing.last_seen_at = now
                # Update category if source changed
                if existing.category != our_cat:
                    existing.category = our_cat
                    existing.source_category = source_cat
                if brand and not existing.brand:
                    existing.brand = brand
                stats["updated"] += 1
            else:
                ref = CategoryReference(
                    product_name=norm_name,
                    brand=brand[:100] if brand else None,
                    category=our_cat,
                    source_category=source_cat,
                    source=source,
                    confidence=0.85 if source_cat else 0.6,
                    times_seen=1,
                    last_seen_at=now,
                    created_at=now,
                )
                db.add(ref)
                stats["new"] += 1
    
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Failed to save category references: %s", e)
    
    logger.info("Category learning: %d new, %d updated, %d skipped", 
                stats["new"], stats["updated"], stats["skipped"])
    return stats


# ── Lookup: use learned categories for categorization ────────────────────────

def lookup_learned_category(description: str, db: Session) -> str | None:
    """Look up a product description against learned category references.
    
    Returns the most confident category match, or None.
    Used by the categorizer as the first check before fuzzy matching.
    """
    if not description or len(description.strip()) < 2:
        return None
    
    desc_lower = description.lower().strip()
    norm_desc = _normalize_product_name(description)
    
    # Strategy 1: exact match on normalized name
    if norm_desc:
        ref = db.query(CategoryReference).filter(
            CategoryReference.product_name == norm_desc,
        ).order_by(
            CategoryReference.confidence.desc(),
            CategoryReference.times_seen.desc(),
        ).first()
        
        if ref and ref.confidence >= 0.6:
            return ref.category
    
    # Strategy 2: LIKE match (product name contained in description or vice versa)
    refs = db.query(CategoryReference).filter(
        CategoryReference.confidence >= 0.6,
    ).all()
    
    best_match: CategoryReference | None = None
    best_score = 0.0
    
    for ref in refs:
        ref_name = ref.product_name
        if not ref_name:
            continue
        
        # Check if reference name is in description or vice versa
        if ref_name in desc_lower or desc_lower in ref_name:
            score = ref.confidence * min(len(ref_name), len(desc_lower)) / max(len(ref_name), len(desc_lower))
            # Boost for longer matches (more specific)
            score *= (1 + len(ref_name) / 50)
            # Boost for frequently seen
            score *= min(1.5, 1 + (ref.times_seen or 1) / 20)
            
            if score > best_score:
                best_score = score
                best_match = ref
    
    if best_match and best_score > 0.3:
        return best_match.category
    
    return None


# ── Bulk lookup with caching for batch operations ────────────────────────────

def build_reference_lookup(db: Session) -> dict[str, str]:
    """Build an in-memory lookup dict from all category references.
    
    Returns: {"normalized_product_name": "category", ...}
    Only includes high-confidence entries (>= 0.6).
    """
    refs = db.query(
        CategoryReference.product_name,
        CategoryReference.category,
        CategoryReference.confidence,
        CategoryReference.times_seen,
    ).filter(
        CategoryReference.confidence >= 0.6,
    ).all()
    
    # Group by product_name, pick highest confidence
    lookup: dict[str, tuple[str, float]] = {}
    for name, cat, conf, times_seen in refs:
        score = (conf or 0.6) * min(2.0, 1 + (times_seen or 1) / 10)
        if name not in lookup or score > lookup[name][1]:
            lookup[name] = (cat, score)
    
    return {name: cat for name, (cat, _) in lookup.items()}


def match_from_lookup(description: str, lookup: dict[str, str]) -> str | None:
    """Match a product description against the pre-built lookup dict."""
    if not description:
        return None
    
    desc_lower = description.lower().strip()
    norm = _normalize_product_name(description)
    
    # Exact match
    if norm and norm in lookup:
        return lookup[norm]
    
    # Substring match — find longest matching reference
    best_key = ""
    for key in lookup:
        if len(key) >= 3 and key in desc_lower and len(key) > len(best_key):
            best_key = key
    
    if best_key:
        return lookup[best_key]
    
    return None


# ── Re-categorization job ────────────────────────────────────────────────────

def recategorize_line_items(
    db: Session,
    force: bool = False,
    user_id: int | None = None,
) -> dict[str, Any]:
    """Re-categorize line items using learned references + existing rules.
    
    Strategy:
    1. Build lookup from category_references
    2. Load all active category_assign rules (manual rules take priority)
    3. For each uncategorized (or force=True for all) line item:
       a. Check manual rules first (never override)
       b. Check learned references
       c. Run standard categorizer
       d. Update if improved
    4. Generate new auto-rules for frequently seen patterns
    
    Args:
        db: database session
        force: if True, re-evaluate ALL items (not just uncategorized)
        user_id: if set, only recategorize this user's items
        
    Returns:
        stats dict with counts
    """
    from app.services.categorizer import categorize_product
    
    stats = {
        "total_checked": 0,
        "updated_from_references": 0,
        "updated_from_categorizer": 0,
        "already_correct": 0,
        "manual_rules_preserved": 0,
        "new_rules_created": 0,
        "rules_updated": 0,
    }
    
    # Build learned reference lookup
    lookup = build_reference_lookup(db)
    logger.info("Recategorize: loaded %d reference mappings", len(lookup))
    
    # Load manual rules — these take absolute priority
    manual_rules = db.query(ExtractionRule).filter(
        ExtractionRule.scope == "line_item",
        ExtractionRule.rule_type == "category_assign",
        ExtractionRule.auto_generated == False,
        ExtractionRule.active == True,
    ).all()
    
    manual_rule_map: dict[str, str] = {}
    for rule in manual_rules:
        cv = (rule.condition_value or "").lower().strip()
        if cv and rule.action_value:
            manual_rule_map[cv] = rule.action_value.lower().strip()
    
    logger.info("Recategorize: %d manual rules loaded", len(manual_rule_map))
    
    # Query line items
    query = db.query(LineItem).filter(LineItem.description.isnot(None))
    if user_id:
        from app.database.models import Document
        query = query.join(Document).filter(Document.user_id == user_id)
    if not force:
        # Only items without category, or with "övrigt"/"skafferi" (often wrong)
        query = query.filter(
            (LineItem.category.is_(None)) |
            (LineItem.category == "") |
            (LineItem.category == "övrigt")
        )
    
    items = query.all()
    logger.info("Recategorize: checking %d line items (force=%s)", len(items), force)
    
    # Track new patterns for auto-rule creation
    pattern_counts: dict[str, dict[str, int]] = {}  # desc → {category: count}
    
    batch_size = 500
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        
        for item in batch:
            stats["total_checked"] += 1
            desc = (item.description or "").strip()
            desc_lower = desc.lower()
            
            if not desc or len(desc) < 2:
                continue
            
            # Check if manual rule applies — never override
            has_manual_rule = False
            for rule_desc, rule_cat in manual_rule_map.items():
                if rule_desc in desc_lower:
                    if item.category and item.category.lower() == rule_cat:
                        stats["manual_rules_preserved"] += 1
                        has_manual_rule = True
                        break
                    elif not item.category or item.category.lower() != rule_cat:
                        # Manual rule says different category → apply it
                        item.category = rule_cat
                        stats["manual_rules_preserved"] += 1
                        has_manual_rule = True
                        break
            
            if has_manual_rule:
                continue
            
            old_cat = (item.category or "").lower()
            new_cat = None
            source = None
            
            # Try learned references first
            ref_cat = match_from_lookup(desc, lookup)
            if ref_cat:
                new_cat = ref_cat
                source = "reference"
            
            # Fall back to standard categorizer
            if not new_cat:
                std_cat = categorize_product(desc)
                if std_cat:
                    new_cat = std_cat
                    source = "categorizer"
            
            if new_cat and new_cat.lower() != old_cat:
                # Don't downgrade: replace "övrigt"/"" but be careful with existing
                if not old_cat or old_cat in ("övrigt", ""):
                    item.category = new_cat
                    if source == "reference":
                        stats["updated_from_references"] += 1
                    else:
                        stats["updated_from_categorizer"] += 1
                    
                    # Track for auto-rule creation
                    if desc_lower not in pattern_counts:
                        pattern_counts[desc_lower] = {}
                    pattern_counts[desc_lower][new_cat] = pattern_counts[desc_lower].get(new_cat, 0) + 1
                elif force and new_cat != old_cat:
                    # In force mode, update if reference is more specific
                    if source == "reference" and old_cat in ("skafferi", "övrigt", ""):
                        item.category = new_cat
                        stats["updated_from_references"] += 1
                    else:
                        stats["already_correct"] += 1
                else:
                    stats["already_correct"] += 1
            else:
                stats["already_correct"] += 1
        
        db.flush()
    
    # Generate auto-rules for patterns seen 3+ times
    for desc_lower, cats in pattern_counts.items():
        if not cats:
            continue
        best_cat = max(cats, key=lambda c: cats[c])
        count = cats[best_cat]
        
        if count >= 2:  # At least 2 occurrences
            existing = db.query(ExtractionRule).filter(
                ExtractionRule.scope == "line_item",
                ExtractionRule.rule_type == "category_assign",
                ExtractionRule.condition_field == "description",
                ExtractionRule.condition_value == desc_lower,
                ExtractionRule.target_field == "category",
            ).first()
            
            if existing:
                if existing.action_value != best_cat:
                    existing.action_value = best_cat
                    existing.active = True
                    stats["rules_updated"] += 1
            else:
                rule = ExtractionRule(
                    name=f"Auto: '{best_cat}' för '{desc_lower[:40]}'",
                    description=f"Inlärd från kampanjdata: '{desc_lower}' → {best_cat}",
                    scope="line_item",
                    rule_type="category_assign",
                    condition_field="description",
                    condition_operator="contains",
                    condition_value=desc_lower,
                    target_field="category",
                    action="set_if_empty",  # Don't override manual
                    action_value=best_cat,
                    auto_generated=True,
                    active=True,
                )
                db.add(rule)
                stats["new_rules_created"] += 1
    
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Recategorize commit failed: %s", e)
        raise
    
    logger.info("Recategorize done: %s", stats)
    return stats


# ── Update existing manual rules with better categories ──────────────────────

def suggest_rule_improvements(db: Session) -> list[dict[str, Any]]:
    """Analyze manual rules and suggest improvements based on learned data.
    
    Returns list of suggestions:
    [{"rule_id": 1, "current": "skafferi", "suggested": "mejeri", 
      "reason": "12 campaign references say 'mejeri'", "product": "arla mjölk"}, ...]
    """
    lookup = build_reference_lookup(db)
    if not lookup:
        return []
    
    rules = db.query(ExtractionRule).filter(
        ExtractionRule.scope == "line_item",
        ExtractionRule.rule_type == "category_assign",
        ExtractionRule.active == True,
    ).all()
    
    suggestions = []
    
    for rule in rules:
        cv = (rule.condition_value or "").strip()
        current_cat = (rule.action_value or "").lower().strip()
        
        if not cv or not current_cat:
            continue
        
        # Check if learned data suggests different category
        ref_cat = match_from_lookup(cv, lookup)
        
        if ref_cat and ref_cat.lower() != current_cat:
            # Count references supporting the suggestion
            ref_count = db.query(func.count(CategoryReference.id)).filter(
                CategoryReference.category == ref_cat,
                CategoryReference.product_name.contains(cv.lower()[:20]),
            ).scalar() or 0
            
            suggestions.append({
                "rule_id": rule.id,
                "rule_name": rule.name,
                "product": cv,
                "current_category": current_cat,
                "suggested_category": ref_cat,
                "reference_count": ref_count,
                "auto_generated": rule.auto_generated,
                "reason": f"{ref_count} kampanjreferenser säger '{ref_cat}'",
            })
    
    return suggestions


def apply_rule_improvements(db: Session, auto_only: bool = True) -> dict[str, int]:
    """Apply suggested improvements to auto-generated rules.
    
    If auto_only=True (default), only updates auto-generated rules.
    Manual rules are never auto-updated — they require explicit user action.
    """
    suggestions = suggest_rule_improvements(db)
    stats = {"updated": 0, "skipped_manual": 0}
    
    for s in suggestions:
        if not s["auto_generated"] and auto_only:
            stats["skipped_manual"] += 1
            continue
        
        rule = db.query(ExtractionRule).get(s["rule_id"])
        if rule and s["reference_count"] >= 3:  # Need at least 3 references
            rule.action_value = s["suggested_category"]
            rule.description = f"Uppdaterad av kampanjdata: {s['reason']}"
            stats["updated"] += 1
    
    if stats["updated"]:
        db.commit()
    
    logger.info("Rule improvements: %s", stats)
    return stats


# ── Stats ────────────────────────────────────────────────────────────────────

def get_learning_stats(db: Session) -> dict[str, Any]:
    """Get statistics about learned category references."""
    total = db.query(func.count(CategoryReference.id)).scalar() or 0
    by_source = dict(
        db.query(CategoryReference.source, func.count(CategoryReference.id))
        .group_by(CategoryReference.source).all()
    )
    by_category = dict(
        db.query(CategoryReference.category, func.count(CategoryReference.id))
        .group_by(CategoryReference.category)
        .order_by(func.count(CategoryReference.id).desc()).all()
    )
    
    # Coverage: how many unique line items could be categorized
    uncategorized = db.query(func.count(LineItem.id)).filter(
        LineItem.description.isnot(None),
        (LineItem.category.is_(None)) | (LineItem.category == "") | (LineItem.category == "övrigt")
    ).scalar() or 0
    
    total_items = db.query(func.count(LineItem.id)).filter(
        LineItem.description.isnot(None),
    ).scalar() or 0
    
    return {
        "total_references": total,
        "by_source": by_source,
        "by_category": dict(list(by_category.items())[:15]),
        "uncategorized_items": uncategorized,
        "total_items": total_items,
        "categorized_pct": round((1 - uncategorized / max(total_items, 1)) * 100, 1),
    }
