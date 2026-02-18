"""Product categorizer using Livsmedelsverket's food database + AI fallback.

Strategy:
1. Download all ~2400 food items from Livsmedelsverket API (cached locally).
2. Fuzzy-match product descriptions against the database.
3. Return Huvudgrupp (main group) as category.
4. For non-food items, fall back to keyword matching + Claude AI.

Data source: https://dataportal.livsmedelsverket.se/livsmedel/api/v1/livsmedel
License: Creative Commons Attribution 4.0 — source: Livsmedelsverkets Livsmedelsdatabas
"""

from __future__ import annotations

import json
import os
import re
import time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

# ── Non-food categories (keyword-based, no API needed) ──────────────

NON_FOOD_CATEGORIES = {
    "hälsa": [
        "vitamin", "kosttillskott", "omega", "c-vitamin", "d-vitamin",
        "magnesium", "zink", "järntablett", "probiotika", "alvedon",
        "ipren", "nässpray", "hostmedicin", "halstablett", "plåster",
        "ibuprofen", "paracetamol", "värktablett",
    ],
    "barnprodukter": [
        "blöja", "blöjor", "nappflaska", "napp ", "välling", "ersättning",
        "barnmat", "hipp ", "semper", "babybjörn", "libero",
    ],
    "hygien": [
        "tvål", "schampo", "deodorant", "tandkräm", "toalettpapper",
        "tvätt", "disk", "rengöring", "tvättmedel", "softlan",
        "sköljmedel", "handtvål", "duschkräm", "hudkräm", "bodylotion",
        "solkräm", "raklödder", "rakblad", "hårspray", "balsam",
        "tandborste", "munskölj", "våtservett", "hushållspapper",
        "dammsugar", "allrengöring", "fönsterputs",
        "ajax", "yes ", "fairy", "mr muscle", "klorin", "wettex",
    ],
    "hushåll": [
        "glödlampa", "batteri", "ljus ", "stearinljus", "tändstickor",
        "påse", "plastpåse", "folie", "bakplåtspapper", "gladpack",
        "fryspåse", "avfallspåse", "sopsäck",
    ],
    "kontorsmaterial": [
        "papper", "penna", "kuvert", "häft", "tejp", "mapp",
        "toner", "bläck", "skrivare", "post-it", "gem", "suddgummi",
    ],
    "tjänst": [
        "konsult", "timmar", "arvode", "rådgivning", "support",
        "underhåll", "service", "abonnemang", "licens", "prenumeration",
    ],
    "transport": [
        "frakt", "porto", "leverans", "transport", "bensin", "diesel",
        "parkering", "biltvätt", "taxi", "uber", "bolt",
    ],
    "djur": [
        "hundmat", "kattmat", "djurfoder", "kattströ", "hundgodis", "husdjur",
    ],
    "tobak": [
        "cigaretter", "snus", "tobak", "nikotinpåse", "vape",
    ],
}

# ── Livsmedelsverket Huvudgrupp → our category name mapping ─────────

# The API returns Swedish group names like "Mjölk och mjölkprodukter"
# We map them to shorter, consistent category names aligned with Matpriskollen
HUVUDGRUPP_MAP = {
    "mjölk och mjölkprodukter": "mejeri",
    "ost": "ost",
    "glass": "glass",
    "matfett": "mejeri",
    "kött": "kött",
    "fisk och skaldjur": "fisk",
    "ägg": "skafferi",
    "baljväxter": "skafferi",
    "spannmål": "skafferi",
    "bröd": "bröd",
    "grönsaker": "grönsaker",
    "frukt och bär": "frukt",
    "potatis": "grönsaker",
    "nötter och frön": "skafferi",
    "socker och sötsaker": "snacks & godis",
    "drycker": "dryck",
    "alkoholhaltiga drycker": "dryck",
    "kryddor och smaksättare": "skafferi",
    "snacks": "snacks & godis",
    "diverse": "skafferi",
    "sammansatta rätter": "färdigmat",
    "soppor och buljonger": "skafferi",
    "såser": "skafferi",
    "barnmat": "barnprodukter",
    "vegetabiliska proteiner": "skafferi",
}

# ── Extra keyword matching for food sub-categories ───────────────────
# These run BEFORE Livsmedelsverket lookup for more precise categorization

FOOD_KEYWORD_CATEGORIES = {
    "ost": [
        "ost ", "ost\t", "prästost", "grevéost", "herrgårdsost", "västerbotten",
        "cheddar", "edamer", "gouda", "brie", "camembert", "mozzarella",
        "halloumi", "mascarpone", "ricotta", "parmesan", "gorgonzola",
        "cream cheese", "färskost", "cottage", "philadelph", "fetaost",
        "cheez", "babybel", "hushållsost",
    ],
    "glass": [
        "glass", "magnum", "piggelin", "cornetto", "gb ", "ben & jerry",
        "solero", "sandwich glass", "glasstrut", "gelato",
    ],
    "chark": [
        "skinka", "salami", "korv", "pålägg", "falukorv", "prinskorv",
        "leverpastej", "bacon", "pancetta", "prosciutto", "serrano",
        "chorizo", "bratwurst", "medwurst", "isterband", "fläskkorv",
        "grillkorv", "varmkorv", "korvbröd",
        "rökt ", "rökt\t", "kalkon skivad", "kycklingpålägg",
        "scan ", "bullens", "lithells",
    ],
    "färdigmat": [
        "pizza", "lasagne", "gratäng", "paj ", "pirog", "vårrull",
        "fryspizza", "mikro", "findus", "felix ", "dafgård",
        "färdigrätt", "lunch ", "middags", "pannkakor",
    ],
    "snacks & godis": [
        "chips", "popcorn", "snacks", "godis", "choklad", "kex",
        "konfekt", "karamell", "gelé", "lakrits", "tuggummi",
        "pringles", "estrella", "olw ", "marabou", "cloetta", "malaco",
        "haribo", "ahlgrens",
    ],
}

# ── Cache ────────────────────────────────────────────────────────────

_food_cache: list[dict[str, str]] | None = None
_cache_path = Path("data/livsmedelsverket_cache.json")
_CACHE_MAX_AGE_DAYS = 30


def _ensure_cache_dir():
    _cache_path.parent.mkdir(parents=True, exist_ok=True)


def _load_food_database() -> list[dict[str, str]]:
    """Load the Livsmedelsverket food database. Download if not cached."""
    global _food_cache

    if _food_cache is not None:
        return _food_cache

    # Try to load from disk cache
    if _cache_path.exists():
        try:
            cache_age = time.time() - _cache_path.stat().st_mtime
            if cache_age < _CACHE_MAX_AGE_DAYS * 86400:
                with open(_cache_path, "r", encoding="utf-8") as f:
                    _food_cache = json.load(f)
                    print(f"✅ Loaded {len(_food_cache)} foods from cache")
                    return _food_cache
        except (json.JSONDecodeError, OSError):
            pass

    # Download from API
    _food_cache = _download_food_database()
    return _food_cache


def _download_food_database() -> list[dict[str, str]]:
    """Download all food items from Livsmedelsverket API.
    
    Phase 1 (fast): Download food names — usually takes <30 seconds.
    Phase 2 (optional): Enrich with Huvudgrupp via classifications endpoint.
    Phase 2 is skipped on first run for speed; call /api/v1/categorizer/enrich to run it.
    Group is estimated from food name patterns in the meantime.
    """
    import urllib.request
    import urllib.error

    print("⬇️  Downloading Livsmedelsverket food database...")
    foods: list[dict[str, str]] = []

    try:
        offset = 0
        limit = 500

        while True:
            url = (
                f"https://dataportal.livsmedelsverket.se/livsmedel/api/v1/livsmedel"
                f"?offset={offset}&limit={limit}&sprak=1"
            )
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            if not data:
                break

            items = data if isinstance(data, list) else data.get("livsmedel", data.get("items", []))
            if not items:
                break

            for item in items:
                nummer = item.get("nummer") or item.get("Nummer")
                namn = item.get("namn") or item.get("Namn") or ""
                if not nummer or not namn:
                    continue

                # Estimate group from name (fast, no extra API calls)
                group = _estimate_group_from_name(namn)

                foods.append({
                    "id": str(nummer),
                    "name": namn.lower().strip(),
                    "name_original": namn,
                    "group": group,
                })

            if len(items) < limit:
                break
            offset += limit

        print(f"✅ Downloaded {len(foods)} food items")

        # Cache to disk
        _ensure_cache_dir()
        try:
            with open(_cache_path, "w", encoding="utf-8") as f:
                json.dump(foods, f, ensure_ascii=False, indent=1)
            print(f"✅ Cached to {_cache_path}")
        except OSError as e:
            print(f"⚠️ Could not save cache: {e}")

        return foods

    except Exception as e:
        print(f"⚠️ Failed to download Livsmedelsverket data: {e}")
        return []


# Name-based group estimation (no API calls needed)
# Maps Swedish food name patterns to Huvudgrupp-style categories
_NAME_GROUP_PATTERNS = [
    # Ost (before mejeri — more specific first)
    (r"\b(ost\b|mozzarella|halloumi|mascarpone|ricotta|cottage|färskost|cream cheese|cheddar|edamer|gouda|brie|camembert|parmesan|gorgonzola|fetaost|prästost|grevé|herrgård|västerbotten)", "ost"),
    # Glass (before socker — more specific first)
    (r"\b(glass\b|magnum|piggelin|cornetto|gelato|sorbet|strut)", "glass"),
    # Mejeri (without ost/glass)
    (r"\b(mjölk|fil\b|filmjölk|yoghurt|grädde|crème|creme|fraiche|kvarg|kefir|smör)", "mjölk och mjölkprodukter"),
    # Chark (before kött — more specific first)
    (r"\b(korv|bacon|skinka|salami|pålägg|leverpastej|prosciutto|chorizo|bratwurst|medwurst|isterband|falukorv|prinskorv|pancetta|serrano)", "chark"),
    # Kött
    (r"\b(kött|fläsk|nöt|oxe?|kalv|lamm|kyckling|höns|anka|vilt|lever|hjärta|blandfärs|nötfärs|kycklingfilé|entrecote|kotlett|biff)", "kött"),
    # Fisk
    (r"\b(fisk|lax|torsk|sill|makrill|tonfisk|räk|krabba|hummer|musslor|skaldjur|sej|kolja|rödspätta|abborre|gädda|öring|kaviar)", "fisk och skaldjur"),
    # Bröd
    (r"\b(bröd|limpa|bulle|fralla|knäckebröd|tortilla|wrap|bagel|croissant|scone|kaka\b|muffin)", "bröd"),
    # Grönsaker
    (r"\b(tomat|gurka|paprika|lök|morot|potatis|sallad|spenat|broccoli|blomkål|zucchini|aubergine|svamp|champinjon|rödbet|selleri|purjolök|kål|vitkål|rödkål|squash|majs\b|ärtor|böna|grönsak|sparris|kronärtskocka|fänkål)", "grönsaker"),
    # Frukt
    (r"\b(äpple|päron|banan|apelsin|citron|lime|mango|ananas|melon|vindruvor?|jordgubb|hallon|blåbär|kiwi|persika|plommon|nektarin|clementin|mandarin|passionsfrukt|granatäpple|fikon|dadel|avokado|frukt)", "frukt och bär"),
    # Färdigmat
    (r"\b(pizza|lasagne|gratäng|paj\b|pirog|vårrull|färdigrätt|pannkakor)", "sammansatta rätter"),
    # Spannmål / Skafferi base
    (r"\b(ris\b|pasta|spagetti|makaroner|nudlar|bulgur|couscous|quinoa|havre|müsli|flingor|mjöl|gryn|korn)", "spannmål"),
    # Dryck
    (r"\b(juice|saft|läsk|vatten|te\b|kaffe|öl\b|vin\b|cider|dricka|cola|fanta|sprite|mineralvatten|smoothie|kombucha|energidryck|lemonad|champagne|whisky|vodka|gin\b|rom\b)", "drycker"),
    # Snacks & godis
    (r"\b(socker|godis|choklad|konfekt|karamell|gelé|lakritts?|tuggummi|chips|popcorn|snacks|nötter|jordnöt|mandel|cashew|pistasch)", "snacks"),
    # Matfett
    (r"\b(margarin|olivolja|rapsolja|kokosolja|smör\b|matfett)", "matfett"),
    # Baljväxter
    (r"\b(linser|kikärt|böna|bönor|tofu|soja|tempeh)", "baljväxter"),
    # Ägg
    (r"\b(ägg\b)", "ägg"),
    # Kryddor
    (r"\b(salt\b|peppar\b|kanel|vanilj|curry|oregano|timjan|basilika|persilja|dill\b|senap|ketchup|sås|vinäger|ättika|soja)", "kryddor och smaksättare"),
]


def _estimate_group_from_name(name: str) -> str:
    """Estimate food group from the Swedish food name using regex patterns."""
    name_lower = name.lower()
    for pattern, group in _NAME_GROUP_PATTERNS:
        if re.search(pattern, name_lower):
            return group
    return "diverse"


def enrich_cache_with_groups() -> int:
    """Enrich the cached food database with Huvudgrupp from the API.
    Call this to get accurate groups (replaces name-based estimates).
    Returns the number of items enriched."""
    import urllib.request

    foods = _load_food_database()
    if not foods:
        return 0

    enriched_count = 0
    total = len(foods)

    for i, food in enumerate(foods):
        try:
            url = (
                f"https://dataportal.livsmedelsverket.se/livsmedel/api/v1/livsmedel"
                f"/{food['id']}/klassificeringar"
            )
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            if isinstance(data, list):
                for item in data:
                    hg = item.get("huvudgrupp") or item.get("Huvudgrupp")
                    if hg:
                        food["group"] = hg.lower().strip()
                        enriched_count += 1
                        break

            if (i + 1) % 100 == 0:
                print(f"  ... {i + 1}/{total} enriched")

            if (i + 1) % 50 == 0:
                time.sleep(0.3)

        except Exception:
            pass

    # Save updated cache
    _ensure_cache_dir()
    try:
        global _food_cache
        _food_cache = foods
        with open(_cache_path, "w", encoding="utf-8") as f:
            json.dump(foods, f, ensure_ascii=False, indent=1)
    except OSError:
        pass

    print(f"✅ Enriched {enriched_count}/{total} foods with Huvudgrupp")
    return enriched_count


# ── Fuzzy matching ───────────────────────────────────────────────────

def _clean_product_name(desc: str) -> str:
    """Clean a product description for matching.
    Remove brand names, weights, quantities, and noise."""
    desc = desc.lower().strip()
    # Remove common noise: quantities, weights, percentages
    desc = re.sub(r"\d+\s*(st|kg|g|ml|l|cl|dl|förp|pkt|pk|x)\b", "", desc)
    desc = re.sub(r"\d+[.,]\d+\s*(kr|sek)?", "", desc)
    desc = re.sub(r"\d+%", "", desc)
    # Remove brand markers
    desc = re.sub(r"\b(eko|ekologisk|krav|fairtrade|garant|ica|coop|willys|hemköp|axfood)\b", "", desc)
    # Clean up whitespace
    desc = re.sub(r"\s+", " ", desc).strip()
    return desc


def _fuzzy_match(query: str, foods: list[dict[str, str]], threshold: float = 0.55) -> dict[str, str] | None:
    """Find the best matching food item using fuzzy string matching."""
    query_clean = _clean_product_name(query)
    if not query_clean or len(query_clean) < 2:
        return None

    best_match = None
    best_score = 0.0

    query_words = set(query_clean.split())

    for food in foods:
        food_name = food["name"]

        # Quick check: any word overlap?
        food_words = set(food_name.split())
        overlap = query_words & food_words
        if not overlap:
            # Try substring containment
            if not any(w in food_name for w in query_words if len(w) > 3):
                continue

        # SequenceMatcher for actual similarity
        score = SequenceMatcher(None, query_clean, food_name).ratio()

        # Boost score for exact word matches
        if overlap:
            score += 0.15 * len(overlap)

        # Boost if query is a substring of food name or vice versa
        if query_clean in food_name or food_name in query_clean:
            score += 0.2

        if score > best_score:
            best_score = score
            best_match = food

    if best_match and best_score >= threshold:
        return best_match

    return None


# ── Main categorization function ────────────────────────────────────

def categorize_product(description: str) -> str | None:
    """Categorize a single product description.

    Returns a category string or None if not categorizable.

    Strategy:
    1. Check non-food keywords first (hygiene, health, office, etc.)
    2. Check food keyword categories (ost, chark, glass, färdigmat, etc.)
    3. Try Livsmedelsverket fuzzy match
    4. Return None (caller should use AI fallback)
    """
    if not description or len(description.strip()) < 2:
        return None

    desc_lower = description.lower().strip()

    # ── Step 1: Non-food keyword matching ──
    for category, keywords in NON_FOOD_CATEGORIES.items():
        if any(kw in desc_lower for kw in keywords):
            return category

    # ── Step 2: Food keyword matching (granular categories) ──
    for category, keywords in FOOD_KEYWORD_CATEGORIES.items():
        if any(kw in desc_lower for kw in keywords):
            return category

    # ── Step 3: Livsmedelsverket fuzzy match ──
    foods = _load_food_database()
    if foods:
        match = _fuzzy_match(desc_lower, foods)
        if match:
            group = match.get("group", "diverse")
            # Map Huvudgrupp to our category name
            category = HUVUDGRUPP_MAP.get(group, None)
            if category:
                return category
            # If no mapping, return the group as-is
            return group

    return None


def categorize_products_batch(descriptions: list[str]) -> list[str | None]:
    """Categorize multiple product descriptions efficiently.

    Pre-loads the database once, then matches all items.
    Returns a list of categories (None for uncategorized).
    """
    # Pre-load database
    foods = _load_food_database()

    results = []
    for desc in descriptions:
        if not desc or len(desc.strip()) < 2:
            results.append(None)
            continue

        desc_lower = desc.lower().strip()

        # Non-food keywords first
        matched = False
        for category, keywords in NON_FOOD_CATEGORIES.items():
            if any(kw in desc_lower for kw in keywords):
                results.append(category)
                matched = True
                break

        if matched:
            continue

        # Food keyword matching (granular categories)
        for category, keywords in FOOD_KEYWORD_CATEGORIES.items():
            if any(kw in desc_lower for kw in keywords):
                results.append(category)
                matched = True
                break

        if matched:
            continue

        # Livsmedelsverket fuzzy match
        if foods:
            match = _fuzzy_match(desc_lower, foods)
            if match:
                group = match.get("group", "diverse")
                category = HUVUDGRUPP_MAP.get(group, group)
                results.append(category)
                continue

        results.append(None)

    return results


def ai_categorize_batch(items: list[tuple[int, str]]) -> dict[int, str]:
    """Use Claude to categorize items that other methods couldn't handle.

    Args:
        items: list of (index, description) tuples

    Returns:
        dict mapping index → category
    """
    if not items:
        return {}

    try:
        import anthropic
        from app.config import settings

        all_categories = (
            list(set(HUVUDGRUPP_MAP.values()))
            + list(NON_FOOD_CATEGORIES.keys())
            + list(FOOD_KEYWORD_CATEGORIES.keys())
            + ["övrigt"]
        )
        # Deduplicate
        all_categories = sorted(set(all_categories))

        prompt = (
            "Kategorisera följande produktrader från ett svenskt kvitto/faktura.\n"
            f"Tillgängliga kategorier: {', '.join(all_categories)}\n\n"
            "Viktiga distinktioner:\n"
            "- 'ost' = alla typer av ost (prästost, cheddar, fetaost, etc.)\n"
            "- 'glass' = glass och sorbet\n"
            "- 'mejeri' = mjölk, yoghurt, grädde, smör, fil, kvarg (EJ ost/glass)\n"
            "- 'chark' = korv, skinka, salami, bacon, pålägg, leverpastej\n"
            "- 'kött' = rått kött, färs, filé, entrecote (EJ chark)\n"
            "- 'skafferi' = pasta, ris, mjöl, olja, kryddor, konserver, ägg\n"
            "- 'färdigmat' = pizza, lasagne, frysta rätter, mikromåltider\n"
            "- 'snacks & godis' = chips, choklad, godis, popcorn, kex\n"
            "- 'barnprodukter' = blöjor, välling, barnmat\n"
            "- 'hälsa' = vitaminer, kosttillskott, receptfri medicin\n\n"
            "Svara ENBART med JSON — en lista med objekt: "
            '[{"index": 0, "category": "kategori"}, ...]\n\n'
            "Produkter:\n"
            + "\n".join(f'{idx}: {desc}' for idx, desc in items)
        )

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model=settings.claude_model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            results = json.loads(match.group(0))
            return {
                r["index"]: r["category"].lower().strip()
                for r in results
                if "index" in r and "category" in r
            }

    except Exception as e:
        print(f"⚠️ AI categorization failed: {e}")

    return {}


# ── Category migration map (old → new) ──────────────────────────────

CATEGORY_MIGRATION = {
    "livsmedel": "skafferi",
    "godis": "snacks & godis",
    "snacks": "snacks & godis",
}

# Categories that were too broad and need re-evaluation
CATEGORIES_TO_RESPLIT = {"mejeri", "kött", "hygien"}


def get_all_categories() -> list[str]:
    """Return the full canonical list of categories."""
    cats = set(HUVUDGRUPP_MAP.values())
    cats.update(NON_FOOD_CATEGORIES.keys())
    cats.update(FOOD_KEYWORD_CATEGORIES.keys())
    cats.add("övrigt")
    return sorted(cats)
