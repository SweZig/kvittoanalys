"""ICA Campaign Service — hämtar kampanjer direkt från ICA:s webbshop.

Primär källa:  handlaprivatkund.ica.se  (butikspecifik, realtid, ingen auth)
Fallback-källa: matpriskollen.se/api/v1  (kedjegenerellt, samma API som campaign_service.py)

Fallback aktiveras automatiskt om ICA-källan:
  • inte svarar (nätverksfel / timeout)
  • returnerar förändrad HTML-struktur (saknar /offers/-länkar)
  • returnerar färre än ICA_MIN_OFFERS_THRESHOLD erbjudanden

Erbjudandeformatet matchar campaign_service._parse_offer() så att övrig
kod i appen kan hantera ICA-data och matpriskollen-data identiskt.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

import httpx
from bs4 import BeautifulSoup

# Återanvänd matpriskollens befintliga parsning
from app.services.campaign_service import (
    MPK_BASE,
    REQUEST_DELAY,
    _extract_chain_name,
    _parse_offer,
)

logger = logging.getLogger(__name__)

# ─── Konfiguration ────────────────────────────────────────────────────────────

ICA_BASE = "https://handlaprivatkund.ica.se"
ICA_MIN_OFFERS_THRESHOLD = 5   # Färre än så → aktivera fallback
ICA_HTTP_TIMEOUT = 15.0
ICA_RETRY_DELAY = 2.0
ICA_MAX_RETRIES = 2

# Mappning slug → visningsnamn för kvittoanalysens kategorier
ICA_CATEGORY_MAP: dict[str, str] = {
    "frukt-grönt":            "Frukt & Grönt",
    "kött-chark-fågel":       "Kött, Chark & Fågel",
    "fisk-skaldjur":          "Fisk & Skaldjur",
    "mejeri-ost":             "Mejeri & Ost",
    "bröd-kakor":             "Bröd & Kakor",
    "vegetariskt":            "Vegetariskt",
    "färdigmat":              "Färdigmat",
    "barn":                   "Barn",
    "glass-godis-snacks":     "Glass, Godis & Snacks",
    "dryck":                  "Dryck",
    "skafferi":               "Skafferi",
    "fryst":                  "Fryst",
    "apotek-hälsa-skönhet":   "Apotek, Hälsa & Skönhet",
    "städ-tvätt-papper":      "Städ, Tvätt & Papper",
    "djur":                   "Djur",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "sv-SE,sv;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


# ─── Hjälpfunktioner ─────────────────────────────────────────────────────────

def _parse_price_str(text: str) -> str:
    """Returnerar priset som sträng t.ex. '25.95' ur '25,95 kr'."""
    m = re.search(r"([\d]+[,.][\d]+|[\d]+)", text.replace("\xa0", "").replace(" ", ""))
    return m.group(1).replace(",", ".") if m else ""


def _parse_compare_price(text: str) -> str:
    """Extraherar jämförpris ur t.ex. '(60,00 kr/kg)' → '60.00 kr/kg'."""
    m = re.search(r"([\d,. ]+)\s*kr/([\w]+)", text)
    if m:
        price = m.group(1).replace(",", ".").replace(" ", "")
        return f"{price} kr/{m.group(2)}"
    return ""


def _parse_weight_volume(text: str) -> str:
    """Extraherar '500g', '1,5l', '4-p' etc. ur produktnamn."""
    m = re.search(r"(\d+[\d,.]*\s*(?:g|kg|ml|l|cl|st|p|pack))", text, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def _validate_html_structure(html: str) -> bool:
    """Kontrollerar att ICA:s HTML fortfarande har förväntade länktyper."""
    return bool(
        re.search(r"/offers/", html) or
        re.search(r"/products/", html) or
        re.search(r"/categories/", html)
    )


def _make_ica_offer(
    product_name: str,
    offer_label: str,
    offer_price: str,
    ordinary_price: str,
    compare_price: str,
    volume: str,
    category: str,
    is_membership: bool,
    qty_limit: Optional[str],
    offer_id: str,
    store_id: str,
) -> dict:
    """
    Bygger ett erbjudandeobjekt i samma format som campaign_service._parse_offer().
    Gör det enkelt att mixa ICA-direct och matpriskollen-data i routes.
    """
    return {
        "id": offer_id,
        "product": {
            "name": product_name,
            "brand": "",
            "origin": "",
            "category": category,
            "parent_category": "",
        },
        "price": offer_price,
        "compare_price": compare_price,
        "regular_price": ordinary_price,
        "volume": volume,
        "description": offer_label,
        "condition": qty_limit or "",
        "valid_from": None,
        "valid_to": None,
        "requires_membership": is_membership,
        "requires_coupon": False,
        "image_url": "",
        # ICA-specifika fält (ignoreras av befintlig kod om den inte känner till dem)
        "ica_offer_id": offer_id,
        "ica_store_id": store_id,
        "source": "ica_direct",
    }


# ─── ICA Direct — JSON API (primär) ──────────────────────────────────────────
#
# handlaprivatkund.ica.se/{storeId}/api/v5/products  (JSON, ingen auth)
# Dokumentation: github.com/HampusAndersson01/ICA-Products-API
#

ICA_API_BASE = "https://handlaprivatkund.ica.se"
ICA_API_PAGE_SIZE = 200  # Maximera per request

# Butikstyp-prioritet: Maxi har flest kampanjer
_ICA_STORE_PRIORITY = {
    "maxi": 0,
    "stormarknad": 0,
    "kvantum": 1,
    "supermarket": 2,
    "nära": 3,
    "nara": 3,
}


def _store_sort_key(store: dict) -> int:
    """Sorterar butiker: Maxi först, Nära sist."""
    name = (store.get("name") or "").lower()
    for keyword, prio in _ICA_STORE_PRIORITY.items():
        if keyword in name:
            return prio
    return 5


def _parse_ica_api_product(product: dict, store_id: str) -> Optional[dict]:
    """
    Konverterar en ICA JSON API-produkt till vårt standardformat.
    Returnerar None om produkten inte har ett kampanjerbjudande.
    """
    # Identifiera kampanjerbjudande
    offer = product.get("offer") or product.get("promotion") or {}
    potentials = product.get("potentialPromotions") or []

    # Grundpriser
    price_val = product.get("price") or product.get("currentPrice") or 0
    price_str = ""
    regular_str = ""

    # Prishantering — varianter av ICA:s JSON-struktur
    if isinstance(price_val, dict):
        price_str = str(price_val.get("amount") or price_val.get("value") or "")
        regular_str = str((price_val.get("regularAmount") or price_val.get("ordinaryPrice") or {})
                         if isinstance(price_val.get("regularAmount"), dict) else "")
    elif isinstance(price_val, (int, float)):
        price_str = str(price_val)

    # Alternativa prisfält
    if not price_str:
        for key in ("priceValue", "currentPrice", "campaignPrice", "discountedPrice"):
            v = product.get(key)
            if v is not None:
                price_str = str(v) if not isinstance(v, dict) else str(v.get("amount", v.get("value", "")))
                break

    if not regular_str:
        for key in ("ordinaryPrice", "regularPrice", "originalPrice", "priceBeforeDiscount"):
            v = product.get(key)
            if v is not None:
                regular_str = str(v) if not isinstance(v, dict) else str(v.get("amount", v.get("value", "")))
                break

    # Kampanjvillkor
    offer_label = ""
    is_campaign = False
    is_membership = False
    qty_limit = None

    if offer:
        offer_label = offer.get("conditionLabel") or offer.get("description") or offer.get("text") or ""
        is_campaign = True
        is_membership = bool(offer.get("loyaltyProgram") or "stammis" in str(offer).lower())
        qty_limit = offer.get("maxQuantity") or offer.get("limitPerCustomer")

    # Kolla potentialPromotions
    for promo in potentials:
        if promo.get("conditionLabel") or promo.get("description"):
            offer_label = offer_label or promo.get("conditionLabel") or promo.get("description") or ""
            is_campaign = True
            is_membership = is_membership or bool("stammis" in str(promo).lower())
            break

    # Kolla rabatt-flaggor
    for flag in ("isCampaign", "isOffer", "isPromotion", "hasDiscount", "onSale"):
        if product.get(flag):
            is_campaign = True
            break

    # Om fortfarande inget: kolla om ordinarie pris > aktuellt pris
    if not is_campaign and regular_str and price_str:
        try:
            p = float(price_str.replace(",", "."))
            r = float(regular_str.replace(",", "."))
            if r > p:
                is_campaign = True
                offer_label = offer_label or f"Spara {round((1 - p/r) * 100)}%"
        except (ValueError, ZeroDivisionError):
            pass

    if not is_campaign:
        return None

    # Produktinfo
    name = product.get("name") or product.get("productName") or product.get("title") or ""
    if not name:
        return None

    brand = product.get("brand") or product.get("brandName") or ""
    if isinstance(brand, dict):
        brand = brand.get("name") or ""
    category = product.get("category") or product.get("categoryName") or ""
    if isinstance(category, dict):
        category = category.get("name") or category.get("displayName") or ""

    volume = _parse_weight_volume(name)
    if not volume:
        volume = product.get("packageSize") or product.get("size") or ""

    compare_price = product.get("comparisonPrice") or product.get("unitPrice") or ""
    if isinstance(compare_price, dict):
        cp_amount = compare_price.get("amount") or compare_price.get("value") or ""
        cp_unit = compare_price.get("unit") or compare_price.get("unitOfMeasure") or ""
        compare_price = f"{cp_amount} kr/{cp_unit}" if cp_amount else ""
    elif compare_price:
        compare_price = str(compare_price)

    product_id = str(product.get("id") or product.get("productId") or product.get("sku") or name[:20])

    return _make_ica_offer(
        product_name=name,
        offer_label=offer_label,
        offer_price=str(price_str).replace(",", "."),
        ordinary_price=str(regular_str).replace(",", "."),
        compare_price=compare_price,
        volume=volume,
        category=category if isinstance(category, str) else "",
        is_membership=is_membership,
        qty_limit=str(qty_limit) if qty_limit else None,
        offer_id=product_id,
        store_id=store_id,
    )


async def _fetch_ica_json_api(store_id: str, client: httpx.AsyncClient) -> dict:
    """
    Hämtar kampanjprodukter via ICA:s JSON API.
    URL: handlaprivatkund.ica.se/{storeId}/api/v5/products
    Kräver INGEN autentisering.

    Optimerat med parallell paginering.
    """
    base_url = f"{ICA_API_BASE}/{store_id}/api/v5/products"
    api_headers = {
        "Accept": "application/json",
        "User-Agent": _HEADERS["User-Agent"],
        "Accept-Language": "sv-SE,sv;q=0.9",
    }

    # ── Steg 1: Hämta första sidan + ta reda på totalt antal ──
    try:
        resp = await client.get(
            base_url,
            params={"limit": ICA_API_PAGE_SIZE, "offset": 0},
            headers=api_headers,
            timeout=ICA_HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            return {"offers": [], "source": "ica_direct",
                    "error": f"ICA API HTTP {resp.status_code}"}
        data = resp.json()
    except Exception as e:
        return {"offers": [], "source": "ica_direct", "error": f"ICA API-fel: {e}"}

    # Extrahera produktlistan
    def _extract_products(d):
        if isinstance(d, list):
            return d
        if isinstance(d, dict):
            for key in ("items", "products", "results", "data", "content"):
                if key in d and isinstance(d[key], list):
                    return d[key]
        return []

    first_products = _extract_products(data)
    total_count = 0
    if isinstance(data, dict):
        total_count = data.get("totalCount") or data.get("total") or data.get("count") or 0
        logger.info("ICA JSON API: toppnivånycklar: %s", list(data.keys())[:15])

    logger.info("ICA JSON API: butik %s — %d produkter på sida 1 (total: %s)",
                store_id, len(first_products), total_count or "?")

    # Debug: logga första produktens struktur
    if first_products and isinstance(first_products[0], dict):
        import json as _json
        p0 = first_products[0]
        logger.info("ICA JSON API: produkt[0] nycklar: %s", list(p0.keys()))
        try:
            logger.info("ICA JSON API: produkt[0] JSON: %s",
                        _json.dumps(p0, ensure_ascii=False, default=str)[:2000])
        except Exception:
            pass

    if not first_products:
        return {"offers": [], "source": "ica_direct", "error": "Inga produkter i svaret"}

    # ── Steg 2: Parallellhämta resterande sidor ──
    remaining_pages_data: list[list] = []

    if len(first_products) >= ICA_API_PAGE_SIZE and total_count > ICA_API_PAGE_SIZE:
        # Beräkna hur många fler sidor vi behöver
        remaining = (total_count or ICA_API_PAGE_SIZE * 10) - ICA_API_PAGE_SIZE
        num_extra_pages = min((remaining + ICA_API_PAGE_SIZE - 1) // ICA_API_PAGE_SIZE, 8)  # Max 8 extra

        async def _fetch_page(offset: int) -> list:
            try:
                r = await client.get(
                    base_url,
                    params={"limit": ICA_API_PAGE_SIZE, "offset": offset},
                    headers=api_headers,
                    timeout=ICA_HTTP_TIMEOUT,
                )
                if r.status_code != 200:
                    return []
                return _extract_products(r.json())
            except Exception:
                return []

        offsets = [ICA_API_PAGE_SIZE * (i + 1) for i in range(num_extra_pages)]

        # Hämta 3 sidor åt gången (artigt men snabbt)
        for batch_start in range(0, len(offsets), 3):
            batch = offsets[batch_start:batch_start + 3]
            results = await asyncio.gather(*[_fetch_page(o) for o in batch])
            for page_products in results:
                if page_products:
                    remaining_pages_data.append(page_products)
                else:
                    break  # Tom sida = slut
            if any(len(p) < ICA_API_PAGE_SIZE for p in results):
                break  # Sista sidan nådd
            await asyncio.sleep(0.15)  # Kort paus mellan batchar

    # ── Steg 3: Parsa alla produkter ──
    all_products = first_products
    for page_products in remaining_pages_data:
        all_products.extend(page_products)

    all_offers: list[dict] = []
    for product in all_products:
        if not isinstance(product, dict):
            continue
        parsed = _parse_ica_api_product(product, store_id)
        if parsed:
            all_offers.append(parsed)

    logger.info("ICA JSON API: %d kampanjprodukter av %d totalt (butik %s, %d sidor)",
                len(all_offers), len(all_products), store_id,
                1 + len(remaining_pages_data))

    if not all_offers and all_products:
        return {
            "offers": [],
            "source": "ica_direct",
            "error": f"Hittade {len(all_products)} produkter men inga kampanjer",
        }

    return {
        "offers": all_offers,
        "source": "ica_direct",
        "error": None if all_offers else "Inga produkter returnerades",
    }


# ─── Legacy HTML scraper (fallback) ─────────────────────────────────────────

async def _fetch_html(client: httpx.AsyncClient, url: str) -> Optional[str]:
    """Hämtar HTML med retry-logik."""
    for attempt in range(ICA_MAX_RETRIES + 1):
        try:
            resp = await client.get(url, headers=_HEADERS, timeout=ICA_HTTP_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (404, 410):
                return None
            logger.warning("ICA HTTP %s för %s (försök %d)", e.response.status_code, url, attempt + 1)
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            logger.warning("ICA nätverksfel %s (försök %d): %s", url, attempt + 1, e)
        if attempt < ICA_MAX_RETRIES:
            await asyncio.sleep(ICA_RETRY_DELAY)
    return None


def _discover_categories(html: str, store_id: str) -> list[tuple[str, str, str]]:
    """Extraherar (slug, uuid, display_name) ur kategori-HTML."""
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(
        rf"/stores/{store_id}/categories/([^/?#]+)/([a-f0-9-]{{36}})"
    )
    seen: set[str] = set()
    categories = []
    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if m:
            slug, uuid = m.group(1), m.group(2)
            if uuid not in seen:
                seen.add(uuid)
                display_name = ICA_CATEGORY_MAP.get(
                    slug, slug.replace("-", " ").title()
                )
                categories.append((slug, uuid, display_name))
    return categories


async def _fetch_ica_html_scraper(store_id: str, client: httpx.AsyncClient) -> dict:
    """Legacy HTML-scraper — används som fallback om JSON API inte fungerar."""
    base = f"{ICA_BASE}/stores/{store_id}"

    html = await _fetch_html(client, f"{base}/categories")
    if not html:
        return {"offers": [], "source": "ica_direct", "error": "Kunde inte nå ICA:s webbshop (HTML)"}

    if not _validate_html_structure(html):
        return {"offers": [], "source": "ica_direct",
                "error": "ICA:s HTML-struktur har förändrats"}

    categories = _discover_categories(html, store_id)
    if not categories:
        return {"offers": [], "source": "ica_direct", "error": "Inga kategorier i HTML"}

    all_offers: list[dict] = []
    for slug, uuid, display_name in categories:
        url = f"{base}/categories/{slug}/{uuid}?campaigns=true&sortBy=favorite"
        cat_html = await _fetch_html(client, url)
        if not cat_html:
            continue

        # Parsa erbjudanden ur kategorisidan
        soup = BeautifulSoup(cat_html, "html.parser")
        offer_pattern = re.compile(
            rf"/stores/{store_id}/offers/([^/?#]+)/([a-f0-9-]{{36}})"
        )
        processed: set[str] = set()

        for a_offer in soup.find_all("a", href=offer_pattern):
            m = offer_pattern.search(a_offer["href"])
            if not m:
                continue
            offer_uuid = m.group(2)
            if offer_uuid in processed:
                continue
            processed.add(offer_uuid)

            label_raw = a_offer.get_text(" ", strip=True)
            is_membership = "stammis" in label_raw.lower()

            container = a_offer.find_parent(
                lambda t: t.name in ("li", "article", "section", "div")
            )
            if not container:
                continue

            h3 = container.find("h3")
            if not h3:
                continue
            product_name = h3.get_text(" ", strip=True)
            container_text = container.get_text(" ", strip=True)

            m_ord = re.search(r"Tidigare pris\s*([\d,]+)\s*kr", container_text)
            ordinary_price = m_ord.group(1).replace(",", ".") if m_ord else ""

            m_pris = re.search(r"Pris(?:Ca)?\s*([\d,]+)\s*kr", container_text)
            offer_price = m_pris.group(1).replace(",", ".") if m_pris else ""

            m_qty = re.search(r"(Max\s+\d+[^,.\n]+)", label_raw, re.IGNORECASE)
            qty_limit = m_qty.group(1).strip() if m_qty else None

            all_offers.append(_make_ica_offer(
                product_name=product_name,
                offer_label=label_raw,
                offer_price=offer_price,
                ordinary_price=ordinary_price,
                compare_price=_parse_compare_price(container_text),
                volume=_parse_weight_volume(product_name),
                category=display_name,
                is_membership=is_membership,
                qty_limit=qty_limit,
                offer_id=offer_uuid,
                store_id=store_id,
            ))

        await asyncio.sleep(0.5)

    return {
        "offers": all_offers,
        "source": "ica_direct",
        "error": None if len(all_offers) >= ICA_MIN_OFFERS_THRESHOLD else
                 f"HTML-scraper: bara {len(all_offers)} erbjudanden",
    }


async def _fetch_ica_direct(store_id: str, client: httpx.AsyncClient) -> dict:
    """
    Hämtar ICA-kampanjer — JSON API först, HTML-scraper som fallback.
    """
    # Primär: JSON API (snabbare, stabilare, immun mot frontend-ändringar)
    result = await _fetch_ica_json_api(store_id, client)
    if result.get("offers"):
        return result

    # Fallback: Legacy HTML scraper
    logger.info("ICA JSON API returnerade inga erbjudanden, provar HTML-scraper")
    html_result = await _fetch_ica_html_scraper(store_id, client)
    if html_result.get("offers"):
        return html_result

    # Båda misslyckades — returnera JSON-felmeddelandet (mer informativt)
    return result


# ─── Matpriskollen Fallback (återanvänder befintligt API) ─────────────────────

async def _fetch_ica_matpriskollen(
    lat: float,
    lon: float,
    max_distance_km: float,
    client: httpx.AsyncClient,
) -> dict:
    """
    Fallback: hämtar ICA-erbjudanden från matpriskollen.se/api/v1.
    Återanvänder samma API-anrop som campaign_service.fetch_campaigns()
    men filtrerar enbart ICA-butiker.
    """
    # Steg 1: Hitta ICA-butiker i närheten
    try:
        resp = await client.get(f"{MPK_BASE}/stores", params={"lat": lat, "lon": lon})
        resp.raise_for_status()
        stores_raw = resp.json()
    except httpx.HTTPError as e:
        return {"offers": [], "source": "matpriskollen", "error": f"matpriskollen stores: {e}"}

    ica_stores = [
        s for s in stores_raw
        if float(s.get("dist", "999")) <= max_distance_km
        and "ica" in s.get("name", "").lower()
    ]

    if not ica_stores:
        return {
            "offers": [],
            "source": "matpriskollen",
            "error": "Inga ICA-butiker hittades inom angivet avstånd på matpriskollen",
        }

    logger.info("matpriskollen fallback: %d ICA-butiker hittades", len(ica_stores))

    # Steg 2: Hämta erbjudanden för ICA-butikerna
    all_offers: list[dict] = []
    seen_ids: set[int] = set()

    batch_size = 5
    for i in range(0, len(ica_stores), batch_size):
        batch = ica_stores[i:i + batch_size]
        tasks = [
            client.get(
                f"{MPK_BASE}/stores/{s['key']}/offers",
                params={"lat": lat, "lon": lon},
            )
            for s in batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for store, result in zip(batch, results):
            if isinstance(result, Exception):
                logger.warning("matpriskollen: fel för %s: %s", store["name"], result)
                continue
            try:
                result.raise_for_status()
                data = result.json()
            except Exception:
                continue

            for raw_offer in data.get("offers") or []:
                oid = raw_offer.get("id", 0)
                if oid not in seen_ids:
                    seen_ids.add(oid)
                    parsed = _parse_offer(raw_offer)
                    parsed["source"] = "matpriskollen"
                    all_offers.append(parsed)

        if i + batch_size < len(ica_stores):
            await asyncio.sleep(REQUEST_DELAY)

    return {"offers": all_offers, "source": "matpriskollen", "error": None}


# ─── Publik API ───────────────────────────────────────────────────────────────

async def fetch_ica_campaigns(
    store_id: str,
    lat: float,
    lon: float,
    max_distance_km: float = 5.0,
    fallback_enabled: bool = True,
) -> dict:
    """
    Hämtar ICA-kampanjer med automatisk fallback.

    Args:
        store_id:          ICA:s webbshop-ID (t.ex. "1004222" = ICA Kvantum Södermalm)
        lat / lon:         Koordinater för fallback-sökning på matpriskollen
        max_distance_km:   Max avstånd för matpriskollen-fallback
        fallback_enabled:  Om False används aldrig matpriskollen som källa

    Returns:
        {
            "store_id":     str,
            "source":       "ica_direct" | "matpriskollen" | "none",
            "offer_count":  int,
            "fetched_at":   str (ISO),
            "fallback_reason": str | None,   # Satt om fallback aktiverades
            "error":        str | None,      # Satt om båda källorna misslyckades
            "offers":       list[dict],      # Samma format som campaign_service
        }
    """
    async with httpx.AsyncClient(follow_redirects=True) as client:

        # ── Primär: ICA Direct ──
        logger.info("ICA: Försöker direkthämtning för butik %s", store_id)
        direct = await _fetch_ica_direct(store_id, client)

        if direct["error"] is None:
            # Framgång – returnera direkt
            logger.info(
                "ICA Direct: Lyckades – %d erbjudanden för butik %s",
                len(direct["offers"]), store_id,
            )
            return {
                "store_id": store_id,
                "source": "ica_direct",
                "offer_count": len(direct["offers"]),
                "fetched_at": datetime.now().isoformat(),
                "fallback_reason": None,
                "error": None,
                "offers": direct["offers"],
            }

        # ── Fallback behövs ──
        fallback_reason = direct["error"]
        logger.warning(
            "ICA Direct misslyckades: %s – %s",
            fallback_reason,
            "aktiverar matpriskollen-fallback" if fallback_enabled else "fallback avstängd",
        )

        if not fallback_enabled:
            return {
                "store_id": store_id,
                "source": "none",
                "offer_count": len(direct["offers"]),
                "fetched_at": datetime.now().isoformat(),
                "fallback_reason": fallback_reason,
                "error": fallback_reason,
                "offers": direct["offers"],   # Kan vara delvis ifylld
            }

        # ── Matpriskollen Fallback ──
        fallback = await _fetch_ica_matpriskollen(lat, lon, max_distance_km, client)

        if fallback["error"]:
            both_failed = f"ICA Direct: {fallback_reason} | matpriskollen: {fallback['error']}"
            logger.error("Alla ICA-källor misslyckades: %s", both_failed)
            return {
                "store_id": store_id,
                "source": "none",
                "offer_count": 0,
                "fetched_at": datetime.now().isoformat(),
                "fallback_reason": fallback_reason,
                "error": both_failed,
                "offers": [],
            }

        logger.info(
            "matpriskollen fallback: Lyckades – %d ICA-erbjudanden",
            len(fallback["offers"]),
        )
        return {
            "store_id": store_id,
            "source": "matpriskollen",
            "offer_count": len(fallback["offers"]),
            "fetched_at": datetime.now().isoformat(),
            "fallback_reason": fallback_reason,
            "error": None,
            "offers": fallback["offers"],
        }


async def get_ica_categories(store_id: str) -> dict:
    """
    Returnerar ICA:s kategoristruktur för en butik.
    Användbart för att mappa kvittorader mot kategorier.
    """
    async with httpx.AsyncClient(follow_redirects=True) as client:
        html = await _fetch_html(client, f"{ICA_BASE}/stores/{store_id}/categories")

    if not html:
        return {"store_id": store_id, "categories": [], "error": "Kunde inte nå ICA"}

    categories = _discover_categories(html, store_id)
    return {
        "store_id": store_id,
        "categories": [
            {
                "slug": slug,
                "uuid": uuid,
                "display_name": display_name,
                "url": f"{ICA_BASE}/stores/{store_id}/categories/{slug}/{uuid}",
            }
            for slug, uuid, display_name in categories
        ],
        "error": None,
    }


async def check_ica_health(store_id: str) -> dict:
    """Kontrollerar om ICA:s direktkälla är tillgänglig."""
    async with httpx.AsyncClient(follow_redirects=True) as client:
        # Primär: JSON API
        try:
            resp = await client.get(
                f"{ICA_API_BASE}/{store_id}/api/v5/products",
                params={"limit": 5, "offset": 0},
                headers={
                    "Accept": "application/json",
                    "User-Agent": _HEADERS["User-Agent"],
                },
                timeout=10.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                products = []
                if isinstance(data, list):
                    products = data
                elif isinstance(data, dict):
                    for key in ("items", "products", "results", "data", "content"):
                        if key in data and isinstance(data[key], list):
                            products = data[key]
                            break
                total = 0
                if isinstance(data, dict):
                    total = data.get("totalCount") or data.get("total") or len(products)
                return {
                    "status": "ok" if products else "degraded",
                    "store_id": store_id,
                    "categories_found": total or len(products),
                    "api": "json_v5",
                    "source_url": f"{ICA_API_BASE}/{store_id}/api/v5/products",
                }
        except Exception as e:
            logger.warning("ICA health JSON API-fel: %s", e)

        # Fallback: HTML
        try:
            resp = await client.get(
                f"{ICA_BASE}/stores/{store_id}/categories",
                headers=_HEADERS,
                timeout=10.0,
            )
            resp.raise_for_status()
            html = resp.text
            is_valid = _validate_html_structure(html)
            categories = _discover_categories(html, store_id)
            return {
                "status": "ok" if is_valid and categories else "degraded",
                "store_id": store_id,
                "categories_found": len(categories),
                "api": "html_scraper",
                "source_url": f"{ICA_BASE}/stores/{store_id}/categories",
            }
        except Exception as e:
            return {"status": "down", "store_id": store_id, "error": str(e)}


# ─── Store Discovery ────────────────────────────────────────────────────────

# ica.se/butiker/handla-online/{city}/ lists ALL ICA stores with online shopping
# This is server-rendered HTML (not SPA) and contains handlaprivatkund.ica.se/stores/{id} links
ICA_SE_STORES_URL = "https://www.ica.se/butiker/handla-online/{city}/"

# Common Swedish city name → URL slug mapping
_CITY_SLUGS: dict[str, str] = {
    "stockholm": "stockholm",
    "göteborg": "goteborg",
    "gothenburg": "goteborg",
    "malmö": "malmo",
    "uppsala": "uppsala",
    "västerås": "vasteras",
    "örebro": "orebro",
    "linköping": "linkoping",
    "norrköping": "norrkoping",
    "helsingborg": "helsingborg",
    "jönköping": "jonkoping",
    "umeå": "umea",
    "lund": "lund",
    "borås": "boras",
    "sundsvall": "sundsvall",
    "gävle": "gavle",
    "eskilstuna": "eskilstuna",
    "karlstad": "karlstad",
    "växjö": "vaxjo",
    "halmstad": "halmstad",
    "luleå": "lulea",
    "trollhättan": "trollhattan",
    "östersund": "ostersund",
    "borlänge": "borlange",
    "falun": "falun",
    "kalmar": "kalmar",
    "skövde": "skovde",
    "kristianstad": "kristianstad",
    "karlskrona": "karlskrona",
    "skellefteå": "skelleftea",
    "uddevalla": "uddevalla",
    "varberg": "varberg",
    "nyköping": "nykoping",
    "lidingö": "lidingo",
    "sollentuna": "sollentuna",
    "nacka": "nacka",
    "täby": "taby",
    "huddinge": "huddinge",
    "södertälje": "sodertalje",
    "tumba": "tumba",
    "solna": "solna",
    "sundbyberg": "sundbyberg",
    "tyresö": "tyreso",
    "järfälla": "jarfalla",
    "sandviken": "sandviken",
    "ockelbo": "ockelbo",
    "hofors": "hofors",
    "gävle": "gavle",
}


def _city_to_slug(city: str) -> str:
    """Konverterar stadsnamn till URL-slug för ica.se."""
    city_lower = city.lower().strip()
    if city_lower in _CITY_SLUGS:
        return _CITY_SLUGS[city_lower]
    # Generisk: byt åäö → aao, ta bort specialtecken
    slug = city_lower
    for src, dst in [("å", "a"), ("ä", "a"), ("ö", "o"), ("é", "e"), ("ü", "u")]:
        slug = slug.replace(src, dst)
    slug = re.sub(r"[^a-z0-9-]", "", slug.replace(" ", "-"))
    return slug


async def _discover_from_ica_se(
    client: httpx.AsyncClient, city: str, max_stores: int = 5,
) -> list[dict]:
    """
    Hämtar ICA Handla-butiker från ica.se/butiker/handla-online/{city}/.
    Denna sida är server-renderad HTML och innehåller
    handlaprivatkund.ica.se/stores/{storeId}-länkar.
    """
    slug = _city_to_slug(city)
    url = ICA_SE_STORES_URL.format(city=slug)

    try:
        resp = await client.get(url, headers={
            "User-Agent": _HEADERS["User-Agent"],
            "Accept": "text/html,application/xhtml+xml,*/*",
        }, timeout=12.0)

        if resp.status_code != 200:
            logger.info("ica.se butikssida %s → HTTP %d", url, resp.status_code)
            return []

        html = resp.text
    except Exception as e:
        logger.warning("ica.se butikssida misslyckades: %s", e)
        return []

    # Extrahera alla handlaprivatkund.ica.se/stores/{id}-länkar
    store_pattern = re.compile(r"handlaprivatkund\.ica\.se/stores/(\d{5,8})")
    found_ids: dict[str, str] = {}  # id → name

    soup = BeautifulSoup(html, "html.parser")
    for a_tag in soup.find_all("a", href=store_pattern):
        m = store_pattern.search(a_tag["href"])
        if not m:
            continue
        sid = m.group(1)
        if sid in found_ids:
            continue

        # Hitta butiksnamnet: det finns i den närmaste överliggande butikslänken
        # eller i sibling-text som "ICA Nära Banér"
        name = ""
        # Kontrollera butikssidan-länk i närheten
        parent = a_tag.find_parent(["div", "li", "section", "article"])
        if parent:
            # Sök efter "ICA ..." i texten
            for text in parent.stripped_strings:
                if text.startswith("ICA "):
                    name = text
                    break
            # Alternativt: a-tag till ica.se/butiker/ som innehåller butiksnamn
            if not name:
                store_link = parent.find("a", href=re.compile(r"ica\.se/butiker/"))
                if store_link:
                    link_text = store_link.get_text(strip=True)
                    if link_text.startswith("ICA ") or "ica" in link_text.lower():
                        name = link_text

        if not name:
            name = a_tag.get_text(strip=True) or f"ICA (butik {sid})"

        found_ids[sid] = name

    stores = [{"id": sid, "name": name, "source": "ica_se"} for sid, name in found_ids.items()]
    # Prioritera: Maxi > Kvantum > Supermarket > Nära (Maxi har flest kampanjer)
    stores.sort(key=_store_sort_key)
    logger.info("ica.se: Hittade %d ICA Handla-butiker i %s: %s",
                len(stores), city, [s["name"] for s in stores[:6]])
    return stores[:max_stores]


async def discover_ica_stores(
    lat: float,
    lon: float,
    max_distance_km: float = 10.0,
    max_stores: int = 5,
    city: str | None = None,
) -> list[dict]:
    """
    Upptäcker ICA Handla-butiker med online-shopping.

    Primär källa: ica.se/butiker/handla-online/{stad}/ (server-renderad, pålitlig)
    Fallback:     matpriskollen-namn (utan Handla-ID)

    Returnerar: [{"id": "1004222", "name": "ICA Kvantum Södermalm", ...}, ...]
    """
    async with httpx.AsyncClient(follow_redirects=True, timeout=20.0) as client:

        # ── Primär: ica.se butikslista ──
        if city:
            stores = await _discover_from_ica_se(client, city, max_stores)
            if stores:
                return stores

            # Försök utan å/ä/ö-konvertering (ibland fungerar originalnamn)
            logger.info("discover_ica_stores: försöker alternativ slug för '%s'", city)

        # ── Fallback: Matpriskollen ──
        try:
            resp = await client.get(f"{MPK_BASE}/stores", params={"lat": lat, "lon": lon})
            resp.raise_for_status()
            stores_raw = resp.json()
            mpk_ica = [
                s for s in stores_raw
                if float(s.get("dist", "999")) <= max_distance_km
                and "ica" in s.get("name", "").lower()
            ]
            mpk_ica.sort(key=lambda s: float(s.get("dist", "999")))

            if mpk_ica:
                logger.info("Matpriskollen fallback: %d ICA-butiker", len(mpk_ica))
                return [{
                    "id": None,
                    "name": s.get("name", ""),
                    "distance_km": str(s.get("dist", "?")),
                    "source": "matpriskollen",
                } for s in mpk_ica[:max_stores]]
        except Exception as e:
            logger.warning("Matpriskollen misslyckades: %s", e)

    logger.info("discover_ica_stores: inga butiker hittades")
    return []
