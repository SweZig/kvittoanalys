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


# ─── ICA Direct Scraper ───────────────────────────────────────────────────────

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
    """
    Extraherar (slug, uuid, display_name) ur kategori-HTML.
    URL-mönster: /stores/{id}/categories/{slug}/{uuid}
    """
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


def _parse_category_offers(html: str, store_id: str, category_name: str) -> list[dict]:
    """Parsar erbjudandekort ur en ICA-kategorisida."""
    soup = BeautifulSoup(html, "html.parser")
    offer_pattern = re.compile(
        rf"/stores/{store_id}/offers/([^/?#]+)/([a-f0-9-]{{36}})"
    )
    offers: list[dict] = []
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

        # Hitta produktens container
        container = a_offer.find_parent(
            lambda t: t.name in ("li", "article", "section", "div")
        )
        if not container:
            continue

        # Produktnamn från h3
        h3 = container.find("h3")
        if not h3:
            continue
        product_name = h3.get_text(" ", strip=True)

        container_text = container.get_text(" ", strip=True)

        # Priser
        m_ord = re.search(r"Tidigare pris\s*([\d,]+)\s*kr", container_text)
        ordinary_price = m_ord.group(1).replace(",", ".") if m_ord else ""

        m_pris = re.search(r"Pris(?:Ca)?\s*([\d,]+)\s*kr", container_text)
        offer_price = m_pris.group(1).replace(",", ".") if m_pris else ""

        compare_price = _parse_compare_price(container_text)
        volume = _parse_weight_volume(product_name)

        # Kvantitetsbegränsning
        m_qty = re.search(r"(Max\s+\d+[^,.\n]+)", label_raw, re.IGNORECASE)
        qty_limit = m_qty.group(1).strip() if m_qty else None

        # Rensa label
        clean_label = label_raw
        if qty_limit:
            clean_label = clean_label.replace(f" -- {qty_limit}", "").strip()

        offers.append(_make_ica_offer(
            product_name=product_name,
            offer_label=clean_label,
            offer_price=offer_price,
            ordinary_price=ordinary_price,
            compare_price=compare_price,
            volume=volume,
            category=category_name,
            is_membership=is_membership,
            qty_limit=qty_limit,
            offer_id=offer_uuid,
            store_id=store_id,
        ))

    return offers


async def _fetch_ica_direct(store_id: str, client: httpx.AsyncClient) -> dict:
    """
    Hämtar alla erbjudanden direkt från ICA:s webbshop.
    Returnerar dict med 'offers', 'source', 'error'.
    """
    base = f"{ICA_BASE}/stores/{store_id}"

    # Steg 1: Hämta kategorisidan
    html = await _fetch_html(client, f"{base}/categories")
    if not html:
        return {"offers": [], "source": "ica_direct", "error": "Kunde inte nå ICA:s webbshop"}

    if not _validate_html_structure(html):
        return {
            "offers": [],
            "source": "ica_direct",
            "error": "ICA:s HTML-struktur verkar ha förändrats – parsern behöver uppdateras",
        }

    categories = _discover_categories(html, store_id)
    if not categories:
        return {"offers": [], "source": "ica_direct", "error": "Inga kategorier hittades"}

    logger.info("ICA Direct: Hittade %d kategorier för butik %s", len(categories), store_id)

    # Steg 2: Hämta erbjudanden per kategori
    all_offers: list[dict] = []
    for slug, uuid, display_name in categories:
        url = f"{base}/categories/{slug}/{uuid}?campaigns=true&sortBy=favorite"
        cat_html = await _fetch_html(client, url)
        if not cat_html:
            logger.warning("ICA Direct: Ingen respons för kategori '%s'", display_name)
            continue

        if not _validate_html_structure(cat_html):
            return {
                "offers": all_offers,
                "source": "ica_direct",
                "error": f"ICA HTML-struktur förändrad i kategori '{display_name}'",
            }

        cat_offers = _parse_category_offers(cat_html, store_id, display_name)
        logger.info("ICA Direct: %d erbjudanden i '%s'", len(cat_offers), display_name)
        all_offers.extend(cat_offers)
        await asyncio.sleep(0.5)   # Artigt mot ICA:s server

    if len(all_offers) < ICA_MIN_OFFERS_THRESHOLD:
        return {
            "offers": all_offers,
            "source": "ica_direct",
            "error": (
                f"ICA returnerade bara {len(all_offers)} erbjudanden "
                f"(förväntar minst {ICA_MIN_OFFERS_THRESHOLD})"
            ),
        }

    return {"offers": all_offers, "source": "ica_direct", "error": None}


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
    """Kontrollerar om ICA:s direktkälla är tillgänglig och strukturellt intakt."""
    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            resp = await client.get(
                f"{ICA_BASE}/stores/{store_id}/categories",
                headers=_HEADERS,
                timeout=10.0,
            )
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            return {"status": "down", "store_id": store_id, "error": str(e)}

    is_valid = _validate_html_structure(html)
    categories = _discover_categories(html, store_id)

    return {
        "status": "ok" if is_valid else "degraded",
        "store_id": store_id,
        "html_structure_valid": is_valid,
        "categories_found": len(categories),
        "source_url": f"{ICA_BASE}/stores/{store_id}/categories",
    }
