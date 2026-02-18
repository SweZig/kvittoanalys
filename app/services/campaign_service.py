"""Campaign service — fetches deals from matpriskollen.se.

Integrated directly into Kvittoanalys so no separate API process is needed.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# ─── Matpriskollen base URL ─────────────────────────────────────────────────
MPK_BASE = "https://matpriskollen.se/api/v1"
REQUEST_DELAY = 0.15  # Seconds between batches — be nice to their server


# ─── Svenska orter → koordinater ────────────────────────────────────────────
SWEDISH_CITIES: dict[str, tuple[float, float]] = {
    # Stor-Stockholm: Centrum
    "stockholm": (59.3293, 18.0686),
    "gamla stan": (59.3233, 18.0711),
    "södermalm": (59.3150, 18.0730),
    "kungsholmen": (59.3340, 18.0370),
    "östermalm": (59.3380, 18.0890),
    "vasastan": (59.3440, 18.0530),
    # Stor-Stockholm: Norr
    "solna": (59.3600, 18.0000),
    "sundbyberg": (59.3610, 17.9720),
    "danderyd": (59.3930, 18.0270),
    "täby": (59.4440, 18.0690),
    "vallentuna": (59.5340, 18.0770),
    "österåker": (59.4810, 18.3010),
    "vaxholm": (59.4020, 18.3490),
    "lidingö": (59.3670, 18.1470),
    "norrtälje": (59.7580, 18.7070),
    "upplands väsby": (59.5180, 17.9130),
    "upplands-bro": (59.5150, 17.6340),
    "sigtuna": (59.6170, 17.7230),
    "märsta": (59.6270, 17.8560),
    "sollentuna": (59.4280, 17.9510),
    "järfälla": (59.4290, 17.8350),
    "kista": (59.4030, 17.9440),
    "bromma": (59.3380, 17.9440),
    "spånga": (59.3830, 17.9070),
    "hässelby": (59.3640, 17.8330),
    "vällingby": (59.3630, 17.8730),
    "jakobsberg": (59.4230, 17.8310),
    "barkarby": (59.4150, 17.8600),
    # Stor-Stockholm: Söder
    "huddinge": (59.2370, 17.9820),
    "botkyrka": (59.2080, 17.8370),
    "tumba": (59.1990, 17.8320),
    "tullinge": (59.2170, 17.8970),
    "salem": (59.1950, 17.7690),
    "södertälje": (59.1950, 17.6260),
    "nykvarn": (59.1780, 17.4340),
    "nynäshamn": (58.9030, 17.9480),
    "haninge": (59.1740, 18.1510),
    "handen": (59.1680, 18.1430),
    "tyresö": (59.2440, 18.2280),
    "nacka": (59.3100, 18.1640),
    "värmdö": (59.3190, 18.3760),
    "gustavsberg": (59.3270, 18.3870),
    "älvsjö": (59.2780, 18.0130),
    "farsta": (59.2570, 18.0930),
    "skärholmen": (59.2760, 17.9530),
    "enskede": (59.2830, 18.0730),
    "hägersten": (59.2960, 18.0130),
    "bandhagen": (59.2680, 18.0470),
    # Stor-Stockholm: Öster
    "saltsjöbaden": (59.2830, 18.3020),
    # Stor-Stockholm: Väster
    "ekerö": (59.2930, 17.8030),
    # Övriga större svenska städer
    "göteborg": (57.7089, 11.9746),
    "malmö": (55.6050, 13.0038),
    "uppsala": (59.8586, 17.6389),
    "linköping": (58.4108, 15.6214),
    "västerås": (59.6099, 16.5448),
    "örebro": (59.2753, 15.2134),
    "norrköping": (58.5942, 16.1826),
    "helsingborg": (56.0465, 12.6945),
    "jönköping": (57.7826, 14.1618),
    "umeå": (63.8258, 20.2630),
    "lund": (55.7047, 13.1910),
    "borås": (57.7210, 12.9401),
    "sundsvall": (62.3908, 17.3069),
    "gävle": (60.6749, 17.1413),
    "eskilstuna": (59.3666, 16.5077),
    "karlstad": (59.3793, 13.5036),
    "växjö": (56.8777, 14.8091),
    "halmstad": (56.6745, 12.8578),
    "luleå": (65.5848, 22.1547),
    "trollhättan": (58.2837, 12.2886),
    "östersund": (63.1792, 14.6357),
    "borlänge": (60.4858, 15.4364),
    "falun": (60.6065, 15.6355),
    "kalmar": (56.6634, 16.3566),
    "skövde": (58.3868, 13.8455),
    "kristianstad": (56.0294, 14.1567),
    "karlskrona": (56.1612, 15.5869),
    "skellefteå": (64.7507, 20.9528),
    "uddevalla": (58.3498, 11.9381),
    "varberg": (57.1057, 12.2508),
    "falkenberg": (56.9050, 12.4913),
    "nyköping": (58.7530, 17.0086),
    "visby": (57.6348, 18.2948),
    "sandviken": (60.6166, 16.7756),
}

_KNOWN_CHAINS = [
    "ICA Maxi", "ICA Kvantum", "ICA Supermarket", "ICA Nära", "ICA",
    "Coop X:-TRA", "Coop Forum", "Coop Konsum", "Coop",
    "Willys Hemma", "Willys",
    "Hemköp",
    "Lidl",
    "City Gross",
    "Dollarstore",
    "Rusta",
    "Tempo",
    "Netto",
    "ÖoB",
    "Matöppet",
    "Flygfyren",
]


# ─── Public helpers ──────────────────────────────────────────────────────────

def get_cities() -> dict[str, dict[str, float]]:
    """Return all known cities with coordinates."""
    return {
        name: {"lat": coords[0], "lon": coords[1]}
        for name, coords in sorted(SWEDISH_CITIES.items())
    }


def resolve_city(city: str) -> tuple[float, float]:
    """Look up coordinates for a Swedish city name."""
    key = city.strip().lower()
    if key in SWEDISH_CITIES:
        return SWEDISH_CITIES[key]
    # Fuzzy: partial match
    for name, coords in SWEDISH_CITIES.items():
        if key in name or name in key:
            return coords
    return None


def resolve_coordinates(
    city: str | None, lat: float | None, lon: float | None
) -> tuple[float, float] | None:
    """Resolve coordinates from city name or explicit lat/lon."""
    if city:
        return resolve_city(city)
    if lat is not None and lon is not None:
        return (lat, lon)
    return None


# ─── Internal helpers ────────────────────────────────────────────────────────

def _unix_to_iso(ts: int) -> str | None:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return None


def _extract_chain_name(store_name: str) -> str:
    for chain in _KNOWN_CHAINS:
        if store_name.lower().startswith(chain.lower()):
            return chain
    return store_name.split(",")[0].split("  ")[0].strip()


def _parse_offer(raw: dict) -> dict:
    """Parse a raw matpriskollen offer into a clean dict."""
    product_raw = raw.get("product") or {}
    categories = product_raw.get("categories") or []
    cat_name = categories[0]["name"] if categories else ""
    parent_cat = ""
    if categories and "parent_category" in categories[0]:
        parent_cat = (categories[0]["parent_category"] or {}).get("name", "")

    return {
        "id": raw.get("id", 0),
        "product": {
            "name": product_raw.get("name") or "",
            "brand": product_raw.get("brand") or "",
            "origin": product_raw.get("origin") or "",
            "category": cat_name,
            "parent_category": parent_cat,
        },
        "price": raw.get("price") or "",
        "compare_price": raw.get("comprice") or "",
        "regular_price": raw.get("regular") or "",
        "volume": raw.get("volume") or "",
        "description": raw.get("description") or "",
        "condition": raw.get("condition") or "",
        "valid_from": _unix_to_iso(raw.get("validFrom") or 0),
        "valid_to": _unix_to_iso(raw.get("validTo") or 0),
        "requires_membership": raw.get("requiresMembershipCard") or False,
        "requires_coupon": raw.get("requiresCoupon") or False,
        "image_url": raw.get("imageURL") or "",
    }


# ─── Main fetch logic ───────────────────────────────────────────────────────

async def fetch_campaigns(
    lat: float,
    lon: float,
    max_distance_km: float = 10.0,
    max_stores: int = 30,
) -> dict:
    """
    Fetch all current campaigns for stores near a location.

    Returns a dict with city info, stores, and offers grouped by chain.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Step 1: Get nearby stores
        try:
            resp = await client.get(
                f"{MPK_BASE}/stores", params={"lat": lat, "lon": lon}
            )
            resp.raise_for_status()
            stores_raw = resp.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch stores from matpriskollen: %s", e)
            raise

        # Filter by distance
        stores_filtered = [
            s for s in stores_raw
            if float(s.get("dist", "999")) <= max_distance_km
        ][:max_stores]

        # Step 2: Fetch offers in parallel batches
        chain_offers: dict[str, list[dict]] = {}
        chain_stores: dict[str, set[str]] = {}
        stores_info = []

        batch_size = 5
        for i in range(0, len(stores_filtered), batch_size):
            batch = stores_filtered[i:i + batch_size]
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
                    logger.warning("Failed to fetch offers for %s: %s", store["name"], result)
                    continue

                try:
                    result.raise_for_status()
                    data = result.json()
                except Exception:
                    continue

                chain = _extract_chain_name(store["name"])
                stores_info.append({
                    "name": store["name"],
                    "key": store["key"],
                    "offer_count": store.get("offerCount", 0),
                    "distance_km": store.get("dist", "?"),
                    "chain": chain,
                })

                offers_list = data.get("offers") or []
                parsed = [_parse_offer(o) for o in offers_list]

                if chain not in chain_offers:
                    chain_offers[chain] = []
                    chain_stores[chain] = set()
                chain_offers[chain].extend(parsed)
                chain_stores[chain].add(store["name"])

            # Delay between batches
            if i + batch_size < len(stores_filtered):
                await asyncio.sleep(REQUEST_DELAY)

    # Build response — deduplicate offers per chain
    chains = []
    for chain_name, offers in sorted(chain_offers.items()):
        seen_ids: set[int] = set()
        unique: list[dict] = []
        for o in offers:
            oid = o["id"]
            if oid not in seen_ids:
                seen_ids.add(oid)
                unique.append(o)
        chains.append({
            "chain": chain_name,
            "stores": sorted(chain_stores.get(chain_name, set())),
            "total_offers": len(unique),
            "offers": unique,
        })

    return {
        "lat": lat,
        "lon": lon,
        "fetched_at": datetime.now().isoformat(),
        "total_stores": len(stores_info),
        "total_offers": sum(c["total_offers"] for c in chains),
        "chains": chains,
    }
