"""
Matpriskollen Kampanj-API
=========================
En lokal API-tjänst som hämtar aktuella kampanjer från matpriskollen.se
och exponerar datan grupperad per kedja för en valfri ort.

Starta med:
    uvicorn app:app --reload --port 8000

Dokumentation:
    http://localhost:8000/docs
"""

import asyncio
import time
from typing import Optional
from datetime import datetime

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ─── Svenska orter → koordinater ────────────────────────────────────────────
# Utökningsbar ordlista. Lägg till fler orter vid behov.
SWEDISH_CITIES: dict[str, tuple[float, float]] = {
    # ── Stor-Stockholm: Centrum ──────────────────────────────────────────
    "stockholm": (59.3293, 18.0686),
    "gamla stan": (59.3233, 18.0711),
    "södermalm": (59.3150, 18.0730),
    "kungsholmen": (59.3340, 18.0370),
    "östermalm": (59.3380, 18.0890),
    "vasastan": (59.3440, 18.0530),
    # ── Stor-Stockholm: Norr ─────────────────────────────────────────────
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
    # ── Stor-Stockholm: Söder ────────────────────────────────────────────
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
    # ── Stor-Stockholm: Öster ────────────────────────────────────────────
    "saltsjöbaden": (59.2830, 18.3020),
    # ── Stor-Stockholm: Väster ───────────────────────────────────────────
    "ekerö": (59.2930, 17.8030),
    # ── Övriga större svenska städer ─────────────────────────────────────
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

# ─── Matpriskollen base URL ─────────────────────────────────────────────────
MPK_BASE = "https://matpriskollen.se/api/v1"
REQUEST_DELAY = 0.15  # Sekunder mellan anrop – var snäll mot deras server


# ─── Pydantic-modeller ──────────────────────────────────────────────────────
class ProductInfo(BaseModel):
    name: str
    brand: str = ""
    origin: str = ""
    category: str = ""
    parent_category: str = ""


class Offer(BaseModel):
    id: int
    product: ProductInfo
    price: str
    compare_price: str = ""
    regular_price: str = ""
    volume: str = ""
    description: str = ""
    condition: str = ""
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None
    requires_membership: bool = False
    requires_coupon: bool = False
    image_url: str = ""


class StoreInfo(BaseModel):
    name: str
    key: str
    offer_count: int
    distance_km: str
    chain: str = ""


class ChainCampaigns(BaseModel):
    chain: str
    stores: list[str]
    total_offers: int
    offers: list[Offer]


class CityResponse(BaseModel):
    city: str
    lat: float
    lon: float
    fetched_at: str
    total_stores: int
    total_offers: int
    chains: list[ChainCampaigns]


# ─── Hjälpfunktioner ────────────────────────────────────────────────────────

def resolve_city(city: str) -> tuple[float, float]:
    """Slår upp koordinater för en svensk ort."""
    key = city.strip().lower()
    if key in SWEDISH_CITIES:
        return SWEDISH_CITIES[key]
    # Fuzzy: prova att hitta partiell match
    for name, coords in SWEDISH_CITIES.items():
        if key in name or name in key:
            return coords
    raise HTTPException(
        status_code=404,
        detail=f"Orten '{city}' finns inte i databasen. "
               f"Använd lat/lon-parametrar istället, eller lägg till orten i SWEDISH_CITIES."
    )


def unix_to_iso(ts: int) -> Optional[str]:
    """Konverterar unix-timestamp till ISO-datum."""
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return None


def parse_offer(raw: dict) -> Offer:
    """Konverterar ett rå-erbjudande till Offer-modell."""
    product_raw = raw.get("product", {})
    categories = product_raw.get("categories", [])
    cat_name = categories[0]["name"] if categories else ""
    parent_cat = ""
    if categories and "parent_category" in categories[0]:
        parent_cat = categories[0]["parent_category"].get("name", "")

    return Offer(
        id=raw.get("id", 0),
        product=ProductInfo(
            name=product_raw.get("name") or "",
            brand=product_raw.get("brand") or "",
            origin=product_raw.get("origin") or "",
            category=cat_name,
            parent_category=parent_cat,
        ),
        price=raw.get("price") or "",
        compare_price=raw.get("comprice") or "",
        regular_price=raw.get("regular") or "",
        volume=raw.get("volume") or "",
        description=raw.get("description") or "",
        condition=raw.get("condition") or "",
        valid_from=unix_to_iso(raw.get("validFrom") or 0),
        valid_to=unix_to_iso(raw.get("validTo") or 0),
        requires_membership=raw.get("requiresMembershipCard") or False,
        requires_coupon=raw.get("requiresCoupon") or False,
        image_url=raw.get("imageURL") or "",
    )


def extract_chain_name(store_name: str) -> str:
    """Extraherar kedjenamn från butiksnamn."""
    known_chains = [
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
    for chain in known_chains:
        if store_name.lower().startswith(chain.lower()):
            return chain
    # Fallback: allt före första mellanslaget/kommatecknet
    return store_name.split(",")[0].split("  ")[0].strip()


async def fetch_stores(client: httpx.AsyncClient, lat: float, lon: float) -> list[dict]:
    """Hämtar alla butiker nära en position."""
    resp = await client.get(f"{MPK_BASE}/stores", params={"lat": lat, "lon": lon})
    resp.raise_for_status()
    return resp.json()


async def fetch_store_detail(client: httpx.AsyncClient, store_key: str, lat: float, lon: float) -> dict:
    """Hämtar butiksdetaljer inkl. chainName."""
    resp = await client.get(f"{MPK_BASE}/stores/{store_key}", params={"lat": lat, "lon": lon})
    resp.raise_for_status()
    return resp.json()


async def fetch_offers(client: httpx.AsyncClient, store_key: str, lat: float, lon: float) -> dict:
    """Hämtar alla erbjudanden för en butik."""
    resp = await client.get(f"{MPK_BASE}/stores/{store_key}/offers", params={"lat": lat, "lon": lon})
    resp.raise_for_status()
    return resp.json()


async def fetch_all_campaigns(
    lat: float,
    lon: float,
    max_distance_km: float = 15.0,
    max_stores: int = 50,
) -> tuple[list[dict], dict[str, list[Offer]]]:
    """
    Hämtar alla kampanjer för alla butiker inom radie.
    Returnerar (stores_info, chain_offers_dict).
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Steg 1: Hämta butikslistor
        stores_raw = await fetch_stores(client, lat, lon)

        # Filtrera på avstånd
        stores_filtered = [
            s for s in stores_raw
            if float(s.get("dist", "999")) <= max_distance_km
        ][:max_stores]

        # Steg 2: Hämta erbjudanden parallellt i batchar om 5
        stores_info = []
        chain_offers: dict[str, list[Offer]] = {}
        chain_stores: dict[str, set[str]] = {}

        batch_size = 5
        for i in range(0, len(stores_filtered), batch_size):
            batch = stores_filtered[i:i + batch_size]
            tasks = [fetch_offers(client, s["key"], lat, lon) for s in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for store, result in zip(batch, results):
                if isinstance(result, Exception):
                    continue

                chain = extract_chain_name(store["name"])
                store_info = {
                    "name": store["name"],
                    "key": store["key"],
                    "offer_count": store.get("offerCount", 0),
                    "distance_km": store.get("dist", "?"),
                    "chain": chain,
                }
                stores_info.append(store_info)

                offers_list = result.get("offers", [])
                parsed = [parse_offer(o) for o in offers_list]

                if chain not in chain_offers:
                    chain_offers[chain] = []
                    chain_stores[chain] = set()
                chain_offers[chain].extend(parsed)
                chain_stores[chain].add(store["name"])

            # Fördröjning mellan batchar
            if i + batch_size < len(stores_filtered):
                await asyncio.sleep(REQUEST_DELAY)

    return stores_info, chain_offers, chain_stores


# ─── FastAPI-app ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Matpriskollen Kampanj-API",
    description=(
        "Hämtar aktuella kampanjer/erbjudanden från matpriskollen.se "
        "grupperade per matkedja för en valfri svensk ort.\n\n"
        "**Exempel:** `GET /campaigns?city=gävle` eller `GET /campaigns?lat=60.67&lon=17.14`"
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Info"])
async def root():
    """API-information och tillgängliga endpoints."""
    return {
        "name": "Matpriskollen Kampanj-API",
        "version": "1.0.0",
        "endpoints": {
            "/campaigns": "Alla kampanjer per kedja för en ort",
            "/stores": "Lista butiker nära en ort",
            "/stores/{store_key}/offers": "Erbjudanden för en specifik butik",
            "/cities": "Lista över fördefinierade orter",
        },
        "usage": "GET /campaigns?city=gävle  eller  GET /campaigns?lat=60.67&lon=17.14",
    }


@app.get("/cities", tags=["Orter"])
async def list_cities():
    """Returnerar alla fördefinierade orter med koordinater."""
    return {
        name: {"lat": coords[0], "lon": coords[1]}
        for name, coords in sorted(SWEDISH_CITIES.items())
    }


@app.get("/stores", response_model=list[StoreInfo], tags=["Butiker"])
async def get_stores(
    city: Optional[str] = Query(None, description="Ortnamn, t.ex. 'gävle'"),
    lat: Optional[float] = Query(None, description="Latitud"),
    lon: Optional[float] = Query(None, description="Longitud"),
    max_distance_km: float = Query(15.0, description="Max radie i km"),
):
    """Listar alla butiker nära en ort, sorterade efter avstånd."""
    lat, lon = _resolve_coordinates(city, lat, lon)

    async with httpx.AsyncClient(timeout=20.0) as client:
        stores_raw = await fetch_stores(client, lat, lon)

    stores = [
        StoreInfo(
            name=s["name"],
            key=s["key"],
            offer_count=s.get("offerCount", 0),
            distance_km=s.get("dist", "?"),
            chain=extract_chain_name(s["name"]),
        )
        for s in stores_raw
        if float(s.get("dist", "999")) <= max_distance_km
    ]
    return stores


@app.get("/stores/{store_key}/offers", tags=["Erbjudanden"])
async def get_store_offers(
    store_key: str,
    lat: float = Query(59.33, description="Latitud (behövs för API-anropet)"),
    lon: float = Query(18.07, description="Longitud"),
):
    """Hämtar alla erbjudanden för en specifik butik via dess nyckel (UUID)."""
    async with httpx.AsyncClient(timeout=20.0) as client:
        data = await fetch_offers(client, store_key, lat, lon)

    offers = [parse_offer(o) for o in data.get("offers", [])]
    return {
        "store_name": data.get("storeName", ""),
        "total_offers": len(offers),
        "offers": offers,
    }


@app.get("/campaigns", response_model=CityResponse, tags=["Kampanjer"])
async def get_campaigns(
    city: Optional[str] = Query(None, description="Ortnamn, t.ex. 'gävle', 'stockholm'"),
    lat: Optional[float] = Query(None, description="Latitud (alternativ till city)"),
    lon: Optional[float] = Query(None, description="Longitud (alternativ till city)"),
    max_distance_km: float = Query(15.0, description="Max radie i km från centrum"),
    max_stores: int = Query(50, description="Max antal butiker att hämta"),
):
    """
    🏪 **Huvudendpoint** – Hämtar alla aktuella kampanjer per kedja för en ort.

    Returnerar kampanjdata grupperad per kedja med produktinfo, priser,
    giltighet och villkor.

    **Exempel:**
    - `GET /campaigns?city=gävle`
    - `GET /campaigns?city=stockholm&max_distance_km=5`
    - `GET /campaigns?lat=59.33&lon=18.07`
    """
    resolved_lat, resolved_lon = _resolve_coordinates(city, lat, lon)

    stores_info, chain_offers, chain_stores = await fetch_all_campaigns(
        resolved_lat, resolved_lon, max_distance_km, max_stores
    )

    chains = []
    for chain_name, offers in sorted(chain_offers.items()):
        # Deduplicera erbjudanden (samma produkt kan finnas på flera butiker i kedjan)
        seen_ids = set()
        unique_offers = []
        for o in offers:
            if o.id not in seen_ids:
                seen_ids.add(o.id)
                unique_offers.append(o)

        chains.append(ChainCampaigns(
            chain=chain_name,
            stores=sorted(chain_stores.get(chain_name, set())),
            total_offers=len(unique_offers),
            offers=unique_offers,
        ))

    total_offers = sum(c.total_offers for c in chains)
    city_label = city.capitalize() if city else f"{resolved_lat},{resolved_lon}"

    return CityResponse(
        city=city_label,
        lat=resolved_lat,
        lon=resolved_lon,
        fetched_at=datetime.now().isoformat(),
        total_stores=len(stores_info),
        total_offers=total_offers,
        chains=chains,
    )


def _resolve_coordinates(
    city: Optional[str], lat: Optional[float], lon: Optional[float]
) -> tuple[float, float]:
    """Löser ut koordinater från antingen city eller lat/lon."""
    if city:
        return resolve_city(city)
    if lat is not None and lon is not None:
        return lat, lon
    raise HTTPException(
        status_code=400,
        detail="Ange antingen 'city' eller 'lat' + 'lon' som parameter."
    )


# ─── Körning ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
