"""Structured data extraction using Claude vision API.

Improved prompt specifically tuned for Swedish invoices, receipts,
and mixed document types.
"""

from __future__ import annotations

import json
import re
from typing import Any

import anthropic
import httpx

from app.config import settings

# Max image dimension to send to Claude (pixels)
_MAX_IMAGE_DIM = 1568  # Claude's recommended max for good quality
_API_TIMEOUT = 120  # seconds

EXTRACTION_PROMPT = """\
Du är en expert på att extrahera strukturerad data från svenska dokument, fakturor och kvitton.

VIKTIGA REGLER FÖR KORREKT TOLKNING:

1. BELOPP:
   - "Att betala" eller "Summa att betala" eller "Total" = total_amount (detta är ALLTID det slutgiltiga beloppet)
   - "Netto" eller "Summa exkl. moms" = nettobelopp (INTE total_amount)
   - "Moms" eller "Varav moms" = vat_amount
   - total_amount ska ALLTID vara det belopp kunden faktiskt betalar (inklusive moms)
   - Om du ser "Totalt inkl. moms: 1250 kr" → total_amount = 1250.0
   - Om du BARA ser netto + moms, beräkna: total_amount = netto + moms
   - Alla belopp ska vara NUMERISKA utan valutasymboler (1234.56 inte "1 234,56 kr")
   - Svenska format: "1 234,56" ska bli 1234.56

2. LEVERANTÖR:
   - vendor = företaget/butiken som SKICKAR fakturan/säljer varan
   - INTE mottagaren/köparen
   - KRITISKT: Inkludera ALLTID butiksort/stad i vendor-namnet om det finns på kvittot!
     Exempel: "Lidl Kungsholmen", "Lidl Fridhemsplan", "ICA Maxi Lindhagen"
   - Orten kan stå i adressen, sidhuvudet eller som del av butiksnamnet
   - Om det bara står t.ex. "Lidl" men adressen visar "Kungsholmsgatan 5, Stockholm"
     → vendor = "Lidl Stockholm" (använd stadsdelen eller staden)
   - Om det står "Lidl Sverige KB" eller liknande bolagsnamn, leta efter den lokala butikens ort
   - Använd det officiella butiksnamnet, inte förkortningar

3. DATUM:
   - Fakturadatum = invoice_date (datumet fakturan utfärdades)
   - Förfallodatum = due_date (sista betalningsdag)
   - Konvertera ALLTID till ISO-format: YYYY-MM-DD
   - "15 jan 2025" → "2025-01-15"

4. NUMMER:
   - invoice_number = fakturanummer (INTE ordernummer)
   - ocr_number = OCR-nummer / betalningsreferens (bara siffror, inga bokstäver)
   - Kundnummer ska INTE hamna i invoice_number

5. RADER (line_items):
   - Varje rad = en produkt/tjänst
   - quantity = antal (numeriskt)
   - unit = enhet (st, kg, m, timmar, liter, förp, etc.)
   - unit_price = pris per enhet EXKLUSIVE moms om möjligt
   - total_price = radtotal (det kunden betalar för denna rad)
   - vat_rate = momssats i procent (6, 12, eller 25 i Sverige)
   - weight = FAKTISK vikt i kilogram (numeriskt, t.ex. 0.348)
   - packaging = förpackningstyp om angivet

6. PANT-RADER:
   - Rader som heter "Pant", "Pant 1kr", "Pant 2kr", "+Pant" etc. är ALLTID en separat rad
   - Sätt is_pant = true på dessa rader
   - KRITISKT: "Pant" får ALDRIG slås ihop med produkten ovanför eller under!
     Exempel från kvitto:
       Entrecôte 189,00
       Pant 2,00
       Coca-Cola 15,90
     → Tre SEPARATA rader: "Entrecôte", "Pant", "Coca-Cola"
     FEL: "Pant Entrecôte" eller "Coca-Cola Pant" — det finns inte!
   - Pant förekommer ENBART på dryckes-förpackningar (burk, flaska)
   - Om raden bara säger "Pant" + eventuellt belopp → egen rad med is_pant=true

7. RABATT-RADER (Willys, Hemköp m.fl.):
   - Rabatter på kvitton visas ofta som SEPARATA rader med negativt belopp, t.ex.:
       Arla Mellanmjölk         15,90
       Rabatt                   -3,00
       Felix Ketchup            29,90
       Prisnedsättning          -5,00
   - Behåll rabatten som EGEN rad med:
     * description = rabattexten som den står (t.ex. "Rabatt", "Prisnedsättning", "Nedsatt pris")
     * total_price = det NEGATIVA beloppet (t.ex. -3.00, -5.00)
     * is_discount = true
   - Ändra ALDRIG produktradens pris — det ska vara originalpriset
   - Lägg ALDRIG ihop rabatt med produkten — de ska vara SEPARATA rader

8. KILOPRIS OCH VIKT:
   - På kvitton visas ofta: "Produktnamn" följt av "X,XXX kg * YY,YY kr/kg = ZZ,ZZ"
   - I sådana fall:
     * quantity = 1 (det är 1 inköp)
     * unit = "kg"
     * unit_price = kilopriset (YY,YY)
     * total_price = slutpriset (ZZ,ZZ)
     * weight = den FAKTISKA vikten i kg (X,XXX) — INTE avrundad till 1.00
   - Beräkna: weight = total_price / unit_price om det behövs
   - Exempel: "Lösviktsgodis 0,348 kg * 99,90 kr/kg = 34,77"
     → weight = 0.348, unit_price = 99.90, total_price = 34.77, unit = "kg"
   - Exempel: "Äpplen Granny Smith 0,652 kg * 29,90 = 19,49"
     → weight = 0.652, unit_price = 29.90, total_price = 19.49, unit = "kg"

8. VALUTA:
   - Anta SEK om inget annat anges

Returnera ENBART giltig JSON med denna struktur:

{{
  "document_type": "invoice|receipt|contract|letter|image|other",
  "vendor": "Leverantörens fullständiga namn",
  "total_amount": 1234.56,
  "vat_amount": 308.64,
  "currency": "SEK",
  "invoice_number": "12345",
  "ocr_number": "1234567890",
  "invoice_date": "2025-01-15",
  "due_date": "2025-02-15",
  "discount": "10% vid betalning inom 10 dagar",
  "free_text": "Övrig relevant text",
  "line_items": [
    {{
      "description": "Produktbeskrivning",
      "quantity": 2.0,
      "unit": "st",
      "unit_price": 150.00,
      "total_price": 300.00,
      "vat_rate": 25.0,
      "discount": null,
      "weight": null,
      "packaging": null,
      "is_pant": false,
      "is_discount": false
    }}
  ]
}}

KRITISKT:
- Returnera BARA JSON, absolut inget annat (ingen markdown, inga kommentarer)
- Använd null för fält som inte finns
- Belopp ska vara numeriska: 1234.56 (INTE "1 234,56 kr")
- weight ska vara numeriskt i kg (0.348, INTE "0,348 kg")
- is_pant = true BARA för pantrader
- is_discount = true BARA för rabattrader (negativt belopp)
- Datum i ISO-format: YYYY-MM-DD
- line_items = tom lista [] om inga rader finns
- Svara på {language}
"""


class StructuredExtractor:
    """Extract structured data from document content blocks using Claude."""

    def __init__(self) -> None:
        self.client = anthropic.Anthropic(
            api_key=settings.anthropic_api_key,
            timeout=httpx.Timeout(_API_TIMEOUT, connect=10),
        )

    def extract(
        self,
        content_blocks: list[dict[str, Any]],
        language: str = "swedish",
    ) -> dict[str, Any]:
        """Send content to Claude and parse the structured JSON response."""

        prompt = EXTRACTION_PROMPT.format(language=language)
        messages_content = self._build_message_content(content_blocks, prompt)

        response = self.client.messages.create(
            model=settings.claude_model,
            max_tokens=8192,
            messages=[{"role": "user", "content": messages_content}],
        )

        raw_text = response.content[0].text
        data = self._parse_json(raw_text)

        # Log if we hit the fallback
        if "free_text" in data and data.get("document_type") == "other" and not data.get("vendor"):
            stop = response.stop_reason
            print(f"⚠️ Structured extraction fell back to free_text. stop_reason={stop}, response length={len(raw_text)}")

        # Post-process: clean up common issues
        data = self._post_process(data)

        return data

    @staticmethod
    def _preprocess_image(b64_data: str, media_type: str) -> tuple[str, str]:
        """Resize and enhance images for better extraction quality.

        For PNG/photos: apply contrast boost, sharpening, and grayscale
        conversion to improve text readability before sending to Claude.
        """
        import base64
        import io
        from PIL import Image, ImageEnhance, ImageFilter

        raw = base64.standard_b64decode(b64_data)
        img = Image.open(io.BytesIO(raw))

        w, h = img.size
        needs_resize = w > _MAX_IMAGE_DIM or h > _MAX_IMAGE_DIM
        is_photo = media_type not in ("image/png",)  # PNGs are usually screenshots/scans

        # ── Enhancement for receipt images (PNG scans, photos) ──
        # Convert to RGB if needed
        if img.mode in ("RGBA", "P", "LA"):
            bg = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "RGBA" or img.mode == "LA":
                bg.paste(img, mask=img.split()[-1])
            else:
                bg.paste(img)
            img = bg
        elif img.mode != "RGB":
            img = img.convert("RGB")

        # Auto-enhance: boost contrast and sharpness for scanned receipts
        # (helps Claude read faded/blurry text)
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(1.3)  # 30% more contrast

        enhancer = ImageEnhance.Sharpness(img)
        img = enhancer.enhance(1.5)  # 50% sharper

        # For very light images (faded receipts), boost brightness slightly
        enhancer = ImageEnhance.Brightness(img)
        img = enhancer.enhance(1.05)

        # ── Resize if needed ──
        if needs_resize:
            ratio = min(_MAX_IMAGE_DIM / w, _MAX_IMAGE_DIM / h)
            new_w, new_h = int(w * ratio), int(h * ratio)
            img = img.resize((new_w, new_h), Image.LANCZOS)

        # ── Re-encode ──
        buf = io.BytesIO()
        if is_photo or media_type in ("image/jpeg", "image/jpg"):
            img.save(buf, format="JPEG", quality=90)
            out_type = "image/jpeg"
        else:
            img.save(buf, format="PNG", optimize=True)
            out_type = "image/png"

        new_b64 = base64.standard_b64encode(buf.getvalue()).decode("utf-8")
        return new_b64, out_type

    @staticmethod
    def _build_message_content(
        content_blocks: list[dict[str, Any]], prompt: str
    ) -> list[dict[str, Any]]:
        """Convert DocumentLoader blocks to Anthropic API format."""
        message_content = []

        for block in content_blocks:
            if block["type"] == "image":
                img_data, img_type = StructuredExtractor._preprocess_image(
                    block["data"], block["media_type"]
                )
                message_content.append(
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": img_type,
                            "data": img_data,
                        },
                    }
                )
            elif block["type"] == "text":
                message_content.append(
                    {
                        "type": "text",
                        "text": f"[Document text from {block['source']}]:\n{block['data']}",
                    }
                )

        message_content.append({"type": "text", "text": prompt})
        return message_content

    @staticmethod
    def _parse_json(text: str) -> dict[str, Any]:
        """Robustly parse JSON from Claude's response."""
        # Step 1: Try direct parse (Claude returned clean JSON)
        cleaned = text.strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        # Step 2: Strip markdown code fences (```json ... ``` or ``` ... ```)
        # Handle various whitespace patterns including \r\n
        stripped = re.sub(r"^```(?:json)?[ \t]*\r?\n?", "", cleaned)
        stripped = re.sub(r"\r?\n?```\s*$", "", stripped).strip()
        if stripped != cleaned:
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                pass

        # Step 3: Find JSON object anywhere in text
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            json_str = match.group(0)
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                # Step 4: Try to fix truncated JSON (missing closing brackets)
                fixed = _fix_truncated_json(json_str)
                if fixed:
                    try:
                        return json.loads(fixed)
                    except json.JSONDecodeError:
                        pass

        print(f"⚠️ JSON parse failed, raw text starts with: {text[:300]}")
        return {"free_text": text, "document_type": "other", "line_items": []}

    @staticmethod
    def _post_process(data: dict[str, Any]) -> dict[str, Any]:
        """Clean up common extraction issues."""

        # ── Clean document-level numeric fields ──
        for field in ("total_amount", "vat_amount"):
            val = data.get(field)
            if isinstance(val, str):
                cleaned = val.replace(" ", "").replace(",", ".").replace("kr", "").replace("SEK", "").strip()
                try:
                    data[field] = float(cleaned)
                except ValueError:
                    data[field] = None

        # Ensure total_amount > vat_amount (common mix-up)
        total = data.get("total_amount")
        vat = data.get("vat_amount")
        if total is not None and vat is not None:
            if isinstance(total, (int, float)) and isinstance(vat, (int, float)):
                if vat > total:
                    data["total_amount"], data["vat_amount"] = vat, total

        # Default currency
        if data.get("total_amount") and not data.get("currency"):
            data["currency"] = "SEK"

        # Clean OCR number
        ocr = data.get("ocr_number")
        if ocr and isinstance(ocr, str):
            data["ocr_number"] = re.sub(r"[^\d\s]", "", ocr).strip()

        # ── Clean line items ──
        items = data.get("line_items", [])
        for item in items:
            # Clean numeric fields
            for num_field in ("quantity", "unit_price", "total_price", "vat_rate", "weight"):
                val = item.get(num_field)
                if isinstance(val, str):
                    cleaned = (
                        val.replace(" ", "").replace(",", ".")
                        .replace("kr", "").replace("kg", "").replace("g", "").strip()
                    )
                    try:
                        item[num_field] = float(cleaned)
                    except ValueError:
                        item[num_field] = None

            # ── Fix weight: calculate from price if weight is missing or 1.00 ──
            _fix_weight(item)

        # ── Fix pant: split mis-merged names, then merge all pant into one ──
        _fix_pant_descriptions(items)
        _merge_pant_rows(items)

        # ── Apply discount rows: link to preceding product, adjust price ──
        _apply_discount_rows(items)

        data["line_items"] = items

        return data


def _fix_truncated_json(text: str) -> str | None:
    """Try to fix truncated JSON by closing open brackets/braces."""
    # Count unmatched brackets
    open_braces = 0
    open_brackets = 0
    in_string = False
    escape = False

    for ch in text:
        if escape:
            escape = False
            continue
        if ch == '\\' and in_string:
            escape = True
            continue
        if ch == '"' and not escape:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == '{':
            open_braces += 1
        elif ch == '}':
            open_braces -= 1
        elif ch == '[':
            open_brackets += 1
        elif ch == ']':
            open_brackets -= 1

    if open_braces <= 0 and open_brackets <= 0:
        return None  # Not a truncation issue

    # Remove trailing incomplete item (partial JSON after last comma)
    fixed = text.rstrip()
    # Remove trailing comma or incomplete key-value
    fixed = re.sub(r',\s*"[^"]*"?\s*:?\s*("?[^"{}[\]]*"?)?\s*$', '', fixed)
    # Also handle trailing incomplete array item
    fixed = re.sub(r',\s*\{[^}]*$', '', fixed)

    # Close open brackets and braces
    fixed += ']' * max(0, open_brackets)
    fixed += '}' * max(0, open_braces)

    return fixed


def _fix_weight(item: dict[str, Any]) -> None:
    """Calculate actual weight when kilo-priced items show weight as 1.00."""
    unit = (item.get("unit") or "").lower().strip()
    weight = item.get("weight")
    total_price = item.get("total_price")
    unit_price = item.get("unit_price")

    if unit != "kg" or not total_price or not unit_price or unit_price == 0:
        return

    calculated_weight = round(total_price / unit_price, 3)

    if weight is None or weight == 1.0 or weight == 1:
        item["weight"] = calculated_weight
    elif isinstance(weight, (int, float)) and weight > 0:
        ratio = weight / calculated_weight if calculated_weight > 0 else 999
        if ratio > 2.0 or ratio < 0.5:
            item["weight"] = calculated_weight


def _is_pant_item(item: dict[str, Any]) -> bool:
    """Check if a line item is a pant row."""
    if item.get("is_pant"):
        return True
    desc = (item.get("description") or "").strip()
    if re.match(r"^\+?\s*pant\s*(\d+[\s,.]?\d*\s*(kr|öre)?)?\s*$", desc, re.IGNORECASE):
        return True
    return False


def _fix_pant_descriptions(items: list[dict[str, Any]]) -> None:
    """Split items where 'Pant' got merged with another product name."""
    inserts: list[tuple[int, dict[str, Any]]] = []

    for i, item in enumerate(items):
        desc = (item.get("description") or "").strip()
        if not desc:
            continue

        m = re.match(r"^(\+?\s*pant)\s+(.+)$", desc, re.IGNORECASE)
        if m:
            rest = m.group(2).strip()
            if not re.match(r"^(\d+[\s,.]?\d*\s*(kr|öre)?|\+)$", rest, re.IGNORECASE):
                item["description"] = rest
                item.pop("is_pant", None)
                inserts.append((i, _make_pant_row()))
                continue

        m2 = re.match(r"^(.+)\s+(pant\+?)\s*$", desc, re.IGNORECASE)
        if m2:
            product = m2.group(1).strip()
            if len(product) > 1:
                item["description"] = product
                item.pop("is_pant", None)
                inserts.append((i + 1, _make_pant_row()))

    for idx, new_item in reversed(inserts):
        items.insert(idx, new_item)


def _make_pant_row() -> dict[str, Any]:
    """Create a standalone pant line item."""
    return {
        "description": "Pant",
        "quantity": 1.0,
        "unit": "st",
        "unit_price": None,
        "total_price": None,
        "vat_rate": None,
        "discount": None,
        "weight": None,
        "packaging": None,
        "category": "dryck",
        "is_pant": True,
    }


def _merge_pant_rows(items: list[dict[str, Any]]) -> None:
    """Find all pant rows, remove them, and insert one combined 'Pant' row."""
    pant_total = 0.0
    pant_count = 0
    to_remove = []

    for i, item in enumerate(items):
        if _is_pant_item(item):
            price = item.get("total_price") or item.get("unit_price") or 0
            if isinstance(price, (int, float)):
                pant_total += price
            pant_count += 1
            to_remove.append(i)

    if pant_count == 0:
        return

    for i in reversed(to_remove):
        items.pop(i)

    items.append({
        "description": "Pant",
        "quantity": float(pant_count),
        "unit": "st",
        "unit_price": round(pant_total / pant_count, 2) if pant_count > 0 else 0,
        "total_price": round(pant_total, 2),
        "vat_rate": None,
        "discount": None,
        "weight": None,
        "packaging": None,
        "category": "dryck",
        "is_pant": True,
    })


# ── Discount-row linking ─────────────────────────────────────────────

_DISCOUNT_PATTERNS = re.compile(
    r"^(rabatt|prisnedsättning|nedsatt|erbjudande|rea\b|kampanj|"
    r"%-rabatt|\d+\s*%\s*rabatt|mängdrabatt|mix\s*&?\s*match|"
    r"kupong|bonus|avdrag|kort[\s-]*rabatt|medlems[\s-]*rabatt)",
    re.IGNORECASE,
)


def _is_discount_item(item: dict[str, Any]) -> bool:
    """Check if a line item is a discount/rebate row."""
    if item.get("is_discount"):
        return True
    desc = (item.get("description") or "").strip()
    price = item.get("total_price")
    if isinstance(price, (int, float)) and price < 0 and _DISCOUNT_PATTERNS.search(desc):
        return True
    if isinstance(price, (int, float)) and price < 0 and len(desc) < 30:
        if _DISCOUNT_PATTERNS.search(desc):
            return True
    return False


def _apply_discount_rows(items: list[dict[str, Any]]) -> None:
    """Link discount rows to their preceding product(s)."""
    to_remove: list[int] = []

    for i, item in enumerate(items):
        if not _is_discount_item(item):
            continue

        discount_amount = item.get("total_price")
        if not isinstance(discount_amount, (int, float)) or discount_amount >= 0:
            continue

        discount_desc = (item.get("description") or "Rabatt").strip()

        target = None
        for j in range(i - 1, -1, -1):
            if j in to_remove:
                continue
            if _is_pant_item(items[j]) or _is_discount_item(items[j]):
                continue
            target = items[j]
            break

        if target is not None:
            orig_price = target.get("total_price")
            if isinstance(orig_price, (int, float)):
                target["total_price"] = round(orig_price + discount_amount, 2)

            existing = target.get("discount") or ""
            new_disc = f"{discount_desc} {discount_amount:.2f} kr"
            target["discount"] = f"{existing}; {new_disc}".lstrip("; ") if existing else new_disc

        to_remove.append(i)

    for i in reversed(to_remove):
        items.pop(i)
