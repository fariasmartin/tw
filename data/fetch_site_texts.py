import os
import json
import re
import unicodedata
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# === CONFIGURATION ===
SCRIPT_DIR = r"C:\Users\faria\tw\data"
INPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_information.xlsx")
OUTPUT_JSONL_PATH = os.path.join(SCRIPT_DIR, "website_texts.jsonl")  # one JSON per line

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 10

def normalize(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8").lower()

# === SUBPAGE DISCOVERY (multilingual keywords + bucket tagging) ===
SUBPAGE_KEYWORDS = {
    "contact": ["contact", "contacto", "contáctanos", "contactanos", "contato", "contatti", "kontakt"],
    "about": [
        "about", "nosotros", "quienes", "quiénes", "quienes-somos", "quiénes-somos",
        "sobre", "sobre-nosotros", "quem-somos", "chi-siamo", "über-uns", "ueber-uns",
        "empresa", "historia"
    ],
    "products_shop": [
        "tienda", "shop", "store", "productos", "producto", "catalogo", "catálogo",
        "catalog", "catalogue", "marcas", "brands", "categorias", "categorías",
        "categories", "comprar", "compra", "pedido", "orders", "oferta", "ofertas",
        "promociones", "promo", "novedades", "destacados"
    ],
    "menu_food": ["menu", "menú", "carta", "carta-digital", "platos", "comida", "food", "menu-del-dia", "menú-del-día"],
    "locations": [
        "sucursales", "ubicaciones", "donde", "dónde", "locales", "puntos-de-venta", "punto-de-venta",
        "stores", "locations", "sedes", "direccion", "dirección", "address", "mapa", "cómo-llegar", "como-llegar"
    ],
    "hours": ["horario", "horarios", "opening-hours", "open-hours", "open", "orari", "öffnungszeiten", "oeffnungszeiten"],
    "delivery_shipping": [
        "envio", "envíos", "envio-gratis", "envío-gratis", "envios", "envíos",
        "shipping", "delivery", "entrega", "reparto", "click-and-collect", "pickup", "retirar", "retira", "takeaway"
    ],
    "booking_orders": [
        "reservas", "reserva", "book", "booking", "appointment", "turnos", "citas",
        "order-online", "pedido-online", "checkout", "basket", "cart", "carrito"
    ]
}
BLACKLIST_SUBSTRINGS = (
    "mailto:", "tel:", "javascript:", "#", ".pdf", ".doc", ".docx", ".xls",
    ".xlsx", ".ppt", ".pptx", ".zip", ".rar", ".7z", ".jpg", ".jpeg", ".png",
    ".gif", ".webp", ".svg", "instagram.com", "facebook.com", "twitter.com",
    "x.com", "tiktok.com", "youtube.com", "youtu.be", "wa.me", "whatsapp.com"
)
MAX_SUBPAGES = 12

EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")

def is_same_site(base_url: str, link_url: str) -> bool:
    b, l = urlparse(base_url), urlparse(link_url)
    return (l.netloc == "" or l.netloc == b.netloc)

def match_bucket(href: str, text: str):
    """Return first matching bucket name or None."""
    h = normalize(href or "")
    t = normalize(text or "")
    for bucket, kws in SUBPAGE_KEYWORDS.items():
        for kw in kws:
            kw_norm = normalize(kw)
            if kw_norm in h or kw_norm in t:
                return bucket
    return None

def score_url(u: str) -> int:
    """Rank URLs: more keyword hits and shorter paths score higher."""
    u_norm = normalize(u)
    length_score = -len(u_norm)
    keyword_hits = sum(any(normalize(kw) in u_norm for kw in kws) for kws in SUBPAGE_KEYWORDS.values())
    return keyword_hits * 1000 + length_score

def extract_page_text_emails_socials(url: str) -> dict:
    """Fetch a single URL and return visible text + emails + socials."""
    out = {"url": url, "status": None, "text": "", "emails": [], "socials": {}}
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        out["status"] = resp.status_code
        if resp.status_code != 200:
            return out
        soup = BeautifulSoup(resp.text, "lxml")

        # visible text
        text = soup.get_text(separator=" ", strip=True)
        out["text"] = text

        # emails
        out["emails"] = sorted(set(EMAIL_RE.findall(text)))

        # socials
        socials = {"instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None}
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if "instagram.com" in href and not socials["instagram"]:
                socials["instagram"] = href
            elif "twitter.com" in href and not socials["twitter"]:
                socials["twitter"] = href
            elif "facebook.com" in href and not socials["facebook"]:
                socials["facebook"] = href
            elif "youtube.com" in href or "youtu.be" in href:
                socials["youtube"] = socials["youtube"] or href
            elif "wa.me" in href or "whatsapp.com" in href:
                socials["whatsapp"] = socials["whatsapp"] or href
        out["socials"] = socials

    except Exception as e:
        out["status"] = f"error: {e}"
    return out

def collect_subpages_with_buckets(base_url: str):
    """Return [{url, bucket}] for candidate subpages on same site, ranked and capped."""
    results = []
    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code != 200:
            return results
        soup = BeautifulSoup(resp.text, "lxml")
        candidates = []
        for a in soup.find_all("a", href=True):
            href_raw = a["href"] or ""
            text_raw = a.get_text(strip=True) or ""
            if not href_raw or any(bad in href_raw.lower() for bad in BLACKLIST_SUBSTRINGS):
                continue
            full_url = urljoin(base_url, href_raw)
            if not is_same_site(base_url, full_url):
                continue
            bucket = match_bucket(href_raw, text_raw)
            if bucket:
                candidates.append({"url": full_url, "bucket": bucket})

        # Dedup + rank + cap
        dedup = {}
        for c in candidates:
            dedup[c["url"]] = c["bucket"]
        ranked = sorted(dedup.items(), key=lambda kv: score_url(kv[0]), reverse=True)[:MAX_SUBPAGES]
        results = [{"url": u, "bucket": b} for u, b in ranked]
    except Exception:
        pass
    return results

def fetch_site_bundle(website: str) -> dict:
    """Fetch main + selected subpages, return combined text + metadata."""
    website = str(website).strip().rstrip("/")
    main = extract_page_text_emails_socials(website)

    subpages_crawled = collect_subpages_with_buckets(website)
    sub_texts = []
    emails = set(main.get("emails", []))
    socials = {k: main.get("socials", {}).get(k) for k in ["instagram", "twitter", "facebook", "youtube", "whatsapp"]}

    for item in subpages_crawled:
        sub = extract_page_text_emails_socials(item["url"])
        sub_texts.append(sub.get("text", ""))
        for e in sub.get("emails", []):
            emails.add(e)
        for k in socials:
            if not socials[k]:
                socials[k] = sub.get("socials", {}).get(k)

    combined_text = " ".join([main.get("text", "")] + sub_texts).strip()

    return {
        "website": website,
        "combined_text": combined_text,          # 🔴 only text (plus below metadata)
        "emails": sorted(emails),
        "socials": socials,
        "subpages_crawled": subpages_crawled     # keep crawl trace for debugging
    }

def main():
    df = pd.read_excel(INPUT_EXCEL_PATH)

    with open(OUTPUT_JSONL_PATH, "w", encoding="utf-8") as f:
        for i, row in df.iterrows():
            website = row.get("website")
            print(f"🌐 [{i+1}/{len(df)}] {website}")
            if pd.notna(website):
                bundle = fetch_site_bundle(website)
            else:
                bundle = {
                    "website": None,
                    "combined_text": "",
                    "emails": [],
                    "socials": {"instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None},
                    "subpages_crawled": []
                }
            # Keep an index so you can merge back to Excel later
            bundle["row_index"] = int(i)
            f.write(json.dumps(bundle, ensure_ascii=False) + "\n")

    print(f"✅ Saved website text bundles to {OUTPUT_JSONL_PATH}")

if __name__ == "__main__":
    main()


# compute the size of "website_texts.jsonl"
file_size = os.path.getsize(OUTPUT_JSONL_PATH)

#number of lines in the file
with open(OUTPUT_JSONL_PATH, 'r', encoding='utf-8') as f:
    num_lines = sum(1 for line in f)
print(f"File size: {file_size / 1:.2f} MB")

# save it as xlsx file
df_output = pd.read_json(OUTPUT_JSONL_PATH, lines=True)
df_output.to_excel(os.path.join(SCRIPT_DIR, "website_texts.xlsx"), index=False)




# import os
# import json
# import re
# import unicodedata
# import requests
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin, urlparse
# import pandas as pd

# # === CONFIGURATION ===
# SCRIPT_DIR = r"C:\Users\faria\tw\data"
# INPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_information.xlsx")
# OUTPUT_JSONL_PATH = os.path.join(SCRIPT_DIR, "website_texts.jsonl")  # one JSON per line

# HEADERS = {"User-Agent": "Mozilla/5.0"}
# TIMEOUT = 10

# def normalize(text: str) -> str:
#     if not isinstance(text, str):
#         return ""
#     return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8").lower()

# # === SUBPAGE DISCOVERY (multilingual keywords + bucket tagging) ===
# SUBPAGE_KEYWORDS = {
#     "contact": ["contact", "contacto", "contáctanos", "contactanos", "contato", "contatti", "kontakt"],
#     "about": [
#         "about", "nosotros", "quienes", "quiénes", "quienes-somos", "quiénes-somos",
#         "sobre", "sobre-nosotros", "quem-somos", "chi-siamo", "über-uns", "ueber-uns",
#         "empresa", "historia"
#     ],
#     "products_shop": [
#         "tienda", "shop", "store", "productos", "producto", "catalogo", "catálogo",
#         "catalog", "catalogue", "marcas", "brands", "categorias", "categorías",
#         "categories", "comprar", "compra", "pedido", "orders", "oferta", "ofertas",
#         "promociones", "promo", "novedades", "destacados"
#     ],
#     "menu_food": ["menu", "menú", "carta", "carta-digital", "platos", "comida", "food", "menu-del-dia", "menú-del-día"],
#     "locations": [
#         "sucursales", "ubicaciones", "donde", "dónde", "locales", "puntos-de-venta", "punto-de-venta",
#         "stores", "locations", "sedes", "direccion", "dirección", "address", "mapa", "cómo-llegar", "como-llegar"
#     ],
#     "hours": ["horario", "horarios", "opening-hours", "open-hours", "open", "orari", "öffnungszeiten", "oeffnungszeiten"],
#     "delivery_shipping": [
#         "envio", "envíos", "envio-gratis", "envío-gratis", "envios", "envíos",
#         "shipping", "delivery", "entrega", "reparto", "click-and-collect", "pickup", "retirar", "retira", "takeaway"
#     ],
#     "booking_orders": [
#         "reservas", "reserva", "book", "booking", "appointment", "turnos", "citas",
#         "order-online", "pedido-online", "checkout", "basket", "cart", "carrito"
#     ]
# }
# BLACKLIST_SUBSTRINGS = (
#     "mailto:", "tel:", "javascript:", "#", ".pdf", ".doc", ".docx", ".xls",
#     ".xlsx", ".ppt", ".pptx", ".zip", ".rar", ".7z", ".jpg", ".jpeg", ".png",
#     ".gif", ".webp", ".svg", "instagram.com", "facebook.com", "twitter.com",
#     "x.com", "tiktok.com", "youtube.com", "youtu.be", "wa.me", "whatsapp.com"
# )
# MAX_SUBPAGES = 12

# EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")

# def is_same_site(base_url: str, link_url: str) -> bool:
#     b, l = urlparse(base_url), urlparse(link_url)
#     return (l.netloc == "" or l.netloc == b.netloc)

# def match_bucket(href: str, text: str):
#     """Return first matching bucket name or None."""
#     h = normalize(href or "")
#     t = normalize(text or "")
#     for bucket, kws in SUBPAGE_KEYWORDS.items():
#         for kw in kws:
#             kw_norm = normalize(kw)
#             if kw_norm in h or kw_norm in t:
#                 return bucket
#     return None

# def score_url(u: str) -> int:
#     """Rank URLs: more keyword hits and shorter paths score higher."""
#     u_norm = normalize(u)
#     length_score = -len(u_norm)
#     keyword_hits = sum(any(normalize(kw) in u_norm for kw in kws) for kws in SUBPAGE_KEYWORDS.values())
#     return keyword_hits * 1000 + length_score

# def extract_info_from_page(url: str) -> dict:
#     out = {"url": url, "status": None, "text": "", "emails": [], "socials": {}}
#     try:
#         resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
#         out["status"] = resp.status_code
#         if resp.status_code != 200:
#             return out
#         soup = BeautifulSoup(resp.text, "lxml")
#         text = soup.get_text(separator=" ", strip=True)
#         emails = EMAIL_RE.findall(text)
#         socials = {"instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None}
#         for a in soup.find_all("a", href=True):
#             href = a["href"].lower()
#             if "instagram.com" in href and not socials["instagram"]:
#                 socials["instagram"] = href
#             elif "twitter.com" in href and not socials["twitter"]:
#                 socials["twitter"] = href
#             elif "facebook.com" in href and not socials["facebook"]:
#                 socials["facebook"] = href
#             elif "youtube.com" in href or "youtu.be" in href:
#                 socials["youtube"] = socials["youtube"] or href
#             elif "wa.me" in href or "whatsapp.com" in href:
#                 socials["whatsapp"] = socials["whatsapp"] or href
#         out["text"] = text
#         out["emails"] = sorted(set(emails))
#         out["socials"] = socials
#         return out
#     except Exception as e:
#         out["status"] = f"error: {e}"
#         return out

# def collect_subpages_with_buckets(base_url: str):
#     """Return [{url, bucket}] for candidate subpages on same site, ranked and capped."""
#     results = []
#     try:
#         resp = requests.get(base_url, headers=HEADERS, timeout=TIMEOUT)
#         if resp.status_code != 200:
#             return results
#         soup = BeautifulSoup(resp.text, "lxml")
#         candidates = []
#         for a in soup.find_all("a", href=True):
#             href_raw = a["href"] or ""
#             text_raw = a.get_text(strip=True) or ""
#             if not href_raw or any(bad in href_raw.lower() for bad in BLACKLIST_SUBSTRINGS):
#                 continue
#             full_url = urljoin(base_url, href_raw)
#             if not is_same_site(base_url, full_url):
#                 continue
#             bucket = match_bucket(href_raw, text_raw)
#             if bucket:
#                 candidates.append({"url": full_url, "bucket": bucket})
#         # Dedup + rank + cap
#         dedup = {}
#         for c in candidates:
#             dedup[c["url"]] = c["bucket"]
#         ranked = sorted(dedup.items(), key=lambda kv: score_url(kv[0]), reverse=True)[:MAX_SUBPAGES]
#         results = [{"url": u, "bucket": b} for u, b in ranked]
#     except Exception:
#         pass
#     return results

# def fetch_site_bundle(website: str) -> dict:
#     website = str(website).strip().rstrip("/")
#     main = extract_info_from_page(website)
#     subpages = []
#     subpages_with_bucket = collect_subpages_with_buckets(website)
#     for item in subpages_with_bucket:
#         subpages.append(extract_info_from_page(item["url"]))

#     combined_text = " ".join([main.get("text", "")] + [sp.get("text", "") for sp in subpages])

#     emails = set(main.get("emails", []))
#     socials = {k: main.get("socials", {}).get(k) for k in ["instagram", "twitter", "facebook", "youtube", "whatsapp"]}
#     for sp in subpages:
#         for e in sp.get("emails", []):
#             emails.add(e)
#         for k in socials:
#             if not socials[k]:
#                 socials[k] = sp.get("socials", {}).get(k)

#     bundle = {
#         "website": website,
#         "combined_text": combined_text.strip(),
#         "emails": sorted(emails),
#         "socials": socials,
#         "subpages_crawled": subpages_with_bucket,  # keep structure for debugging
#     }
#     return bundle

# def main():
#     df = pd.read_excel(INPUT_EXCEL_PATH)
#     with open(OUTPUT_JSONL_PATH, "w", encoding="utf-8") as f:
#         for i, row in df.iterrows():
#             website = row.get("website")
#             print(f"🌐 [{i+1}/{len(df)}] {website}")
#             if pd.notna(website):
#                 bundle = fetch_site_bundle(website)
#             else:
#                 bundle = {
#                     "website": None,
#                     "combined_text": "",
#                     "emails": [],
#                     "socials": {"instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None},
#                     "subpages_crawled": []
#                 }
#             bundle["row_index"] = int(i)
#             f.write(json.dumps(bundle, ensure_ascii=False) + "\n")
#     print(f"✅ Saved website text bundles to {OUTPUT_JSONL_PATH}")

# if __name__ == "__main__":
#     main()
