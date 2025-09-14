import os
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import unicodedata

# === CONFIGURATION ===
SCRIPT_DIR = r'C:\Users\faria\tw\data'
INPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_information.xlsx")
OUTPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.xlsx")
OUTPUT_JSON_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.json")

# === PRODUCT KEYWORDS BY COUNTRY ===
# You can keep growing these. Variants will be auto-generated.
product_keywords = {
    "argentina": {
        "products": ["argentina", "yerba mate", "mate cocido", "dulce de leche", "bombilla"],
        "dishes":   ["empanada", "alfajor", "asado", "provoleta", "milanesa", "choripan", "ñoqui", "humita", "medialuna"],
        "brands":   ["taragüi", "playadito", "havanna", "balcarce"]
    },
    "uruguay": {
        "products": ["yerba uruguaya", "mate uruguayo", "bombilla", "dulce de leche"],
        "dishes":   ["chivito", "fainá", "torta frita", "bizcocho", "milanesa", "asado", "alfajor"],
        "brands":   ["canarias"]
    },
    "colombia": {
        "products": ["panela", "bocadillo", "aji", "arequipe"],
        "dishes":   ["arepa", "ajiaco", "bandeja paisa", "sancocho", "lechona", "empanada", "patacon", "buñuelo", "tamales tolimenses"],
        "brands":   ["juan valdez", "postobón", "postobon"]
    },
    "mexico": {
        "products": ["chile", "tajin", "tortilla", "masa harina"],
        "dishes":   ["tamal", "pozole", "horchata", "taco", "mole", "enchilada", "tlayuda", "barbacoa", "birria", "aguachile"],
        "brands":   []
    },
    "peru": {
        "products": ["aji amarillo", "quinua", "quinoa", "pisco", "chicha morada"],
        "dishes":   ["ceviche", "lomo saltado", "aji de gallina", "causa", "anticucho", "rocoto relleno", "papa a la huancaina"],
        "brands":   ["inca kola", "inca cola"]
    },
}

# === NORMALIZATION ===
def normalize(text: str) -> str:
    """Lowercase and strip accents for robust matching."""
    if not isinstance(text, str):
        return ""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8").lower()

# === SPANISH PLURALIZER (simple heuristic) ===
def pluralize_es(word: str) -> str:
    w = word.strip()
    if not w:
        return w
    # common Spanish rules
    if w.endswith(('a','e','i','o','u')):
        return w + 's'
    if w.endswith('z'):
        return w[:-1] + 'ces'
    # default
    return w + 'es'

# === ALIASES / VARIANTS (diacritics, common alt spellings, singular/plural) ===
ALIASES = {
    # diacritics / synonyms
    "aji": ["ají"],
    "aji de gallina": ["ají de gallina"],
    "papa a la huancaina": ["papas a la huancaina", "papa a la huancaína", "papas a la huancaína"],
    "torta frita": ["tortas fritas"],
    "alfajor": ["alfajores"],
    "empanada": ["empanadas"],
    "arepa": ["arepas"],
    "patacon": ["patacones", "patacón", "patacónes"],
    "buñuelo": ["buñuelos", "bunuelos"],
    "tamal": ["tamales"],
    "tortilla": ["tortillas"],
    "taco": ["tacos"],
    "enchilada": ["enchiladas"],
    "ñoqui": ["ñoquis", "ñoquis", "noqui", "noquis"],
    "choripan": ["choripanes", "choripán", "choripanes"],
    "humita": ["humitas"],
    "bizcocho": ["bizcochos"],
    "bocadillo": ["bocadillos"],
    "chile": ["chiles"],
    "quinua": ["quinoa"],
    "quinoa": ["quinua"],
    "postobón": ["postobon"],
    "inca kola": ["inca cola"],
}

def expand_phrase_variants(phrase: str) -> set[str]:
    """
    Generate normalized variants for a phrase:
    - base (normalized)
    - aliases (normalized)
    - pluralize first token and last token (normalized)
    """
    norm_base = normalize(phrase)
    variants = {norm_base}

    # aliases
    if norm_base in ALIASES:
        for alt in ALIASES[norm_base]:
            variants.add(normalize(alt))

    # pluralize first and last tokens of the phrase (if multiword)
    tokens = norm_base.split()
    if tokens:
        # pluralize first token
        t_first = tokens[:]
        t_first[0] = pluralize_es(t_first[0])
        variants.add(" ".join(t_first))
        # pluralize last token
        t_last = tokens[:]
        t_last[-1] = pluralize_es(t_last[-1])
        variants.add(" ".join(t_last))
    return variants

# === SAFE ADDRESS PARSING ===
def parse_city(addr: str) -> str | None:
    if not isinstance(addr, str):
        return None
    parts = [p.strip() for p in addr.split(',') if p.strip()]
    if len(parts) < 2:
        return None
    city_part = parts[-2]
    tokens = city_part.split()
    if tokens and any(ch.isdigit() for ch in tokens[0]):
        tokens = tokens[1:]
    city = " ".join(tokens).strip()
    return city or city_part

def parse_country(addr: str) -> str | None:
    if not isinstance(addr, str):
        return None
    parts = [p.strip() for p in addr.split(',') if p.strip()]
    return parts[-1] if parts else None

# === PAGE SCRAPER ===
def extract_info_from_page(url: str) -> dict:
    result = {"text": "", "emails": [], "socials": {}}
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return result
        soup = BeautifulSoup(response.text, "lxml")

        result["text"] = soup.get_text(separator=' ', strip=True)

        result["emails"] = re.findall(
            r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
            result["text"]
        )

        result["socials"] = {
            "instagram": None,
            "twitter": None,
            "facebook": None,
            "youtube": None,
            "whatsapp": None
        }
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if "instagram.com" in href and not result["socials"]["instagram"]:
                result["socials"]["instagram"] = href
            elif "twitter.com" in href and not result["socials"]["twitter"]:
                result["socials"]["twitter"] = href
            elif "facebook.com" in href and not result["socials"]["facebook"]:
                result["socials"]["facebook"] = href
            elif "youtube.com" in href or "youtu.be" in href:
                result["socials"]["youtube"] = result["socials"]["youtube"] or href
            elif "wa.me" in href or "whatsapp.com" in href:
                result["socials"]["whatsapp"] = result["socials"]["whatsapp"] or href

    except Exception:
        pass

    return result

# === WEBSITE SCRAPER ===
def extract_info_from_website(base_url: str) -> dict:
    combined_text = ""
    emails_set = set()
    socials = {"instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None}

    base_url = str(base_url).strip().rstrip("/")
    try:
        # Main page
        main = extract_info_from_page(base_url)
        combined_text += " " + main["text"]
        emails_set.update(main["emails"])
        socials.update({k: v or socials[k] for k, v in main["socials"].items()})

        # Subpages
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(base_url, headers=headers, timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")
            anchors = soup.find_all("a", href=True)
            keywords = [
                "contact", "about", "quienes", "nosotros", "contacto", "contactanos", "contact-us",
                "sobre-mi", "actividades", "quienes-somos", "que-ofrecemos", "precios", "horarios", "suscripciones"
            ]
            subpages = set()
            for a in anchors:
                href = a["href"].lower()
                text = a.get_text(strip=True).lower()
                if any(k in href or k in text for k in keywords):
                    subpages.add(urljoin(base_url, a["href"]))

            for sub_url in subpages:
                sub = extract_info_from_page(sub_url)
                combined_text += " " + sub["text"]
                emails_set.update(sub["emails"])
                socials.update({k: v or socials[k] for k, v in sub["socials"].items()})
    except Exception:
        pass

    # === MATCHING + SCORING ===
    normalized_text = normalize(combined_text)

    matched_keywords = {}
    country_matches = []
    country_scores = {}

    for country, types in product_keywords.items():
        matched_keywords[country] = {"products": [], "dishes": [], "brands": []}
        total_keywords = sum(len(v) for v in types.values())
        total_matched = 0

        for category, base_list in types.items():
            cat_matches = []

            for base_kw in base_list:
                variants = expand_phrase_variants(base_kw)
                # if ANY variant matches, record the BASE keyword as matched
                hit = False
                for var in variants:
                    # word boundary search on normalized text
                    if re.search(r'\b' + re.escape(var) + r'\b', normalized_text):
                        hit = True
                        break
                if hit:
                    cat_matches.append(base_kw)

            matched_keywords[country][category] = cat_matches
            total_matched += len(cat_matches)

        if total_keywords > 0 and total_matched > 0:
            score = round(total_matched / total_keywords, 3)
            country_scores[country] = score
            country_matches.append(country)

    # Aggregate found keywords (canonical bases)
    found_products = [kw for c in matched_keywords.values() for kw in c["products"]]
    found_dishes   = [kw for c in matched_keywords.values() for kw in c["dishes"]]
    found_brands   = [kw for c in matched_keywords.values() for kw in c["brands"]]

    # Top country and sorted matches
    top_country = max(country_scores.items(), key=lambda x: x[1])[0] if country_scores else None
    top_country_score = country_scores.get(top_country) if top_country else 0

    strong_country_matches = [
        country for country, score in sorted(country_scores.items(), key=lambda x: x[1], reverse=True)
        if score >= 0.5
    ]
    all_positive_countries = [
        country for country, score in sorted(country_scores.items(), key=lambda x: x[1], reverse=True)
        if score > 0
    ]

    return {
        "email": list(emails_set)[0] if emails_set else None,
        "instagram": socials["instagram"],
        "twitter": socials["twitter"],
        "facebook": socials["facebook"],
        "youtube": socials["youtube"],
        "whatsapp": socials["whatsapp"],
        "products": ", ".join(sorted(set(found_products))) if found_products else None,
        "dishes":   ", ".join(sorted(set(found_dishes)))   if found_dishes   else None,
        "brands":   ", ".join(sorted(set(found_brands)))   if found_brands   else None,
        "origin_countries": ", ".join(country_matches) if country_matches else None,
        "product_count": len(set(found_products + found_dishes + found_brands)),
        "country_match_count": len(country_matches),
        "top_country": top_country,
        "top_country_score": top_country_score,
        "strong_country_matches": ", ".join(strong_country_matches) if strong_country_matches else None,
        "all_positive_countries": ", ".join(all_positive_countries) if all_positive_countries else None,
        "country_scores": country_scores
    }

# === MAIN WORKFLOW ===
def main():
    df = pd.read_excel(INPUT_EXCEL_PATH)

    info_list = []
    for i, row in df.iterrows():
        website = row.get("website")
        print(f"🔍 Processing [{i+1}/{len(df)}]: {website}")
        if pd.notna(website):
            info = extract_info_from_website(str(website))
        else:
            info = {
                "email": None, "instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None,
                "products": None, "dishes": None, "brands": None, "origin_countries": None,
                "product_count": 0, "country_match_count": 0,
                "top_country": None, "top_country_score": 0,
                "strong_country_matches": None, "all_positive_countries": None,
                "country_scores": {}
            }
        info_list.append(info)

    # Flatten nested dicts (country_scores.*) into columns
    info_df = pd.json_normalize(info_list)
    final_df = pd.concat([df.reset_index(drop=True), info_df], axis=1)

    # Fill score NaNs with 0
    score_cols = [c for c in final_df.columns if c.startswith("country_scores.")]
    if score_cols:
        final_df[score_cols] = final_df[score_cols].fillna(0)

    # Robust city / country parsing from formatted_address
    if 'formatted_address' in final_df.columns:
        final_df['city'] = final_df['formatted_address'].apply(parse_city)
        final_df['country'] = final_df['formatted_address'].apply(parse_country)
    else:
        final_df['city'] = None
        final_df['country'] = None

    # Rename lat/lng if present
    final_df.rename(columns={'geometry_location_lat': 'lat', 'geometry_location_lng': 'lng'}, inplace=True)

    # Compute the most frequent cities and coutries and save them in a json
    city_counts = final_df['city'].value_counts().to_dict()
    country_counts = final_df['country'].value_counts().to_dict()
    with open(os.path.join(SCRIPT_DIR, "city_counts.json"), "w", encoding="utf-8") as f:
        json.dump(city_counts, f, ensure_ascii=False, indent=2)
    with open(os.path.join(SCRIPT_DIR, "country_counts.json"), "w", encoding="utf-8") as f:
        json.dump(country_counts, f, ensure_ascii=False, indent=2)
    print(f"✅ Computed city and country counts.")
    

    # Save
    final_df.to_excel(OUTPUT_EXCEL_PATH, index=False)
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        f.write(final_df.to_json(orient="records", force_ascii=False, indent=2))

    print(f"✅ Saved Excel to {OUTPUT_EXCEL_PATH}")
    print(f"✅ Saved JSON to {OUTPUT_JSON_PATH}")

if __name__ == "__main__":
    main()



import os
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import unicodedata
import json

# === CONFIGURATION ===
SCRIPT_DIR = 'C:\\Users\\faria\\tw\\data'
INPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_information.xlsx")
OUTPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.xlsx")
OUTPUT_JSON_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.json")

# === PRODUCT KEYWORDS BY COUNTRY ===
product_keywords = {
    "argentina": {
        "products": ["yerba mate", "dulce de leche", "asado", "chimichurri", "humita", "helado", "alfajores", "fernet", "vino malbec", "mate cocido", "milanesa", "provoleta", "choripán", "empanadas", "locro", "chocotorta", "facturas", "medialunas", "guiso de lentejas", "matambre"],
        "dishes": ["asado", "empanadas", "milanesa", "choripán", "provoleta", "locro", "humita", "carbonada", "matambre a la pizza", "matambre arrollado", "bondiola", "vacío", "bife de chorizo", "morcilla", "tamales salteños", "milanesa a la napolitana", "puchero", "guiso de lentejas", "fugazzeta", "revuelto gramajo"],
        "brands": ["Taragüi", "Playadito", "CBSé", "La Merced", "Cruz de Malta", "Rosamonte", "Nobleza Gaucha", "Anna Park", "Corrientes", "Yerba Madre", "Union", "Aguantadora", "Kraus", "Canarias", "Arcor", "Bagley", "La Serenísima", "Havanna", "Grido Helado", "Molinos Río de la Plata", "Marolio", "Terrabusi", "Lucchetti", "Matarazzo", "Paty", "Paladini", "Cachamai", "Bon o Bon", "Flynn Paff", "Cepita", "Manaos", "Quilmes", "Fernet Branca", "Coca-Cola Argentina", "Don Satur", "Trío Pepas", "Fantoche", "Jorgito", "Rhodesia", "San Ignacio", "Havannet", "Dulce de Leche Colonial", "Valley", "Cachay", "Gallo Snacks", "Molto", "Bagley Criollitas", "Tía Maruca", "Serenisima Ser", "Cindor", "Villavicencio", "Eco de los Andes", "Aguas Sierra de los Padres", "Natura", "Ades", "Ilolay", "Milkaut", "Verónica", "Estancia Santa Rosa", "Chango", "Molinos Ala", "La Virginia", "Cabrales", "Bonafide", "Georgalos", "Misky", "Vauquita", "Suchard Argentina", "Block", "Melba", "Caricia", "Toddy Argentina", "Nesquik Argentina", "Bagley Chocolinas", "Oreo Argentina", "Ser", "Yogurísimo", "Frutigran", "Arlistan", "Cachafaz", "El Noble", "Mostaza", "Freddo", "Volta", "Persicco", "El Trébol", "Sancor", "Tregar", "Maple", "Lheritier", "Granix", "Chocolates Águila", "La Campagnola", "Cocinero", "Knorr Argentina", "Hellmann’s Argentina", "Molinos Don Vicente", "Estancia El Rosario", "Mendoza Malbec Wines", "Trapiche"]
    },
    "uruguay": {
        "products": ["yerba mate", "dulce de leche", "grappamiel", "medio y medio"],
        "dishes": ["chivito", "fainá", "tortas fritas", "bizcochos", "croquetas", "milanesa", "asado", "alfajores", "buseca", "feijoada", "pizza", "pasteles"],
        "brands": ["canarias"]
    },
    "colombia": {
        "products": ["panela", "bocadillo", "aji", "arroz con coco"],
        "dishes": ["arepas", "bandeja paisa", "ajiaco", "sancocho", "lechona", "empanadas", "patacones", "chicharron", "tamales", "tamales tolimenses", "cazuela de mariscos", "calentado", "trucha", "arroz con pollo", "buñuelos"],
        "brands": ["Juan Valdez", "Postobón"]
    },
    "mexico": {
        "products": ["tortillas", "elote", "mole", "barbacoa", "cochinita pibil", "tacos al pastor", "tlayudas", "pozole", "tamales", "sopa de lima", "enchiladas suizas", "torta ahogada", "birria", "quesadillas", "burritos", "menudo"],
        "dishes": ["tacos", "tamales", "mole", "enchiladas", "pozole", "barbacoa", "tlayudas", "menudo", "torta ahogada", "birria", "aguachile"],
        "brands": []
    },
    "peru": {
        "products": ["aji amarillo", "quinoa", "pisco", "chicha morada", "aji de gallina", "tiradito"],
        "dishes": ["ceviche", "lomo saltado", "causa", "cuy", "anticuchos", "rocoto relleno", "arroz con pato", "pollo a la brasa", "chupe de camarones", "papas a la huancaina"],
        "brands": ["Inca Kola"]
    },
}

def normalize(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8").lower()

# === SUBPAGE DISCOVERY (multilingual keywords + bucket tagging) ===
SUBPAGE_KEYWORDS = {
    "contact": [
        "contact", "contacto", "contáctanos", "contactanos", "contato", "contatti", "kontakt"
    ],
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
    "menu_food": [
        "menu", "menú", "carta", "carta-digital", "platos", "comida",
        "food", "menu-del-dia", "menú-del-día"
    ],
    "locations": [
        "sucursales", "ubicaciones", "donde", "dónde", "locales", "puntos-de-venta",
        "punto-de-venta", "stores", "locations", "sedes", "direccion", "dirección",
        "address", "mapa", "cómo-llegar", "como-llegar"
    ],
    "hours": [
        "horario", "horarios", "opening-hours", "open-hours", "open", "orari",
        "öffnungszeiten", "oeffnungszeiten"
    ],
    "delivery_shipping": [
        "envio", "envíos", "envio-gratis", "envío-gratis", "envios", "envíos",
        "shipping", "delivery", "entrega", "reparto", "click-and-collect",
        "pickup", "retirar", "retira", "takeaway"
    ],
    "booking_orders": [
        "reservas", "reserva", "book", "booking", "appointment", "turnos",
        "citas", "order-online", "pedido-online", "checkout", "basket", "cart", "carrito"
    ]
}
BLACKLIST_SUBSTRINGS = (
    "mailto:", "tel:", "javascript:", "#", ".pdf", ".doc", ".docx", ".xls",
    ".xlsx", ".ppt", ".pptx", ".zip", ".rar", ".7z", ".jpg", ".jpeg", ".png",
    ".gif", ".webp", ".svg", "instagram.com", "facebook.com", "twitter.com",
    "x.com", "tiktok.com", "youtube.com", "youtu.be", "wa.me", "whatsapp.com"
)
MAX_SUBPAGES = 12

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
    keyword_hits = sum(
        any(normalize(kw) in u_norm for kw in kws) for kws in SUBPAGE_KEYWORDS.values()
    )
    return keyword_hits * 1000 + length_score

# === PAGE SCRAPER ===
def extract_info_from_page(url: str) -> dict:
    result = {"text": "", "emails": [], "socials": {}}
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return result
        soup = BeautifulSoup(response.text, "lxml")
        result["text"] = soup.get_text(separator=' ', strip=True)
        result["emails"] = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", result["text"])
        result["socials"] = {"instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None}
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if "instagram.com" in href and not result["socials"]["instagram"]:
                result["socials"]["instagram"] = href
            elif "twitter.com" in href and not result["socials"]["twitter"]:
                result["socials"]["twitter"] = href
            elif "facebook.com" in href and not result["socials"]["facebook"]:
                result["socials"]["facebook"] = href
            elif "youtube.com" in href or "youtu.be" in href:
                result["socials"]["youtube"] = result["socials"]["youtube"] or href
            elif "wa.me" in href or "whatsapp.com" in href:
                result["socials"]["whatsapp"] = result["socials"]["whatsapp"] or href
    except Exception:
        pass
    return result

# === WEBSITE SCRAPER ===
def extract_info_from_website(base_url: str) -> dict:
    combined_text = ""
    emails_set = set()
    socials = {"instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None}
    subpages_with_bucket = []  # list of {"url": ..., "bucket": ...}

    base_url = base_url.strip().rstrip("/")
    try:
        # Step 1: Main page
        main = extract_info_from_page(base_url)
        combined_text += " " + main["text"]
        emails_set.update(main["emails"])
        socials.update({k: v or socials[k] for k, v in main["socials"].items()})

        # Step 2: Discover candidate subpages (bucket-tagged)
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(base_url, headers=headers, timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")
        else:
            soup = None

        candidates = []
        for a in soup.find_all("a", href=True):
            href_raw = a["href"]
            text_raw = a.get_text(strip=True)
            if not href_raw or any(x in href_raw.lower() for x in BLACKLIST_SUBSTRINGS):
                continue
            full_url = urljoin(base_url, href_raw)
            if not is_same_site(base_url, full_url):
                continue
            bucket = match_bucket(href_raw, text_raw)
            if bucket:
                candidates.append({"url": full_url, "bucket": bucket})

        # Deduplicate by URL, score and limit
        dedup = {}
        for c in candidates:
            dedup[c["url"]] = c["bucket"]
        ranked = sorted(dedup.items(), key=lambda kv: score_url(kv[0]), reverse=True)[:MAX_SUBPAGES]
        subpages_with_bucket = [{"url": u, "bucket": b} for u, b in ranked]

        # Crawl selected subpages
        for item in subpages_with_bucket:
            sub_url, bucket = item["url"], item["bucket"]
            sub = extract_info_from_page(sub_url)
            combined_text += " " + sub["text"]
            emails_set.update(sub["emails"])
            socials.update({k: v or socials[k] for k, v in sub["socials"].items()})
    except Exception:
        pass

    # === MATCHING + SCORING ===
    normalized_text = normalize(combined_text)
    matched_keywords = {}
    country_matches = []
    country_scores = {}

    for country, types in product_keywords.items():
        matched_keywords[country] = {"products": [], "dishes": [], "brands": []}
        total_keywords = sum(len(v) for v in types.values())
        total_matched = 0
        for category, kws in types.items():
            matches = [kw for kw in kws if re.search(r'\b' + re.escape(normalize(kw)) + r'\b', normalized_text)]
            matched_keywords[country][category] = matches
            total_matched += len(matches)
        if total_matched > 0 and total_keywords > 0:
            score = round(total_matched / total_keywords, 3)
            country_scores[country] = score
            country_matches.append(country)

    # Aggregate found keywords
    found_products = [kw for c in matched_keywords.values() for kw in c["products"]]
    found_dishes = [kw for c in matched_keywords.values() for kw in c["dishes"]]
    found_brands = [kw for c in matched_keywords.values() for kw in c["brands"]]

    # Top country and strong matches (>= 0.5), sorted desc
    top_country = max(country_scores.items(), key=lambda x: x[1])[0] if country_scores else None
    top_country_score = country_scores.get(top_country) if top_country else 0
    strong_country_matches = [country for country, score in sorted(country_scores.items(), key=lambda x: x[1], reverse=True) if score >= 0.5]
    all_positive_countries = [country for country, score in sorted(country_scores.items(), key=lambda x: x[1], reverse=True) if score > 0]

    return {
        "email": list(emails_set)[0] if emails_set else None,
        "instagram": socials["instagram"],
        "twitter": socials["twitter"],
        "facebook": socials["facebook"],
        "youtube": socials["youtube"],
        "whatsapp": socials["whatsapp"],
        "products": ", ".join(sorted(set(found_products))) if found_products else None,
        "dishes": ", ".join(sorted(set(found_dishes))) if found_dishes else None,
        "brands": ", ".join(sorted(set(found_brands))) if found_brands else None,
        "origin_countries": ", ".join(country_matches) if country_matches else None,
        "product_count": len(set(found_products + found_dishes + found_brands)),
        "country_match_count": len(country_matches),
        "top_country": top_country,
        "top_country_score": top_country_score,
        "strong_country_matches": ", ".join(strong_country_matches) if strong_country_matches else None,
        "all_positive_countries": ", ".join(all_positive_countries) if all_positive_countries else None,
        "country_scores": country_scores,
        "subpages_crawled": json.dumps(subpages_with_bucket, ensure_ascii=False),
        "text_content": combined_text.strip() if combined_text else None   # 🔥 new field
    }

# === MAIN WORKFLOW ===
def main():
    df = pd.read_excel(INPUT_EXCEL_PATH)
    info_list = []

    for i, row in df.iterrows():
        website = row.get("website")
        print(f"🔍 Processing [{i+1}/{len(df)}]: {website}")
        if pd.notna(website):
            info = extract_info_from_website(str(website))
        else:
            info = {
                "email": None, "instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None,
                "products": None, "dishes": None, "brands": None, "origin_countries": None,
                "product_count": 0, "country_match_count": 0,
                "top_country": None, "top_country_score": 0, "strong_country_matches": None,
                "all_positive_countries": None, "country_scores": {}, "subpages_crawled": "[]", "text_content": None
            }
        info_list.append(info)

    # Flatten nested dicts (country_scores.*) into columns
    info_df = pd.json_normalize(info_list)
    final_df = pd.concat([df.reset_index(drop=True), info_df], axis=1)

    # Ensure score columns have 0 instead of NaN
    score_cols = [c for c in final_df.columns if c.startswith("country_scores.")]
    if score_cols:
        final_df[score_cols] = final_df[score_cols].fillna(0)

    # City / Country parsing from formatted_address
    # final_df['city'] = final_df['formatted_address'].apply(
    #     lambda x: ' '.join(x.split(',')[-2].strip().split(' ')[1:]) if pd.notna(x) else None
    # )
    # final_df['country'] = final_df['formatted_address'].apply(
    #     lambda x: x.split(',')[-1].strip() if pd.notna(x) else None
    # )
    # Rename lat/lng
    # final_df.rename(columns={'geometry_location_lat': 'lat', 'geometry_location_lng': 'lng'}, inplace=True)

    # Save
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        f.write(final_df.to_json(orient="records", force_ascii=False, indent=2))

    final_df.to_excel(OUTPUT_EXCEL_PATH, index=False)
    
    print(f"✅ Saved Excel to {OUTPUT_EXCEL_PATH}")
    print(f"✅ Saved JSON to {OUTPUT_JSON_PATH}")

if __name__ == "__main__":
    main()
