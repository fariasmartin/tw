# process_site_texts.py
import os
import json
import re
import unicodedata
import pandas as pd
import ast  # safe eval
import math

from product_keywords import product_keywords, ALIASES, category_mapping


from typing import Any, Dict, Optional, Tuple, List


import unicodedata

def _sortkey(s: str) -> str:
    # orden alfabético ignorando acentos y mayúsculas
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()

def types_to_category(ts):
    if not ts:
        return None  # no poner "Otros"
    mapped = set()
    for t in ts:
        if t is None:
            continue
        # intenta con la clave tal cual y en minúsculas (por si las claves del mapping están en lower)
        v = category_mapping.get(t) or category_mapping.get(str(t).lower())
        if v:
            mapped.add(v)
    if not mapped:
        return None  # ningún tipo mapeó -> sin categoría
    return ", ".join(sorted(mapped, key=_sortkey))  # único + orden alfabético



CITY_TYPES_PRIORITY: List[str] = [
        "locality",                  # ciudad/municipio (lo más fiable)
        "postal_town",               # usado en UK/Irlanda
        "administrative_area_level_3",
        "sublocality",
        "sublocality_level_1",
        "administrative_area_level_2" # provincia (fallback si no hay ciudad)
        ]



def _coerce_components(x: Any) -> List[dict]:
    # Vacíos / NaN
    if x is None:
        return []
    if isinstance(x, float) and math.isnan(x):
        return []

    # Ya-lista
    if isinstance(x, list):
        return x

    # A veces llega un dict (poco común)
    if isinstance(x, dict):
        # Si viniera envuelto (por ejemplo {"address_components": [...]})
        if "address_components" in x and isinstance(x["address_components"], list):
            return x["address_components"]
        # Si fuera un único componente suelto
        return [x]

    # Cadena: JSON o repr con comillas simples
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(s)
            except (ValueError, SyntaxError):
                # Último intento: reemplazar comillas simples por dobles (riesgo bajo aquí)
                try:
                    return json.loads(s.replace("'", '"'))
                except Exception:
                    return []

    # Cualquier otro tipo: devuelve vacío
    return []


CITY_TYPES_PRIORITY: List[str] = [
    "locality",
    "postal_town",
    "administrative_area_level_3",
    "sublocality",
    "sublocality_level_1",
    "administrative_area_level_2"  # fallback
]

def extract_city_country_from_components(address_components: Any) -> Dict[str, Optional[str]]:
    comps = _coerce_components(address_components)

    def find_by_type(target: str) -> Optional[dict]:
        for c in comps:
            if target in set(c.get("types", [])):
                return c
        return None

    city = None
    for t in CITY_TYPES_PRIORITY:
        comp = find_by_type(t)
        if comp:
            city = comp.get("long_name") or comp.get("short_name")
            if city:
                break

    country_comp = find_by_type("country")
    country = country_comp.get("long_name") if country_comp else None
    country_code = country_comp.get("short_name") if country_comp else None

    return {"city": city, "country": country, "country_code": country_code}

# Optional: only if you also need to parse a plain address string somewhere else.
def extract_city_country_from_address_string(address: str):
    if not isinstance(address, str) or not address.strip():
        return None, None
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    if len(parts) == 1:
        return parts[0], None
    return None, None




# === CONFIGURATION ===
SCRIPT_DIR = r'C:\Users\faria\tw\data'
INPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_information.xlsx")
INPUT_JSONL_PATH = os.path.join(SCRIPT_DIR, "website_texts.jsonl")
OUTPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.xlsx")
OUTPUT_JSON_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.json")

# === NORMALIZATION ===
def normalize(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8").lower()

def pluralize_es(word: str) -> str:
    w = word.strip()
    if not w:
        return w
    if w.endswith(('a','e','i','o','u')):
        return w + 's'
    if w.endswith('z'):
        return w[:-1] + 'ces'
    return w + 'es'

def expand_phrase_variants(phrase: str) -> set[str]:
    base = normalize(phrase)
    variants = {base}
    if base in ALIASES:
        for alt in ALIASES[base]:
            variants.add(normalize(alt))
    tokens = base.split()
    if tokens:
        t1 = tokens[:]; t1[0] = pluralize_es(t1[0]); variants.add(" ".join(t1))
        t2 = tokens[:]; t2[-1] = pluralize_es(t2[-1]); variants.add(" ".join(t2))
    return variants

# === PROCESSING ===
def score_combined_text(combined_text: str, emails: list, socials: dict, subpages_crawled) -> dict:
    normalized_text = normalize(combined_text or "")

    matched_keywords = {}
    country_matches = []
    country_scores = {}

    for country, types in product_keywords.items():
        matched_keywords[country] = {"products": [], "dishes": [], "brands": []}
        total_keywords = sum(len(v) for v in types.values())
        total_matched = 0

        for category, kws in types.items():
            cat_matches = []
            for kw in kws:
                variants = expand_phrase_variants(kw)
                if any(re.search(r'\b' + re.escape(v) + r'\b', normalized_text) for v in variants):
                    cat_matches.append(kw)
            matched_keywords[country][category] = cat_matches
            total_matched += len(cat_matches)

        if total_matched > 0 and total_keywords > 0:
            score = round(total_matched / total_keywords, 3)
            country_scores[country] = score
            country_matches.append(country)

    found_products = [kw for c in matched_keywords.values() for kw in c["products"]]
    found_dishes   = [kw for c in matched_keywords.values() for kw in c["dishes"]]
    found_brands   = [kw for c in matched_keywords.values() for kw in c["brands"]]

    top_country = max(country_scores.items(), key=lambda x: x[1])[0] if country_scores else None
    top_country_score = country_scores.get(top_country) if top_country else 0

    strong_country_matches = [c for c, s in sorted(country_scores.items(), key=lambda x:x[1], reverse=True) if s >= 0.5]
    all_positive_countries = [c for c, s in sorted(country_scores.items(), key=lambda x:x[1], reverse=True) if s > 0]

    return {
        "email": emails[0] if emails else None,
        "instagram": socials.get("instagram") if isinstance(socials, dict) else None,
        "twitter": socials.get("twitter") if isinstance(socials, dict) else None,
        "facebook": socials.get("facebook") if isinstance(socials, dict) else None,
        "youtube": socials.get("youtube") if isinstance(socials, dict) else None,
        "whatsapp": socials.get("whatsapp") if isinstance(socials, dict) else None,
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
        "country_scores": country_scores,
        "subpages_crawled": json.dumps(subpages_crawled or [], ensure_ascii=False),
        "text_content": (combined_text or "").strip() or None
    }

def read_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def parse_types(x):
    try:
        return ast.literal_eval(x) if isinstance(x, str) else (x if isinstance(x, list) else [])
    except Exception:
        return []

def extract_city_country(address: str):
    if not isinstance(address, str) or not address.strip():
        return None, None
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    if len(parts) == 1:
        return parts[0], None
    return None, None

def main():
    base_df = pd.read_excel(INPUT_EXCEL_PATH)
    bundles = {rec["row_index"]: rec for rec in read_jsonl(INPUT_JSONL_PATH)}

    info_list = []
    for i in range(len(base_df)):
        b = bundles.get(i)
        if b:
            result = score_combined_text(
                b.get("combined_text", ""),
                b.get("emails", []),
                b.get("socials", {}),
                b.get("subpages_crawled", [])
            )
        else:
            result = {
                "email": None, "instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None,
                "products": None, "dishes": None, "brands": None, "origin_countries": None,
                "product_count": 0, "country_match_count": 0,
                "top_country": None, "top_country_score": 0,
                "strong_country_matches": None, "all_positive_countries": None,
                "country_scores": {}, "subpages_crawled": "[]", "text_content": None
            }
        info_list.append(result)

    info_df = pd.json_normalize(info_list)
    final_df = pd.concat([base_df.reset_index(drop=True), info_df], axis=1)

    # Rellenar NaN en columnas de scores
    score_cols = [c for c in final_df.columns if c.startswith("country_scores.")]
    if score_cols:
        final_df[score_cols] = final_df[score_cols].fillna(0)

    # Procesar tipos
    final_df["type_list"] = final_df["types"].apply(parse_types)
    


    final_df["category"] = final_df["type_list"].apply(types_to_category)

    def safe_extract(addr_comp):
        try:
            return extract_city_country_from_components(addr_comp)
        except Exception:
            return {"city": None, "country": None, "country_code": None}

    tmp = final_df["address_components"].map(safe_extract).apply(pd.Series)
    final_df["city"] = tmp["city"]
    final_df["country"] = tmp["country"]
    final_df["country_code"] = tmp["country_code"]


    # create origins column
    final_df["origins"] = pd.Series([["Argentina"] for _ in range(len(final_df))])

    # Renombrar coordenadas
    final_df.rename(columns={
        "geometry_location_lat": "lat",
        "geometry_location_lng": "lng"
    }, inplace=True)

    # Guardar
    final_df.to_excel(OUTPUT_EXCEL_PATH, index=False)
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        f.write(final_df.to_json(orient="records", force_ascii=False, indent=2))

    print(f"✅ Saved Excel to {OUTPUT_EXCEL_PATH}")
    print(f"✅ Saved JSON to {OUTPUT_JSON_PATH}")

if __name__ == "__main__":
    main()


#Load the final data
final_data = pd.read_excel(OUTPUT_EXCEL_PATH)

# Display the list of city and country anmes
print("Cities:", final_data['city'].dropna().unique())
print("Countries:", final_data['country'].dropna().unique())

# Change city names
final_data['city'] = final_data['city'].replace({
    'A Coruña': 'La Coruña',
    'Bologna': 'Bolonia',
    'Alicante (Alacant)': 'Alicante',
    'Sagunt': 'Sagunto',
    'London': 'Londres',
    'București': 'Bucarest',
    'Paris': 'París',
    'València': 'Valencia',
    "L'Hospitalet de Llobregat": 'Hospitalet de Llobregat',
    'Milano': 'Milán',
    "L'Hospitalet de l'Infant": 'Hospitalet del Infante',
    'Port de Sagunt': 'Puerto de Sagunto',
    'Antwerpen': 'Amberes',
    'Alexandria': 'Alejandría',
    'Bruxelles': 'Bruselas',
    'el Gran Alacant': 'Gran Alacant',
    'Alacant': 'Alicante',
    'Illes Balears': 'Islas Baleares',
    'Alacant': 'Alicante'  
})

# still have to change "el"

# Guardar
final_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
    f.write(final_data.to_json(orient="records", force_ascii=False, indent=2))

print(f"✅ Saved Excel to {OUTPUT_EXCEL_PATH}")
print(f"✅ Saved JSON to {OUTPUT_JSON_PATH}")