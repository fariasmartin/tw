# process_site_texts.py
import os
import json
import re
import unicodedata
import pandas as pd
import ast  # safe eval

from product_keywords import product_keywords, ALIASES, category_mapping

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
    final_df["category"] = final_df["type_list"].apply(
        lambda ts: ", ".join(sorted({category_mapping.get(t, "Other") for t in ts})) if ts else "Other"
    )

    # Extraer ciudad y país desde formatted_address
    final_df["city"], final_df["country"] = zip(*final_df["formatted_address"].apply(extract_city_country))

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



# # process_site_texts.py
# import os
# import json
# import re
# import unicodedata
# import pandas as pd
# from product_keywords import product_keywords, ALIASES, category_mapping
# import ast  # safe way to evaluate list-like strings

# # === CONFIGURATION ===
# SCRIPT_DIR = r'C:\Users\faria\tw\data'
# INPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_information.xlsx")
# INPUT_JSONL_PATH = os.path.join(SCRIPT_DIR, "website_texts.jsonl")  # produced by your fetcher
# OUTPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.xlsx")
# OUTPUT_JSON_PATH = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.json")


# def parse_types(x):
#     try:
#         return ast.literal_eval(x) if isinstance(x, str) else []
#     except Exception:
#         return []


# def _safe_literal_list(x):
#     """Parsea listas stringificadas, respeta listas reales y tolera NaN."""
#     if isinstance(x, list):
#         return x
#     if isinstance(x, str):
#         try:
#             val = ast.literal_eval(x)
#             return val if isinstance(val, list) else [x]
#         except Exception:
#             return [x]  # deja el string como único tipo
#     return []  # NaN u otros


# # === NORMALIZATION ===
# def normalize(text: str) -> str:
#     """Lowercase + strip accents."""
#     if not isinstance(text, str):
#         return ""
#     return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8").lower()

# def pluralize_es(word: str) -> str:
#     w = word.strip()
#     if not w:
#         return w
#     if w.endswith(('a','e','i','o','u')):
#         return w + 's'
#     if w.endswith('z'):
#         return w[:-1] + 'ces'
#     return w + 'es'

# def expand_phrase_variants(phrase: str) -> set[str]:
#     """
#     Return a set of normalized variants for a phrase:
#       - base (normalized)
#       - aliases (normalized)
#       - pluralize first and last tokens (normalized)
#     """
#     base = normalize(phrase)
#     variants = {base}

#     # aliases
#     if base in ALIASES:
#         for alt in ALIASES[base]:
#             variants.add(normalize(alt))

#     tokens = base.split()
#     if tokens:
#         # pluralize first token
#         t1 = tokens[:]
#         t1[0] = pluralize_es(t1[0])
#         variants.add(" ".join(t1))
#         # pluralize last token
#         t2 = tokens[:]
#         t2[-1] = pluralize_es(t2[-1])
#         variants.add(" ".join(t2))

#     return variants

# # === PROCESSING ===
# def score_combined_text(combined_text: str, emails: list, socials: dict, subpages_crawled) -> dict:
#     """
#     - normalize site text
#     - word-boundary search for each keyword (with variants) in normalized text
#     - per-country score = matched_keywords / total_keywords
#     - compute top_country, strong_country_matches (>= 0.5), all_positive_countries (> 0)
#     - include text_content and subpages_crawled in output
#     """
#     normalized_text = normalize(combined_text or "")

#     matched_keywords = {}
#     country_matches = []
#     country_scores = {}

#     for country, types in product_keywords.items():
#         matched_keywords[country] = {"products": [], "dishes": [], "brands": []}
#         total_keywords = sum(len(v) for v in types.values())
#         total_matched = 0

#         for category, kws in types.items():
#             cat_matches = []
#             for kw in kws:
#                 # generate normalized variants for the keyword
#                 variants = expand_phrase_variants(kw)
#                 # if any variant matches, record the canonical keyword
#                 if any(re.search(r'\b' + re.escape(v) + r'\b', normalized_text) for v in variants):
#                     cat_matches.append(kw)
#             matched_keywords[country][category] = cat_matches
#             total_matched += len(cat_matches)

#         if total_matched > 0 and total_keywords > 0:
#             score = round(total_matched / total_keywords, 3)
#             country_scores[country] = score
#             country_matches.append(country)

#     # Aggregate found keywords
#     found_products = [kw for c in matched_keywords.values() for kw in c["products"]]
#     found_dishes   = [kw for c in matched_keywords.values() for kw in c["dishes"]]
#     found_brands   = [kw for c in matched_keywords.values() for kw in c["brands"]]

#     # Top + strong/all positive (sorted desc by score)
#     top_country = max(country_scores.items(), key=lambda x: x[1])[0] if country_scores else None
#     top_country_score = country_scores.get(top_country) if top_country else 0

#     strong_country_matches = [
#         country for country, score in sorted(country_scores.items(), key=lambda x: x[1], reverse=True)
#         if score >= 0.5
#     ]
#     all_positive_countries = [
#         country for country, score in sorted(country_scores.items(), key=lambda x: x[1], reverse=True)
#         if score > 0
#     ]

#     return {
#         "email": emails[0] if emails else None,
#         "instagram": socials.get("instagram") if isinstance(socials, dict) else None,
#         "twitter": socials.get("twitter") if isinstance(socials, dict) else None,
#         "facebook": socials.get("facebook") if isinstance(socials, dict) else None,
#         "youtube": socials.get("youtube") if isinstance(socials, dict) else None,
#         "whatsapp": socials.get("whatsapp") if isinstance(socials, dict) else None,

#         "products": ", ".join(sorted(set(found_products))) if found_products else None,
#         "dishes":   ", ".join(sorted(set(found_dishes)))   if found_dishes   else None,
#         "brands":   ", ".join(sorted(set(found_brands)))   if found_brands   else None,

#         "origin_countries": ", ".join(country_matches) if country_matches else None,
#         "product_count": len(set(found_products + found_dishes + found_brands)),
#         "country_match_count": len(country_matches),

#         "top_country": top_country,
#         "top_country_score": top_country_score,
#         "strong_country_matches": ", ".join(strong_country_matches) if strong_country_matches else None,
#         "all_positive_countries": ", ".join(all_positive_countries) if all_positive_countries else None,
#         "country_scores": country_scores,

#         "subpages_crawled": json.dumps(subpages_crawled or [], ensure_ascii=False),
#         "text_content": (combined_text or "").strip() or None
#     }

# def read_jsonl(path: str):
#     with open(path, "r", encoding="utf-8") as f:
#         for line in f:
#             if line.strip():
#                 yield json.loads(line)

# def main():
#     # Load base Excel to merge back your original columns
#     base_df = pd.read_excel(INPUT_EXCEL_PATH)

#     # Read bundles from the fetcher (by row_index)
#     bundles = {rec["row_index"]: rec for rec in read_jsonl(INPUT_JSONL_PATH)}

#     info_list = []
#     for i in range(len(base_df)):
#         b = bundles.get(i)
#         if b:
#             combined_text = b.get("combined_text", "")
#             emails = b.get("emails", [])
#             socials = b.get("socials", {"instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None})
#             subpages_crawled = b.get("subpages_crawled", [])
#             result = score_combined_text(combined_text, emails, socials, subpages_crawled)
#         else:
#             # fallback if no bundle
#             result = {
#                 "email": None, "instagram": None, "twitter": None, "facebook": None, "youtube": None, "whatsapp": None,
#                 "products": None, "dishes": None, "brands": None, "origin_countries": None,
#                 "product_count": 0, "country_match_count": 0,
#                 "top_country": None, "top_country_score": 0,
#                 "strong_country_matches": None, "all_positive_countries": None,
#                 "country_scores": {}, "subpages_crawled": "[]", "text_content": None
#             }
#         info_list.append(result)

#     # Flatten nested dicts (country_scores.*) into columns
#     info_df = pd.json_normalize(info_list)
#     final_df = pd.concat([base_df.reset_index(drop=True), info_df], axis=1)

#     # Ensure score columns have 0 instead of NaN
#     score_cols = [c for c in final_df.columns if c.startswith("country_scores.")]
#     if score_cols:
#         final_df[score_cols] = final_df[score_cols].fillna(0)

#     ###################################################################################################################
#     ###################################################################################################################
#     # Data cleaning
#     ###################################################################################################################
#     ###################################################################################################################








#     ###################################################################################################################
#     ###################################################################################################################
#     ###################################################################################################################
#     ###################################################################################################################

#     # Save outputs
#     final_df.to_excel(OUTPUT_EXCEL_PATH, index=False)
#     with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
#         f.write(final_df.to_json(orient="records", force_ascii=False, indent=2))

#     print(f"✅ Saved Excel to {OUTPUT_EXCEL_PATH}")
#     print(f"✅ Saved JSON to {OUTPUT_JSON_PATH}")

# if __name__ == "__main__":
#     main()


# # Load final_df
# final_df = pd.read_excel(OUTPUT_EXCEL_PATH)





    

#     type_col = 'types'
#     if type_col:
#         final_df["type_list"] = final_df[type_col].apply(_safe_literal_list)
#         def map_category(types):
#             cats = {category_mapping.get(t, "Other") for t in types}
#             # prioriza tus 5-6 categorías si quieres (Groceries/Cafe/Bar/Restaurant/Bakery)
#             # si solo hay "Other" y nada más, deja "Other"
#             return ", ".join(sorted(cats)) if cats else "Other"
#         final_df["category"] = final_df["type_list"].apply(map_category)
#     else:
#         final_df["type_list"] = [[] for _ in range(len(final_df))]
#         final_df["category"] = "Other"

#     # Create city and country columns from address
#     def extract_city_country(address):
#         if not isinstance(address, str) or not address.strip():
#             return None, None
#         parts = [part.strip() for part in address.split(",")]
#         if len(parts) >= 2:
#             city = parts[-2]
#             country = parts[-1]
#             return city, country
#         elif len(parts) == 1:
#             return parts[0], None
#         return None, None

#     final_df["city"], final_df["country"] = zip(*final_df["formatted_address"].apply(extract_city_country))

#     # Rename latitude and longitude columns
#     final_df.rename(columns={"latitude": "lat", "longitude": "lng"}, inplace=True)