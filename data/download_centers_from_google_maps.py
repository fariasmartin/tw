import requests
import pandas as pd
from pandas import json_normalize
import time, os

SCRIPT_DIR = r'C:\Users\faria\tw\data'
API_KEY = "AIzaSyDNkzJmsTIW2RVwjfZWnYRVBqJYmKHWicY" # Replace with your actual API key I
INPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "place_ids_from_text_search.xlsx")
SHEET_NAME = 'Sheet1'
PLACE_ID_COLUMN = "place_id"
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "centers_with_google_maps_information.xlsx")

KEEP_FIELDS = [
    "address_components","adr_address","business_status","formatted_address",
    "formatted_phone_number","geometry_location_lat","geometry_location_lng",
    "icon_mask_base_uri","icon_background_color","international_phone_number",
    "name","opening_hours_weekday_text","place_id","plus_code_global_code",
    "plus_code_compound_code","types","url","website","wheelchair_accessible_entrance"
]

# Usa proxies del sistema si existen (útil en redes corporativas)
PROXIES = {
    "http": os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy"),
    "https": os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"),
}
# Limpia claves vacías
PROXIES = {k: v for k, v in PROXIES.items() if v}

SESSION = requests.Session()

def get_place_details(place_id, lang="es", region="ES"):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "key": API_KEY,
        "language": lang,
        "region": region,
        # limita campos (ahorras cuota y procesas menos):
        "fields": "place_id,name,formatted_address,adr_address,address_components,geometry/location,formatted_phone_number,international_phone_number,opening_hours/weekday_text,url,website,icon_mask_base_uri,icon_background_color,plus_code,types,business_status,wheelchair_accessible_entrance",
        # opcional:
        "reviews_no_translations": "true",
    }
    try:
        r = SESSION.get(url, params=params, timeout=(5, 25), proxies=PROXIES)
        print("🔧 Debug:", r.status_code, r.url)
        r.raise_for_status()  # lanza si 4xx/5xx
    except requests.exceptions.RequestException as e:
        print(f"🌐 Network error for {place_id}: {e}")
        return None

    data = r.json()
    if data.get("status") != "OK":
        print(f"⚠️ API error for {place_id}: {data.get('status')} | {data.get('error_message')}")
        return None

    return data["result"]

# === LOAD PLACE IDS ===
df_ids = pd.read_excel(INPUT_EXCEL_PATH, sheet_name=SHEET_NAME)
place_ids = df_ids[PLACE_ID_COLUMN].dropna().astype(str).unique()
print(f"🔢 Loaded {len(place_ids)} unique Place IDs")

results = []
for pid in place_ids:
    print(f"🔍 Fetching: {pid}")
    result = get_place_details(pid)

    row_data = {field: None for field in KEEP_FIELDS}
    row_data["place_id"] = pid
    if result:
        flat = json_normalize(result, sep='_')
        for field in KEEP_FIELDS:
            row_data[field] = flat[field].iloc[0] if field in flat.columns and not flat.empty else None
        row_data["appears_in_google_maps"] = 1
    else:
        row_data["appears_in_google_maps"] = 0

    results.append(pd.DataFrame([row_data]))
    time.sleep(0.8)  # respira un poco

# ⚠️ Elimina tu bloque que forzaba 1 siempre (estaba pisando el 0):
# for df in results: df['appears_in_google_maps'] = 1   <-- NO HACER

if results:
    final_df = pd.concat(results, ignore_index=True)
    final_df.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Saved enriched data to {OUTPUT_FILE}")
else:
    print("❌ No valid results to save.")
