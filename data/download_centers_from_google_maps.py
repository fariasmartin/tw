import requests
import pandas as pd
from pandas import json_normalize
import time
import os

SCRIPT_DIR = 'C:\\Users\\faria\\tw\\data'
#SCRIPT_DIR = '/Users/marinabosque/Documents/yw/data'

# === CONFIGURATION ===
API_KEY = "AIzaSyDNkzJmsTIW2RVwjfZWnYRVBqJYmKHWicY"  # Replace with your actual API key
INPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "place_ids_from_text_search.xlsx")   # Replace with your Excel file name
SHEET_NAME = 'Sheet1'  # or use the sheet name, e.g., "Sheet1"
PLACE_ID_COLUMN = "place_id"  # Column in your Excel that contains Place IDs
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "centers_with_google_maps_information.xlsx")

# === FIELDS TO KEEP ===
KEEP_FIELDS = [
    "address_components",
    "adr_address",
    "business_status",
    "curbside_pickup",
    "delivery",
    "dine_in",
    "editorial_summary_overview",
    "formatted_address",
    "formatted_phone_number",
    "geometry_location_lat",
    "geometry_location_lng",
    "icon_mask_base_uri",
    "icon_background_color",
    "international_phone_number",
    "name",
    "opening_hours_weekday_text",
    "place_id",
    "plus_code_global_code",
    "plus_code_compound_code",
    "price_level",
    "rating",
    "reservable",
    "reviews",
    "serves_beer",
    "serves_breakfast",
    "serves_brunch",
    "serves_dinner",
    "serves_lunch",
    "serves_vegetarian_food",
    "serves_wine",
    "takeout",
    "types",
    "url",
    "user_ratings_total",
    "utc_offset_minutes",
    "vicinity",
    "website",
    "wheelchair_accessible_entrance"
]

# === LOAD PLACE IDS FROM EXCEL ===
df_ids = pd.read_excel(INPUT_EXCEL_PATH, sheet_name=SHEET_NAME)
place_ids = df_ids[PLACE_ID_COLUMN].dropna().unique()

print(f"🔢 Loaded {len(place_ids)} unique Place IDs from {INPUT_EXCEL_PATH}")

# === API CALL FUNCTION ===
def get_place_details(place_id, lang="es"):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "reviews_no_translations":"true",
        "key": API_KEY,
        "language": lang
    }
    response = requests.get(url, params=params)
    print("🔧 Debug:", response.status_code, response.url)
    #print("🔧 Raw JSON:", response.json())  # <-- this shows error message if any

    if response.status_code != 200:
        print(f"⚠️ Failed request for {place_id}")
        return {}

    data = response.json()
    if data.get("status") != "OK":
        print(f"⚠️ API returned error for {place_id}: {data.get('status')} - {data.get('error_message')}")
        return {}

    return data.get("result", {})


# === PROCESS ALL PLACE IDS ===
results = []

for pid in place_ids:
    print(f"🔍 Fetching: {pid}")
    result = get_place_details(pid)
    
    flat = pd.DataFrame(columns=KEEP_FIELDS)
    row_data = {field: None for field in KEEP_FIELDS}
    row_data["place_id"] = pid  # always preserve place_id

    if result:
        flat = json_normalize(result, sep='_')
        for field in KEEP_FIELDS:
            row_data[field] = flat[field].values[0] if field in flat.columns else None
        row_data["appears_in_google_maps"] = 1
    else:
        row_data["appears_in_google_maps"] = 0  # explicitly mark missing

    results.append(pd.DataFrame([row_data]))  # wrap in DataFrame to concatenate later
    time.sleep(1)


# añadir columna equal to 1 always
if results:
    for df in results:
        df['appears_in_google_maps'] = 1


# === COMBINE AND SAVE ===
if results: 
    final_df = pd.concat(results, ignore_index=True)
    final_df.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Saved enriched data to {OUTPUT_FILE}")
else:
    print("❌ No valid results to save.")
