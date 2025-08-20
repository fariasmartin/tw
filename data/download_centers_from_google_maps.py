import requests
import pandas as pd
from pandas import json_normalize
import time
import os

SCRIPT_DIR = 'C:\\Users\\faria\\yw\\data'
SCRIPT_DIR = '/Users/marinabosque/Documents/yw/data'

# === CONFIGURATION ===
API_KEY = "AIzaSyDNkzJmsTIW2RVwjfZWnYRVBqJYmKHWicY"  # Replace with your actual API key
INPUT_EXCEL_PATH = os.path.join(SCRIPT_DIR, "place_ids.xlsx")   # Replace with your Excel file name
SHEET_NAME = 'Sheet1'  # or use the sheet name, e.g., "Sheet1"
PLACE_ID_COLUMN = "place_id"  # Column in your Excel that contains Place IDs
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "centers_with_google_maps_information.xlsx")


# === FIELDS TO KEEP ===
KEEP_FIELDS = [
    "business_status",
    "formatted_address",
    "international_phone_number",
    "name",
    "place_id",
    "rating",
    "url",
    "user_ratings_total",
    "website",
    "geometry_location_lat",
    "geometry_location_lng"
]

# === LOAD PLACE IDS FROM EXCEL ===
df_ids = pd.read_excel(INPUT_EXCEL_PATH, sheet_name=SHEET_NAME)
place_ids = df_ids[PLACE_ID_COLUMN].dropna().unique()

# === API CALL FUNCTION ===
def get_place_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "key": API_KEY
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
    if result:
        flat = json_normalize(result, sep='_')
        for field in KEEP_FIELDS:
            if field not in flat.columns:
                flat[field] = None
        row = flat[KEEP_FIELDS]
        results.append(row)
    time.sleep(1)  # Sleep to respect rate limits


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
