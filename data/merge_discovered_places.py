"""
Merge Discovered Places into Main Dataset

This script merges newly discovered places into your main centers JSON file.
It handles deduplication and can optionally fetch full details for new places.

USAGE:
1. First run: discover_new_places.py
2. (Optional) Manually review discovered_places.json and remove false positives
3. Run: python merge_discovered_places.py
4. Run: python generate_professors_and_centers_files/generate_qmd_files.py
5. Run: quarto render
"""

import json
import os
import requests
import time
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "YOUR_API_KEY_HERE")

MAIN_DATA_FILE = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.json")
DISCOVERED_FILE = os.path.join(SCRIPT_DIR, "discovered_places.json")
BACKUP_FILE = os.path.join(SCRIPT_DIR, f"centers_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

SESSION = requests.Session()

# Fields to fetch for new places (same as your original download script)
DETAIL_FIELDS = [
    "place_id", "name", "formatted_address", "adr_address", "address_components",
    "geometry/location", "formatted_phone_number", "international_phone_number",
    "opening_hours/weekday_text", "url", "website", "icon_mask_base_uri",
    "icon_background_color", "plus_code", "types", "business_status",
    "wheelchair_accessible_entrance", "rating", "user_ratings_total", "photos"
]


def get_full_place_details(place_id):
    """Fetch complete details for a place."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "key": API_KEY,
        "language": "es",
        "fields": ",".join(DETAIL_FIELDS),
    }

    try:
        r = SESSION.get(url, params=params, timeout=(5, 30))
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "OK":
            return data.get("result")
    except Exception as e:
        print(f"  Error fetching details: {e}")
    return None


def normalize_place(raw, discovered_info=None):
    """
    Normalize a place from Google API response to match your data structure.
    """
    # Extract nested fields
    geometry = raw.get("geometry", {})
    location = geometry.get("location", {})
    plus_code = raw.get("plus_code", {})
    opening_hours = raw.get("opening_hours", {})

    # Parse address components to get city and country
    address_components = raw.get("address_components", [])
    city = ""
    country = ""
    country_code = ""

    for comp in address_components:
        types = comp.get("types", [])
        if "locality" in types:
            city = comp.get("long_name", "")
        elif "administrative_area_level_2" in types and not city:
            city = comp.get("long_name", "")
        elif "country" in types:
            country = comp.get("long_name", "")
            country_code = comp.get("short_name", "")

    # Use discovered info as fallback
    if discovered_info:
        if not city:
            city = discovered_info.get("discovered_in_city", "")
        if not country:
            country = discovered_info.get("discovered_in_country", "")

    # Build photo URL if available
    photos = raw.get("photos", [])
    photo_url = None
    if photos:
        photo_ref = photos[0].get("photo_reference")
        if photo_ref:
            photo_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photo_reference={photo_ref}&key={API_KEY}"

    return {
        "place_id": raw.get("place_id"),
        "name": raw.get("name", ""),
        "formatted_address": raw.get("formatted_address", ""),
        "adr_address": raw.get("adr_address", ""),
        "address_components": json.dumps(address_components) if address_components else "",
        "lat": location.get("lat"),
        "lng": location.get("lng"),
        "city": city,
        "country": country,
        "country_code": country_code,
        "formatted_phone_number": raw.get("formatted_phone_number", ""),
        "international_phone_number": raw.get("international_phone_number", ""),
        "opening_hours_weekday_text": json.dumps(opening_hours.get("weekday_text", [])),
        "url": raw.get("url", ""),
        "website": raw.get("website", ""),
        "icon_mask_base_uri": raw.get("icon_mask_base_uri", ""),
        "icon_background_color": raw.get("icon_background_color", ""),
        "plus_code_global_code": plus_code.get("global_code", ""),
        "plus_code_compound_code": plus_code.get("compound_code", ""),
        "types": json.dumps(raw.get("types", [])),
        "type_list": ", ".join(raw.get("types", [])),
        "business_status": raw.get("business_status", ""),
        "wheelchair_accessible_entrance": raw.get("wheelchair_accessible_entrance"),
        "rating": raw.get("rating"),
        "user_ratings_total": raw.get("user_ratings_total"),
        "photo_url": photo_url,
        "appears_in_google_maps": 1,
        # Default values for fields that need manual/automatic enrichment
        "category": "Otros",  # Default category, can be updated later
        "origins": '["Argentina"]',  # Default to Argentina
        "email": None,
        "whatsapp": "",
        "instagram": "",
        "facebook": None,
        "twitter": None,
        "youtube": None,
        "text_content": "",
        "products": None,
        "dishes": None,
        "brands": None,
        "subpages_crawled": "",
        # Scoring fields
        "country_scores.argentina": 1.0,
        "country_scores.uruguay": 0.0,
        "country_scores.chile": 0.0,
        "country_scores.brazil": 0.0,
        "country_scores.colombia": 0.0,
        "country_scores.mexico": 0.0,
        "country_scores.peru": 0.0,
        "country_scores.venezuela": 0.0,
        "country_scores.ecuador": 0.0,
        "country_scores.bolivia": 0.0,
        "country_scores.paraguay": 0.0,
        "country_scores.cuba": 0.0,
        "country_scores.dominican_republic": 0.0,
        "product_count": 0,
        "country_match_count": 1,
        "top_country": "argentina",
        "top_country_score": 1.0,
        "all_positive_countries": None,
        "strong_country_matches": None,
        "origin_countries": None,
    }


def main():
    print("="*60)
    print("MERGE DISCOVERED PLACES")
    print("="*60)

    # Check for discovered places file
    if not os.path.exists(DISCOVERED_FILE):
        print(f"ERROR: {DISCOVERED_FILE} not found!")
        print("Run discover_new_places.py first.")
        return

    # Load discovered places
    print(f"\nLoading discovered places from {DISCOVERED_FILE}...")
    with open(DISCOVERED_FILE, 'r', encoding='utf-8') as f:
        discovered = json.load(f)
    print(f"Found {len(discovered)} discovered places")

    if len(discovered) == 0:
        print("No new places to merge.")
        return

    # Load main dataset
    print(f"\nLoading main dataset from {MAIN_DATA_FILE}...")
    with open(MAIN_DATA_FILE, 'r', encoding='utf-8') as f:
        main_data = json.load(f)
    print(f"Main dataset has {len(main_data)} places")

    # Get existing place_ids
    existing_ids = set(p.get("place_id") for p in main_data)

    # Create backup
    print(f"\nCreating backup at {BACKUP_FILE}...")
    with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
        json.dump(main_data, f, ensure_ascii=False, indent=2)

    # Check API key for fetching details
    fetch_details = API_KEY != "YOUR_API_KEY_HERE"
    if not fetch_details:
        print("\nWARNING: No API key set. Will merge with basic info only.")
        print("Set GOOGLE_MAPS_API_KEY to fetch full details.\n")

    # Merge new places
    added = 0
    skipped = 0

    for i, place in enumerate(discovered):
        pid = place.get("place_id")
        name = place.get("name", "Unknown")

        if pid in existing_ids:
            print(f"[{i+1}/{len(discovered)}] SKIP (duplicate): {name}")
            skipped += 1
            continue

        print(f"[{i+1}/{len(discovered)}] ADD: {name}")

        if fetch_details:
            # Fetch full details from Google
            details = get_full_place_details(pid)
            if details:
                normalized = normalize_place(details, place)
            else:
                # Use basic info from discovery
                normalized = normalize_place(place, place)
            time.sleep(0.15)  # Rate limiting
        else:
            # Use basic info from discovery
            normalized = normalize_place(place, place)

        main_data.append(normalized)
        existing_ids.add(pid)
        added += 1

    # Save merged data
    print(f"\nSaving merged dataset to {MAIN_DATA_FILE}...")
    with open(MAIN_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(main_data, f, ensure_ascii=False, indent=2)

    # Summary
    print(f"\n{'='*60}")
    print("MERGE COMPLETE")
    print(f"{'='*60}")
    print(f"Places added: {added}")
    print(f"Duplicates skipped: {skipped}")
    print(f"New total: {len(main_data)}")

    if added > 0:
        print(f"\nNEXT STEPS:")
        print("1. Regenerate center pages:")
        print("   python generate_professors_and_centers_files/generate_qmd_files.py")
        print("2. Rebuild the site:")
        print("   quarto render")
        print("3. (Optional) Run process_site_texts.py to enrich with categories")


if __name__ == "__main__":
    main()
