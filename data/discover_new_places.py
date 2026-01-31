"""
Google Places Text Search Discovery Script

This script searches for Argentine-related establishments in cities where
you have low or no coverage. It uses the Google Places Text Search API.

COST ESTIMATE:
- Text Search: $32 per 1000 requests
- Each city × each query = 1 request
- Example: 50 cities × 10 queries = 500 requests = ~$16

USAGE:
1. Set your API key (environment variable or edit below)
2. Run: python discover_new_places.py --cities 10 --dry-run  (test first)
3. Run: python discover_new_places.py --cities 50  (actual run)

OUTPUT:
- discovered_places.json: New places not in your existing dataset
- discovery_log.json: Full log of what was searched
"""

import requests
import json
import time
import os
import argparse
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "AIzaSyDNkzJmsTIW2RVwjfZWnYRVBqJYmKHWicY")

# Input files
EXISTING_DATA_FILE = os.path.join(SCRIPT_DIR, "centers_with_google_maps_and_website_information.json")
EXPANSION_CITIES_FILE = os.path.join(SCRIPT_DIR, "expansion_cities.json")

# Output files
DISCOVERED_PLACES_FILE = os.path.join(SCRIPT_DIR, "discovered_places.json")
DISCOVERY_LOG_FILE = os.path.join(SCRIPT_DIR, "discovery_log.json")

# Search queries - these have proven effective for finding Argentine businesses
SEARCH_QUERIES = [
    "tienda argentina {city}",
    "productos argentinos {city}",
    "empanadas argentinas {city}",
    "restaurante argentino {city}",
    "yerba mate {city}",
    "alfajores {city}",
    "parrilla argentina {city}",
    "argentine restaurant {city}",
    "argentinian food {city}",
    "south american grocery {city}",
]

# Rate limiting
REQUESTS_PER_SECOND = 5  # Google allows ~10 QPS, we'll be conservative
DELAY_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND

SESSION = requests.Session()

# =============================================================================
# GOOGLE PLACES TEXT SEARCH
# =============================================================================

def text_search(query, location_bias=None):
    """
    Perform a Google Places Text Search.

    Returns list of places with: place_id, name, formatted_address, geometry, etc.
    """
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": query,
        "key": API_KEY,
    }

    # Add location bias if provided (helps prioritize results in a region)
    if location_bias:
        params["location"] = f"{location_bias['lat']},{location_bias['lng']}"
        params["radius"] = location_bias.get("radius", 50000)  # 50km default

    try:
        r = SESSION.get(url, params=params, timeout=(5, 30))
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"    Network error: {e}")
        return []

    data = r.json()
    status = data.get("status")

    if status == "OK":
        return data.get("results", [])
    elif status == "ZERO_RESULTS":
        return []
    else:
        print(f"    API status: {status} - {data.get('error_message', '')}")
        return []


def get_place_details_minimal(place_id):
    """
    Get minimal details for a place (just what we need for initial discovery).
    This is a separate call but helps verify the place and get more info.
    """
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "key": API_KEY,
        "fields": "place_id,name,formatted_address,geometry,types,business_status,website",
    }

    try:
        r = SESSION.get(url, params=params, timeout=(5, 30))
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "OK":
            return data.get("result")
    except:
        pass
    return None


# =============================================================================
# MAIN DISCOVERY LOGIC
# =============================================================================

def load_existing_place_ids():
    """Load existing place_ids to avoid duplicates."""
    try:
        with open(EXISTING_DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return set(d.get('place_id') for d in data if d.get('place_id'))
    except FileNotFoundError:
        print(f"Warning: {EXISTING_DATA_FILE} not found. Will discover all places as new.")
        return set()


def load_expansion_cities(max_cities=None, priority_levels=None):
    """
    Load cities from expansion list.

    Args:
        max_cities: Maximum number of cities to return
        priority_levels: List of priority levels to include (e.g., [1, 2, 3])
    """
    with open(EXPANSION_CITIES_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    cities = []
    priority_map = {
        1: "priority_1_major_capitals",
        2: "priority_2_spain_expansion",
        3: "priority_3_major_european",
        4: "priority_4_nordic_eastern",
        5: "priority_5_swiss_austrian",
        6: "priority_6_americas_for_reference",
    }

    levels_to_use = priority_levels or [1, 2, 3, 4, 5]

    for level in levels_to_use:
        key = priority_map.get(level)
        if key and key in data:
            for city_data in data[key].get("cities", []):
                cities.append({
                    "city": city_data["city"],
                    "country": city_data["country"],
                    "country_code": city_data.get("country_code", ""),
                    "priority": level,
                })

    if max_cities:
        cities = cities[:max_cities]

    return cities


def discover_places(cities, queries, existing_ids, dry_run=False):
    """
    Main discovery function.

    Args:
        cities: List of city dicts with 'city', 'country', 'country_code'
        queries: List of query templates with {city} placeholder
        existing_ids: Set of existing place_ids to skip
        dry_run: If True, don't make API calls, just show what would happen

    Returns:
        dict with 'discovered' (new places) and 'log' (search log)
    """
    discovered = {}  # place_id -> place data
    log = {
        "started_at": datetime.now().isoformat(),
        "cities_searched": [],
        "queries_used": queries,
        "total_api_calls": 0,
        "total_results": 0,
        "new_places_found": 0,
        "duplicates_skipped": 0,
    }

    total_searches = len(cities) * len(queries)
    print(f"\n{'='*60}")
    print(f"DISCOVERY RUN {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"Cities to search: {len(cities)}")
    print(f"Queries per city: {len(queries)}")
    print(f"Total API calls: {total_searches}")
    print(f"Estimated cost: ${total_searches * 0.032:.2f}")
    print(f"Existing places in DB: {len(existing_ids)}")
    print(f"{'='*60}\n")

    if dry_run:
        print("DRY RUN - No API calls will be made\n")
        for city in cities[:5]:  # Show first 5 as example
            print(f"  Would search: {city['city']}, {city['country']}")
            for q in queries[:3]:  # Show first 3 queries
                print(f"    - {q.format(city=city['city'])}")
        print(f"  ... and {len(cities) - 5} more cities")
        return {"discovered": {}, "log": log}

    search_count = 0

    for city_data in cities:
        city = city_data["city"]
        country = city_data["country"]
        city_log = {
            "city": city,
            "country": country,
            "searches": [],
            "places_found": 0,
            "new_places": 0,
        }

        print(f"\n[{cities.index(city_data) + 1}/{len(cities)}] Searching: {city}, {country}")

        for query_template in queries:
            query = query_template.format(city=city)
            search_count += 1

            print(f"  [{search_count}/{total_searches}] Query: {query[:50]}...")

            results = text_search(query)
            log["total_api_calls"] += 1
            log["total_results"] += len(results)

            search_log = {
                "query": query,
                "results_count": len(results),
                "new_count": 0,
            }

            for place in results:
                pid = place.get("place_id")
                if not pid:
                    continue

                if pid in existing_ids:
                    log["duplicates_skipped"] += 1
                    continue

                if pid in discovered:
                    continue  # Already found in this run

                # New place found!
                discovered[pid] = {
                    "place_id": pid,
                    "name": place.get("name"),
                    "formatted_address": place.get("formatted_address"),
                    "lat": place.get("geometry", {}).get("location", {}).get("lat"),
                    "lng": place.get("geometry", {}).get("location", {}).get("lng"),
                    "types": place.get("types", []),
                    "business_status": place.get("business_status"),
                    "discovered_via_query": query,
                    "discovered_in_city": city,
                    "discovered_in_country": country,
                    "discovered_at": datetime.now().isoformat(),
                }

                search_log["new_count"] += 1
                city_log["new_places"] += 1
                log["new_places_found"] += 1

                print(f"    NEW: {place.get('name')}")

            city_log["places_found"] += len(results)
            city_log["searches"].append(search_log)

            # Rate limiting
            time.sleep(DELAY_BETWEEN_REQUESTS)

        log["cities_searched"].append(city_log)

        # Save progress after each city
        save_results(discovered, log)

    log["completed_at"] = datetime.now().isoformat()
    return {"discovered": discovered, "log": log}


def save_results(discovered, log):
    """Save discovered places and log to files."""
    # Save discovered places
    places_list = list(discovered.values())
    with open(DISCOVERED_PLACES_FILE, 'w', encoding='utf-8') as f:
        json.dump(places_list, f, ensure_ascii=False, indent=2)

    # Save log
    with open(DISCOVERY_LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(log, f, ensure_ascii=False, indent=2)


def print_summary(log):
    """Print discovery summary."""
    print(f"\n{'='*60}")
    print("DISCOVERY SUMMARY")
    print(f"{'='*60}")
    print(f"Cities searched: {len(log['cities_searched'])}")
    print(f"Total API calls: {log['total_api_calls']}")
    print(f"Total results from Google: {log['total_results']}")
    print(f"Already in database (skipped): {log['duplicates_skipped']}")
    print(f"NEW PLACES DISCOVERED: {log['new_places_found']}")
    print(f"\nResults saved to:")
    print(f"  - {DISCOVERED_PLACES_FILE}")
    print(f"  - {DISCOVERY_LOG_FILE}")
    print(f"{'='*60}")

    if log['new_places_found'] > 0:
        print("\nNEXT STEPS:")
        print("1. Review discovered_places.json")
        print("2. Run enrich_with_ratings_and_photos.py on new places")
        print("3. Merge into main dataset")
        print("4. Regenerate site with: quarto render")


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Discover Argentine establishments in new cities using Google Places API"
    )
    parser.add_argument(
        "--cities", type=int, default=10,
        help="Maximum number of cities to search (default: 10)"
    )
    parser.add_argument(
        "--priority", type=int, nargs="+", default=[1, 2, 3],
        help="Priority levels to include (default: 1 2 3)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be searched without making API calls"
    )
    parser.add_argument(
        "--queries", type=int, default=None,
        help="Limit number of queries per city (default: all)"
    )

    args = parser.parse_args()

    # Check API key
    if API_KEY == "YOUR_API_KEY_HERE" and not args.dry_run:
        print("ERROR: Please set your Google Maps API key!")
        print("Either:")
        print("  1. Set GOOGLE_MAPS_API_KEY environment variable")
        print("  2. Edit API_KEY in this script")
        return

    # Load data
    print("Loading existing places...")
    existing_ids = load_existing_place_ids()
    print(f"Found {len(existing_ids)} existing place_ids")

    print("\nLoading expansion cities...")
    cities = load_expansion_cities(max_cities=args.cities, priority_levels=args.priority)
    print(f"Loaded {len(cities)} cities to search")

    # Optionally limit queries
    queries = SEARCH_QUERIES
    if args.queries:
        queries = queries[:args.queries]

    # Run discovery
    results = discover_places(
        cities=cities,
        queries=queries,
        existing_ids=existing_ids,
        dry_run=args.dry_run
    )

    if not args.dry_run:
        save_results(results["discovered"], results["log"])
        print_summary(results["log"])


if __name__ == "__main__":
    main()
