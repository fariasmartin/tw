import requests
import pandas as pd
import time

# === CONFIGURATION ===
API_KEY = "AIzaSyDwtnQ-tOhH_1_B0474wCQBJN1HJaP8WzA"  # API key to get place IDs
INPUT_FILE = "store_names.xlsx"
OUTPUT_FILE = "place_ids_from_text_search.xlsx"
FAILED_OUTPUT_FILE = "failed_place_ids.xlsx"
SLEEP_BETWEEN = 0.3  # seconds between requests
FIELD_MASK = "places.displayName,places.formattedAddress,places.id,places.location"

def get_place_ids_from_text(query):
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": FIELD_MASK + ",places.types"  # add 'types' so we can filter
    }

    body = {
        "textQuery": query,
    }

    all_results = []
    while True:
        response = requests.post(url, headers=headers, json=body)
        if response.status_code != 200:
            print(f"❌ Error {response.status_code} for: {query}")
            break

        data = response.json()
        for place in data.get("places", []):
            types = place.get("types", [])
            if "establishment" not in types:
                continue  # Skip non-establishments

            all_results.append({
                "input_query": query,
                "name": place.get("displayName", {}).get("text"),
                "formatted_address": place.get("formattedAddress"),
                "place_id": place.get("id"),
                "lat": place.get("location", {}).get("latitude"),
                "lng": place.get("location", {}).get("longitude")
            })

        next_token = data.get("nextPageToken")
        if not next_token:
            break
        body = {"textQuery": query, "pageToken": next_token}
        time.sleep(SLEEP_BETWEEN)

    return all_results


def main():
    df = pd.read_excel(INPUT_FILE)
    if "name" not in df.columns:
        raise ValueError("Excel must have a 'name' column")

    all_places = []
    failed_queries = []

    for query in df["name"]:
        print(f"\n🔍 Searching: {query}")
        results = get_place_ids_from_text(query)
        print(f"➡️ Found {len(results)} match(es)")

        if not results:
            failed_queries.append({"input_query": query})
            all_places.append({
                "input_query": query,
                "name": None,
                "formatted_address": None,
                "place_id": None,
                "lat": None,
                "lng": None
            })
        else:
            all_places.extend(results)
    
    time.sleep(SLEEP_BETWEEN)

    # Save results
    pd.DataFrame(all_places).to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Saved results to: {OUTPUT_FILE}")

    # Save failed queries
    if failed_queries:
        pd.DataFrame(failed_queries).to_excel(FAILED_OUTPUT_FILE, index=False)
        print(f"⚠️ Saved {len(failed_queries)} failed queries to: {FAILED_OUTPUT_FILE}")
    else:
        print("✅ No failed queries!")

if __name__ == "__main__":
    main()


# import pandas as pd
# import requests
# import time

# API_KEY = "AIzaSyDNkzJmsTIW2RVwjfZWnYRVBqJYmKHWicY"
# INPUT_FILE = "store_names.xlsx"
# OUTPUT_FILE = "place_id_downloaded_automatically.xlsx"
# SLEEP_BETWEEN = 0.15

# def autocomplete_all_predictions(query):
#     url = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
#     params = {
#         "input": query,
#         "key": API_KEY,
#         "types": "establishment",      # Optional: only business-type places
#         "components": "country:es"     # Optional: restrict to Spain
#     }
#     response = requests.get(url, params=params)
#     if response.status_code != 200:
#         print(f"❌ HTTP error {response.status_code} for: {query}")
#         return []
    
#     data = response.json()
#     predictions = data.get("predictions", [])
    
#     return [
#         {
#             "input_query": query,
#             "description": p["description"],
#             "place_id": p["place_id"]
#         }
#         for p in predictions
#     ]

# def main():
#     df = pd.read_excel(INPUT_FILE)
#     if "name" not in df.columns:
#         raise ValueError("Excel must have a 'name' column")

#     all_results = []

#     for name in df["name"]:
#         print(f"🔍 Searching for: {name}")
#         predictions = autocomplete_all_predictions(name)
#         if not predictions:
#             all_results.append({
#                 "input_query": name,
#                 "description": None,
#                 "place_id": None
#             })
#         else:
#             all_results.extend(predictions)
#         time.sleep(SLEEP_BETWEEN)

#     pd.DataFrame(all_results).to_excel(OUTPUT_FILE, index=False)
#     print(f"✅ Done. Saved to {OUTPUT_FILE}")

# if __name__ == "__main__":
#     main()
