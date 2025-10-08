# Load the json file
import json

with open('data/centers_with_google_maps_and_website_information.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# List the frecuency of cities
cities = {}
for center in data:
    city = center.get('city')
    cities[city] = cities.get(city, 0) + 1

# sort cities by count
cities = dict(sorted(cities.items(), key=lambda item: item[1], reverse=True))   
cities

# Print the cities and their counts
for city, count in sorted(cities.items(), key=lambda item: item[1], reverse=True):
    print(f"{city}: {count}")
# Total number of centers

