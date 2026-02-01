#!/usr/bin/env python3
"""
City Validator Module

Reusable module to validate and fix city assignments in place data.
Can be used:
1. As a standalone script to fix existing data
2. Imported into data pipelines to validate new places during import

Usage as module:
    from city_validator import CityValidator

    validator = CityValidator()

    # Validate a single place
    place = {"lat": 40.42, "lng": -3.70, "city": "Sevilla", "formatted_address": "...Madrid..."}
    fixed_place = validator.fix_place(place)

    # Validate a list of places
    fixed_places = validator.fix_places(places_list)

    # Check if a place has correct city
    is_valid, suggested_city = validator.validate_place(place)

Usage as CLI:
    python city_validator.py                    # Preview changes
    python city_validator.py --apply            # Apply changes to new file
    python city_validator.py --apply --replace  # Apply and replace original
"""

import json
import re
import argparse
from typing import Optional, Tuple, List, Dict, Any
from collections import defaultdict
from pathlib import Path


class CityValidator:
    """Validates and fixes city assignments based on coordinates and address."""

    # Major cities with coordinate bounds (lat_min, lat_max, lng_min, lng_max)
    MAJOR_CITY_BOUNDS = {
        'Madrid': (40.35, 40.50, -3.80, -3.60),
        'Barcelona': (41.36, 41.44, 2.10, 2.20),
        'Valencia': (39.44, 39.50, -0.42, -0.34),
        'Sevilla': (37.35, 37.42, -6.02, -5.95),
        'Bilbao': (43.24, 43.28, -2.96, -2.90),
        'Málaga': (36.70, 36.74, -4.46, -4.38),
        'Paris': (48.82, 48.90, 2.28, 2.42),
        'London': (51.47, 51.55, -0.18, 0.02),
        'Berlin': (52.48, 52.55, 13.35, 13.48),
        'Rome': (41.87, 41.93, 12.44, 12.52),
        'Amsterdam': (52.35, 52.40, 4.87, 4.93),
        'Lisbon': (38.71, 38.78, -9.18, -9.12),
        'Vienna': (48.18, 48.26, 16.33, 16.47),
        'Munich': (48.10, 48.18, 11.52, 11.62),
        'Milan': (45.44, 45.50, 9.14, 9.22),
    }

    # City name variations (lowercase) -> canonical name
    CITY_NAME_VARIATIONS = {
        'madrid': 'Madrid',
        'barcelona': 'Barcelona',
        'valencia': 'Valencia',
        'valència': 'Valencia',
        'sevilla': 'Sevilla',
        'seville': 'Sevilla',
        'bilbao': 'Bilbao',
        'málaga': 'Málaga',
        'malaga': 'Málaga',
        'paris': 'Paris',
        'parís': 'Paris',
        'london': 'London',
        'londres': 'London',
        'berlin': 'Berlin',
        'berlín': 'Berlin',
        'rome': 'Rome',
        'roma': 'Rome',
        'amsterdam': 'Amsterdam',
        'ámsterdam': 'Amsterdam',
        'lisbon': 'Lisbon',
        'lisboa': 'Lisbon',
        'vienna': 'Vienna',
        'viena': 'Vienna',
        'munich': 'Munich',
        'múnich': 'Munich',
        'münchen': 'Munich',
        'milan': 'Milan',
        'milán': 'Milan',
        'milano': 'Milan',
    }

    def __init__(self, custom_bounds: Optional[Dict] = None):
        """
        Initialize the validator.

        Args:
            custom_bounds: Optional dict of additional city bounds to add/override
        """
        self.city_bounds = self.MAJOR_CITY_BOUNDS.copy()
        if custom_bounds:
            self.city_bounds.update(custom_bounds)

    def get_city_from_coords(self, lat: Optional[float], lng: Optional[float]) -> Optional[str]:
        """
        Check if coordinates fall within a known major city.

        Args:
            lat: Latitude
            lng: Longitude

        Returns:
            City name if coordinates match, None otherwise
        """
        if lat is None or lng is None:
            return None

        for city, (lat_min, lat_max, lng_min, lng_max) in self.city_bounds.items():
            if lat_min <= lat <= lat_max and lng_min <= lng <= lng_max:
                return city
        return None

    def normalize_city_name(self, name: str) -> str:
        """Normalize city name for comparison."""
        if not name:
            return ''
        return name.lower().strip()

    def get_canonical_city(self, name: str) -> Optional[str]:
        """Get canonical city name from a variation."""
        normalized = self.normalize_city_name(name)
        return self.CITY_NAME_VARIATIONS.get(normalized)

    def cities_match(self, city1: str, city2: str) -> bool:
        """
        Check if two city names refer to the same city.

        Args:
            city1: First city name
            city2: Second city name

        Returns:
            True if cities match, False otherwise
        """
        c1 = self.normalize_city_name(city1)
        c2 = self.normalize_city_name(city2)

        if c1 == c2:
            return True

        # Check if both normalize to the same canonical city
        canonical1 = self.CITY_NAME_VARIATIONS.get(c1)
        canonical2 = self.CITY_NAME_VARIATIONS.get(c2)

        if canonical1 and canonical2 and canonical1 == canonical2:
            return True

        # Check partial matches
        if canonical1 and c2 == canonical1.lower():
            return True
        if canonical2 and c1 == canonical2.lower():
            return True

        return False

    def city_in_address(self, city: str, address: str) -> bool:
        """
        Check if a city name appears in the address.

        Args:
            city: City name to look for
            address: Formatted address string

        Returns:
            True if city found in address
        """
        if not address:
            return False

        addr_lower = address.lower()
        city_lower = city.lower()

        # Check the city name itself
        if city_lower in addr_lower:
            return True

        # Check all variations
        for variation, canonical in self.CITY_NAME_VARIATIONS.items():
            if canonical.lower() == city_lower and variation in addr_lower:
                return True

        return False

    def validate_place(self, place: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Validate a place's city assignment.

        Args:
            place: Place dict with 'lat', 'lng', 'city', 'formatted_address' fields

        Returns:
            Tuple of (is_valid, suggested_city, reason)
            - is_valid: True if city assignment is correct
            - suggested_city: Correct city if invalid, None if valid
            - reason: Description of the issue if invalid
        """
        lat = place.get('lat')
        lng = place.get('lng')
        current_city = place.get('city', '') or ''
        address = place.get('formatted_address', '') or ''

        if not current_city:
            return True, None, None  # No city to validate

        # Check if coordinates are in a major city
        coord_city = self.get_city_from_coords(lat, lng)

        if not coord_city:
            return True, None, None  # Not in a known major city bounds

        # Check if current city matches coordinate city
        if self.cities_match(current_city, coord_city):
            return True, None, None  # City is correct

        # Verify that the coordinate city appears in the address
        if not self.city_in_address(coord_city, address):
            return True, None, None  # Can't confirm mismatch

        # This is a mismatch
        reason = f"Coordinates are in {coord_city} but labeled as {current_city}"
        return False, coord_city, reason

    def fix_place(self, place: Dict[str, Any], in_place: bool = False) -> Dict[str, Any]:
        """
        Fix a place's city assignment if needed.

        Args:
            place: Place dict to fix
            in_place: If True, modify the original dict. If False, return a copy.

        Returns:
            Fixed place dict (same object if in_place=True, copy otherwise)
        """
        is_valid, suggested_city, _ = self.validate_place(place)

        if is_valid:
            return place

        if in_place:
            place['city'] = suggested_city
            return place
        else:
            fixed = place.copy()
            fixed['city'] = suggested_city
            return fixed

    def fix_places(self, places: List[Dict[str, Any]], in_place: bool = False) -> Tuple[List[Dict[str, Any]], List[Dict]]:
        """
        Fix city assignments for a list of places.

        Args:
            places: List of place dicts
            in_place: If True, modify original dicts

        Returns:
            Tuple of (fixed_places, changes_log)
            - fixed_places: List of fixed place dicts
            - changes_log: List of dicts describing each change made
        """
        changes = []
        result = places if in_place else []

        for i, place in enumerate(places):
            is_valid, suggested_city, reason = self.validate_place(place)

            if not is_valid:
                changes.append({
                    'index': i,
                    'name': place.get('name', 'Unknown'),
                    'old_city': place.get('city'),
                    'new_city': suggested_city,
                    'reason': reason,
                    'address': place.get('formatted_address', '')[:60]
                })

                if in_place:
                    place['city'] = suggested_city
                else:
                    fixed = place.copy()
                    fixed['city'] = suggested_city
                    result.append(fixed)
            elif not in_place:
                result.append(place)

        return result, changes

    def validate_and_report(self, places: List[Dict[str, Any]]) -> Dict:
        """
        Validate places and return a detailed report without modifying data.

        Args:
            places: List of place dicts

        Returns:
            Report dict with statistics and issues found
        """
        issues = []
        issues_by_pattern = defaultdict(list)

        for i, place in enumerate(places):
            is_valid, suggested_city, reason = self.validate_place(place)

            if not is_valid:
                issue = {
                    'index': i,
                    'name': place.get('name', 'Unknown'),
                    'old_city': place.get('city'),
                    'new_city': suggested_city,
                    'reason': reason,
                    'lat': place.get('lat'),
                    'lng': place.get('lng'),
                    'address': place.get('formatted_address', '')
                }
                issues.append(issue)

                pattern = f"{suggested_city} coords, labeled {place.get('city')}"
                issues_by_pattern[pattern].append(issue)

        return {
            'total_places': len(places),
            'issues_found': len(issues),
            'issues': issues,
            'issues_by_pattern': dict(issues_by_pattern)
        }


def main():
    """CLI interface for the city validator."""
    parser = argparse.ArgumentParser(
        description='Validate and fix city assignments in places data'
    )
    parser.add_argument(
        'input_file',
        nargs='?',
        default='centers_with_google_maps_and_website_information.json',
        help='Input JSON file (default: centers_with_google_maps_and_website_information.json)'
    )
    parser.add_argument(
        '-o', '--output',
        default=None,
        help='Output file (default: <input>_fixed.json)'
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Apply fixes and save to output file'
    )
    parser.add_argument(
        '--replace',
        action='store_true',
        help='Replace the original file (use with --apply)'
    )

    args = parser.parse_args()

    input_path = Path(args.input_file)
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_stem(input_path.stem + '_fixed')

    # Load data
    print(f"Loading {input_path}...")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Total places: {len(data)}")

    # Validate
    validator = CityValidator()
    report = validator.validate_and_report(data)

    # Print summary
    print(f"\n{'='*70}")
    print(f"VALIDATION REPORT: {report['issues_found']} issues found")
    print(f"{'='*70}\n")

    if report['issues_found'] == 0:
        print("No issues found. All city assignments are correct.")
        return

    print("Issues by pattern:\n")
    for pattern, issues in sorted(report['issues_by_pattern'].items(), key=lambda x: -len(x[1])):
        print(f"  {pattern}: {len(issues)} places")
        for issue in issues[:3]:
            print(f"    - {issue['name'][:40]}")
        if len(issues) > 3:
            print(f"    ... and {len(issues) - 3} more")
        print()

    # Apply fixes if requested
    if args.apply:
        print(f"\n{'='*70}")
        print("APPLYING FIXES...")
        print(f"{'='*70}\n")

        fixed_data, changes = validator.fix_places(data, in_place=True)

        # Determine output path
        if args.replace:
            final_output = input_path
        else:
            final_output = output_path

        print(f"Saving to {final_output}...")
        with open(final_output, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Done! {len(changes)} places fixed.")

        if not args.replace:
            print(f"\nTo replace the original file, run:")
            print(f"  mv {output_path} {input_path}")
            print(f"\nOr run with --replace flag:")
            print(f"  python {Path(__file__).name} --apply --replace")
    else:
        print(f"\n{'='*70}")
        print("DRY RUN - No changes applied")
        print(f"{'='*70}")
        print(f"\nTo apply fixes, run:")
        print(f"  python {Path(__file__).name} --apply")
        print(f"\nTo apply and replace original:")
        print(f"  python {Path(__file__).name} --apply --replace")


if __name__ == '__main__':
    main()
