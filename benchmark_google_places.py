#!/usr/bin/env python3
"""
Google Places API Benchmark Script
===================================
Standalone script to test 7 textQuery variants against Google Places API v1.
No project imports - uses only standard library + httpx + python-dotenv.

Output: Single TXT log file with all results per case.
"""

import os
import sys
import json
from datetime import datetime, timezone
from time import sleep
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

try:
    import httpx
except ImportError:
    print("ERROR: httpx is required. Install with: pip install httpx")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv is optional
    pass


# ============================================================================
# CONFIGURATION
# ============================================================================

# Hardcoded search plan for this benchmark
SEARCH_PLAN = {
    "request_id": "02f7782f-b53f-48e5-a7bf-635120fd1664",
    "language": "en",
    "prompt_target": "SEO AI tech companies",
    "prompt_keywords": [
        "seo",
        "ai",
        "tech",
        "technology",
        "software",
        "platform",
        "agency",
        "startup",
        "artificial",
        "intelligence"
    ],
    "prompt_location": ["France"],
    "resolved_locations": [
        {
            "city": None,
            "state": None,
            "country": "FR",
            "country_name": "France",
            "continent": "Europe",
            "region": "EMEA"
        }
    ],
    "matched_industries": [
        "Digital Accessibility Services",
        "Advertising Services",
        "Marketing Services",
        "Information Technology and Services",
        "Executive Search Services",
        "Climate Technology Product Manufacturing",
        "Technology, Information and Internet",
        "Internet Marketplace Platforms",
        "Technology, Information and Media",
        "E-Learning Providers",
        "Professional Services",
        "Online Media",
        "IT Services and IT Consulting",
        "Data Infrastructure and Analytics",
        "Engineering Services",
        "Social Networking Platforms",
        "Business Consulting and Services",
        "Staffing and Recruiting",
        "Dance Companies",
        "Design Services"
    ]
}

# API Configuration
MAX_PAGES = 5
PAGE_SIZE = 20
PAGE_TOKEN_DELAY = 2.0  # seconds to wait before using nextPageToken

# API Endpoints
GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"
PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# Field mask for Places API (only request needed fields)
PLACES_FIELD_MASK = "places.id,places.displayName,places.shortFormattedAddress,places.websiteUri,places.editorialSummary,nextPageToken"


# ============================================================================
# ENVIRONMENT VARIABLES
# ============================================================================

def check_environment() -> Tuple[str, str]:
    """
    Check and retrieve API keys from environment variables.
    Returns: (google_maps_api_key, geocoding_api_key)
    """
    google_key = os.getenv("GOOGLE_MAPS_API_KEY")
    geocoding_key = os.getenv("GEOCODING_API_KEY")
    
    print("=" * 80)
    print("ENVIRONMENT CHECK")
    print("=" * 80)
    print(f"GOOGLE_MAPS_API_KEY: {'✓ Present' if google_key else '✗ Missing'}")
    print(f"GEOCODING_API_KEY: {'✓ Present' if geocoding_key else '✗ Missing'}")
    print()
    
    if not google_key or not geocoding_key:
        print("ERROR: Missing required API keys in environment variables")
        sys.exit(1)
    
    return google_key, geocoding_key


# ============================================================================
# GEOCODING
# ============================================================================

def geocode_location(address: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Geocode a location using Google Geocoding API.
    Returns the viewport rectangle or None if geocoding fails.
    """
    print(f"Geocoding location: {address}")
    
    params = {
        "address": address,
        "key": api_key
    }
    
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(GEOCODING_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") != "OK" or not data.get("results"):
                print(f"  ✗ Geocoding failed: {data.get('status', 'UNKNOWN')}")
                return None
            
            # Extract viewport from first result
            geometry = data["results"][0].get("geometry", {})
            viewport = geometry.get("viewport")
            
            if not viewport:
                print("  ✗ No viewport in geocoding response")
                return None
            
            # Convert to Places API rectangle format
            # low = southwest, high = northeast
            rectangle = {
                "low": {
                    "latitude": viewport["southwest"]["lat"],
                    "longitude": viewport["southwest"]["lng"]
                },
                "high": {
                    "latitude": viewport["northeast"]["lat"],
                    "longitude": viewport["northeast"]["lng"]
                }
            }
            
            print(f"  ✓ Geocoded successfully")
            print(f"    SW: ({rectangle['low']['latitude']:.4f}, {rectangle['low']['longitude']:.4f})")
            print(f"    NE: ({rectangle['high']['latitude']:.4f}, {rectangle['high']['longitude']:.4f})")
            
            return rectangle
            
    except Exception as e:
        print(f"  ✗ Geocoding error: {e}")
        return None


# ============================================================================
# QUERY VARIANTS
# ============================================================================

def build_query_variants(search_plan: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Build 7 textQuery variants based on the search plan.
    Returns list of (case_name, text_query) tuples.
    """
    keywords = search_plan["prompt_keywords"]
    target = search_plan["prompt_target"]
    
    # Join keywords for different formats
    keywords_comma = ", ".join(keywords)
    keywords_space = " ".join(keywords)
    keywords_paren = f"({keywords_comma})"
    
    variants = [
        ("keywords_comma", keywords_comma),
        ("keywords_space", keywords_space),
        ("companies_for_keywords", f"companies for {keywords_paren}"),
        ("b2b_companies_for_keywords", f"b2b companies for {keywords_paren}"),
        ("target", target),
        ("target_companies", f"{target} companies"),
        ("target_b2b_companies", f"{target} b2b companies"),
    ]
    
    return variants


# ============================================================================
# PLACES API
# ============================================================================

def search_places(
    text_query: str,
    language_code: str,
    location_restriction: Optional[Dict[str, Any]],
    api_key: str,
    page_token: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Search places using Google Places API v1 Text Search.
    
    Returns: (places_list, next_page_token, error_message)
    """
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": PLACES_FIELD_MASK
    }
    
    body = {
        "textQuery": text_query,
        "languageCode": language_code,
        "includePureServiceAreaBusinesses": True,
        "pageSize": PAGE_SIZE
    }
    
    # Add location restriction only if provided
    if location_restriction:
        body["locationRestriction"] = {
            "rectangle": location_restriction
        }
    
    # Add pageToken only when paging
    if page_token:
        body["pageToken"] = page_token
    
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(PLACES_SEARCH_URL, headers=headers, json=body)
            
            if response.status_code != 200:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                return [], None, error_msg
            
            data = response.json()
            places = data.get("places", [])
            next_token = data.get("nextPageToken")
            
            return places, next_token, None
            
    except Exception as e:
        return [], None, f"Exception: {str(e)}"


def fetch_all_pages(
    text_query: str,
    language_code: str,
    location_restriction: Optional[Dict[str, Any]],
    api_key: str,
    max_pages: int = MAX_PAGES
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Fetch all pages for a query (up to max_pages).
    Returns: (all_places, errors)
    """
    all_places = []
    errors = []
    page_token = None
    
    for page_num in range(1, max_pages + 1):
        print(f"    Fetching page {page_num}...", end=" ")
        
        # Wait before using pageToken (Google requirement)
        if page_token:
            sleep(PAGE_TOKEN_DELAY)
        
        places, next_token, error = search_places(
            text_query=text_query,
            language_code=language_code,
            location_restriction=location_restriction,
            api_key=api_key,
            page_token=page_token
        )
        
        if error:
            print(f"✗ Error: {error}")
            errors.append(f"Page {page_num}: {error}")
            break
        
        print(f"✓ Got {len(places)} places")
        all_places.extend(places)
        
        # Check if there are more pages
        if not next_token:
            print(f"    No more pages (stopped at page {page_num})")
            break
        
        page_token = next_token
    
    return all_places, errors


# ============================================================================
# DEDUPLICATION
# ============================================================================

def dedupe_places(places: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate places by place.id, keeping first occurrence.
    """
    seen_ids = set()
    unique_places = []
    
    for place in places:
        place_id = place.get("id")
        if place_id and place_id not in seen_ids:
            seen_ids.add(place_id)
            unique_places.append(place)
    
    return unique_places


# ============================================================================
# RESULT PROCESSING
# ============================================================================

def extract_place_info(place: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract relevant information from a place object.
    """
    return {
        "id": place.get("id", ""),
        "name": place.get("displayName", {}).get("text", "-"),
        "website": place.get("websiteUri", "-"),
        "address": place.get("shortFormattedAddress", "-"),
        "description": place.get("editorialSummary", {}).get("text", "-") if place.get("editorialSummary") else "-"
    }


def count_with_website(places: List[Dict[str, Any]]) -> int:
    """
    Count how many places have a website.
    """
    return sum(1 for p in places if p.get("websiteUri"))


# ============================================================================
# LOGGING
# ============================================================================

def create_log_file() -> Path:
    """
    Create logs directory and return path for new log file.
    """
    logs_dir = Path("./logs")
    logs_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    log_file = logs_dir / f"benchmark_{timestamp}.txt"
    
    return log_file


def write_log_header(f, search_plan: Dict[str, Any], location_str: str):
    """
    Write log file header with configuration details.
    """
    f.write("=" * 80 + "\n")
    f.write("GOOGLE PLACES API BENCHMARK RESULTS\n")
    f.write("=" * 80 + "\n")
    f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n")
    f.write(f"Request ID: {search_plan['request_id']}\n")
    f.write(f"Language: {search_plan['language']}\n")
    f.write(f"Location: {location_str}\n")
    f.write(f"Page Size: {PAGE_SIZE}\n")
    f.write(f"Max Pages: {MAX_PAGES}\n")
    f.write("\n")
    f.write(f"Target: {search_plan['prompt_target']}\n")
    f.write(f"Keywords: {', '.join(search_plan['prompt_keywords'])}\n")
    f.write("=" * 80 + "\n\n")


def write_case_results(
    f,
    case_name: str,
    text_query: str,
    unique_places: List[Dict[str, Any]],
    total_returned: int,
    with_website: int,
    errors: List[str]
):
    """
    Write results for a single test case.
    """
    f.write("=" * 80 + "\n")
    f.write(f"CASE: {case_name}\n")
    f.write("=" * 80 + "\n")
    f.write(f"Text Query: {text_query}\n")
    f.write(f"Total Returned: {total_returned}\n")
    f.write(f"Unique Places: {len(unique_places)}\n")
    f.write(f"With Website: {with_website}\n")
    
    if errors:
        f.write("\nErrors:\n")
        for error in errors:
            f.write(f"  - {error}\n")
    
    f.write("\nResults:\n")
    f.write("-" * 80 + "\n")
    
    if not unique_places:
        f.write("(No results)\n")
    else:
        for idx, place in enumerate(unique_places, 1):
            info = extract_place_info(place)
            f.write(f"{idx}) {info['name']} | {info['website']} | {info['address']} | {info['description']}\n")
    
    f.write("\n\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function.
    """
    print("=" * 80)
    print("GOOGLE PLACES API BENCHMARK")
    print("=" * 80)
    print()
    
    # Check environment
    google_key, geocoding_key = check_environment()
    
    # Geocode location from resolved_locations (always present)
    print("=" * 80)
    print("GEOCODING")
    print("=" * 80)
    
    # Use first resolved_location's country_name for geocoding
    location_str = SEARCH_PLAN["resolved_locations"][0]["country_name"]
    location_restriction = geocode_location(location_str, geocoding_key)
    
    if not location_restriction:
        print("\nERROR: Failed to geocode location. Cannot proceed.")
        
        # Write error to log file
        log_file = create_log_file()
        with open(log_file, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("GOOGLE PLACES API BENCHMARK - ERROR\n")
            f.write("=" * 80 + "\n")
            f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"Request ID: {SEARCH_PLAN['request_id']}\n")
            f.write("\n")
            f.write(f"ERROR: Failed to geocode location '{location_str}'\n")
            f.write("Geocoding returned 0 results or encountered an error.\n")
            f.write("Cannot proceed with Places API queries.\n")
        
        print(f"\nError logged to: {log_file}")
        sys.exit(1)
    
    print()
    
    # Build query variants
    print("=" * 80)
    print("QUERY VARIANTS")
    print("=" * 80)
    variants = build_query_variants(SEARCH_PLAN)
    for idx, (case_name, text_query) in enumerate(variants, 1):
        print(f"{idx}. {case_name}: {text_query}")
    print()
    
    # Create log file
    log_file = create_log_file()
    print(f"Log file: {log_file}")
    print()
    
    # Open log file for writing
    with open(log_file, "w", encoding="utf-8") as f:
        # Write header
        write_log_header(f, SEARCH_PLAN, location_str)
        
        # Execute each test case
        print("=" * 80)
        print("EXECUTING TEST CASES")
        print("=" * 80)
        
        for case_num, (case_name, text_query) in enumerate(variants, 1):
            print(f"\n[{case_num}/{len(variants)}] {case_name}")
            print(f"  Query: {text_query}")
            
            # Fetch all pages
            all_places, errors = fetch_all_pages(
                text_query=text_query,
                language_code=SEARCH_PLAN["language"],
                location_restriction=location_restriction,
                api_key=google_key,
                max_pages=MAX_PAGES
            )
            
            # Deduplicate
            unique_places = dedupe_places(all_places)
            with_website = count_with_website(unique_places)
            
            print(f"  Summary: {len(all_places)} total → {len(unique_places)} unique → {with_website} with website")
            
            # Write to log
            write_case_results(
                f=f,
                case_name=case_name,
                text_query=text_query,
                unique_places=unique_places,
                total_returned=len(all_places),
                with_website=with_website,
                errors=errors
            )
    
    print("\n" + "=" * 80)
    print("BENCHMARK COMPLETE")
    print("=" * 80)
    print(f"Results written to: {log_file}")
    print()


if __name__ == "__main__":
    main()