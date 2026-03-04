from __future__ import annotations

# ============================================================================
# geo.py
#
# Role:
# - Deterministic location resolution using Google Geocoding API
# - Convert prompt_location tokens into structured ResolvedLocation objects
# - Enrich with geo metadata (lat/lng/viewport/rectangle)
# - Handle global_search and DACH defaults
#
# Separation of Concerns:
# - Google Geocoding API client (internal)
# - Location resolution logic (internal)
# - Public API: resolve_locations()
# ============================================================================

import logging
from typing import Any, Optional
import httpx

from .models import ResolvedLocation, GeoData, GeoCoordinates, GeoViewport, GeoRectangle
from .constants import DACH_DEFAULT_RESOLVED_LOCATIONS
from ...core.config import settings

logger = logging.getLogger(__name__)


# ============================================================================
# Exceptions
# ============================================================================

class GeoResolutionError(Exception):
    """Raised when location cannot be resolved via Google Geocoding API."""
    pass


# ============================================================================
# Google Geocoding Client (Internal)
# ============================================================================

class _GoogleGeocodingClient:
    """
    Internal Google Geocoding API client for location resolution.
    Returns viewport and rectangle coordinates for Places API integration.
    """
    
    def __init__(self) -> None:
        api_key = getattr(settings, "geocoding_api_key", None) or getattr(settings, "google_maps_api_key", None)
        if not api_key:
            raise ValueError("Missing Geocoding API key (settings.geocoding_api_key or settings.google_maps_api_key)")
        
        self._api_key = api_key
        self._http = httpx.Client(timeout=30.0)
        self._url = "https://maps.googleapis.com/maps/api/geocode/json"
    
    def close(self) -> None:
        self._http.close()
    
    def __enter__(self) -> "_GoogleGeocodingClient":
        return self
    
    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
    
    def geocode(self, *, address: str) -> dict[str, Any]:
        """
        Geocode an address using Google Geocoding API.
        
        Args:
            address: Address string to geocode (e.g., "Berlin, Germany")
            
        Returns:
            Geocoding API response JSON
            
        Raises:
            httpx.HTTPError: If API request fails
        """
        try:
            resp = self._http.get(
                self._url,
                params={"address": address, "key": self._api_key}
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            logger.error(f"Geocoding API error for '{address}': {e}")
            raise


# ============================================================================
# Geocoding Helper Functions (Internal)
# ============================================================================

def _extract_viewport(geo_json: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Extract viewport from geocoding response."""
    results = geo_json.get("results") or []
    if not isinstance(results, list) or not results:
        return None
    
    r0 = results[0]
    if not isinstance(r0, dict):
        return None
    
    geom = r0.get("geometry") or {}
    if not isinstance(geom, dict):
        return None
    
    viewport = geom.get("viewport")
    return viewport if isinstance(viewport, dict) else None


def _extract_location(geo_json: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Extract location (lat/lng) from geocoding response."""
    results = geo_json.get("results") or []
    if not isinstance(results, list) or not results:
        return None
    
    r0 = results[0]
    if not isinstance(r0, dict):
        return None
    
    geom = r0.get("geometry") or {}
    if not isinstance(geom, dict):
        return None
    
    location = geom.get("location")
    return location if isinstance(location, dict) else None


def _viewport_to_rectangle(viewport: dict[str, Any]) -> dict[str, Any]:
    """Convert geocoding viewport to Places API rectangle format."""
    ne = viewport.get("northeast") or {}
    sw = viewport.get("southwest") or {}
    return {
        "low": {"latitude": float(sw["lat"]), "longitude": float(sw["lng"])},
        "high": {"latitude": float(ne["lat"]), "longitude": float(ne["lng"])},
    }


def _extract_address_components(geo_json: dict[str, Any]) -> dict[str, str]:
    """Extract address components from geocoding response."""
    results = geo_json.get("results") or []
    if not isinstance(results, list) or not results:
        return {}
    
    r0 = results[0]
    if not isinstance(r0, dict):
        return {}
    
    components = r0.get("address_components") or []
    
    result = {
        "city": None,
        "state": None,
        "country": None,
        "country_code": None,
    }
    
    for comp in components:
        if not isinstance(comp, dict):
            continue
        
        types = comp.get("types") or []
        long_name = comp.get("long_name")
        short_name = comp.get("short_name")
        
        if "locality" in types:
            result["city"] = long_name
        elif "administrative_area_level_1" in types:
            result["state"] = long_name
        elif "country" in types:
            result["country"] = long_name
            result["country_code"] = short_name
    
    return result


def _determine_type(geo_json: dict[str, Any]) -> str:
    """Determine location type from geocoding response."""
    results = geo_json.get("results") or []
    if not isinstance(results, list) or not results:
        return "unknown"
    
    r0 = results[0]
    if not isinstance(r0, dict):
        return "unknown"
    
    types = r0.get("types") or []
    
    # Determine type based on Google's type classification
    if "locality" in types or "sublocality" in types:
        return "city"
    elif "administrative_area_level_1" in types:
        return "state"
    elif "country" in types:
        return "country"
    elif "continent" in types:
        return "continent"
    
    return "unknown"


def _geocode_with_google(location_str: str, client: _GoogleGeocodingClient) -> dict[str, Any]:
    """
    Geocode a location string using Google Geocoding API.
    
    Args:
        location_str: Location string to geocode
        client: Reusable geocoding client (avoids creating new HTTP client per call)
        
    Returns:
        Dict with geocoding result including type, viewport, rectangle, etc.
        
    Raises:
        GeoResolutionError: If location cannot be resolved
    """
    if not location_str or not location_str.strip():
        raise GeoResolutionError("Empty location string")
    
    logger.debug(f"Geocoding with Google API: {location_str}")
    
    try:
        geo_json = client.geocode(address=location_str)
    except Exception as e:
        logger.error(f"Failed to geocode '{location_str}': {e}")
        raise GeoResolutionError(f"Geocoding failed: {e}") from e
    
    # Check API status
    status = geo_json.get("status")
    if status != "OK":
        error_msg = geo_json.get("error_message", status)

        if status == "ZERO_RESULTS":
            # Classification: User input invalid but system healthy
            logger.info("Geocoding ZERO_RESULTS for '%s'", location_str)
        else:
            # Classification: Potential API or configuration issue
            logger.warning(
                "Geocoding API returned status '%s' for '%s': %s",
                status,
                location_str,
                error_msg,
            )

        raise GeoResolutionError(f"Geocoding failed with status: {status}")
    
    # Extract viewport
    viewport = _extract_viewport(geo_json)
    if not viewport:
        logger.warning(f"No viewport found for '{location_str}'")
        raise GeoResolutionError("No viewport in geocoding response")
    
    # Extract location (center point)
    location = _extract_location(geo_json)
    if not location:
        logger.warning(f"No location found for '{location_str}'")
        raise GeoResolutionError("No location in geocoding response")
    
    # Convert viewport to rectangle
    rectangle = _viewport_to_rectangle(viewport)
    
    # Extract address components
    components = _extract_address_components(geo_json)
    
    # Determine type
    location_type = _determine_type(geo_json)
    
    # Build result
    result = {
        "type": location_type,
        "iso2": components.get("country_code"),
        "viewport": viewport,
        "rectangle": rectangle,
        "location": location,
        "city": components.get("city"),
        "state": components.get("state"),
        "country": components.get("country"),
        "country_code": components.get("country_code"),
    }
    
    logger.debug(f"Geocoded '{location_str}' as {location_type}: {components.get('country_code')}")
    
    return result


# ============================================================================
# Location Resolution Functions (Internal)
# ============================================================================

def _resolve_city(result: dict[str, Any]) -> list[ResolvedLocation]:
    """Resolve a city result into ResolvedLocation with Google Geocoding data."""
    city_name = result.get("city")
    state_name = result.get("state")
    country_code = result.get("iso2")
    country_name = result.get("country")
    
    if not country_code:
        return []
    
    # Extract geo data from Google Geocoding API response
    viewport = result.get("viewport")
    rectangle = result.get("rectangle")
    location = result.get("location")
    
    geo_data = None
    if viewport and rectangle and location:
        geo_data = GeoData(
            lat=location["lat"],
            lng=location["lng"],
            viewport=GeoViewport(
                northeast=GeoCoordinates(
                    lat=viewport["northeast"]["lat"],
                    lng=viewport["northeast"]["lng"]
                ),
                southwest=GeoCoordinates(
                    lat=viewport["southwest"]["lat"],
                    lng=viewport["southwest"]["lng"]
                ),
            ),
            rectangle=GeoRectangle(
                low=rectangle["low"],
                high=rectangle["high"],
            ),
        )
    
    resolved = ResolvedLocation(
        city=city_name,
        state=state_name,
        country=country_code,
        country_name=country_name,
        geo=geo_data,
    )
    
    return [resolved]


def _resolve_state(result: dict[str, Any]) -> list[ResolvedLocation]:
    """Resolve a state result into ResolvedLocation with Google Geocoding data."""
    state_name = result.get("state")
    country_code = result.get("iso2")
    country_name = result.get("country")
    
    if not country_code:
        return []
    
    # Extract geo data from Google Geocoding API response
    viewport = result.get("viewport")
    rectangle = result.get("rectangle")
    location = result.get("location")
    
    geo_data = None
    if viewport and rectangle and location:
        geo_data = GeoData(
            lat=location["lat"],
            lng=location["lng"],
            viewport=GeoViewport(
                northeast=GeoCoordinates(
                    lat=viewport["northeast"]["lat"],
                    lng=viewport["northeast"]["lng"]
                ),
                southwest=GeoCoordinates(
                    lat=viewport["southwest"]["lat"],
                    lng=viewport["southwest"]["lng"]
                ),
            ),
            rectangle=GeoRectangle(
                low=rectangle["low"],
                high=rectangle["high"],
            ),
        )
    
    resolved = ResolvedLocation(
        state=state_name,
        country=country_code,
        country_name=country_name,
        geo=geo_data,
    )
    
    return [resolved]


def _resolve_country(result: dict[str, Any]) -> list[ResolvedLocation]:
    """Resolve a country result into ResolvedLocation with Google Geocoding data."""
    country_name = result.get("country")
    country_code = result.get("iso2")
    
    if not country_code:
        return []
    
    # Extract geo data from Google Geocoding API response
    viewport = result.get("viewport")
    rectangle = result.get("rectangle")
    location = result.get("location")
    
    geo_data = None
    if viewport and rectangle and location:
        geo_data = GeoData(
            lat=location["lat"],
            lng=location["lng"],
            viewport=GeoViewport(
                northeast=GeoCoordinates(
                    lat=viewport["northeast"]["lat"],
                    lng=viewport["northeast"]["lng"]
                ),
                southwest=GeoCoordinates(
                    lat=viewport["southwest"]["lat"],
                    lng=viewport["southwest"]["lng"]
                ),
            ),
            rectangle=GeoRectangle(
                low=rectangle["low"],
                high=rectangle["high"],
            ),
        )
    
    resolved = ResolvedLocation(
        state=None,  # Classification: country-level result should not populate state
        country=country_code,
        country_name=country_name,
        geo=geo_data,
    )
    
    return [resolved]


def _dach_default() -> list[ResolvedLocation]:
    """
    Return DACH default locations as ResolvedLocation objects.
    
    Converts the constant data into proper Pydantic models.
    """
    result = []
    
    for loc_data in DACH_DEFAULT_RESOLVED_LOCATIONS:
        geo_data = loc_data["geo"]
        
        resolved = ResolvedLocation(
            country=loc_data["country"],
            country_name=loc_data["country_name"],
            continent="Europe",
            region="EMEA",
            geo=GeoData(
                lat=geo_data["lat"],
                lng=geo_data["lng"],
                viewport=GeoViewport(
                    northeast=GeoCoordinates(**geo_data["viewport"]["northeast"]),
                    southwest=GeoCoordinates(**geo_data["viewport"]["southwest"]),
                ),
                rectangle=GeoRectangle(
                    low=geo_data["rectangle"]["low"],
                    high=geo_data["rectangle"]["high"],
                ),
            ),
        )
        result.append(resolved)
    
    return result


# ============================================================================
# Public API
# ============================================================================

def resolve_locations(
    *,
    prompt_location: list[str] | None,
    global_search: bool,
) -> list[ResolvedLocation] | None:
    """
    Deterministically resolve location tokens into structured ResolvedLocation objects.
    
    Resolution logic:
    1. global_search == True → return None (no location filtering)
    2. prompt_location is None/empty → return DACH_DEFAULT
    3. prompt_location has tokens → geocode each token independently (with fallback to DACH on failure)
    
    Args:
        prompt_location: Raw location tokens from LLM (e.g., ["Berlin", "Germany"] or ["Berlin", "Paris"])
        global_search: Whether user expressed global intent
        
    Returns:
        - None if global_search is True
        - List of ResolvedLocation objects otherwise
        
    Notes:
        - This function is deterministic and does NOT call LLM
        - Uses Google Geocoding API for location resolution
        - Each token is geocoded independently to handle multiple locations correctly
        - Returns viewport and rectangle coordinates for Google Places API
        - Falls back to DACH defaults on geocoding failure
    """
    
    # ------------------------------------------------------------------------
    # Case 1: Global search → no location filtering
    # ------------------------------------------------------------------------
    if global_search:
        logger.debug("RESOLVE_LOCATIONS | global_search=True | result=None")
        return None
    
    # ------------------------------------------------------------------------
    # Case 2: No location tokens → DACH default
    # ------------------------------------------------------------------------
    if not prompt_location or not any(prompt_location):
        logger.debug("RESOLVE_LOCATIONS | prompt_location=empty | result=DACH_DEFAULT")
        return _dach_default()
    
    # ------------------------------------------------------------------------
    # Case 3: Geocode each location token independently using Google API
    # ------------------------------------------------------------------------
    logger.debug("RESOLVE_LOCATIONS | geocoding_tokens=%r", prompt_location)
    
    # Store (ResolvedLocation, type, iso2) so we can filter country entries deterministically later
    all_resolved: list[tuple[ResolvedLocation, str, str | None]] = []
    failed_tokens: list[str] = []
    
    # Create a single HTTP client for all geocoding operations
    # This is more efficient than creating a new client for each token
    try:
        with _GoogleGeocodingClient() as client:
            for token in prompt_location:
                token = token.strip()
                if not token:
                    continue
                
                try:
                    # Geocode this token with the shared client
                    result = _geocode_with_google(token, client)
                    
                    if not result:
                        failed_tokens.append(token)
                        continue
                    
                    result_type = result.get("type", "").lower()
                    iso2 = result.get("iso2")
                    
                    # Resolve based on type
                    if result_type == "city":
                        resolved = _resolve_city(result)
                    elif result_type == "state":
                        resolved = _resolve_state(result)
                    elif result_type == "country":
                        resolved = _resolve_country(result)
                    elif result_type == "continent":
                        logger.warning(f"Continent-level resolution not supported: {token}")
                        failed_tokens.append(token)
                        continue
                    else:
                        logger.warning(f"Unknown location type for '{token}': {result_type}")
                        failed_tokens.append(token)
                        continue
                    
                    if resolved:
                        for r in resolved:
                            all_resolved.append((r, result_type, iso2))
                        logger.info(f"Geocoded '{token}' as {result_type}")
                    else:
                        failed_tokens.append(token)
                        
                except GeoResolutionError as e:
                    logger.debug(f"Failed to geocode '{token}': {e}")
                    failed_tokens.append(token)
                except Exception as e:
                    logger.exception(f"Unexpected error geocoding '{token}': {e}")
                    failed_tokens.append(token)
    
    except Exception as e:
        logger.error(f"Failed to create geocoding client: {e}")
        # Fall through to DACH default
        all_resolved = []
        failed_tokens = list(prompt_location)
    
    # ------------------------------------------------------------------------
    # Scope filtering (deterministic, country-linked)
    # If a country-level result exists AND we also have city/state within same iso2,
    # drop that country result (too broad / redundant).
    #
    # Examples:
    # - ["Berlin","Germany"] -> keep Berlin (DE), drop Germany (DE)
    # - ["Mysore","Bangalore","India","Germany"] -> drop India (IN), keep Germany (DE)
    # ------------------------------------------------------------------------
    specific_iso2 = {iso2 for (_, t, iso2) in all_resolved if t in {"city", "state"} and iso2}
    
    filtered: list[ResolvedLocation] = []
    for (loc, t, iso2) in all_resolved:
        if t == "country" and iso2 and iso2 in specific_iso2:
            continue
        filtered.append(loc)
    
    # ------------------------------------------------------------------------
    # Return results or fallback to DACH
    # ------------------------------------------------------------------------
    if filtered:
        logger.info(f"RESOLVE_LOCATIONS | success={len(filtered)} | failed={len(failed_tokens)}")
        return filtered
    
    # All tokens failed → fallback to DACH
    logger.warning(f"RESOLVE_LOCATIONS | all_failed={failed_tokens} | fallback=DACH")
    return _dach_default()