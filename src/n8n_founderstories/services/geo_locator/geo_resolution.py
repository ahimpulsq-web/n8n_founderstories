from __future__ import annotations

# =============================================================================
# geo_resolution.py
# Deterministic location extraction + lookup based on GeoNames datasets.
#
# Classification:
# - Role: pure deterministic resolution; no network; no LLM.
# - Data: GeoNames flat files stored alongside this package (geo_locator/data).
# - Policy: if no location is found, raise GeoResolutionError (caller decides fallback).
# =============================================================================

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple


# =============================================================================
# Exceptions
# =============================================================================

class GeoResolutionError(Exception):
    """Raised when no location signal can be resolved from the prompt."""


class GeoDataError(Exception):
    """Raised when required GeoNames data files are missing or unreadable."""


# =============================================================================
# Tunables
# =============================================================================

COUNTRY_CITY_LIMIT = 5
STATE_CITY_LIMIT = 4

CONTINENT_TOP_COUNTRIES = {
    "Asia": ["Japan", "South Korea", "Singapore", "India", "China", "United Arab Emirates"],
    "Europe": ["Germany", "United Kingdom", "France", "Netherlands", "Italy", "Spain"],
    "North America": ["United States", "Canada", "Mexico"],
    "South America": ["Brazil", "Argentina", "Chile", "Colombia"],
    "Africa": ["South Africa", "Nigeria", "Kenya", "Egypt"],
    "Oceania": ["Australia", "New Zealand"],
}


# =============================================================================
# File system paths (package-relative)
# =============================================================================

_GEOLOCATOR_DIR = Path(__file__).resolve().parent
_DATA_DIR = _GEOLOCATOR_DIR / "data"

COUNTRIES_PATH = _DATA_DIR / "countryInfo.txt"
STATES_PATH = _DATA_DIR / "admin1CodesASCII.txt"
CITIES_PATH = _DATA_DIR / "cities15000.txt"


def _require_file(path: Path) -> None:
    if not path.exists():
        raise GeoDataError(f"GeoNames file not found: {path}")


# =============================================================================
# Normalization
# =============================================================================

def normalize(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[\s\-]+", " ", s.lower())
    return s.strip()


def primary_hl_from_languages(languages_field: str) -> str:
    """
    GeoNames Languages examples:
      'en-IN,hi,bn' -> 'en'
      'de,fr,it,rm' -> 'de'
      'ar-AE,en,fa,hi,ur' -> 'ar'
    """
    if not languages_field:
        return "en"
    first = languages_field.split(",", 1)[0].strip()
    if not first:
        return "en"
    return (first.split("-", 1)[0].strip().lower() or "en")


# =============================================================================
# Dataset (lazy-loaded)
# =============================================================================

@dataclass(frozen=True)
class GeoDataset:
    countries: Dict[str, Dict[str, Any]]
    states: Dict[str, Dict[str, Any]]
    city_lookup: Dict[str, Dict[str, Any]]
    cities_by_country: Dict[str, List[Dict[str, Any]]]
    cities_by_state: Dict[Tuple[str, str], List[Dict[str, Any]]]
    continent_lookup: Dict[str, str]


_DATASET: GeoDataset | None = None


def dataset() -> GeoDataset:
    """Get cached GeoDataset (loads GeoNames files on first use)."""
    global _DATASET
    if _DATASET is None:
        _DATASET = _load_dataset()
    return _DATASET


def _load_dataset() -> GeoDataset:
    _require_file(COUNTRIES_PATH)
    _require_file(STATES_PATH)
    _require_file(CITIES_PATH)

    countries = load_countries(COUNTRIES_PATH)
    states = load_states(STATES_PATH)
    city_lookup, cities_by_country, cities_by_state = load_cities(CITIES_PATH)
    continent_lookup = {normalize(k): k for k in CONTINENT_TOP_COUNTRIES}

    return GeoDataset(
        countries=countries,
        states=states,
        city_lookup=city_lookup,
        cities_by_country=cities_by_country,
        cities_by_state=cities_by_state,
        continent_lookup=continent_lookup,
    )


# =============================================================================
# Loaders (GeoNames)
# =============================================================================

def load_countries(path: Path) -> Dict[str, Dict[str, Any]]:
    countries: Dict[str, Dict[str, Any]] = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")

            iso2 = (parts[0] or "").strip().upper()
            name = (parts[4] or "").strip()
            capital = (parts[5] or "").strip()
            languages = (parts[15] or "").strip() if len(parts) > 15 else ""
            hl = primary_hl_from_languages(languages)

            if not iso2 or not name:
                continue

            countries[normalize(name)] = {"name": name, "iso2": iso2, "capital": capital, "hl": hl}
    return countries


def load_states(path: Path) -> Dict[str, Dict[str, Any]]:
    states: Dict[str, Dict[str, Any]] = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            code, name = line.strip().split("\t")[:2]
            country_iso2, admin1 = code.split(".")
            states[normalize(name)] = {"name": name, "country": country_iso2.upper(), "admin1": admin1}
    return states


def load_cities(path: Path):
    city_lookup: Dict[str, Dict[str, Any]] = {}
    cities_by_country: Dict[str, List[Dict[str, Any]]] = {}
    cities_by_state: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    with path.open(encoding="utf-8") as f:
        for line in f:
            p = line.strip().split("\t")
            name = p[1]
            country_iso2 = p[8].upper()
            admin1 = p[10]
            population = int(p[14]) if p[14].isdigit() else 0

            city = {"name": name, "population": population, "country_iso2": country_iso2}

            city_lookup[normalize(name)] = city
            cities_by_country.setdefault(country_iso2, []).append(city)
            cities_by_state.setdefault((country_iso2, admin1), []).append(city)

    return city_lookup, cities_by_country, cities_by_state


# =============================================================================
# Candidate extraction
# =============================================================================

def extract_location_candidates(prompt: str) -> List[str]:
    tokens = re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'\-\.]*", (prompt or "").lower())
    spans: list[str] = []
    for size in range(min(4, len(tokens)), 0, -1):
        for i in range(len(tokens) - size + 1):
            spans.append(normalize(" ".join(tokens[i:i + size])))
    return spans


# =============================================================================
# Resolution (public)
# =============================================================================

def resolve(prompt: str) -> Dict[str, Any]:
    ds = dataset()
    prompt_n = normalize(prompt)

    # 0) Continent
    for key, cont_name in ds.continent_lookup.items():
        if key in prompt_n:
            countries = []
            for cn in CONTINENT_TOP_COUNTRIES[cont_name]:
                meta = ds.countries.get(normalize(cn))
                if meta:
                    countries.append({"name": meta["name"], "iso2": meta["iso2"], "hl": meta["hl"]})
            return {"type": "continent", "name": cont_name, "countries": countries}

    candidates = extract_location_candidates(prompt)

    # 1) Multi-word city
    for key in candidates:
        if " " in key and key in ds.city_lookup:
            c = ds.city_lookup[key]
            iso2 = c["country_iso2"]
            hl = next((v["hl"] for v in ds.countries.values() if v["iso2"] == iso2), "en")
            return {"type": "city", "name": c["name"], "iso2": iso2, "hl": hl}

    # 2) State
    for key in candidates:
        if key in ds.states:
            st = ds.states[key]
            iso2 = st["country"]
            hl = next((v["hl"] for v in ds.countries.values() if v["iso2"] == iso2), "en")

            cities = list(ds.cities_by_state.get((iso2, st["admin1"]), []))
            cities.sort(key=lambda x: x["population"], reverse=True)

            result: list[str] = []
            seen: set[str] = set()

            for c in cities:
                if normalize(c["name"]) == normalize(st["name"]):
                    result.append(c["name"])
                    seen.add(normalize(c["name"]))
                    break

            for c in cities:
                n = normalize(c["name"])
                if n not in seen:
                    result.append(c["name"])
                    seen.add(n)
                if len(result) >= STATE_CITY_LIMIT:
                    break

            return {"type": "state", "name": st["name"], "iso2": iso2, "hl": hl, "cities": result}

    # 3) Single-word city
    for key in candidates:
        if key in ds.city_lookup:
            c = ds.city_lookup[key]
            iso2 = c["country_iso2"]
            hl = next((v["hl"] for v in ds.countries.values() if v["iso2"] == iso2), "en")
            return {"type": "city", "name": c["name"], "iso2": iso2, "hl": hl}

    # 4) Country
    for key in candidates:
        if key in ds.countries:
            co = ds.countries[key]
            iso2 = co["iso2"]
            hl = co["hl"]

            cities = list(ds.cities_by_country.get(iso2, []))
            cities.sort(key=lambda x: x["population"], reverse=True)

            result: list[str] = []
            capital = co["capital"]

            for c in cities:
                if normalize(c["name"]) == normalize(capital):
                    result.append(c["name"])
                    break

            for c in cities:
                if c["name"] not in result:
                    result.append(c["name"])
                if len(result) >= COUNTRY_CITY_LIMIT:
                    break

            return {"type": "country", "name": co["name"], "iso2": iso2, "hl": hl, "cities": result}

    raise GeoResolutionError("Location not found")
