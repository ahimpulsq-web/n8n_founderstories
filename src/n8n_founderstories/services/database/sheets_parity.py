"""
Shared sheets parity conversion utilities.

This module provides standardized conversion functions between database records
and Google Sheets format, eliminating duplication across tools.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional

from ...core.utils.text import norm


def results_to_sheets_rows(
    results: List[Dict[str, Any]], 
    headers: List[str],
    field_mapping: Optional[Dict[str, str]] = None,
    value_transformers: Optional[Dict[str, Callable[[Any], str]]] = None,
) -> List[List[str]]:
    """
    Convert database results to Google Sheets format.
    
    This is a generic function that can handle any result type by accepting
    a field mapping and optional value transformers.
    
    Args:
        results: List of database result dictionaries
        headers: List of sheet column headers (defines output order)
        field_mapping: Optional mapping from header names to database field names
                      If None, uses header names as-is (lowercased)
        value_transformers: Optional transformers for specific fields
                           Key is header name, value is transform function
    
    Returns:
        List of rows in sheets format, each row matching headers order
    
    Example:
        # For Hunter.io results
        headers = ["Organisation", "Domain", "Location", "Headcount", "Search Query", "Debug Filters"]
        field_mapping = {
            "Organisation": "organisation",
            "Domain": "domain", 
            "Location": "location",
            "Headcount": "headcount",
            "Search Query": "search_query",
            "Debug Filters": "debug_filters"
        }
        sheets_rows = results_to_sheets_rows(db_results, headers, field_mapping)
    """
    sheets_rows = []
    
    # Default field mapping: header name -> lowercase field name
    if field_mapping is None:
        field_mapping = {header: header.lower().replace(" ", "_") for header in headers}
    
    for result in results:
        row = []
        for header in headers:
            # Get the database field name for this header
            field_name = field_mapping.get(header, header.lower().replace(" ", "_"))
            
            # Get the raw value
            raw_value = result.get(field_name, "")
            
            # Apply transformer if available
            if value_transformers and header in value_transformers:
                try:
                    transformed_value = value_transformers[header](raw_value)
                except Exception:
                    transformed_value = str(raw_value) if raw_value is not None else ""
            else:
                # Default transformation: normalize to string
                transformed_value = norm(raw_value) if raw_value is not None else ""
            
            row.append(transformed_value)
        
        sheets_rows.append(row)
    
    return sheets_rows


def enriched_to_sheets_rows(
    enriched: List[Dict[str, Any]], 
    headers: List[str],
    field_mapping: Optional[Dict[str, str]] = None,
    value_transformers: Optional[Dict[str, Callable[[Any], str]]] = None,
) -> List[List[str]]:
    """
    Convert database enriched records to Google Sheets format.
    
    This is essentially the same as results_to_sheets_rows but with a different
    name for clarity when dealing with enriched data.
    
    Args:
        enriched: List of database enriched record dictionaries
        headers: List of sheet column headers (defines output order)
        field_mapping: Optional mapping from header names to database field names
        value_transformers: Optional transformers for specific fields
    
    Returns:
        List of rows in sheets format, each row matching headers order
    """
    return results_to_sheets_rows(enriched, headers, field_mapping, value_transformers)


def audit_to_sheets_rows(
    audit_records: List[Dict[str, Any]], 
    headers: List[str],
    field_mapping: Optional[Dict[str, str]] = None,
    value_transformers: Optional[Dict[str, Callable[[Any], str]]] = None,
) -> List[List[str]]:
    """
    Convert database audit records to Google Sheets format.
    
    Args:
        audit_records: List of database audit record dictionaries
        headers: List of sheet column headers (defines output order)
        field_mapping: Optional mapping from header names to database field names
        value_transformers: Optional transformers for specific fields
    
    Returns:
        List of rows in sheets format, each row matching headers order
    """
    return results_to_sheets_rows(audit_records, headers, field_mapping, value_transformers)


# Common value transformers that can be reused across tools
def json_transformer(value: Any) -> str:
    """Transform a value to JSON string, handling None and errors gracefully."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value  # Already a string, assume it's JSON
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def int_transformer(value: Any) -> str:
    """Transform a value to string representation of integer."""
    if value is None:
        return ""
    try:
        return str(int(value))
    except (ValueError, TypeError):
        return ""


def float_transformer(value: Any) -> str:
    """Transform a value to string representation of float."""
    if value is None:
        return ""
    try:
        return str(float(value))
    except (ValueError, TypeError):
        return ""


def bool_transformer(value: Any) -> str:
    """Transform a boolean value to string."""
    if value is None:
        return ""
    return str(bool(value))


# Pre-configured transformers for common use cases
COMMON_TRANSFORMERS = {
    "json": json_transformer,
    "int": int_transformer,
    "float": float_transformer,
    "bool": bool_transformer,
}


def create_hunter_results_converter() -> Callable[[List[Dict[str, Any]]], List[List[str]]]:
    """
    Create a converter function for Hunter.io results.
    
    Returns:
        Function that converts Hunter.io database results to sheets format
    """
    headers = ["Organisation", "Domain", "Location", "Headcount", "Search Query", "Debug Filters"]
    field_mapping = {
        "Organisation": "organisation",
        "Domain": "domain",
        "Location": "location", 
        "Headcount": "headcount",
        "Search Query": "search_query",
        "Debug Filters": "debug_filters"
    }
    
    def converter(results: List[Dict[str, Any]]) -> List[List[str]]:
        return results_to_sheets_rows(results, headers, field_mapping)
    
    return converter


def create_hunter_audit_converter() -> Callable[[List[Dict[str, Any]]], List[List[str]]]:
    """
    Create a converter function for Hunter.io audit records.
    
    Returns:
        Function that converts Hunter.io audit database records to sheets format
    """
    headers = [
        "Job ID", "Request ID", "Query Type", "Intended Location", "Intended Headcount",
        "Applied Location", "Applied Headcount", "Query Text", "Keywords", "Keyword Match",
        "Total Results", "Returned Count", "Appended Rows", "Applied Filters (JSON)"
    ]
    field_mapping = {
        "Job ID": "job_id",
        "Request ID": "request_id",
        "Query Type": "query_type",
        "Intended Location": "intended_location",
        "Intended Headcount": "intended_headcount",
        "Applied Location": "applied_location",
        "Applied Headcount": "applied_headcount",
        "Query Text": "query_text",
        "Keywords": "keywords",
        "Keyword Match": "keyword_match",
        "Total Results": "total_results",
        "Returned Count": "returned_count",
        "Appended Rows": "appended_rows",
        "Applied Filters (JSON)": "applied_filters"
    }
    value_transformers = {
        "Total Results": int_transformer,
        "Returned Count": int_transformer,
        "Appended Rows": int_transformer,
        "Applied Filters (JSON)": json_transformer,
    }
    
    def converter(audit_records: List[Dict[str, Any]]) -> List[List[str]]:
        return audit_to_sheets_rows(audit_records, headers, field_mapping, value_transformers)
    
    return converter


def create_google_maps_results_converter() -> Callable[[List[Dict[str, Any]]], List[List[str]]]:
    """
    Create a converter function for Google Maps results.
    
    Returns:
        Function that converts Google Maps database results to sheets format
    """
    headers = [
        "Place Name", "Location Label", "Address", "Place ID", "Type", 
        "Website", "Domain", "Phone", "Search Query", "Business Status", "Google Maps URL"
    ]
    field_mapping = {
        "Place Name": "name",
        "Location Label": "location_label",
        "Address": "address",
        "Place ID": "place_id",
        "Type": "category",
        "Website": "website",
        "Domain": "domain",
        "Phone": "phone",
        "Search Query": "query_text",
        "Business Status": "business_status",
        "Google Maps URL": "google_maps_url"
    }
    
    def converter(results: List[Dict[str, Any]]) -> List[List[str]]:
        return results_to_sheets_rows(results, headers, field_mapping)
    
    return converter


def create_google_maps_enriched_converter() -> Callable[[List[Dict[str, Any]]], List[List[str]]]:
    """
    Create a converter function for Google Maps enriched records.
    
    Returns:
        Function that converts Google Maps enriched database records to sheets format
    """
    headers = ["Place ID", "Rating", "Reviews Count", "Photos Count", "Opening Hours"]
    field_mapping = {
        "Place ID": "place_id",
        "Rating": "rating",
        "Reviews Count": "reviews_count", 
        "Photos Count": "photos_count",
        "Opening Hours": "opening_hours"
    }
    value_transformers = {
        "Rating": float_transformer,
        "Reviews Count": int_transformer,
        "Photos Count": int_transformer,
        "Opening Hours": json_transformer,
    }
    
    def converter(enriched_records: List[Dict[str, Any]]) -> List[List[str]]:
        return enriched_to_sheets_rows(enriched_records, headers, field_mapping, value_transformers)
    
    return converter