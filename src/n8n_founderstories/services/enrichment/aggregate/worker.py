"""
═══════════════════════════════════════════════════════════════════════════════
AGGREGATE WORKER - Background Domain Aggregation Processor
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [WORKER] - Background daemon for result aggregation

PURPOSE:
    Continuously monitors domains with extraction_status = 'succeeded' or 'reused'
    and aggregates their page-level LLM results into domain-level enrichment results.

WORKER BEHAVIOR:
    - Runs in daemon thread (started at app startup)
    - Polls every 5 seconds for unaggregated domains
    - Processes domains one at a time
    - Aggregates all llm_ext_results for each domain
    - Normalizes and deduplicates emails and contacts
    - Stores results in enrichment_results table

AGGREGATION LOGIC:
    1. Company Name: Take first non-null value, aggregate all evidence
    2. Description: Take first non-null value, aggregate all evidence
    3. Emails: Normalize (lowercase), deduplicate, aggregate evidence per email
    4. Contacts: Deduplicate by (name, role), aggregate evidence per contact

NORMALIZATION:
    - Emails: Lowercase for deduplication
    - Contacts: Case-sensitive name matching, normalize role to lowercase

LOGGING:
    Format: AGGREGATE | <domain> | <request_id> | <job_id> | <sheet_id> | <status>
    Example: AGGREGATE | example.com | req_123 | job_456 | 1A2B3C4D5E6F | SUCCESS

INTEGRATION:
    Started in main.py startup event:
    
    _aggregate_worker_thread = threading.Thread(
        target=lambda: run_worker(poll_interval_s=5.0),
        name="AggregateWorker",
        daemon=True
    )
    _aggregate_worker_thread.start()

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import json
import logging
import time
import traceback
from typing import Any, Optional

import psycopg

from n8n_founderstories.core.config import settings
from n8n_founderstories.services.master.repo import update_enrichment_status
from .repository import (
    ensure_table,
    get_next_unaggregated_domain,
    get_llm_results_for_domain,
    get_det_results_for_domain,
    get_extraction_error_for_domain,
    upsert_enrichment_result,
)
from .company_selector import select_company_name
from .email_selector import select_best_email

logger = logging.getLogger(__name__)


# =============================================================================
# AGGREGATION LOGIC
# =============================================================================

def _aggregate_company(pages: list[dict[str, Any]]) -> Optional[str]:
    """
    Aggregate company name from multiple pages.
    
    Strategy:
    - Collect all unique company values with their evidence
    - If all values are the same (after normalization): return single object with evidence array
    - If values differ: return array of objects, each with value and evidence
    - Order: impressum first, then other pages
    
    Args:
        pages: List of page results with company_json
    
    Returns:
        JSON string with aggregated company or None
    """
    # Collect all company values with their evidence, preserving page_type for sorting
    company_data_by_value = {}
    
    for page in pages:
        if not page.get("company_json"):
            continue
        
        try:
            company_data = json.loads(page["company_json"])
            
            if company_data.get("value") and "evidence" in company_data and company_data["evidence"]:
                value = company_data["value"].strip()
                evidence = company_data["evidence"]
                page_type = page.get("page_type", "")
                
                # Normalize value for grouping (case-insensitive, strip whitespace)
                normalized_value = value.lower().strip()
                
                if normalized_value not in company_data_by_value:
                    company_data_by_value[normalized_value] = {
                        "value": value,  # Keep original case
                        "evidence": []
                    }
                
                company_data_by_value[normalized_value]["evidence"].append({
                    "url": page["url"],
                    "page_type": page_type,
                    "quote": evidence.get("quote", ""),
                    "_sort_key": 0 if page_type == "impressum" else 1,  # For sorting
                })
                
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse company_json from {page.get('url')}: {e}")
            continue
    
    if not company_data_by_value:
        return None
    
    # Sort evidence within each value group (impressum first)
    for data in company_data_by_value.values():
        data["evidence"].sort(key=lambda x: x["_sort_key"])
        # Remove sort key from output
        for ev in data["evidence"]:
            del ev["_sort_key"]
    
    # If only one unique value, return single object with evidence array
    if len(company_data_by_value) == 1:
        single_value = list(company_data_by_value.values())[0]
        return json.dumps({
            "value": single_value["value"],
            "evidence": single_value["evidence"],
        })
    
    # If multiple different values, return array of objects
    # Sort array: impressum value first, then others
    result_array = []
    for data in company_data_by_value.values():
        has_impressum = any(ev["page_type"] == "impressum" for ev in data["evidence"])
        result_array.append({
            "value": data["value"],
            "evidence": data["evidence"][0] if len(data["evidence"]) == 1 else data["evidence"],
            "_has_impressum": has_impressum,  # For sorting
        })
    
    # Sort: impressum entries first
    result_array.sort(key=lambda x: (0 if x["_has_impressum"] else 1))
    
    # Remove sort key from output
    for item in result_array:
        del item["_has_impressum"]
    
    return json.dumps(result_array)


def _aggregate_description(pages: list[dict[str, Any]]) -> Optional[str]:
    """
    Aggregate description from multiple pages.
    
    Strategy:
    - Collect all unique descriptions with their evidence
    - If all descriptions are the same (after normalization): return single object with evidence array
    - If descriptions differ: return array of objects, each with value and evidence
    
    Args:
        pages: List of page results with description_json
    
    Returns:
        JSON string with aggregated description or None
    """
    # Collect all description values with their evidence
    description_data_by_value = {}
    
    for page in pages:
        if not page.get("description_json"):
            continue
        
        try:
            desc_data = json.loads(page["description_json"])
            
            if desc_data.get("value") and "evidence" in desc_data and desc_data["evidence"]:
                value = desc_data["value"].strip()
                evidence = desc_data["evidence"]
                
                # Normalize value for grouping (case-insensitive, strip whitespace)
                normalized_value = value.lower().strip()
                
                if normalized_value not in description_data_by_value:
                    description_data_by_value[normalized_value] = {
                        "value": value,  # Keep original case
                        "evidence": []
                    }
                
                description_data_by_value[normalized_value]["evidence"].append({
                    "url": page["url"],
                    "page_type": page.get("page_type"),
                    "quote": evidence.get("quote", ""),
                })
                
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse description_json from {page.get('url')}: {e}")
            continue
    
    if not description_data_by_value:
        return None
    
    # If only one unique value, return single object with evidence array
    if len(description_data_by_value) == 1:
        single_value = list(description_data_by_value.values())[0]
        return json.dumps({
            "value": single_value["value"],
            "evidence": single_value["evidence"],
        })
    
    # If multiple different values, return array of objects
    result_array = []
    for data in description_data_by_value.values():
        # For multiple values, evidence is a single object (not array)
        result_array.append({
            "value": data["value"],
            "evidence": data["evidence"][0] if len(data["evidence"]) == 1 else data["evidence"],
        })
    
    return json.dumps(result_array)


def _aggregate_emails(pages: list[dict[str, Any]]) -> Optional[str]:
    """
    Aggregate and deduplicate emails from multiple pages.
    
    Strategy:
    - Normalize emails to lowercase for deduplication
    - Aggregate evidence for each unique email
    - Order: impressum evidence first, then other pages
    - If email has multiple evidence, return as array; if single, return as object
    
    Args:
        pages: List of page results with emails_json
    
    Returns:
        JSON string with deduplicated emails or None
    """
    # Map: normalized_email -> {"email": str, "evidence": []}
    email_map: dict[str, dict[str, Any]] = {}
    
    for page in pages:
        if not page.get("emails_json"):
            continue
        
        try:
            emails_data = json.loads(page["emails_json"])
            
            if not isinstance(emails_data, list):
                continue
            
            for email_obj in emails_data:
                if not isinstance(email_obj, dict):
                    continue
                
                email = email_obj.get("email")
                if not email:
                    continue
                
                # Normalize for deduplication
                normalized_email = email.lower().strip()
                page_type = page.get("page_type", "")
                
                # Initialize if first occurrence
                if normalized_email not in email_map:
                    email_map[normalized_email] = {
                        "email": normalized_email,
                        "evidence": [],
                    }
                
                # Add evidence with sort key
                if "evidence" in email_obj and email_obj["evidence"]:
                    evidence = email_obj["evidence"]
                    email_map[normalized_email]["evidence"].append({
                        "url": page["url"],
                        "page_type": page_type,
                        "quote": evidence.get("quote", ""),
                        "_sort_key": 0 if page_type == "impressum" else 1,
                    })
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse emails_json from {page.get('url')}: {e}")
            continue
    
    if not email_map:
        return None
    
    # Process each email
    emails_list = []
    for email_data in email_map.values():
        # Sort evidence (impressum first)
        email_data["evidence"].sort(key=lambda x: x["_sort_key"])
        
        # Remove sort key
        for ev in email_data["evidence"]:
            del ev["_sort_key"]
        
        # If single evidence, use object; if multiple, use array
        if len(email_data["evidence"]) == 1:
            emails_list.append({
                "email": email_data["email"],
                "evidence": email_data["evidence"][0],
            })
        else:
            emails_list.append({
                "email": email_data["email"],
                "evidence": email_data["evidence"],
            })
    
    # Sort emails list: emails with impressum evidence first
    def has_impressum(email_obj):
        evidence = email_obj["evidence"]
        if isinstance(evidence, dict):
            return 0 if evidence.get("page_type") == "impressum" else 1
        elif isinstance(evidence, list):
            return 0 if any(ev.get("page_type") == "impressum" for ev in evidence) else 1
        return 1
    
    emails_list.sort(key=has_impressum)
    
    return json.dumps(emails_list)


def _aggregate_contacts(pages: list[dict[str, Any]]) -> Optional[str]:
    """
    Aggregate and deduplicate contacts from multiple pages.
    
    Strategy:
    - Deduplicate by (name, role) tuple
    - Normalize role to lowercase for matching
    - Aggregate evidence for each unique contact
    - Order: impressum evidence first, then other pages
    - If contact has multiple evidence, return as array; if single, return as object
    
    Args:
        pages: List of page results with contacts_json
    
    Returns:
        JSON string with deduplicated contacts or None
    """
    # Map: (name, normalized_role) -> {"name": str, "role": str, "evidence": []}
    contact_map: dict[tuple[str, str], dict[str, Any]] = {}
    
    for page in pages:
        if not page.get("contacts_json"):
            continue
        
        try:
            contacts_data = json.loads(page["contacts_json"])
            
            if not isinstance(contacts_data, list):
                continue
            
            for contact_obj in contacts_data:
                if not isinstance(contact_obj, dict):
                    continue
                
                name = contact_obj.get("name")
                role = contact_obj.get("role")
                
                if not name:
                    continue
                
                # Normalize role for deduplication (None becomes empty string)
                normalized_role = (role or "").lower().strip()
                page_type = page.get("page_type", "")
                
                # Create key for deduplication
                key = (name.strip(), normalized_role)
                
                # Initialize if first occurrence
                if key not in contact_map:
                    contact_map[key] = {
                        "name": name.strip(),
                        "role": role.strip() if role else None,
                        "evidence": [],
                    }
                
                # Add evidence with sort key
                if "evidence" in contact_obj and contact_obj["evidence"]:
                    evidence = contact_obj["evidence"]
                    contact_map[key]["evidence"].append({
                        "url": page["url"],
                        "page_type": page_type,
                        "quote": evidence.get("quote", ""),
                        "_sort_key": 0 if page_type == "impressum" else 1,
                    })
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse contacts_json from {page.get('url')}: {e}")
            continue
    
    if not contact_map:
        return None
    
    # Process each contact
    contacts_list = []
    for contact_data in contact_map.values():
        # Sort evidence (impressum first)
        contact_data["evidence"].sort(key=lambda x: x["_sort_key"])
        
        # Remove sort key
        for ev in contact_data["evidence"]:
            del ev["_sort_key"]
        
        # If single evidence, use object; if multiple, use array
        if len(contact_data["evidence"]) == 1:
            contacts_list.append({
                "name": contact_data["name"],
                "role": contact_data["role"],
                "evidence": contact_data["evidence"][0],
            })
        else:
            contacts_list.append({
                "name": contact_data["name"],
                "role": contact_data["role"],
                "evidence": contact_data["evidence"],
            })
    
    # Sort contacts list: contacts with impressum evidence first
    def has_impressum(contact_obj):
        evidence = contact_obj["evidence"]
        if isinstance(evidence, dict):
            return 0 if evidence.get("page_type") == "impressum" else 1
        elif isinstance(evidence, list):
            return 0 if any(ev.get("page_type") == "impressum" for ev in evidence) else 1
        return 1
    
    contacts_list.sort(key=has_impressum)
    
    return json.dumps(contacts_list)


def _aggregate_det_emails(pages: list[dict[str, Any]]) -> Optional[str]:
    """
    Aggregate deterministic emails from multiple pages.
    
    Strategy:
    - Parse emails_json arrays from det_ext_results (format: ["email1", "email2"])
    - Normalize emails to lowercase for deduplication
    - Aggregate evidence for each unique email
    - Order: impressum evidence first, then other pages
    - If email has multiple evidence, return as array; if single, return as object
    
    Args:
        pages: List of page results from det_ext_results with emails_json
    
    Returns:
        JSON string with deduplicated deterministic emails or None
    """
    # Map: normalized_email -> {"email": str, "evidence": []}
    email_map: dict[str, dict[str, Any]] = {}
    
    for page in pages:
        if not page.get("emails_json"):
            continue
        
        try:
            # Parse JSON array of emails
            emails_data = json.loads(page["emails_json"])
            
            if not isinstance(emails_data, list):
                continue
            
            page_type = page.get("page_type", "")
            
            for email in emails_data:
                if not email or not isinstance(email, str):
                    continue
                
                # Normalize for deduplication
                normalized_email = email.lower().strip()
                
                # Initialize if first occurrence
                if normalized_email not in email_map:
                    email_map[normalized_email] = {
                        "email": normalized_email,
                        "evidence": [],
                    }
                
                # Add evidence with sort key (avoid duplicates from same page)
                evidence_key = (page["url"], page_type)
                if not any(ev.get("url") == page["url"] and ev.get("page_type") == page_type
                          for ev in email_map[normalized_email]["evidence"]):
                    email_map[normalized_email]["evidence"].append({
                        "url": page["url"],
                        "page_type": page_type,
                        "quote": email,  # Use the email itself as the quote
                        "_sort_key": 0 if page_type == "impressum" else 1,
                    })
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse emails_json from {page.get('url')}: {e}")
            continue
    
    if not email_map:
        return None
    
    # Process each email
    emails_list = []
    for email_data in email_map.values():
        # Sort evidence (impressum first)
        email_data["evidence"].sort(key=lambda x: x["_sort_key"])
        
        # Remove sort key
        for ev in email_data["evidence"]:
            del ev["_sort_key"]
        
        # If single evidence, use object; if multiple, use array
        if len(email_data["evidence"]) == 1:
            emails_list.append({
                "email": email_data["email"],
                "evidence": email_data["evidence"][0],
            })
        else:
            emails_list.append({
                "email": email_data["email"],
                "evidence": email_data["evidence"],
            })
    
    # Sort emails list: emails with impressum evidence first
    def has_impressum(email_obj):
        evidence = email_obj["evidence"]
        if isinstance(evidence, dict):
            return 0 if evidence.get("page_type") == "impressum" else 1
        elif isinstance(evidence, list):
            return 0 if any(ev.get("page_type") == "impressum" for ev in evidence) else 1
        return 1
    
    emails_list.sort(key=has_impressum)
    
    return json.dumps(emails_list)


# =============================================================================
# DOMAIN PROCESSING
# =============================================================================

def _extract_final_description(description_json: Optional[str]) -> Optional[str]:
    """
    Extract final description from description_json.
    
    For single value: returns the value
    For array: returns the first value (home prioritized)
    
    Args:
        description_json: JSON string with description data
    
    Returns:
        Description as TEXT or None
    """
    if not description_json:
        return None
    
    try:
        data = json.loads(description_json)
        
        # If it's an array, take first item
        if isinstance(data, list) and len(data) > 0:
            return data[0].get("value")
        
        # If it's a single object, get the value
        if isinstance(data, dict):
            return data.get("value")
        
        return None
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def _extract_final_contacts(contacts_json: Optional[str]) -> Optional[str]:
    """
    Extract final contacts from contacts_json as formatted string.
    
    Format: "Name1 (Role1), Name2 (Role2), Name3, ..."
    (Role is included only if present)
    
    Args:
        contacts_json: JSON string with contacts array
    
    Returns:
        Formatted contacts as TEXT or None
    """
    if not contacts_json:
        return None
    
    try:
        data = json.loads(contacts_json)
        
        if not isinstance(data, list):
            return None
        
        # Extract all contacts with name and role
        contacts = []
        for item in data:
            if not isinstance(item, dict):
                continue
            
            name = item.get("name")
            role = item.get("role")
            
            if name:
                if role:
                    contacts.append(f"{name} ({role})")
                else:
                    contacts.append(name)
        
        if not contacts:
            return None
        
        # Return as comma-separated string
        return ", ".join(contacts)
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def _process_domain(domain_data: dict[str, Any]) -> bool:
    """
    Process a single domain by aggregating its LLM and deterministic results.
    
    Args:
        domain_data: Domain information from get_next_unaggregated_domain
    
    Returns:
        True if processing succeeded, False otherwise
    """
    request_id = domain_data["request_id"]
    job_id = domain_data["job_id"]
    sheet_id = domain_data.get("sheet_id", "")
    organization = domain_data.get("organization")
    domain = domain_data["domain"]
    extraction_status = domain_data.get("extraction_status")
    
    try:
        # Check if extraction failed
        if extraction_status == 'failed':
            # Get error message from failed extraction
            with psycopg.connect(settings.postgres_dsn) as conn:
                error_message = get_extraction_error_for_domain(conn, request_id, domain)
            
            # Create failed enrichment entry
            with psycopg.connect(settings.postgres_dsn) as conn:
                upsert_enrichment_result(
                    conn,
                    request_id=request_id,
                    job_id=job_id,
                    sheet_id=sheet_id,
                    organization=organization,
                    domain=domain,
                    company_json=None,
                    description_json=None,
                    emails_json=None,
                    det_emails_json=None,
                    contacts_json=None,
                    company=None,
                    email=None,
                    emails=None,
                    description=None,
                    contacts=None,
                    status='failed',
                    error=error_message,
                )
                conn.commit()
            
            # Set enrichment_status to 'succeeded' (enrichment completed, even though extraction failed)
            with psycopg.connect(settings.postgres_dsn) as conn:
                update_enrichment_status(conn, request_id, domain, 'succeeded')
                conn.commit()
            
            logger.info(
                "AGGREGATE | %s | %s | %s | %s | FAILED_EXTRACTION | error=%s",
                domain,
                request_id,
                job_id,
                sheet_id,
                error_message,
            )
            
            return True
        
        # Get all LLM results for this domain
        with psycopg.connect(settings.postgres_dsn) as conn:
            llm_pages = get_llm_results_for_domain(conn, request_id, domain)
            det_pages = get_det_results_for_domain(conn, request_id, domain)
        
        if not llm_pages:
            logger.warning(
                "AGGREGATE | %s | %s | %s | %s | NO_LLM_PAGES",
                domain,
                request_id,
                job_id,
                sheet_id,
            )
            return False
        
        # Aggregate LLM results
        company_json = _aggregate_company(llm_pages)
        description_json = _aggregate_description(llm_pages)
        emails_json = _aggregate_emails(llm_pages)
        contacts_json = _aggregate_contacts(llm_pages)
        
        # Aggregate deterministic results
        det_emails_json = _aggregate_det_emails(det_pages) if det_pages else None
        
        # Extract final values for TEXT columns
        description = _extract_final_description(description_json)
        contacts = _extract_final_contacts(contacts_json)
        
        # Select best company name using intelligent selector
        company = None
        if company_json:
            try:
                company_candidates = json.loads(company_json)
                # Handle both single object and array formats
                if isinstance(company_candidates, dict):
                    company_candidates = [company_candidates]
                
                selection_result = select_company_name(organization, company_candidates)
                
                # Format company as JSON object with name, score, and page_type
                if selection_result.get("company"):
                    selected_candidate = selection_result.get("selected_candidate", {})
                    company_obj = {
                        "name": selection_result["company"],
                        "score": round(selection_result.get("confidence", 0.0), 4),
                        "page_type": selected_candidate.get("page_type", "unknown")
                    }
                    company = json.dumps(company_obj)
                
                logger.debug(
                    "COMPANY_SELECTION | domain=%s | selected=%s | confidence=%.3f | raw_score=%d",
                    domain,
                    selection_result.get("company"),
                    selection_result.get("confidence", 0.0),
                    selection_result.get("raw_score", 0)
                )
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("Failed to parse company_json for domain %s: %s", domain, str(e))
        
        # Select best email using intelligent selector
        email = None
        emails = None
        email_found = False
        
        if emails_json or det_emails_json:
            try:
                # Parse LLM emails
                emails_llm = []
                if emails_json:
                    emails_llm = json.loads(emails_json)
                    if not isinstance(emails_llm, list):
                        emails_llm = []
                
                # Parse deterministic emails
                emails_det = []
                if det_emails_json:
                    emails_det = json.loads(det_emails_json)
                    if not isinstance(emails_det, list):
                        emails_det = []
                
                # Select best email
                selection_result = select_best_email(emails_llm, emails_det, domain, organization)
                
                if selection_result and selection_result.get("email"):
                    # Format email as JSON object with email, score, page_type
                    email_obj = {
                        "email": selection_result["email"],
                        "score": selection_result["score"],
                        "page_type": selection_result["page_type"]
                    }
                    email = json.dumps(email_obj)
                    
                    # Format emails as JSON array with all scored emails
                    emails = json.dumps(selection_result["all_scored"])
                    
                    email_found = True
                    
                    logger.debug(
                        "EMAIL_SELECTION | domain=%s | selected=%s | score=%.4f | total_emails=%d",
                        domain,
                        selection_result["email"],
                        selection_result["score"],
                        len(selection_result["all_scored"])
                    )
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("Failed to parse emails for domain %s: %s", domain, str(e))
        
        # Check if email was found - if not, mark as failed
        if not email_found:
            error_message = "No valid email found during enrichment"
            
            # Store failed enrichment result
            with psycopg.connect(settings.postgres_dsn) as conn:
                upsert_enrichment_result(
                    conn,
                    request_id=request_id,
                    job_id=job_id,
                    sheet_id=sheet_id,
                    organization=organization,
                    domain=domain,
                    company_json=company_json,
                    description_json=description_json,
                    emails_json=emails_json,
                    det_emails_json=det_emails_json,
                    contacts_json=contacts_json,
                    company=company,
                    email=None,
                    emails=None,
                    description=description,
                    contacts=contacts,
                    status='failed',
                    error=error_message,
                )
                conn.commit()
            
            # Set enrichment_status to 'failed' in mstr_results
            with psycopg.connect(settings.postgres_dsn) as conn:
                update_enrichment_status(conn, request_id, domain, 'failed')
                conn.commit()
            
            logger.info(
                "AGGREGATE | %s | %s | %s | %s | FAILED_NO_EMAIL",
                domain,
                request_id,
                job_id,
                sheet_id,
            )
            
            return True
        
        # Store successful aggregated result
        with psycopg.connect(settings.postgres_dsn) as conn:
            upsert_enrichment_result(
                conn,
                request_id=request_id,
                job_id=job_id,
                sheet_id=sheet_id,
                organization=organization,
                domain=domain,
                company_json=company_json,
                description_json=description_json,
                emails_json=emails_json,
                det_emails_json=det_emails_json,
                contacts_json=contacts_json,
                company=company,  # Populated using intelligent selector
                email=email,  # Populated using intelligent selector
                emails=emails,  # All emails with scores
                description=description,  # Populated from description_json
                contacts=contacts,  # Populated from contacts_json
            )
            conn.commit()
        
        # Set enrichment_status to 'succeeded'
        with psycopg.connect(settings.postgres_dsn) as conn:
            update_enrichment_status(conn, request_id, domain, 'succeeded')
            conn.commit()
        
        logger.info(
            "AGGREGATE | %s | %s | %s | %s | SUCCESS",
            domain,
            request_id,
            job_id,
            sheet_id,
        )
        
        return True
        
    except Exception as e:
        # Create failed enrichment entry with error details
        error_message = f"Aggregation error: {str(e)}"
        
        try:
            with psycopg.connect(settings.postgres_dsn) as conn:
                upsert_enrichment_result(
                    conn,
                    request_id=request_id,
                    job_id=job_id,
                    sheet_id=sheet_id,
                    organization=organization,
                    domain=domain,
                    company_json=None,
                    description_json=None,
                    emails_json=None,
                    det_emails_json=None,
                    contacts_json=None,
                    company=None,
                    email=None,
                    emails=None,
                    description=None,
                    contacts=None,
                    status='failed',
                    error=error_message,
                )
                conn.commit()
        except Exception as upsert_error:
            logger.error(
                "AGGREGATE | %s | %s | %s | %s | ENRICHMENT_RESULT_UPSERT_ERROR: %s",
                domain,
                request_id,
                job_id,
                sheet_id,
                str(upsert_error),
            )
        
        # Set enrichment_status to 'failed' in mstr_results
        try:
            with psycopg.connect(settings.postgres_dsn) as conn:
                update_enrichment_status(conn, request_id, domain, 'failed')
                conn.commit()
        except Exception as status_error:
            logger.error(
                "AGGREGATE | %s | %s | %s | %s | STATUS_UPDATE_ERROR: %s",
                domain,
                request_id,
                job_id,
                sheet_id,
                str(status_error),
            )
        
        logger.error(
            "AGGREGATE | %s | %s | %s | %s | ERROR: %s",
            domain,
            request_id,
            job_id,
            sheet_id,
            str(e),
            exc_info=True,
        )
        return False


# =============================================================================
# WORKER LOOP
# =============================================================================

def run_worker(poll_interval_s: float = 5.0) -> None:
    """
    Run the aggregation worker loop.
    
    This function runs continuously in a daemon thread, polling for
    unaggregated domains and processing them one at a time.
    
    Args:
        poll_interval_s: Seconds to wait between polls (default: 5.0)
    
    Worker Loop:
        1. Ensure table exists
        2. Get next unaggregated domain
        3. If domain found: aggregate and store results
        4. If no domain: sleep for poll_interval_s
        5. Repeat
    
    Notes:
        - Runs in daemon thread (stops with application)
        - Processes domains sequentially
        - Never blocks other workers
    """
    # Ensure tables exist on startup
    try:
        with psycopg.connect(settings.postgres_dsn) as conn:
            # Ensure mstr_results table exists
            from n8n_founderstories.services.master import repo as master_repo
            master_repo.ensure_table(conn)
            
            # Ensure llm_ext_results table exists
            from n8n_founderstories.services.enrichment.extract.llm.storage import ensure_table as ensure_llm_table
            ensure_llm_table(conn)
            
            # Ensure enrichment_results table exists
            ensure_table(conn)
            
            # Mark enrichment as failed for domains where extraction failed
            failed_count = master_repo.mark_enrichment_failed_for_failed_extraction(conn)
            conn.commit()
            
        logger.info("AGGREGATE | INIT | Tables initialized | marked_failed=%d", failed_count)
    except Exception as e:
        logger.error(
            "AGGREGATE | TABLE_INIT_ERROR | error=%s | traceback=%s",
            str(e),
            traceback.format_exc(),
        )
        return
    
    logger.info("AGGREGATE | START | poll_interval=%.1fs", poll_interval_s)
    
    # Main worker loop
    poll_count = 0
    while True:
        try:
            poll_count += 1
            
            # Get next unaggregated domain
            with psycopg.connect(settings.postgres_dsn) as conn:
                domain_data = get_next_unaggregated_domain(conn)
            
            if domain_data:
                # Process domain
                _process_domain(domain_data)
            else:
                # No domains to process, sleep for poll interval
                time.sleep(poll_interval_s)
                
        except KeyboardInterrupt:
            logger.info("AGGREGATE | INTERRUPTED")
            break
        except Exception as e:
            logger.error(
                "AGGREGATE | LOOP_ERROR | error=%s | traceback=%s",
                str(e),
                traceback.format_exc(),
            )
            # Sleep before retrying
            time.sleep(poll_interval_s)
    
    logger.info("AGGREGATE | STOPPED")