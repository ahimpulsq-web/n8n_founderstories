from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Tuple
from uuid import UUID

from .master_row_index import get_master_row_index_map
from .sheets import SheetsClient, default_sheets_config
from .sheets_delta import update_web_enrichment_columns_batch
from .sheets_manager import GoogleSheetsManager
from .sheets_schema import TAB_MASTER_MAIN, TAB_WEB_ENRICHMENT_MAIN, HEADERS_WEB_ENRICHMENT_MAIN
from ...core.utils.text import norm
from ..database.connection import get_connection_context
from ...core.config import settings

logger = logging.getLogger(__name__)


def _col_index_to_letter(col_index: int) -> str:
    n = col_index + 1
    letters: list[str] = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


def _ensure_tab_and_headers(client: SheetsClient) -> None:
    client.ensure_tab(TAB_WEB_ENRICHMENT_MAIN)
    end_col = _col_index_to_letter(len(HEADERS_WEB_ENRICHMENT_MAIN) - 1)
    rng = f"{TAB_WEB_ENRICHMENT_MAIN}!A1:{end_col}1"
    client.values_batch_update(
        data=[{"range": rng, "values": [HEADERS_WEB_ENRICHMENT_MAIN]}],
        value_input_option="RAW",
    )


def _parse_primary_email(emails_json_str: str | None) -> str:
    """
    Your DB stores emails as json string for now.
    We'll return first email if present; otherwise blank.
    Accepts shapes like:
      [ ["a@b.com", 0.8, "url"], ... ] OR [ "a@b.com", ... ]
    """
    if not emails_json_str:
        return ""
    try:
        import json
        data = json.loads(emails_json_str)
        if not data:
            return ""
        first = data[0]
        if isinstance(first, list) and first:
            return norm(str(first[0]))
        if isinstance(first, str):
            return norm(first)
        return ""
    except Exception:
        return ""


def _parse_primary_contact_name(contacts_json_str: str | None) -> str:
    """
    Your DB stores contacts as json string for now.
    Accepts shapes like:
      [ ["Name", "Role", 0.7, "url"], ... ] OR [ "Name", ... ] OR [ {"name": ...}, ... ]
    """
    if not contacts_json_str:
        return ""
    try:
        import json
        data = json.loads(contacts_json_str)
        if not data:
            return ""
        first = data[0]
        if isinstance(first, list) and first:
            return norm(str(first[0]))
        if isinstance(first, dict):
            return norm(str(first.get("name", "")))
        if isinstance(first, str):
            return norm(first)
        return ""
    except Exception:
        return ""


# ============================================================================
# COMBINE RESULTS → MASTER SHEET SYNC
# ============================================================================

def _transform_company_name(combined_company: Any, organization: str) -> str:
    """
    Extract company name(s) from combined_company JSON or fallback to organization.
    
    Format: {"name": "AllerLiebe GmbH", "sources": [...], "frequency": 1, "confidence": 0.7}
    Or list: [{"name": "Company1", ...}, {"name": "Company2", ...}]
    
    Output: "Company1, Company2, ..." if multiple names present
    
    Args:
        combined_company: JSONB field (dict/list or JSON string)
        organization: Fallback organization name
        
    Returns:
        Company name string (comma-separated if multiple)
    """
    if not combined_company:
        return norm(organization or "")
    
    try:
        # Handle if it's already a dict/list (JSONB) or needs parsing (JSON string)
        if isinstance(combined_company, str):
            data = json.loads(combined_company)
        else:
            data = combined_company
        
        # Handle single company object
        if isinstance(data, dict):
            name = data.get("name", "")
            return norm(name) if name else norm(organization or "")
        
        # Handle list of companies
        if isinstance(data, list):
            names = []
            for item in data:
                if isinstance(item, dict):
                    name = item.get("name", "").strip()
                    if name:
                        names.append(name)
            return ", ".join(names) if names else norm(organization or "")
        
        return norm(organization or "")
    except Exception:
        return norm(organization or "")


def _transform_emails(combined_emails: Any) -> str:
    """
    Transform combined_emails JSON to comma-separated string.
    
    Input: [{"email": "info@example.com"}, {"email": "sales@example.com"}]
    Output: "info@example.com, sales@example.com"
    
    Args:
        combined_emails: JSONB field (list or JSON string)
        
    Returns:
        Comma-separated email string
    """
    if not combined_emails:
        return ""
    
    try:
        # Handle if it's already a list (JSONB) or needs parsing (JSON string)
        if isinstance(combined_emails, str):
            data = json.loads(combined_emails)
        else:
            data = combined_emails
        
        if not isinstance(data, list):
            return ""
        
        # Extract emails, deduplicate while preserving order
        seen = set()
        emails = []
        for item in data:
            if isinstance(item, dict):
                email = item.get("email", "").strip()
                if email and email.lower() not in seen:
                    seen.add(email.lower())
                    emails.append(email)
        
        return ", ".join(emails)
    except Exception:
        return ""


def _transform_contacts(combined_people: Any) -> str:
    """
    Transform combined_people JSON to formatted contact string.
    
    Input: [{"name": "John Doe", "role": "CEO"}, {"name": "Jane Smith", "role": null}]
    Output: "John Doe (CEO), Jane Smith"
    
    Args:
        combined_people: JSONB field (list or JSON string)
        
    Returns:
        Formatted contact string
    """
    if not combined_people:
        return ""
    
    try:
        # Handle if it's already a list (JSONB) or needs parsing (JSON string)
        if isinstance(combined_people, str):
            data = json.loads(combined_people)
        else:
            data = combined_people
        
        if not isinstance(data, list):
            return ""
        
        contacts = []
        for item in data:
            if isinstance(item, dict):
                name = item.get("name", "").strip()
                role = item.get("role", "").strip() if item.get("role") else None
                
                if name:
                    if role:
                        contacts.append(f"{name} ({role})")
                    else:
                        contacts.append(name)
        
        return ", ".join(contacts)
    except Exception:
        return ""


def _transform_descriptions(combined_description: Any) -> Tuple[str, str]:
    """
    Extract short and long descriptions from combined_description JSON.
    
    For now, both short and long use the same combined_description value.
    Future: May differentiate between short/long versions.
    
    Input: {"text": "Company description", "sources": [...], "confidence": 0.8}
    Or: [{"text": "Description 1", ...}, {"text": "Description 2", ...}]
    Output: ("Description", "Description") - same value for both
    
    Args:
        combined_description: JSONB field (dict/list or JSON string)
        
    Returns:
        Tuple of (short_description, long_description)
    """
    if not combined_description:
        return ("", "")
    
    try:
        # Handle if it's already a dict/list (JSONB) or needs parsing (JSON string)
        if isinstance(combined_description, str):
            data = json.loads(combined_description)
        else:
            data = combined_description
        
        # Handle single description object
        if isinstance(data, dict):
            text = data.get("text", "").strip()
            return (text, text)  # Use same text for both short and long
        
        # Handle list of descriptions - use first one
        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, dict):
                text = first.get("text", "").strip()
                return (text, text)  # Use same text for both short and long
        
        return ("", "")
    except Exception:
        return ("", "")


def _get_combined_results_for_sync(
    *,
    request_id: str,
    dsn: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Query DB for combined results ready for sync.
    
    Args:
        request_id: Request ID to filter by
        dsn: Database connection string (optional)
        
    Returns:
        List of row dicts with combined fields
    """
    dsn = dsn or settings.postgres_dsn
    
    sql = """
    SELECT
        master_result_id,
        organization,
        combined_company,
        combined_emails,
        combined_people,
        combined_descriptions
    FROM web_scraper_enrichment_results
    WHERE request_id = %s
      AND combine_status IN ('ok', 'partial')
    ORDER BY master_result_id;
    """
    
    with get_connection_context(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (request_id,))
            rows = cur.fetchall()
    
    results = []
    for row in rows:
        results.append({
            "master_result_id": row[0],
            "organization": row[1],
            "combined_company": row[2],
            "combined_emails": row[3],
            "combined_people": row[4],
            "combined_descriptions": row[5],
        })
    
    return results


def sync_combined_web_enrichment_to_master_sheet(
    *,
    request_id: str,
    spreadsheet_id: str | None = None,
) -> None:
    """
    Sync COMBINE results to Master sheet (Master_v2 tab).
    
    This function:
    1. Queries DB for rows with combine_status IN ('ok', 'partial')
    2. Transforms combined fields according to spec
    3. Matches rows using master_result_id
    4. Updates only changed cells via delta update
    
    Target columns in Master_v2:
    - Column E (4): Company Name
    - Column F (5): E-mail ID
    - Column G (6): Contact Names
    - Column H (7): Short Company Description
    - Column I (8): Long Company Description (reserved for future use)
    
    Note: Currently only columns E:H are updated. Column I is reserved.
    
    Args:
        request_id: Request ID to sync
        spreadsheet_id: Google Sheets spreadsheet ID (optional, uses settings default)
    """
    logger.info("WEB_ENRICHMENT_SHEETS_SYNC_START request_id=%s", request_id)
    
    # Get spreadsheet ID from settings if not provided
    if not spreadsheet_id:
        spreadsheet_id = getattr(settings, "google_sheets_spreadsheet_id", None)
        if not spreadsheet_id:
            logger.error("WEB_ENRICHMENT_SHEETS_SYNC_ERROR error=no_spreadsheet_id")
            raise ValueError("spreadsheet_id not provided and not found in settings")
    
    # Query DB for combined results
    rows = _get_combined_results_for_sync(request_id=request_id)
    
    if not rows:
        logger.info("WEB_ENRICHMENT_SHEETS_SYNC_ROWS count=0")
        return
    
    logger.info("WEB_ENRICHMENT_SHEETS_SYNC_ROWS count=%d", len(rows))
    
    # Create sheets manager
    client = SheetsClient(config=default_sheets_config(spreadsheet_id=spreadsheet_id))
    mgr = GoogleSheetsManager(client=client)
    
    # Get master_result_id -> row index mapping
    master_result_ids = [row["master_result_id"] for row in rows]
    row_index_map = get_master_row_index_map(
        mgr=mgr,
        master_result_ids=master_result_ids
    )
    
    # Build updates for delta sync
    updates: List[Tuple[int, str, str, str, str]] = []
    
    for row in rows:
        master_result_id = str(row["master_result_id"])
        row_idx = row_index_map.get(norm(master_result_id))
        
        if not row_idx:
            logger.warning(
                "WEB_ENRICHMENT_SHEETS_SYNC_SKIP_ROW master_result_id=%s reason=not_found_in_master",
                master_result_id
            )
            continue
        
        # Transform fields
        company_name = _transform_company_name(
            row["combined_company"],
            row["organization"]
        )
        email_ids = _transform_emails(row["combined_emails"])
        contacts = _transform_contacts(row["combined_people"])
        short_desc, long_desc = _transform_descriptions(row["combined_descriptions"])
        
        # Now we map both short and long descriptions to Master sheet
        updates.append((
            row_idx,
            company_name,
            email_ids,
            contacts,
            short_desc,   # Short Company Description (Column H)
            long_desc     # Long Company Description (Column I)
        ))
    
    # Apply delta updates
    if updates:
        update_web_enrichment_columns_batch(
            client=client,
            spreadsheet_id=spreadsheet_id,
            updates=updates
        )
        logger.info("WEB_ENRICHMENT_SHEETS_SYNC_APPLIED updated_cells=%d", len(updates))
    else:
        logger.info("WEB_ENRICHMENT_SHEETS_SYNC_APPLIED updated_cells=0")
    
    logger.info("WEB_ENRICHMENT_SHEETS_SYNC_DONE")


# ============================================================================
# LEGACY FUNCTION (kept for backward compatibility)
# ============================================================================

def sync_web_enrichment_to_sheets_db_first(
    *,
    spreadsheet_id: str,
    request_id: str,
    master_result_ids: Iterable[UUID],
    repo: Any = None,
) -> None:
    """
    LEGACY: DB-first batch sync to WebEnrichment tab (append-only).
    
    This function is kept for backward compatibility but is not used
    for the new COMBINE → Master sheet sync workflow.
    
    NOTE: This is the minimal working version (append-only).
    For delta updates to Master sheet, use sync_combined_web_enrichment_to_master_sheet.
    """
    from ..web_scraper_enrichment.repos import WebScraperEnrichmentResultsRepository
    
    ids = list(master_result_ids or [])
    if not ids:
        return

    client = SheetsClient(config=default_sheets_config(spreadsheet_id=spreadsheet_id))
    _ensure_tab_and_headers(client)

    repo = repo or WebScraperEnrichmentResultsRepository()
    rows = repo.get_rows_for_sheet_sync(request_id=request_id, master_result_ids=ids)

    # Map DB -> Sheets columns
    out_rows: list[list[Any]] = []
    for r in rows:
        out_rows.append(
            [
                norm(str(r.get("master_result_id", ""))),
                norm(r.get("organization", "")),
                norm(r.get("domain", "")),
                norm(r.get("source", "")),
                norm(r.get("company_name", "")),
                _parse_primary_email(r.get("emails")),
                _parse_primary_contact_name(r.get("contacts")),
                norm(r.get("company_description", "")),
                norm(r.get("extraction_status", "")),
            ]
        )

    # Append (writes after last row)
    client.append_values(
        tab_name=TAB_WEB_ENRICHMENT_MAIN,
        rows=out_rows,
        value_input_option="RAW",
    )

    logger.info(
        "WEB_ENRICH_SHEETS_SYNC | request_id=%s | appended=%d",
        request_id,
        len(out_rows),
    )
