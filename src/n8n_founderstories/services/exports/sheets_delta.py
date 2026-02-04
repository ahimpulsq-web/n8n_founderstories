"""
Delta updates for Google Sheets - incremental updates without full rewrites.

This module provides functions for updating specific columns in existing sheets
without rewriting entire rows or sheets.
"""

from __future__ import annotations

from typing import List, Tuple, Dict, Any

from .sheets import SheetsClient
from .sheets_schema import TAB_MASTER_MAIN, TAB_MAIL_CONTENT


def update_web_enrichment_columns_batch(
    client: SheetsClient,
    spreadsheet_id: str,
    updates: List[Tuple[int, str, str, str, str, str, str, str, str, str, str, str, str]] = []
) -> None:
    """
    Update web enrichment columns (E:I) in Master_v2 for multiple rows.
    
    This function updates ALL enrichment columns:
    - Column E: Company Name
    - Column F: E-mail ID
    - Column G: Contact Names
    - Column H: Short Company Description
    - Column I: Long Company Description
    
    Args:
        client: SheetsClient instance
        spreadsheet_id: Google Sheets spreadsheet ID
        updates: List of tuples (row_index_1based, company_name, email_id, contact_names, short_description, long_description)
                 row_index must be >= 2 (row 1 is headers)
    
    Raises:
        ValueError: If any row_index < 2
    """
    if not updates:
        return
    
    # Validate all row indices
    for row_idx, _, _, _, _, _ in updates:
        if row_idx < 2:
            raise ValueError(f"row_index must be >= 2 (row 1 is headers), got {row_idx}")
    
    # Build batch update data
    data = []
    for row_idx, company_name, email_id, contact_names, short_desc, long_desc in updates:
        # Convert None to empty string
        company_name = company_name or ""
        email_id = email_id or ""
        contact_names = contact_names or ""
        short_desc = short_desc or ""
        long_desc = long_desc or ""
        
        # Update columns E:I for this row (all 5 enrichment columns)
        range_str = f"{TAB_MASTER_MAIN}!E{row_idx}:I{row_idx}"
        data.append({
            "range": range_str,
            "values": [[company_name, email_id, contact_names, short_desc, long_desc]]
        })
    
    # Execute batch update
    if data:
        client.values_batch_update(data=data, value_input_option="RAW")


def update_mail_content_columns_batch(
    client: SheetsClient,
    spreadsheet_id: str,
    updates: List[Tuple],
    formatting_requests: List[Dict[str, Any]] | None = None,
) -> None:
    """
    Update Mail Content columns in Mail_Content_v2 for multiple rows.

    Supported shapes:

    1) Legacy (B:H) - 7 values after row_idx:
       (row_idx, organisation, domain, company_name, email_id, contact_names, subject, content)

    2) New (B:L) - 11 values after row_idx:
       (row_idx,
        organisation, domain, company_name, email_id,
        test_recipient, contact_names,
        subject, content,
        mail_status, send_status, notes)

    3) New with key (A:L) - 12 values after row_idx:
       (row_idx,
        master_result_id,
        organisation, domain, company_name, email_id,
        test_recipient, contact_names,
        subject, content,
        mail_status, send_status, notes)
    """
    if not updates:
        return

    widths = {len(u) for u in updates}
    if len(widths) != 1:
        raise ValueError(f"Inconsistent update tuple sizes: {sorted(widths)}")

    tuple_len = next(iter(widths))   # includes row_idx
    payload_len = tuple_len - 1      # excludes row_idx

    # Determine target range start/end and expected payload size
    if payload_len == 7:
        # B..H
        start_col = "B"
        end_col = "H"
    elif payload_len == 11:
        # B..L
        start_col = "B"
        end_col = "L"
    elif payload_len == 12:
        # A..L
        start_col = "A"
        end_col = "L"
    else:
        raise ValueError(
            f"Unsupported mail content update width={payload_len}. "
            f"Expected 7 (B:H), 11 (B:L), or 12 (A:L)."
        )

    data = []
    for u in updates:
        row_idx = u[0]
        if row_idx < 2:
            raise ValueError(f"row_index must be >= 2 (row 1 is headers), got {row_idx}")

        values = [("" if v is None else v) for v in u[1:]]
        range_str = f"{TAB_MAIL_CONTENT}!{start_col}{row_idx}:{end_col}{row_idx}"
        data.append({"range": range_str, "values": [values]})

    client.values_batch_update(data=data, value_input_option="RAW")

    if formatting_requests:
        client.batch_update_requests(requests=formatting_requests)


__all__ = ["update_web_enrichment_columns_batch", "update_mail_content_columns_batch"]
