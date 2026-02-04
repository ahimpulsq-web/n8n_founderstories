from __future__ import annotations

import json
import logging
from difflib import SequenceMatcher
from typing import Any, Dict, List, Tuple

from .master_row_index import get_master_row_index_map
from .sheets import SheetsClient, default_sheets_config
from .sheets_delta import update_mail_content_columns_batch
from .sheets_manager import GoogleSheetsManager
from .sheets_schema import TAB_MAIL_CONTENT, HEADERS_MAIL_CONTENT
from ...core.utils.text import norm
from ..database.connection import get_connection_context
from ...core.config import settings

logger = logging.getLogger(__name__)

# Track which tabs have been formatted to avoid redundant formatting (same pattern as sheets_exporter.py)
_FORMATTED_TABS: set[str] = set()

TEST_RECIPIENT_EMAIL = "abhishekhosamath14@gmail.com"
ORG_COMPANY_MATCH_THRESHOLD = 0.75  # tweak if needed

# Expected Mail Content schema (A-L = 12 columns):
# A  master_result_id (hidden)
# B  Organisation
# C  Domain
# D  Company Name
# E  E-mail ID
# F  Test Recipient
# G  Contact Names
# H  Subject
# I  Content
# J  Mail Status
# K  Send Status
# L  Notes


def _format_guard_key(spreadsheet_id: str, tab_name: str) -> str:
    return f"{norm(spreadsheet_id)}::{norm(tab_name)}"


def _col_index_to_letter(col_index: int) -> str:
    """Convert 0-based column index to Excel-style letter (A, B, ..., Z, AA, AB, ...)."""
    n = col_index + 1
    letters: list[str] = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


def _string_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _is_green(color: Dict[str, Any] | None) -> bool:
    """
    Approx check for "green" from our own palette. Use only if you later want to infer colors.
    Not used right now (we compute greens from confidence/similarity).
    """
    if not color:
        return False
    return (
        float(color.get("red", 0.0)) == 0.7
        and float(color.get("green", 0.0)) == 1.0
        and float(color.get("blue", 0.0)) == 0.7
    )


def _green_color() -> Dict[str, Any]:
    return {"red": 0.7, "green": 1.0, "blue": 0.7}


def _get_confidence_color(confidence: float) -> Dict[str, Any]:
    if confidence > 0.8:
        return _green_color()
    if confidence >= 0.5:
        return {"red": 1.0, "green": 1.0, "blue": 0.7}  # yellow
    return {"red": 1.0, "green": 0.7, "blue": 0.7}  # red

def _get_mail_status_color(status: str) -> Dict[str, Any]:
    s = (status or "").strip().upper()
    if s == "OK":
        return {"red": 0.7, "green": 1.0, "blue": 0.7}   # green
    if s == "CHECK":
        return {"red": 1.0, "green": 1.0, "blue": 0.7}   # yellow
    return {"red": 1.0, "green": 0.7, "blue": 0.7}       # red (optional fallback)

def ensure_mail_status_conditional_formatting_once(client: SheetsClient) -> None:
    """
    Adds conditional formatting rules for Mail Status column (J) in TAB_MAIL_CONTENT:
    - "OK"    => green
    - "CHECK" => yellow

    Applies to J2:J1000.
    Runs only once per spreadsheet+tab (guarded).
    """
    spreadsheet_id = client._spreadsheet_id
    key = _format_guard_key(spreadsheet_id, f"{TAB_MAIL_CONTENT}::mail_status_cf")
    if key in _FORMATTED_TABS:
        return

    sheet_id = client.get_sheet_id(TAB_MAIL_CONTENT)
    if sheet_id is None:
        logger.warning("MAIL_STATUS_CF_SKIP | sheet_not_found")
        return

    # Column J is 0-based index 9. Range J2:J1000 => rows 1..1000 exclusive in API indices.
    # startRowIndex is 1 (row 2), endRowIndex 1000
    target_range = {
        "sheetId": sheet_id,
        "startRowIndex": 1,
        "endRowIndex": 1000,
        "startColumnIndex": 9,   # J
        "endColumnIndex": 10,    # J only
    }

    green = {"red": 0.7, "green": 1.0, "blue": 0.7}
    yellow = {"red": 1.0, "green": 1.0, "blue": 0.7}

    requests: list[dict] = []

    # Rule 1: Text exactly "OK" => green
    requests.append(
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [target_range],
                    "booleanRule": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": [{"userEnteredValue": "OK"}],
                        },
                        "format": {
                            "backgroundColor": green,
                        },
                    },
                },
                "index": 0,
            }
        }
    )

    # Rule 2: Text exactly "CHECK" => yellow
    requests.append(
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [target_range],
                    "booleanRule": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": [{"userEnteredValue": "CHECK"}],
                        },
                        "format": {
                            "backgroundColor": yellow,
                        },
                    },
                },
                "index": 0,
            }
        }
    )

    try:
        client._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

        _FORMATTED_TABS.add(key)
        logger.info("MAIL_STATUS_CF_SUCCESS | rules_added=%d", len(requests))
    except Exception as e:
        logger.warning("MAIL_STATUS_CF_FAILED | error=%s", e)



def format_mail_content_tab_once(client: SheetsClient) -> None:
    """
    One-time formatting for Mail Content tab using the SAME style as Master/Hunter formatting.

    Specs:
    - Columns A-L (12 columns total), A hidden
    - Column widths:
        A: hidden
        B: 320 (Organisation)
        C: 320 (Domain)
        D: 320 (Company Name)
        E: 320 (E-mail ID)
        F: 320 (Test Recipient)
        G: 320 (Contact Names)
        H: 500 (Subject)
        I: 1000 (Content)
        J: 120 (Mail Status)
        K: 120 (Send Status)
        L: 300 (Notes)
    - Row heights: Header 35px, Data rows 50px
    - Header: Bold, Grey (#F1F3F4), Font size 10, centered
    - Wrap: Contact Names (G), Subject (H), Content (I)
    """
    spreadsheet_id = client._spreadsheet_id
    key = _format_guard_key(spreadsheet_id, TAB_MAIL_CONTENT)
    if key in _FORMATTED_TABS:
        return

    try:
        sheet_id = client.get_sheet_id(TAB_MAIL_CONTENT)
        if sheet_id is None:
            logger.warning("MAIL_CONTENT_FORMAT_SKIP | sheet_not_found")
            return

        requests: list[dict] = []

        # A-L widths (12 cols)
        column_widths = [
            0,    # A (hidden)
            320,  # B
            320,  # C
            320,  # D
            320,  # E
            320,  # F
            320,  # G
            500,  # H
            1000, # I
            120,  # J
            120,  # K
            300,  # L
        ]
        for col_idx, width in enumerate(column_widths):
            if width > 0:
                requests.append(
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": col_idx,
                                "endIndex": col_idx + 1,
                            },
                            "properties": {"pixelSize": width},
                            "fields": "pixelSize",
                        }
                    }
                )

        # Hide column A
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": 1,
                    },
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser",
                }
            }
        )

        # Header row height
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": 1,
                    },
                    "properties": {"pixelSize": 35},
                    "fields": "pixelSize",
                }
            }
        )

        # Data rows height (rows 2-1000)
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 1,
                        "endIndex": 1000,
                    },
                    "properties": {"pixelSize": 50},
                    "fields": "pixelSize",
                }
            }
        )

        # Header formatting
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(HEADERS_MAIL_CONTENT),
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.945, "green": 0.953, "blue": 0.957},
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "textFormat": {"fontSize": 10, "bold": True},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)",
                }
            }
        )

        # Wrap Contact Names (G), Subject (H), Content (I): indices 6..8 inclusive => end exclusive = 9
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 1000,
                        "startColumnIndex": 6,  # G
                        "endColumnIndex": 9,    # up to I (exclusive)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "WRAP",
                            "verticalAlignment": "BOTTOM",
                        }
                    },
                    "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)",
                }
            }
        )

        client._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

        _FORMATTED_TABS.add(key)
        logger.info("MAIL_CONTENT_FORMAT_SUCCESS | requests=%d", len(requests))

    except Exception as e:
        logger.warning("MAIL_CONTENT_FORMAT_FAILED | error=%s", e)


def _ensure_tab_and_headers(client: SheetsClient, spreadsheet_id: str) -> None:
    # 1) Ensure tab exists
    client.ensure_tab(TAB_MAIL_CONTENT)

    # 2) Trim columns to A-L (12 cols) if needed
    meta = client.get_spreadsheet_metadata()
    sheet_id = None
    col_count = None
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == TAB_MAIL_CONTENT:
            sheet_id = props.get("sheetId")
            col_count = (props.get("gridProperties") or {}).get("columnCount")
            break

    if sheet_id is None:
        raise RuntimeError(f"Failed to find sheetId for tab '{TAB_MAIL_CONTENT}'")

    TARGET_COLS = 12
    if isinstance(col_count, int) and col_count > TARGET_COLS:
        client._service.spreadsheets().batchUpdate(
            spreadsheetId=client._spreadsheet_id,
            body={
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": TARGET_COLS,  # delete from M onwards
                                "endIndex": col_count,
                            }
                        }
                    }
                ]
            },
        ).execute()

    # 3) Write headers once (A1:..)
    end_col = _col_index_to_letter(len(HEADERS_MAIL_CONTENT) - 1)  # should be L for 12 cols
    rng = f"{TAB_MAIL_CONTENT}!A1:{end_col}1"
    client.values_batch_update(
        data=[{"range": rng, "values": [HEADERS_MAIL_CONTENT]}],
        value_input_option="RAW",
    )

    # 4) Apply one-time formatting
    format_mail_content_tab_once(client)
    ensure_mail_status_conditional_formatting_once(client)



def _get_highest_confidence_item(items_list: List[Dict[str, Any]], key: str = "name") -> Tuple[str, float]:
    if not items_list:
        return ("", 0.0)

    sorted_items = sorted(items_list, key=lambda x: x.get("confidence", 0.0), reverse=True)
    best = sorted_items[0]
    return (best.get(key, "").strip(), float(best.get("confidence", 0.0) or 0.0))


def _transform_company_name_with_confidence(combined_company: Any) -> Tuple[str, float]:
    if not combined_company:
        return ("", 0.0)

    try:
        data = json.loads(combined_company) if isinstance(combined_company, str) else combined_company

        if isinstance(data, dict):
            return (data.get("name", "").strip(), float(data.get("confidence", 0.0) or 0.0))

        if isinstance(data, list):
            return _get_highest_confidence_item(data, key="name")

        return ("", 0.0)
    except Exception:
        return ("", 0.0)


def _transform_email_with_confidence(combined_emails: Any) -> Tuple[str, float]:
    if not combined_emails:
        return ("", 0.0)

    try:
        data = json.loads(combined_emails) if isinstance(combined_emails, str) else combined_emails
        if not isinstance(data, list):
            return ("", 0.0)
        return _get_highest_confidence_item(data, key="email")
    except Exception:
        return ("", 0.0)


def _transform_contacts(combined_people: Any) -> str:
    if not combined_people:
        return ""

    try:
        data = json.loads(combined_people) if isinstance(combined_people, str) else combined_people
        if not isinstance(data, list):
            return ""

        out: list[str] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            name = (item.get("name") or "").strip()
            role = (item.get("role") or "").strip() if item.get("role") else ""
            if not name:
                continue
            out.append(f"{name} ({role})" if role else name)
        return ", ".join(out)
    except Exception:
        return ""


def _get_combined_results_for_mail_content(*, request_id: str, dsn: str | None = None) -> List[Dict[str, Any]]:
    dsn = dsn or settings.postgres_dsn

    sql = """
    SELECT
        master_result_id,
        organization,
        domain,
        combined_company,
        combined_emails,
        combined_people
    FROM web_scraper_enrichment_results
    WHERE request_id = %s
      AND combine_status IN ('ok', 'partial')
    ORDER BY master_result_id;
    """

    with get_connection_context(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (request_id,))
            rows = cur.fetchall()

    return [
        {
            "master_result_id": row[0],
            "organization": row[1],
            "domain": row[2],
            "combined_company": row[3],
            "combined_emails": row[4],
            "combined_people": row[5],
        }
        for row in rows
    ]


def sync_combined_to_mail_content_sheet(*, request_id: str, spreadsheet_id: str | None = None) -> None:
    logger.info("EMAIL_SHEETS_SYNC_START request_id=%s", request_id)

    if not spreadsheet_id:
        spreadsheet_id = getattr(settings, "google_sheets_spreadsheet_id", None)
        if not spreadsheet_id:
            logger.error("EMAIL_SHEETS_SYNC_ERROR error=no_spreadsheet_id")
            raise ValueError("spreadsheet_id not provided and not found in settings")

    rows = _get_combined_results_for_mail_content(request_id=request_id)
    if not rows:
        logger.info("EMAIL_SHEETS_SYNC_ROWS count=0")
        return

    client = SheetsClient(config=default_sheets_config(spreadsheet_id=spreadsheet_id))
    mgr = GoogleSheetsManager(client=client)

    # Ensure tab + headers + formatting
    _ensure_tab_and_headers(client, spreadsheet_id)

    master_result_ids = [row["master_result_id"] for row in rows]
    row_index_map = get_master_row_index_map(mgr=mgr, master_result_ids=master_result_ids)

    # Tuple structure expected by update_mail_content_columns_batch:
    # (row_idx, B, C, D, E, F, G, H, I, J, K, L)
    updates: List[Tuple[int, str, str, str, str, str, str, str, str, str, str, str]] = []

    # Keep per-row confidences + org/company similarity decision for formatting
    formatting_data: List[Tuple[int, float, float, bool, str]] = [] # (row_idx, company_conf, email_conf, company_is_good)

    for row in rows:
        master_result_id = str(row["master_result_id"])
        row_idx = row_index_map.get(norm(master_result_id))
        if not row_idx:
            logger.warning(
                "EMAIL_SHEETS_SYNC_SKIP_ROW master_result_id=%s reason=not_found_in_master",
                master_result_id,
            )
            continue

        organisation = norm(row["organization"] or "")
        domain = norm(row["domain"] or "")

        company_name, company_conf = _transform_company_name_with_confidence(row["combined_company"])
        email_id, email_conf = _transform_email_with_confidence(row["combined_emails"])
        contacts = _transform_contacts(row["combined_people"])

        # Test Recipient: always set if Email ID present
        test_recipient = TEST_RECIPIENT_EMAIL if email_id else ""

        # Similarity-based "green" override for company name
        similarity = _string_similarity(organisation, company_name)
        company_is_good = (company_conf > 0.8) or (similarity >= ORG_COMPANY_MATCH_THRESHOLD)
        email_is_good = (email_conf > 0.8)

        # Mail Status rule: if BOTH green => "0"
        if not email_id:
            mail_status = ""
        elif company_is_good and email_is_good:
            mail_status = "OK"
        else:
            mail_status = "CHECK"


        # You said these exist after Content:
        send_status = ""  # leave empty for now
        notes = ""        # leave empty for now

        subject = ""      # leave empty for now
        content = ""      # leave empty for now

        updates.append(
            (
                row_idx,
                norm(master_result_id),  # A (hidden master_result_id)
                organisation,   # B
                domain,         # C
                norm(company_name),  # D
                norm(email_id),      # E
                norm(test_recipient),# F
                norm(contacts),      # G
                subject,             # H
                content,             # I
                mail_status,         # J
                send_status,         # K
                notes,               # L
            )
        )
        formatting_data.append((row_idx, company_conf, email_conf, company_is_good, mail_status))

    if not updates:
        logger.info("EMAIL_SHEETS_SYNC_APPLIED updated_cells=0")
        logger.info("EMAIL_SHEETS_SYNC_DONE")
        return

    # Get sheet_id for formatting
    meta = client.get_spreadsheet_metadata()
    sheet_id = None
    for sheet in meta.get("sheets", []):
        if sheet.get("properties", {}).get("title") == TAB_MAIL_CONTENT:
            sheet_id = sheet.get("properties", {}).get("sheetId")
            break

    # Formatting:
    # - Company Name (D, idx 3): green if company_is_good else by confidence
    # - Email ID (E, idx 4): by confidence
    formatting_requests: list[dict] = []
    if sheet_id is not None:
        for row_idx, company_conf, email_conf, company_is_good, mail_status in formatting_data:
            company_color = _green_color() if company_is_good else _get_confidence_color(company_conf)
            email_color = _get_confidence_color(email_conf)

            # Company Name cell (D = index 3)
            formatting_requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_idx - 1,
                            "endRowIndex": row_idx,
                            "startColumnIndex": 3,
                            "endColumnIndex": 4,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": company_color}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                }
            )

            # Email ID cell (E = index 4)
            formatting_requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_idx - 1,
                            "endRowIndex": row_idx,
                            "startColumnIndex": 4,
                            "endColumnIndex": 5,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": email_color}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                }
            )

    update_mail_content_columns_batch(
        client=client,
        spreadsheet_id=spreadsheet_id,
        updates=updates,
        formatting_requests=formatting_requests or None,
    )

    logger.info("EMAIL_SHEETS_SYNC_APPLIED updated_rows=%d", len(updates))
    logger.info("EMAIL_SHEETS_SYNC_DONE")


__all__ = ["sync_combined_to_mail_content_sheet"]
