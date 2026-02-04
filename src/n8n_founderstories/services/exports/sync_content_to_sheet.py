from __future__ import annotations

import logging
from typing import List, Dict, Any, Tuple

from .master_row_index import get_master_row_index_map
from .sheets import SheetsClient, default_sheets_config
from .sheets_manager import GoogleSheetsManager
from .sheets_schema import TAB_MAIL_CONTENT
from ...core.utils.text import norm
from ..database.connection import get_connection_context
from ...core.config import settings

logger = logging.getLogger(__name__)

def _fetch_mailer_subject_content(
    *, master_result_ids: List[str], dsn: str | None = None
) -> List[Dict[str, Any]]:
    dsn = dsn or settings.postgres_dsn

    sql = """
    SELECT master_result_id, subject, content
    FROM mailer_outbox
    WHERE master_result_id = ANY(%s)
    """

    with get_connection_context(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (master_result_ids,))
            rows = cur.fetchall()

    return [{"master_result_id": r[0], "subject": r[1], "content": r[2]} for r in rows]


def sync_mailer_generated_to_sheet(
    *, request_id: str, spreadsheet_id: str, master_result_ids: List[str]
) -> None:
    print("Started sheets sync")
    if not master_result_ids:
        return

    client = SheetsClient(config=default_sheets_config(spreadsheet_id=spreadsheet_id))
    mgr = GoogleSheetsManager(client=client)

    # Map master_result_id -> row index in the sheet (based on Master tab)
    row_index_map = get_master_row_index_map(mgr=mgr, master_result_ids=master_result_ids)

    db_rows = _fetch_mailer_subject_content(master_result_ids=master_result_ids)

    data = []
    updated = 0

    for r in db_rows:
        mid = norm(str(r["master_result_id"]))
        row_idx = row_index_map.get(mid)
        if not row_idx:
            continue

        subject = (r["subject"] or "").strip()
        content = (r["content"] or "").strip()

        # Update only H:I for the row
        range_str = f"{TAB_MAIL_CONTENT}!H{row_idx}:I{row_idx}"
        data.append({"range": range_str, "values": [[subject, content]]})
        updated += 1

    if data:
        client.values_batch_update(data=data, value_input_option="RAW")

    logger.info(
        "MAILER_SHEETS_SYNC_DONE | request_id=%s | input=%d | updated=%d",
        request_id,
        len(master_result_ids),
        updated,
    )
