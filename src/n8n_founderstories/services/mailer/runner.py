from __future__ import annotations

from typing import List, Dict, Any

from n8n_founderstories.services.database.connection import get_connection_context
from n8n_founderstories.services.mailer.repos import (
    seed_mailer_outbox,
    fetch_outbox_rows_for_generation,
    update_mail_content,
)
from n8n_founderstories.services.mailer.base import generate_mail_content
from n8n_founderstories.services.exports.sync_content_to_sheet import (
    sync_mailer_generated_to_sheet,
)


def start_mailer(request_id: str) -> int:
    """
    Ensure mailer_outbox rows exist for a given request_id.
    Returns number of inserted rows (depends on seed_mailer_outbox implementation).
    """
    if not request_id or not request_id.strip():
        raise ValueError("request_id is required")

    with get_connection_context() as conn:
        return seed_mailer_outbox(conn, request_id)


def generate_mailer_content_batch(
    *,
    request_id: str,
    spreadsheet_id: str,
    batch_size: int = 20,
) -> Dict[str, Any]:
    """
    Process a single batch:
      - fetch up to batch_size rows needing generation
      - generate subject/content
      - update DB
      - sync those rows to sheet

    Returns:
      {
        "processed": int,
        "master_result_ids": list[str]
      }
    """
    if not request_id or not request_id.strip():
        raise ValueError("request_id is required")
    if not spreadsheet_id or not spreadsheet_id.strip():
        raise ValueError("spreadsheet_id is required")
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    processed_master_ids: List[str] = []
    processed = 0

    with get_connection_context() as conn:
        rows = fetch_outbox_rows_for_generation(conn, batch_size)

        for row in rows:
            # Debug (optional): uncomment if you want runner-level visibility
            # print(f"MAILER_RUNNER | generating | master_result_id={row.get('master_result_id')} | company={row.get('company_name')}")

            subject, content = generate_mail_content(row)

            update_mail_content(
                conn=conn,
                outbox_id=row["id"],
                subject=subject,
                content=content,
            )

            mid = row.get("master_result_id")
            if mid:
                processed_master_ids.append(str(mid))

            processed += 1

    # Sheets viewer update for only those processed rows
    if processed_master_ids:
        sync_mailer_generated_to_sheet(
            request_id=request_id,
            spreadsheet_id=spreadsheet_id,
            master_result_ids=processed_master_ids,
        )

    return {"processed": processed, "master_result_ids": processed_master_ids}


def generate_mailer_content_all(
    *,
    request_id: str,
    spreadsheet_id: str,
    batch_size: int = 20,
    max_total: int = 5000,
) -> Dict[str, Any]:
    """
    Keep processing batches until there are no rows left to generate.
    Syncs to Sheets after each batch.

    max_total is a safety guard against infinite loops if the fetch criteria is wrong.

    Returns:
      {
        "total_processed": int,
        "batches": int
      }
    """
    if not request_id or not request_id.strip():
        raise ValueError("request_id is required")
    if not spreadsheet_id or not spreadsheet_id.strip():
        raise ValueError("spreadsheet_id is required")
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if not isinstance(max_total, int) or max_total <= 0:
        raise ValueError("max_total must be a positive integer")

    total_processed = 0
    batches = 0

    while True:
        result = generate_mailer_content_batch(
            request_id=request_id,
            spreadsheet_id=spreadsheet_id,
            batch_size=batch_size,
        )

        processed = int(result.get("processed", 0))
        batches += 1 if processed > 0 else 0
        total_processed += processed

        # Stop when no more rows are returned by fetch_outbox_rows_for_generation
        if processed == 0:
            break

        # Safety stop (prevents infinite loops if fetch keeps returning already-processed rows)
        if total_processed >= max_total:
            break

    return {"total_processed": total_processed, "batches": batches}
