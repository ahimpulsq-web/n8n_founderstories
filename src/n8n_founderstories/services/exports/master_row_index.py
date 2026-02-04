from __future__ import annotations

from typing import Dict, Iterable
from uuid import UUID

from .sheets_manager import GoogleSheetsManager
from .sheets_schema import TAB_MASTER_MAIN
from ...core.utils.text import norm


def get_master_row_index_map(
    *,
    mgr: GoogleSheetsManager,
    master_result_ids: Iterable[UUID],
) -> Dict[str, int]:
    """
    Returns map: master_result_id (string) -> 1-based row index in Master_v2 sheet.

    Reads column A (A2:A) and builds a mapping. Safe and fast enough per batch.
    """
    wanted = {norm(str(x)) for x in master_result_ids if x}
    if not wanted:
        return {}

    # Read column A from row 2 down. "A2:A" returns only filled cells.
    col = mgr.read_range(tab_name=TAB_MASTER_MAIN, a1_range="A2:A") or []

    out: Dict[str, int] = {}
    # row_index in sheet = i + 2 (because A2 is row 2)
    for i, row in enumerate(col):
        if not row:
            continue
        key = norm(row[0])
        if key in wanted:
            out[key] = i + 2

    return out


__all__ = ["get_master_row_index_map"]
