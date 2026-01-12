from __future__ import annotations

from pydantic import BaseModel, Field


class EmailExtractorRequest(BaseModel):
    search_plan: str = Field(..., description="Search plan identifier/name.")
    spreadsheet_id: str = Field(..., description="Google Spreadsheet ID.")
    sheet_title: str = Field(default="Master", description="Sheet tab title.")
