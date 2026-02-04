from __future__ import annotations

from typing import List, Literal
from pydantic import BaseModel, EmailStr


class EmailOccurrence(BaseModel):
    email: EmailStr
    source: Literal["deterministic", "llm"]
    page_category: str
    domain_score: int  # 0, 1, 2
    source_url: str
    order_index: int


class CombinedEmail(BaseModel):
    email: EmailStr
    frequency: int
    source_agreement: Literal["deterministic", "llm", "both"]
    confidence: float
    sources: List[dict]

class CompanyOccurrence(BaseModel):
    name: str
    source_url: str


class CombinedCompany(BaseModel):
    name: str
    frequency: int
    confidence: float
    sources: List[str]

from typing import List, Optional
from pydantic import BaseModel


class CombinedDescription(BaseModel):
    kind: str  # "short" | "long"
    text: str
    source_url: str


class CombinedPerson(BaseModel):
    name: str
    role: Optional[str]
    sources: List[str]

