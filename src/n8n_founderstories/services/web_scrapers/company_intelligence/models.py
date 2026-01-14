from __future__ import annotations

from typing import List, Literal, Optional, Set

from pydantic import BaseModel, EmailStr, Field, HttpUrl


DachCountry = Literal["DE", "AT", "CH"]
Lang = Literal["de", "en"]


class Contact(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    email: EmailStr
    role: str = Field(default="other", max_length=120)  # allow raw role text (e.g. "Geschäftsführer")
    title: Optional[str] = Field(default=None, max_length=120)
    source_url: Optional[HttpUrl] = None


class CompanyAbout(BaseModel):
    summary: str = Field(..., min_length=20, max_length=1200)
    industry: Optional[str] = Field(default=None, max_length=120)
    location: Optional[str] = Field(default=None, max_length=120)
    countries: Set[DachCountry] = Field(default_factory=set)
    website_url: Optional[HttpUrl] = None
    linkedin_url: Optional[HttpUrl] = None


class LLMExtractionResultModel(BaseModel):
    contacts: List[Contact] = Field(default_factory=list)
    emails: List[EmailStr] = Field(default_factory=list)
    about: CompanyAbout
    language: Lang = "de"
