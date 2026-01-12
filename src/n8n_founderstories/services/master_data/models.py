"""
Master data models for DB-first architecture.

This module defines data structures for the unified Master results table
and related entities.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class MasterRow:
    """
    Data structure for master_results database rows.
    
    This represents a unified result from any tool (Hunter, Google Maps, Google Search).
    """
    job_id: str
    request_id: str
    domain: str
    source_tool: str
    company: Optional[str] = None
    website: Optional[str] = None
    location: Optional[str] = None
    lead_query: Optional[str] = None
    dup_in_run: Optional[str] = None
    source_ref: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MasterRow":
        """
        Create MasterRow from dictionary.
        
        Args:
            data: Dictionary with master row data
            
        Returns:
            MasterRow instance
        """
        return cls(
            job_id=data.get("job_id", ""),
            request_id=data.get("request_id", ""),
            domain=data.get("domain", ""),
            source_tool=data.get("source_tool", ""),
            company=data.get("company"),
            website=data.get("website"),
            location=data.get("location"),
            lead_query=data.get("lead_query"),
            dup_in_run=data.get("dup_in_run"),
            source_ref=data.get("source_ref"),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert MasterRow to dictionary.
        
        Returns:
            Dictionary representation
        """
        return {
            "job_id": self.job_id,
            "request_id": self.request_id,
            "domain": self.domain,
            "source_tool": self.source_tool,
            "company": self.company,
            "website": self.website,
            "location": self.location,
            "lead_query": self.lead_query,
            "dup_in_run": self.dup_in_run,
            "source_ref": self.source_ref,
        }


@dataclass
class MasterWatermark:
    """
    Data structure for master_watermarks database rows.
    
    Tracks the last processed timestamp for each source tool.
    Note: last_seen_created_at is a misnomer - it actually tracks the watermark
    timestamp which can be either created_at or updated_at depending on the tool.
    """
    request_id: str
    source_tool: str
    last_seen_created_at: Optional[datetime] = None  # Generic watermark timestamp
    last_processed_count: int = 0
    total_processed: int = 0
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MasterWatermark":
        """
        Create MasterWatermark from dictionary.
        
        Args:
            data: Dictionary with watermark data
            
        Returns:
            MasterWatermark instance
        """
        last_seen = data.get("last_seen_created_at")
        if isinstance(last_seen, str):
            last_seen = datetime.fromisoformat(last_seen)
        
        return cls(
            request_id=data.get("request_id", ""),
            source_tool=data.get("source_tool", ""),
            last_seen_created_at=last_seen,
            last_processed_count=data.get("last_processed_count", 0),
            total_processed=data.get("total_processed", 0),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert MasterWatermark to dictionary.
        
        Returns:
            Dictionary representation
        """
        return {
            "request_id": self.request_id,
            "source_tool": self.source_tool,
            "last_seen_created_at": self.last_seen_created_at.isoformat() if self.last_seen_created_at else None,
            "last_processed_count": self.last_processed_count,
            "total_processed": self.total_processed,
        }


@dataclass
class MasterSource:
    """
    Data structure for master_sources database rows.
    
    Represents a registered source tool with its configuration.
    """
    source_tool: str
    display_name: Optional[str] = None
    source_table: Optional[str] = None
    is_active: bool = True
    column_mapping: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MasterSource":
        """
        Create MasterSource from dictionary.
        
        Args:
            data: Dictionary with source data
            
        Returns:
            MasterSource instance
        """
        return cls(
            source_tool=data.get("source_tool", ""),
            display_name=data.get("display_name"),
            source_table=data.get("source_table"),
            is_active=data.get("is_active", True),
            column_mapping=data.get("column_mapping"),
            description=data.get("description"),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert MasterSource to dictionary.
        
        Returns:
            Dictionary representation
        """
        return {
            "source_tool": self.source_tool,
            "display_name": self.display_name,
            "source_table": self.source_table,
            "is_active": self.is_active,
            "column_mapping": self.column_mapping,
            "description": self.description,
        }