"""
Google Maps PostgreSQL repositories.

This module provides safe database operations for Google Maps results and enriched data
with proper error handling and transaction management.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ....core.utils.text import norm
from ...database.connection import get_connection_context, DatabaseConnectionError
from ...database.config import db_config
from ....core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class GoogleMapsResultRow:
    """
    Data structure for Google Maps results database rows.
    
    This matches the structure expected by the PostgreSQL google_maps_results table.
    Updated for DB-first architecture with unified schema.
    """
    job_id: str
    request_id: str
    place_id: Optional[str] = None
    organisation: Optional[str] = None  # Renamed from 'name'
    address: Optional[str] = None
    type: Optional[str] = None  # Renamed from 'category'
    lat: Optional[float] = None
    lng: Optional[float] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    search_query: Optional[str] = None  # Renamed from 'query_text'
    intended_location: Optional[str] = None
    source: Optional[str] = None
    raw_json: Optional[Dict[str, Any]] = None
    location_label: Optional[str] = None
    domain: Optional[str] = None
    business_status: Optional[str] = None
    google_maps_url: Optional[str] = None
    country: Optional[str] = None  # ISO2 country code
    
    @classmethod
    def from_runner_data(
        cls,
        job_id: str,
        request_id: str,
        place_id: Optional[str] = None,
        organisation: Optional[str] = None,
        address: Optional[str] = None,
        type: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        phone: Optional[str] = None,
        website: Optional[str] = None,
        search_query: Optional[str] = None,
        intended_location: Optional[str] = None,
        source: Optional[str] = None,
        raw_json: Optional[Dict[str, Any]] = None,
        location_label: Optional[str] = None,
        domain: Optional[str] = None,
        business_status: Optional[str] = None,
        google_maps_url: Optional[str] = None,
        country: Optional[str] = None,
        # Backward compatibility aliases
        name: Optional[str] = None,
        category: Optional[str] = None,
        query_text: Optional[str] = None,
    ) -> "GoogleMapsResultRow":
        """
        Create GoogleMapsResultRow from runner data.
        
        Args:
            job_id: Job identifier
            request_id: Request identifier
            place_id: Google Place ID
            organisation: Place name (renamed from 'name')
            address: Formatted address
            type: Primary category/type (renamed from 'category')
            lat: Latitude
            lng: Longitude
            phone: Phone number
            website: Website URL
            search_query: The actual maps query used (renamed from 'query_text')
            intended_location: Location bucket used (e.g., "Berlin" / "DE")
            source: Source identifier (default: "google_maps")
            raw_json: Raw JSON data from API
            location_label: Location label from search plan
            domain: Extracted domain from website
            business_status: Business status from Google
            google_maps_url: Google Maps URL
            country: ISO2 country code
            name: (deprecated) Use organisation instead
            category: (deprecated) Use type instead
            query_text: (deprecated) Use search_query instead
            
        Returns:
            GoogleMapsResultRow instance with normalized data
        """
        # Handle backward compatibility
        final_organisation = norm(organisation) or norm(name) or None
        final_type = norm(type) or norm(category) or None
        final_search_query = norm(search_query) or norm(query_text) or None
        
        return cls(
            job_id=norm(job_id),
            request_id=norm(request_id),
            place_id=norm(place_id) or None,
            organisation=final_organisation,
            address=norm(address) or None,
            type=final_type,
            lat=lat,
            lng=lng,
            phone=norm(phone) or None,
            website=norm(website) or None,
            search_query=final_search_query,
            intended_location=norm(intended_location) or None,
            source=norm(source) or "google_maps",
            raw_json=raw_json,
            location_label=norm(location_label) or None,
            domain=norm(domain) or None,
            business_status=norm(business_status) or None,
            google_maps_url=norm(google_maps_url) or None,
            country=norm(country) or None,
        )


@dataclass
class GoogleMapsEnrichedRow:
    """
    Data structure for Google Maps enriched database rows.
    
    This matches the structure expected by the PostgreSQL google_maps_enriched table.
    """
    job_id: str
    request_id: str
    place_id: str
    opening_hours: Optional[Dict[str, Any]] = None
    rating: Optional[float] = None
    reviews_count: Optional[int] = None
    photos_count: Optional[int] = None
    raw_json: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_runner_data(
        cls,
        job_id: str,
        request_id: str,
        place_id: str,
        opening_hours: Optional[Dict[str, Any]] = None,
        rating: Optional[float] = None,
        reviews_count: Optional[int] = None,
        photos_count: Optional[int] = None,
        raw_json: Optional[Dict[str, Any]] = None,
    ) -> "GoogleMapsEnrichedRow":
        """
        Create GoogleMapsEnrichedRow from runner data.
        
        Args:
            job_id: Job identifier
            request_id: Request identifier
            place_id: Google Place ID (required)
            opening_hours: Opening hours data
            rating: Place rating
            reviews_count: Number of reviews
            photos_count: Number of photos
            raw_json: Raw JSON data from API
            
        Returns:
            GoogleMapsEnrichedRow instance with normalized data
        """
        return cls(
            job_id=norm(job_id),
            request_id=norm(request_id),
            place_id=norm(place_id),
            opening_hours=opening_hours,
            rating=rating,
            reviews_count=reviews_count,
            photos_count=photos_count,
            raw_json=raw_json,
        )


@dataclass
class GoogleMapsEnrichQueueRow:
    """
    Data structure for Google Maps enrichment queue database rows.
    
    This matches the structure expected by the PostgreSQL gmaps_enrich_queue table.
    Updated with retry gating and locking fields for proper queue state machine.
    """
    job_id: str
    request_id: str
    place_id: str
    iso2: Optional[str] = None
    hl: Optional[str] = None
    state: str = "PENDING"
    attempts: int = 0
    last_error: Optional[str] = None
    next_retry_at: Optional[str] = None  # ISO timestamp for retry gating
    locked_at: Optional[str] = None  # ISO timestamp when claimed
    locked_by: Optional[str] = None  # Worker identifier (optional)
    
    @classmethod
    def from_runner_data(
        cls,
        job_id: str,
        request_id: str,
        place_id: str,
        iso2: Optional[str] = None,
        hl: Optional[str] = None,
    ) -> "GoogleMapsEnrichQueueRow":
        """
        Create GoogleMapsEnrichQueueRow from runner data.
        
        CRITICAL: place_id must be the canonical value from Google API.
        Do NOT lowercase or otherwise mutate it - place IDs are opaque identifiers.
        
        Args:
            job_id: Job identifier
            request_id: Request identifier
            place_id: Google Place ID (canonical, immutable)
            iso2: Country code for region parameter
            hl: Language code for language parameter
            
        Returns:
            GoogleMapsEnrichQueueRow instance with canonical place_id
        """
        # CRITICAL: Only normalize whitespace, do NOT lowercase place_id
        canonical_place_id = norm(place_id)
        if not canonical_place_id:
            raise ValueError("place_id must not be empty for queue row")
        
        return cls(
            job_id=norm(job_id),
            request_id=norm(request_id),
            place_id=canonical_place_id,  # Store canonical value
            iso2=norm(iso2) or None,
            hl=norm(hl) or None,
            state="PENDING",
            attempts=0,
            last_error=None,
            next_retry_at=None,
            locked_at=None,
            locked_by=None,
        )


class GoogleMapsResultsRepository:
    """
    Repository for Google Maps results database operations.
    
    Provides safe database operations with proper error handling
    and transaction management using psycopg v3.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize repository with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
    
    def insert_many(self, rows: List[GoogleMapsResultRow]) -> tuple[bool, Optional[str], int]:
        """
        Insert multiple Google Maps result rows in a single transaction.
        
        Args:
            rows: List of GoogleMapsResultRow instances to insert
            
        Returns:
            Tuple of (success, error_message, inserted_count)
        """
        if not rows:
            return True, None, 0
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", 0
        
        # Filter out rows with null/empty place_id (required by NOT NULL constraint)
        valid_rows = [row for row in rows if row.place_id and row.place_id.strip()]
        if not valid_rows:
            logger.warning("All rows filtered out due to missing place_id")
            return True, None, 0
        
        insert_sql = """
        INSERT INTO google_maps_results (
            id, job_id, request_id, place_id, organisation, address, type,
            lat, lng, phone, website, search_query, intended_location,
            source, raw_json, location_label, domain, business_status, google_maps_url, country
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ON CONSTRAINT uq_google_maps_results_place_job_request DO NOTHING
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Prepare batch data (only valid rows)
                    batch_data = []
                    for row in valid_rows:
                        batch_data.append((
                            str(uuid.uuid4()),  # Generate UUID for each row
                            row.job_id,
                            row.request_id,
                            row.place_id,
                            row.organisation,
                            row.address,
                            row.type,
                            row.lat,
                            row.lng,
                            row.phone,
                            row.website,
                            row.search_query,
                            row.intended_location,
                            row.source,
                            json.dumps(row.raw_json) if row.raw_json else None,
                            row.location_label,
                            row.domain,
                            row.business_status,
                            row.google_maps_url,
                            row.country,
                        ))
                    
                    # Execute batch insert
                    cur.executemany(insert_sql, batch_data)
                    conn.commit()
                    
                    inserted_count = len(batch_data)
                    logger.debug(
                        "Successfully inserted %d Google Maps results to PostgreSQL",
                        inserted_count
                    )
                    return True, None, inserted_count
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Failed to insert Google Maps results: {e}"
            logger.error(error_msg)
            return False, error_msg, 0
    
    def get_results_by_job(self, job_id: str) -> tuple[bool, Optional[str], List[dict]]:
        """
        Retrieve Google Maps results for a specific job ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Tuple of (success, error_message, results_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        select_sql = """
        SELECT id, created_at, job_id, request_id, place_id, organisation, address, type,
               lat, lng, phone, website, search_query, intended_location, source, raw_json,
               location_label, domain, business_status, google_maps_url, country
        FROM google_maps_results
        WHERE job_id = %s
        ORDER BY created_at, place_id, id
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (job_id,))
                    rows = cur.fetchall()
                    
                    results = []
                    for row in rows:
                        results.append({
                            'id': str(row[0]),
                            'created_at': row[1].isoformat() if row[1] else None,
                            'job_id': row[2],
                            'request_id': row[3],
                            'place_id': row[4],
                            'organisation': row[5],
                            'address': row[6],
                            'type': row[7],
                            'lat': row[8],
                            'lng': row[9],
                            'phone': row[10],
                            'website': row[11],
                            'search_query': row[12],
                            'intended_location': row[13],
                            'source': row[14],
                            'raw_json': row[15],  # Already parsed as dict by psycopg
                            'location_label': row[16],
                            'domain': row[17],
                            'business_status': row[18],
                            'google_maps_url': row[19],
                            'country': row[20],
                        })
                    
                    return True, None, results
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to retrieve Google Maps results: {e}"
            logger.error(error_msg)
            return False, error_msg, []
    
    def get_results_by_request(self, request_id: str) -> tuple[bool, Optional[str], List[dict]]:
        """
        Retrieve Google Maps results for a specific request ID.
        
        Args:
            request_id: Request identifier
            
        Returns:
            Tuple of (success, error_message, results_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        select_sql = """
        SELECT id, created_at, job_id, request_id, place_id, organisation, address, type,
               lat, lng, phone, website, search_query, intended_location, source, raw_json,
               location_label, domain, business_status, google_maps_url, country
        FROM google_maps_results
        WHERE request_id = %s
        ORDER BY created_at, place_id, id
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (request_id,))
                    rows = cur.fetchall()
                    
                    results = []
                    for row in rows:
                        results.append({
                            'id': str(row[0]),
                            'created_at': row[1].isoformat() if row[1] else None,
                            'job_id': row[2],
                            'request_id': row[3],
                            'place_id': row[4],
                            'organisation': row[5],
                            'address': row[6],
                            'type': row[7],
                            'lat': row[8],
                            'lng': row[9],
                            'phone': row[10],
                            'website': row[11],
                            'search_query': row[12],
                            'intended_location': row[13],
                            'source': row[14],
                            'raw_json': row[15],  # Already parsed as dict by psycopg
                            'location_label': row[16],
                            'domain': row[17],
                            'business_status': row[18],
                            'google_maps_url': row[19],
                            'country': row[20],
                        })
                    
                    return True, None, results
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to retrieve Google Maps results: {e}"
            logger.error(error_msg)
            return False, error_msg, []
    
    def update_contact_fields(
        self,
        job_id: str,
        request_id: str,
        place_id: str,
        website: Optional[str] = None,
        domain: Optional[str] = None,
        phone: Optional[str] = None,
        google_maps_url: Optional[str] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        Update contact fields for a specific place in gmaps_results.
        
        This is used during enrichment to update website, domain, phone, and google_maps_url
        fields after fetching place details.
        
        Args:
            job_id: Job identifier
            request_id: Request identifier
            place_id: Google Place ID
            website: Website URL
            domain: Extracted domain from website
            phone: Phone number
            google_maps_url: Google Maps URL
            
        Returns:
            Tuple of (success, error_message)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured"
        
        update_sql = """
        UPDATE google_maps_results
        SET
            website = COALESCE(%s, website),
            domain = COALESCE(%s, domain),
            phone = COALESCE(%s, phone),
            google_maps_url = COALESCE(%s, google_maps_url)
        WHERE request_id = %s
        AND place_id = %s
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        update_sql,
                        (
                            norm(website) or None,
                            norm(domain) or None,
                            norm(phone) or None,
                            norm(google_maps_url) or None,
                            request_id,
                            place_id,
                        )
                    )
                    conn.commit()
                    
                    rows_updated = cur.rowcount
                    logger.debug(
                        "Updated contact fields for place_id=%s (rows=%d)",
                        place_id,
                        rows_updated
                    )
                    return True, None
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Failed to update contact fields: {e}"
            logger.error(error_msg)
            return False, error_msg


class GoogleMapsEnrichedRepository:
    """
    Repository for Google Maps enriched database operations.
    
    Provides safe database operations with proper error handling
    and transaction management using psycopg v3.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize repository with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
    
    def insert_many(self, rows: List[GoogleMapsEnrichedRow]) -> tuple[bool, Optional[str], int]:
        """
        Insert multiple Google Maps enriched rows in a single transaction.
        
        Args:
            rows: List of GoogleMapsEnrichedRow instances to insert
            
        Returns:
            Tuple of (success, error_message, inserted_count)
        """
        if not rows:
            return True, None, 0
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", 0
        
        insert_sql = """
        INSERT INTO google_maps_enriched (
            id, job_id, request_id, place_id, opening_hours, rating,
            reviews_count, photos_count, raw_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (place_id, job_id, request_id) DO NOTHING
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Prepare batch data
                    batch_data = []
                    for row in rows:
                        batch_data.append((
                            str(uuid.uuid4()),  # Generate UUID for each row
                            row.job_id,
                            row.request_id,
                            row.place_id,
                            json.dumps(row.opening_hours) if row.opening_hours else None,
                            row.rating,
                            row.reviews_count,
                            row.photos_count,
                            json.dumps(row.raw_json) if row.raw_json else None,
                        ))
                    
                    # Execute batch insert
                    cur.executemany(insert_sql, batch_data)
                    conn.commit()
                    
                    inserted_count = len(batch_data)
                    logger.debug(
                        "Successfully inserted %d Google Maps enriched records to PostgreSQL",
                        inserted_count
                    )
                    return True, None, inserted_count
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Failed to insert Google Maps enriched records: {e}"
            logger.error(error_msg)
            return False, error_msg, 0
    
    def get_enriched_by_job(self, job_id: str) -> tuple[bool, Optional[str], List[dict]]:
        """
        Retrieve enriched records for a specific job ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Tuple of (success, error_message, enriched_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        select_sql = """
        SELECT id, created_at, job_id, request_id, place_id, opening_hours,
               rating, reviews_count, photos_count, raw_json
        FROM google_maps_enriched
        WHERE job_id = %s
        ORDER BY created_at, place_id, id
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (job_id,))
                    rows = cur.fetchall()
                    
                    enriched_records = []
                    for row in rows:
                        enriched_records.append({
                            'id': str(row[0]),
                            'created_at': row[1].isoformat() if row[1] else None,
                            'job_id': row[2],
                            'request_id': row[3],
                            'place_id': row[4],
                            'opening_hours': row[5],  # Already parsed as dict by psycopg
                            'rating': row[6],
                            'reviews_count': row[7],
                            'photos_count': row[8],
                            'raw_json': row[9],  # Already parsed as dict by psycopg
                        })
                    
                    return True, None, enriched_records
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to retrieve Google Maps enriched records: {e}"
            logger.error(error_msg)
            return False, error_msg, []


def safe_insert_google_maps_results(
    job_id: str,
    request_id: str,
    result_rows: List[GoogleMapsResultRow]
) -> None:
    """
    Safe wrapper for inserting Google Maps results to PostgreSQL.
    
    DEPRECATED: Use the shared bulk_writer.safe_bulk_insert instead.
    This function is kept for backward compatibility.
    """
    from ...database.utils.bulk_writer import safe_bulk_insert
    
    repo = GoogleMapsResultsRepository()
    safe_bulk_insert(
        repo=repo,
        rows=result_rows,
        job_id=job_id,
        request_id=request_id,
        feature_enabled=db_config.is_google_maps_results_enabled,
        log_prefix="Google Maps results",
    )


def safe_insert_google_maps_enriched(
    job_id: str,
    request_id: str,
    enriched_rows: List[GoogleMapsEnrichedRow]
) -> None:
    """
    Safe wrapper for inserting Google Maps enriched records to PostgreSQL.
    
    DEPRECATED: Use the shared bulk_writer.safe_bulk_insert instead.
    This function is kept for backward compatibility.
    """
    from ...database.utils.bulk_writer import safe_bulk_insert
    
    repo = GoogleMapsEnrichedRepository()
    safe_bulk_insert(
        repo=repo,
        rows=enriched_rows,
        job_id=job_id,
        request_id=request_id,
        feature_enabled=db_config.is_google_maps_enriched_enabled,
        log_prefix="Google Maps enriched",
    )


def convert_db_results_to_sheets_format(results: List[dict]) -> List[List[str]]:
    """
    Convert database results to Google Sheets format.
    
    NEW simplified format (DB-first):
    ["Organisation","Domain","Phone Number","Location","Address","Type","Google Maps URL","Search Query"]
    
    Note: Only OPERATIONAL results should be exported (filtered in query/view)
    
    Args:
        results: List of database result dictionaries
        
    Returns:
        List of rows in sheets format
    """
    sheets_rows = []
    
    for result in results:
        row = [
            norm(result.get('organisation', '')),
            norm(result.get('domain', '')),
            norm(result.get('phone', '')),
            norm(result.get('location_label', '')),  # "Location" column
            norm(result.get('address', '')),
            norm(result.get('type', '')),
            norm(result.get('google_maps_url', '')),
            norm(result.get('search_query', '')),
        ]
        sheets_rows.append(row)
    
    return sheets_rows


class GoogleMapsEnrichQueueRepository:
    """
    Repository for Google Maps enrichment queue database operations.
    
    Provides concurrency-safe queue operations with proper locking
    and state management using PostgreSQL FOR UPDATE SKIP LOCKED.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize repository with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
    
    def insert_many(self, rows: List[GoogleMapsEnrichQueueRow]) -> tuple[bool, Optional[str], int]:
        """
        Insert multiple enrichment queue rows in a single transaction.
        
        Args:
            rows: List of GoogleMapsEnrichQueueRow instances to insert
            
        Returns:
            Tuple of (success, error_message, inserted_count)
        """
        if not rows:
            return True, None, 0
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", 0
        
        # Filter out rows with null/empty place_id
        valid_rows = [row for row in rows if row.place_id and row.place_id.strip()]
        if not valid_rows:
            logger.warning("All queue rows filtered out due to missing place_id")
            return True, None, 0
        
        insert_sql = """
        INSERT INTO gmaps_enrich_queue (
            id, job_id, request_id, place_id, iso2, hl, state, attempts, last_error,
            next_retry_at, locked_at, locked_by
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (job_id, request_id, place_id) DO NOTHING
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Prepare batch data
                    batch_data = []
                    for row in valid_rows:
                        batch_data.append((
                            str(uuid.uuid4()),
                            row.job_id,
                            row.request_id,
                            row.place_id,  # CRITICAL: Canonical place_id from Google
                            row.iso2,
                            row.hl,
                            row.state,
                            row.attempts,
                            row.last_error,
                            row.next_retry_at,
                            row.locked_at,
                            row.locked_by,
                        ))
                    
                    # Execute batch insert
                    cur.executemany(insert_sql, batch_data)
                    conn.commit()
                    
                    inserted_count = len(batch_data)
                    logger.debug(
                        "Successfully inserted %d enrichment queue items to PostgreSQL",
                        inserted_count
                    )
                    return True, None, inserted_count
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Failed to insert enrichment queue items: {e}"
            logger.error(error_msg)
            return False, error_msg, 0
    
    def fetch_next_batch(
        self,
        job_id: str,
        request_id: str,
        batch_size: int = 50,
        max_attempts: int = 3,
    ) -> tuple[bool, Optional[str], List[dict]]:
        """
        Atomically fetch and claim next batch of eligible queue items.
        
        CRITICAL REQUIREMENTS:
        1. Atomic claim: Items are locked in a single transaction using FOR UPDATE SKIP LOCKED
        2. Retry gating: FAILED items are only eligible if next_retry_at <= NOW()
        3. Max attempts: Items with attempts >= max_attempts are never returned
        4. State transition: PENDING/FAILED → IN_PROGRESS atomically
        5. Concurrency-safe: Multiple workers can process without conflicts
        
        NOTE: Filters by request_id only (not job_id) to support discover→enrich pipeline
        where discover and enrich may have different job_ids but share the same request_id.
        
        Args:
            job_id: Job identifier (kept for compatibility, used for logging only)
            request_id: Request identifier (primary filter)
            batch_size: Maximum number of items to fetch
            max_attempts: Maximum retry attempts before permanent failure
            
        Returns:
            Tuple of (success, error_message, queue_items)
            Each queue_item is a dict with: id, place_id, iso2, hl, attempts
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        # CRITICAL: Atomic claim with retry gating
        # This query:
        # 1. Selects eligible items (PENDING or retry-eligible FAILED)
        # 2. Locks them with FOR UPDATE SKIP LOCKED (concurrency-safe)
        # 3. Updates state to IN_PROGRESS and increments attempts
        # 4. Returns claimed items in a single transaction
        select_sql = """
        WITH eligible AS (
            SELECT q.id, q.place_id, q.iso2, q.hl, q.attempts
            FROM gmaps_enrich_queue AS q
            WHERE q.request_id = %s
              AND q.state IN ('PENDING', 'FAILED')
              AND q.attempts < %s
              AND (q.next_retry_at IS NULL OR q.next_retry_at <= NOW())
            ORDER BY q.created_at ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE gmaps_enrich_queue AS q
        SET
            state = 'IN_PROGRESS',
            attempts = q.attempts + 1,
            locked_at = NOW(),
            locked_by = %s
        FROM eligible AS e
        WHERE q.id = e.id
        RETURNING
            q.id,
            e.place_id,
            e.iso2,
            e.hl,
            q.attempts
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Use job_id as locked_by for debugging
                    cur.execute(
                        select_sql,
                        (request_id, max_attempts, batch_size, job_id)
                    )
                    rows = cur.fetchall()
                    conn.commit()
                    
                    items = []
                    for row in rows:
                        items.append({
                            'id': str(row[0]),
                            'place_id': row[1],  # CRITICAL: Canonical place_id
                            'iso2': row[2],
                            'hl': row[3],
                            'attempts': row[4],
                        })
                    
                    if items:
                        logger.debug(
                            "QUEUE_FETCH | job_id=%s request_id=%s | claimed=%d items",
                            job_id,
                            request_id,
                            len(items)
                        )
                    
                    return True, None, items
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to fetch queue batch: {e}"
            logger.error(error_msg)
            return False, error_msg, []
    
    def mark_done(self, ids: List[str]) -> tuple[bool, Optional[str]]:
        """
        Mark queue items as DONE (terminal success state).
        
        Clears error state and locks. Items in DONE state are never re-processed.
        
        Args:
            ids: List of queue item UUIDs
            
        Returns:
            Tuple of (success, error_message)
        """
        if not ids:
            return True, None
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured"
        
        update_sql = """
        UPDATE gmaps_enrich_queue
        SET
            state = 'DONE',
            last_error = NULL,
            next_retry_at = NULL,
            locked_at = NULL,
            locked_by = NULL
        WHERE id = ANY(%s)
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(update_sql, (ids,))
                    conn.commit()
                    
                    rows_updated = cur.rowcount
                    logger.debug("QUEUE_DONE | marked=%d items as DONE", rows_updated)
                    return True, None
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Failed to mark items as done: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def mark_failed(
        self,
        ids: List[str],
        error: str,
        max_attempts: int = 3,
    ) -> tuple[bool, Optional[str]]:
        """
        Mark queue items as FAILED with exponential backoff retry gating.
        
        CRITICAL BEHAVIOR:
        - If attempts < max_attempts: Set state=FAILED with next_retry_at (exponential backoff)
        - If attempts >= max_attempts: Set state=FAILED_FINAL (terminal, never retried)
        - Exponential backoff: min(300s, 2s * 2^attempts)
        
        This prevents infinite retry loops by:
        1. Gating retries with next_retry_at timestamp
        2. Using exponential backoff to space out retries
        3. Terminal FAILED_FINAL state after max_attempts
        
        Args:
            ids: List of queue item UUIDs
            error: Error message to store
            max_attempts: Maximum retry attempts before terminal failure
            
        Returns:
            Tuple of (success, error_message)
        """
        if not ids:
            return True, None
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured"
        
        # CRITICAL: Use exponential backoff for retry gating
        # This prevents immediate re-fetch of failed items
        update_sql = """
        UPDATE gmaps_enrich_queue
        SET
            state = CASE
                WHEN attempts < %s THEN 'FAILED'
                ELSE 'FAILED_FINAL'
            END,
            last_error = %s,
            next_retry_at = CASE
                WHEN attempts < %s THEN calculate_next_retry_at(attempts, 2, 300)
                ELSE NULL
            END,
            locked_at = NULL,
            locked_by = NULL
        WHERE id = ANY(%s)
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        update_sql,
                        (max_attempts, norm(error), max_attempts, ids)
                    )
                    conn.commit()
                    
                    rows_updated = cur.rowcount
                    logger.debug(
                        "QUEUE_FAILED | marked=%d items | error=%s | max_attempts=%d",
                        rows_updated,
                        error[:100],
                        max_attempts
                    )
                    return True, None
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Failed to mark items as failed: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def get_queue_summary(
        self,
        job_id: str,
        request_id: str,
    ) -> tuple[bool, Optional[str], dict]:
        """
        Get summary statistics for a job's enrichment queue.
        
        NOTE: Filters by request_id only (not job_id) to support discover→enrich pipeline
        where discover and enrich may have different job_ids but share the same request_id.
        
        Args:
            job_id: Job identifier (kept for compatibility but not used in filtering)
            request_id: Request identifier (primary filter)
            
        Returns:
            Tuple of (success, error_message, summary_dict)
            summary_dict contains: total, pending, processing, done, failed
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", {}
        
        # Use the enhanced view created by migration 0005
        summary_sql = """
        SELECT
            total,
            pending,
            processing,
            done,
            failed,
            failed_final,
            retry_eligible,
            avg_attempts,
            max_attempts
        FROM v_gmaps_enrich_queue_summary
        WHERE request_id = %s
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(summary_sql, (request_id,))
                    row = cur.fetchone()
                    
                    if row:
                        summary = {
                            'total': row[0] or 0,
                            'pending': row[1] or 0,
                            'processing': row[2] or 0,
                            'done': row[3] or 0,
                            'failed': row[4] or 0,
                            'failed_final': row[5] or 0,
                            'retry_eligible': row[6] or 0,
                            'avg_attempts': float(row[7]) if row[7] else 0.0,
                            'max_attempts': row[8] or 0,
                        }
                        return True, None, summary
                    else:
                        return True, None, {
                            'total': 0,
                            'pending': 0,
                            'processing': 0,
                            'done': 0,
                            'failed': 0,
                            'failed_final': 0,
                            'retry_eligible': 0,
                            'avg_attempts': 0.0,
                            'max_attempts': 0,
                        }
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, {}
        except Exception as e:
            error_msg = f"Failed to get queue summary: {e}"
            logger.error(error_msg)
            return False, error_msg, {}


def safe_insert_google_maps_enrich_queue(
    job_id: str,
    request_id: str,
    queue_rows: List[GoogleMapsEnrichQueueRow]
) -> None:
    """
    Safe wrapper for inserting Google Maps enrichment queue items to PostgreSQL.
    
    Uses the shared bulk_writer.safe_bulk_insert pattern.
    """
    from ...database.utils.bulk_writer import safe_bulk_insert
    
    repo = GoogleMapsEnrichQueueRepository()
    safe_bulk_insert(
        repo=repo,
        rows=queue_rows,
        job_id=job_id,
        request_id=request_id,
        feature_enabled=db_config.is_google_maps_results_enabled,  # Use same flag as results
        log_prefix="Google Maps enrich queue",
    )


def convert_db_enriched_to_sheets_format(enriched_records: List[dict]) -> List[List[str]]:
    """
    Convert database enriched records to Google Sheets format.
    
    This is a placeholder for enriched data format conversion.
    The actual format would depend on how enriched data is displayed in sheets.
    
    Args:
        enriched_records: List of database enriched record dictionaries
        
    Returns:
        List of rows in sheets format
    """
    sheets_rows = []
    
    for record in enriched_records:
        # This is a basic conversion - adjust based on actual sheets format
        row = [
            norm(record.get('place_id', '')),
            str(record.get('rating', '') or ''),
            str(record.get('reviews_count', '') or ''),
            str(record.get('photos_count', '') or ''),
            json.dumps(record.get('opening_hours', {})) if record.get('opening_hours') else '',
        ]
        sheets_rows.append(row)
    
    return sheets_rows


@dataclass
class GoogleMapsAuditRow:
    """
    Data structure for Google Maps audit database rows.
    
    This matches the structure expected by the PostgreSQL google_maps_audit table.
    """
    job_id: str
    request_id: str
    phase: str  # 'discover' or 'enrich'
    country_iso2: Optional[str] = None
    hl_plan: Optional[str] = None
    language_used: Optional[str] = None
    location_label: Optional[str] = None
    base_query: Optional[str] = None
    final_query: Optional[str] = None
    region_param: Optional[str] = None
    take_n: Optional[int] = None
    returned_count: Optional[int] = None
    eligible_after_dedupe: Optional[int] = None
    appended_rows: Optional[int] = None
    unique_places_job: Optional[int] = None
    stop_reason: Optional[str] = None
    error: Optional[str] = None
    timestamp: Optional[str] = None
    raw_meta_json: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_run_result(
        cls,
        job_id: str,
        request_id: str,
        run_result: Any,  # GoogleMapsRunResult
    ) -> "GoogleMapsAuditRow":
        """
        Create GoogleMapsAuditRow from a GoogleMapsRunResult.
        
        Args:
            job_id: Job identifier
            request_id: Request identifier
            run_result: GoogleMapsRunResult instance
            
        Returns:
            GoogleMapsAuditRow instance with normalized data
        """
        return cls(
            job_id=norm(job_id),
            request_id=norm(request_id),
            phase=norm(getattr(run_result, 'phase', 'discover')),
            country_iso2=norm(getattr(run_result, 'iso2', None)),
            hl_plan=norm(getattr(run_result, 'hl_plan', None)),
            language_used=norm(getattr(run_result, 'language_used', None)),
            location_label=norm(getattr(run_result, 'location_label', None)),
            base_query=norm(getattr(run_result, 'base_query', None)),
            final_query=norm(getattr(run_result, 'final_query', None)),
            region_param=norm(getattr(run_result, 'region_param', None)),
            take_n=getattr(run_result, 'take_n', None),
            returned_count=getattr(run_result, 'returned_count', None),
            eligible_after_dedupe=getattr(run_result, 'eligible_after_dedupe', None),
            appended_rows=getattr(run_result, 'appended_rows', None),
            unique_places_job=getattr(run_result, 'unique_places_job', None),
            stop_reason=norm(getattr(run_result, 'stop_reason', None)),
            error=norm(getattr(run_result, 'error', None)),
            timestamp=None,  # Will be set by DB default
            raw_meta_json=getattr(run_result, 'meta', None),
        )


class GoogleMapsAuditRepository:
    """
    Repository for Google Maps audit database operations.
    
    Provides safe database operations for audit logging with proper error handling
    and transaction management using psycopg v3.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize repository with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
    
    def insert_many(self, rows: List[GoogleMapsAuditRow]) -> tuple[bool, Optional[str], int]:
        """
        Insert multiple Google Maps audit rows in a single transaction.
        
        Args:
            rows: List of GoogleMapsAuditRow instances to insert
            
        Returns:
            Tuple of (success, error_message, inserted_count)
        """
        if not rows:
            return True, None, 0
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", 0
        
        insert_sql = """
        INSERT INTO google_maps_audit (
            id, job_id, request_id, phase, country_iso2, hl_plan, language_used,
            location_label, base_query, final_query, region_param, take_n,
            returned_count, eligible_after_dedupe, appended_rows, unique_places_job,
            stop_reason, error, raw_meta_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Prepare batch data
                    batch_data = []
                    for row in rows:
                        batch_data.append((
                            str(uuid.uuid4()),  # Generate UUID for each row
                            row.job_id,
                            row.request_id,
                            row.phase,
                            row.country_iso2,
                            row.hl_plan,
                            row.language_used,
                            row.location_label,
                            row.base_query,
                            row.final_query,
                            row.region_param,
                            row.take_n,
                            row.returned_count,
                            row.eligible_after_dedupe,
                            row.appended_rows,
                            row.unique_places_job,
                            row.stop_reason,
                            row.error,
                            json.dumps(row.raw_meta_json) if row.raw_meta_json else None,
                        ))
                    
                    # Execute batch insert
                    cur.executemany(insert_sql, batch_data)
                    conn.commit()
                    
                    inserted_count = len(batch_data)
                    logger.debug(
                        "Successfully inserted %d Google Maps audit records to PostgreSQL",
                        inserted_count
                    )
                    return True, None, inserted_count
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Failed to insert Google Maps audit records: {e}"
            logger.error(error_msg)
            return False, error_msg, 0
    
    def get_audit_by_job(self, job_id: str) -> tuple[bool, Optional[str], List[dict]]:
        """
        Retrieve audit records for a specific job ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Tuple of (success, error_message, audit_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        select_sql = """
        SELECT id, created_at, job_id, request_id, phase, country_iso2, hl_plan,
               language_used, location_label, base_query, final_query, region_param,
               take_n, returned_count, eligible_after_dedupe, appended_rows,
               unique_places_job, stop_reason, error, timestamp, raw_meta_json
        FROM google_maps_audit
        WHERE job_id = %s
        ORDER BY timestamp, created_at
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (job_id,))
                    rows = cur.fetchall()
                    
                    audit_records = []
                    for row in rows:
                        audit_records.append({
                            'id': str(row[0]),
                            'created_at': row[1].isoformat() if row[1] else None,
                            'job_id': row[2],
                            'request_id': row[3],
                            'phase': row[4],
                            'country_iso2': row[5],
                            'hl_plan': row[6],
                            'language_used': row[7],
                            'location_label': row[8],
                            'base_query': row[9],
                            'final_query': row[10],
                            'region_param': row[11],
                            'take_n': row[12],
                            'returned_count': row[13],
                            'eligible_after_dedupe': row[14],
                            'appended_rows': row[15],
                            'unique_places_job': row[16],
                            'stop_reason': row[17],
                            'error': row[18],
                            'timestamp': row[19].isoformat() if row[19] else None,
                            'raw_meta_json': row[20],  # Already parsed as dict by psycopg
                        })
                    
                    return True, None, audit_records
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to retrieve Google Maps audit records: {e}"
            logger.error(error_msg)
            return False, error_msg, []


def safe_insert_google_maps_audit(
    job_id: str,
    request_id: str,
    audit_rows: List[GoogleMapsAuditRow]
) -> None:
    """
    Safe wrapper for inserting Google Maps audit records to PostgreSQL.
    
    Uses the shared bulk_writer.safe_bulk_insert pattern.
    """
    from ...database.utils.bulk_writer import safe_bulk_insert
    
    repo = GoogleMapsAuditRepository()
    safe_bulk_insert(
        repo=repo,
        rows=audit_rows,
        job_id=job_id,
        request_id=request_id,
        feature_enabled=db_config.is_google_maps_results_enabled,  # Use same flag
        log_prefix="Google Maps audit",
    )


def convert_db_audit_to_sheets_format(audit_records: List[dict]) -> List[List[str]]:
    """
    Convert database audit records to Google Sheets format.
    
    Expected sheets format matches HEADERS_AUDIT:
    ["Job ID","Request ID","Phase","Country (ISO2)","HL (Plan)","Language Used",
     "Location Label","Base Query","Final Query","Region Param","Take N",
     "Returned Count","Eligible After Dedupe","Appended Rows","Unique Places (Job)",
     "Stop Reason","Error","Timestamp","Raw Meta (JSON)"]
    
    Args:
        audit_records: List of database audit record dictionaries
        
    Returns:
        List of rows in sheets format
    """
    sheets_rows = []
    
    for record in audit_records:
        row = [
            norm(record.get('job_id', '')),
            norm(record.get('request_id', '')),
            norm(record.get('phase', '')),
            norm(record.get('country_iso2', '')),
            norm(record.get('hl_plan', '')),
            norm(record.get('language_used', '')),
            norm(record.get('location_label', '')),
            norm(record.get('base_query', '')),
            norm(record.get('final_query', '')),
            norm(record.get('region_param', '')),
            str(record.get('take_n', '') or ''),
            str(record.get('returned_count', '') or ''),
            str(record.get('eligible_after_dedupe', '') or ''),
            str(record.get('appended_rows', '') or ''),
            str(record.get('unique_places_job', '') or ''),
            norm(record.get('stop_reason', '')),
            norm(record.get('error', '')),
            norm(record.get('timestamp', '')),
            json.dumps(record.get('raw_meta_json', {})) if record.get('raw_meta_json') else '',
        ]
        sheets_rows.append(row)
    
    return sheets_rows