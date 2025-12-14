"""
Data models for ADB Projects Scraper
Defines the structure for project data extracted from listing and detail pages.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, List
from datetime import datetime
import json


@dataclass
class ProjectListing:
    """
    Data model for project information from the main listing page.
    """
    project_id: str
    title: str
    url: str
    country: Optional[str] = None
    sector: Optional[str] = None
    status: Optional[str] = None
    approval_year: Optional[str] = None
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ProjectListing':
        """Create instance from dictionary"""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class ProjectDetail:
    """
    Data model for detailed project information from individual project pages.
    Extends ProjectListing with additional fields.
    """
    project_id: str
    title: str
    url: str
    country: Optional[str] = None
    sector: Optional[str] = None
    status: Optional[str] = None
    approval_year: Optional[str] = None
    
    # Additional detail page fields
    project_type: Optional[str] = None  # e.g., "Sovereign Project"
    modality: Optional[str] = None  # e.g., "Loan"
    financing_source: Optional[str] = None  # e.g., "Ordinary capital resources"
    financing_amount: Optional[str] = None  # e.g., "US$ 150.00 million"
    subsector: Optional[str] = None
    description: Optional[str] = None
    rationale: Optional[str] = None
    impact: Optional[str] = None
    outcome: Optional[str] = None
    outputs: Optional[str] = None
    geographical_location: Optional[str] = None
    gender_tag: Optional[str] = None
    
    # Safeguard Categories
    safeguard_environment: Optional[str] = None
    safeguard_involuntary_resettlement: Optional[str] = None
    safeguard_indigenous_peoples: Optional[str] = None
    
    # Contact Information
    responsible_adb_officer: Optional[str] = None
    responsible_adb_department: Optional[str] = None
    responsible_adb_division: Optional[str] = None
    executing_agencies: Optional[str] = None
    
    # Timetable
    concept_clearance: Optional[str] = None
    fact_finding: Optional[str] = None
    approval_date: Optional[str] = None
    last_pds_update: Optional[str] = None
    
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ProjectDetail':
        """Create instance from dictionary"""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


def validate_project_listing(data: dict) -> tuple[bool, List[str]]:
    """
    Validate that a project listing has the minimum required fields.
    
    Args:
        data: Dictionary containing project data
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    required_fields = ['project_id', 'title', 'url']
    errors = []
    
    for field_name in required_fields:
        if not data.get(field_name):
            errors.append(f"Missing required field: {field_name}")
    
    return len(errors) == 0, errors


def validate_project_detail(data: dict) -> tuple[bool, List[str]]:
    """
    Validate that a project detail has the minimum required fields.
    
    Args:
        data: Dictionary containing project data
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    required_fields = ['project_id', 'title', 'url']
    warnings = []
    errors = []
    
    # Check required fields
    for field_name in required_fields:
        if not data.get(field_name):
            errors.append(f"Missing required field: {field_name}")
    
    # Check recommended fields (warnings only)
    recommended_fields = ['country', 'sector', 'status', 'financing_amount']
    for field_name in recommended_fields:
        if not data.get(field_name):
            warnings.append(f"Missing recommended field: {field_name}")
    
    return len(errors) == 0, errors + warnings
