"""
Checkpoint Manager for ADB Projects Scraper
Handles saving and loading scraping progress to enable resume-on-failure.
"""

import json
import os
from typing import Dict, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class CheckpointManager:
    """
    Manages checkpoint data for scraping progress.
    Enables resuming scraping sessions after interruption.
    """
    
    def __init__(self, checkpoint_file: str = "scraping_checkpoint.json"):
        """
        Initialize checkpoint manager.
        
        Args:
            checkpoint_file: Path to checkpoint file
        """
        self.checkpoint_file = checkpoint_file
        self.data = self._load_checkpoint()
    
    def _load_checkpoint(self) -> dict:
        """Load existing checkpoint data or create new structure"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded checkpoint from {self.checkpoint_file}")
                    return data
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}. Starting fresh.")
        
        # Default structure
        return {
            "last_updated": None,
            "last_page_scraped": -1,
            "total_projects_scraped": 0,
            "scraped_project_ids": [],
            "failed_urls": [],
            "statistics": {
                "listing_pages_scraped": 0,
                "detail_pages_scraped": 0,
                "errors_encountered": 0
            }
        }
    
    def save_checkpoint(self):
        """Save current checkpoint data to file"""
        try:
            self.data["last_updated"] = datetime.now().isoformat()
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2)
            logger.debug(f"Checkpoint saved to {self.checkpoint_file}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def update_page_progress(self, page_number: int):
        """Update the last successfully scraped page"""
        self.data["last_page_scraped"] = page_number
        self.data["statistics"]["listing_pages_scraped"] = page_number + 1
    
    def add_scraped_project(self, project_id: str):
        """Add a project ID to the scraped list"""
        if project_id not in self.data["scraped_project_ids"]:
            self.data["scraped_project_ids"].append(project_id)
            self.data["total_projects_scraped"] = len(self.data["scraped_project_ids"])
    
    def is_project_scraped(self, project_id: str) -> bool:
        """Check if a project has already been scraped"""
        return project_id in self.data["scraped_project_ids"]
    
    def add_failed_url(self, url: str, error: str):
        """Record a failed URL with error message"""
        self.data["failed_urls"].append({
            "url": url,
            "error": error,
            "timestamp": datetime.now().isoformat()
        })
    
    def increment_detail_pages(self):
        """Increment count of detail pages scraped"""
        self.data["statistics"]["detail_pages_scraped"] += 1
    
    def increment_errors(self):
        """Increment error counter"""
        self.data["statistics"]["errors_encountered"] += 1
    
    def get_resume_page(self) -> int:
        """Get the page number to resume from"""
        return self.data["last_page_scraped"] + 1
    
    def get_statistics(self) -> dict:
        """Get scraping statistics"""
        return {
            "total_projects": self.data["total_projects_scraped"],
            "listing_pages": self.data["statistics"]["listing_pages_scraped"],
            "detail_pages": self.data["statistics"]["detail_pages_scraped"],
            "errors": self.data["statistics"]["errors_encountered"],
            "failed_urls": len(self.data["failed_urls"]),
            "last_updated": self.data["last_updated"]
        }
    
    def reset(self):
        """Reset checkpoint data"""
        self.data = {
            "last_updated": None,
            "last_page_scraped": -1,
            "total_projects_scraped": 0,
            "scraped_project_ids": [],
            "failed_urls": [],
            "statistics": {
                "listing_pages_scraped": 0,
                "detail_pages_scraped": 0,
                "errors_encountered": 0
            }
        }
        self.save_checkpoint()
        logger.info("Checkpoint reset")
