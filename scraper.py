"""
ADB Projects Scraper - Core Scraping Logic
Extracts project data from ADB's projects listing and detail pages.
"""

import time
import random
import logging
from typing import List, Optional, Dict
from urllib.parse import urljoin

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from bs4 import BeautifulSoup

from models import ProjectListing, ProjectDetail, validate_project_listing, validate_project_detail

logger = logging.getLogger(__name__)


class ADBProjectsScraper:
    """
    Scraper for ADB Projects website.
    Uses undetected-chromedriver to bypass Cloudflare protection.
    """
    
    BASE_URL = "https://www.adb.org"
    PROJECTS_URL = "https://www.adb.org/projects"
    
    def __init__(self, headless: bool = False):
        """
        Initialize the scraper.
        
        Args:
            headless: Whether to run browser in headless mode
        """
        self.headless = headless
        self.driver = None
        self.total_projects = 0
        self.total_pages = 0
    
    def init_driver(self):
        """Initialize undetected Chrome driver"""
        try:
            options = uc.ChromeOptions()
            
            if self.headless:
                options.add_argument('--headless=new')
            
            # Performance optimizations
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')
            
            # Disable images for faster loading (optional)
            # prefs = {"profile.managed_default_content_settings.images": 2}
            # options.add_experimental_option("prefs", prefs)
            
            self.driver = uc.Chrome(options=options)
            self.driver.set_page_load_timeout(30)
            logger.info("Chrome driver initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            raise
    
    def close_driver(self):
        """Close the browser driver"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Chrome driver closed")
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")
    
    def fetch_page(self, url: str, retries: int = 3) -> Optional[str]:
        """
        Fetch a page with retry logic and Cloudflare detection.
        
        Args:
            url: URL to fetch
            retries: Number of retry attempts
            
        Returns:
            Page HTML source or None if failed
        """
        for attempt in range(retries):
            try:
                logger.debug(f"Fetching {url} (attempt {attempt + 1}/{retries})")
                self.driver.get(url)
                
                # Wait for page to load - look for main content
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "adb-main"))
                )
                
                # Check for Cloudflare challenge
                if "cloudflare" in self.driver.page_source.lower() and "checking your browser" in self.driver.page_source.lower():
                    logger.warning("Cloudflare challenge detected, waiting...")
                    time.sleep(5)
                    # Wait for challenge to complete
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "adb-main"))
                    )
                
                # Random delay to mimic human behavior
                time.sleep(random.uniform(1.5, 3.0))
                
                return self.driver.page_source
                
            except TimeoutException:
                logger.warning(f"Timeout loading {url}, attempt {attempt + 1}/{retries}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(3, 5))
                    
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(3, 5))
        
        return None
    
    def parse_listing_page(self, html: str) -> List[ProjectListing]:
        """
        Parse the projects listing page to extract project information.
        
        Args:
            html: HTML source of the listing page
            
        Returns:
            List of ProjectListing objects
        """
        projects = []
        soup = BeautifulSoup(html, 'lxml')
        
        # Find all project items
        project_items = soup.find_all('div', class_='item linked')
        logger.info(f"Found {len(project_items)} project items on page")
        
        for item in project_items:
            try:
                # Extract title and URL
                title_elem = item.find('div', class_='item-title')
                if not title_elem:
                    logger.warning("No title element found, skipping item")
                    continue
                
                title_link = title_elem.find('a')
                if not title_link:
                    logger.warning("No link in title, skipping item")
                    continue
                
                title = title_link.get_text(strip=True)
                relative_url = title_link.get('href', '')
                url = urljoin(self.BASE_URL, relative_url)
                
                # Extract summary (project_id; country; sector)
                summary_elem = item.find('div', class_='item-summary')
                summary_text = summary_elem.get_text(strip=True) if summary_elem else ""
                
                # Parse summary: "59364-001; Thailand; Finance"
                project_id = None
                country = None
                sector = None
                
                if summary_text:
                    parts = [p.strip() for p in summary_text.split(';')]
                    if len(parts) >= 1:
                        project_id = parts[0]
                    if len(parts) >= 2:
                        country = parts[1]
                    if len(parts) >= 3:
                        sector = parts[2]
                
                # Extract status
                status = None
                status_elem = item.find('span', class_=['Proposed', 'Active', 'Approved', 'Closed', 'Completed'])
                if status_elem:
                    status = status_elem.get_text(strip=True)
                
                # Extract approval year
                approval_year = None
                time_elem = item.find('time')
                if time_elem:
                    approval_year = time_elem.get_text(strip=True)
                
                # Create project listing object
                project = ProjectListing(
                    project_id=project_id or "UNKNOWN",
                    title=title,
                    url=url,
                    country=country,
                    sector=sector,
                    status=status,
                    approval_year=approval_year
                )
                
                # Validate before adding
                is_valid, errors = validate_project_listing(project.to_dict())
                if is_valid:
                    projects.append(project)
                else:
                    logger.warning(f"Invalid project data: {errors}")
                
            except Exception as e:
                logger.error(f"Error parsing project item: {e}")
                continue
        
        return projects
    
    def parse_detail_page(self, html: str, project_url: str) -> Optional[ProjectDetail]:
        """
        Parse individual project detail page - extracts comprehensive project information.
        
        Args:
            html: HTML source of the detail page
            project_url: URL of the project page
            
        Returns:
            ProjectDetail object or None if parsing failed
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract project ID and type from header
            project_id = None
            project_type = None
            
            h4_elem = soup.find('h4', string=lambda s: s and 'Project' in s and '|' in s)
            if h4_elem:
                h4_text = h4_elem.get_text(strip=True)
                # Format: "Sovereign Project | 59364-001"
                parts = [p.strip() for p in h4_text.split('|')]
                if len(parts) >= 2:
                    project_type = parts[0]
                    project_id = parts[1]
            
            # Extract title from h1
            title_elem = soup.find('h1')
            title = title_elem.get_text(strip=True) if title_elem else None
            
            # Extract status
            status = None
            status_elem = soup.find('div', class_='project-status')
            if status_elem:
                status_text = status_elem.get_text(strip=True)
                # Format: "Status: Proposed"
                if 'Status:' in status_text:
                    status = status_text.replace('Status:', '').strip()
            
            # Parse ALL dt/dd pairs from project data sheet (PDS)
            pds_data = {}
            all_dls = soup.find_all('dl', class_='pds')
            
            for dl in all_dls:
                dt_elements = dl.find_all('dt', class_='col-md-3')
                dd_elements = dl.find_all('dd', class_='col-md-9')
                
                for dt, dd in zip(dt_elements, dd_elements):
                    key = dt.get_text(strip=True)
                    value = dd.get_text(strip=True)
                    pds_data[key] = value
            
            # Extract basic fields
            project_name = pds_data.get('Project Name')
            project_number = pds_data.get('Project Number')
            country = pds_data.get('Country / Economy')
            project_status = pds_data.get('Project Status')
            
            # Extract sector and subsector
            sector = None
            subsector = None
            sector_subsector_dd = None
            
            for dl in all_dls:
                dt = dl.find('dt', string='Sector / Subsector')
                if dt:
                    sector_subsector_dd = dt.find_next_sibling('dd')
                    break
            
            if sector_subsector_dd:
                sector_elem = sector_subsector_dd.find('strong', class_='sector')
                if sector_elem:
                    sector = sector_elem.get_text(strip=True)
                    # Get text after the sector for subsector
                    full_text = sector_subsector_dd.get_text(strip=True)
                    if '/' in full_text:
                        subsector = full_text.split('/', 1)[1].strip()
            
            # Extract modality
            modality = pds_data.get('Project Type / Modality of Assistance')
            
            # Extract financing information from table
            financing_source = None
            financing_amount = None
            
            fund_table = soup.find('table', class_='fund-table')
            if fund_table:
                tbody = fund_table.find('tbody')
                if tbody:
                    row = tbody.find('tr')
                    if row:
                        cols = row.find_all('td')
                        if len(cols) >= 2:
                            financing_source = cols[0].get_text(strip=True)
                            financing_amount = cols[1].get_text(strip=True)
            
            # Extract comprehensive project details
            description = pds_data.get('Description')
            rationale = pds_data.get('Project Rationale and Linkage to Country/Regional Strategy')
            impact = pds_data.get('Impact')
            outcome = pds_data.get('Outcome')
            outputs = pds_data.get('Outputs')
            geographical_location = pds_data.get('Geographical Location')
            gender_tag = pds_data.get('Gender')
            
            # Extract safeguard categories
            safeguard_environment = pds_data.get('Environment')
            safeguard_involuntary_resettlement = pds_data.get('Involuntary Resettlement')
            safeguard_indigenous_peoples = pds_data.get('Indigenous Peoples')
            
            # Extract contact information
            responsible_adb_officer = pds_data.get('Responsible ADB Officer')
            responsible_adb_department = pds_data.get('Responsible ADB Department')
            responsible_adb_division = pds_data.get('Responsible ADB Division')
            
            # Extract executing agencies
            executing_agencies = None
            exec_agency_dd = None
            for dl in all_dls:
                dt = dl.find('dt', string='Executing Agencies')
                if dt:
                    exec_agency_dd = dt.find_next_sibling('dd')
                    break
            
            if exec_agency_dd:
                agency_span = exec_agency_dd.find('span', class_='address-company')
                if agency_span:
                    executing_agencies = agency_span.get_text(strip=True)
            
            # Extract timetable information
            concept_clearance = pds_data.get('Concept Clearance')
            fact_finding = pds_data.get('Fact Finding')
            approval_date = pds_data.get('Approval')
            last_pds_update = pds_data.get('Last PDS Update')
            
            # Create ProjectDetail object with all fields
            project_detail = ProjectDetail(
                project_id=project_number or project_id or "UNKNOWN",
                title=project_name or title or "UNKNOWN",
                url=project_url,
                country=country,
                sector=sector,
                status=project_status or status,
                approval_year=None,  # Will be extracted from approval_date if needed
                project_type=project_type,
                modality=modality,
                financing_source=financing_source,
                financing_amount=financing_amount,
                subsector=subsector,
                description=description,
                rationale=rationale,
                impact=impact,
                outcome=outcome,
                outputs=outputs,
                geographical_location=geographical_location,
                gender_tag=gender_tag,
                safeguard_environment=safeguard_environment,
                safeguard_involuntary_resettlement=safeguard_involuntary_resettlement,
                safeguard_indigenous_peoples=safeguard_indigenous_peoples,
                responsible_adb_officer=responsible_adb_officer,
                responsible_adb_department=responsible_adb_department,
                responsible_adb_division=responsible_adb_division,
                executing_agencies=executing_agencies,
                concept_clearance=concept_clearance,
                fact_finding=fact_finding,
                approval_date=approval_date,
                last_pds_update=last_pds_update
            )
            
            # Validate
            is_valid, messages = validate_project_detail(project_detail.to_dict())
            if not is_valid:
                logger.warning(f"Validation issues for {project_detail.project_id}: {messages}")
            
            return project_detail
            
        except Exception as e:
            logger.error(f"Error parsing detail page {project_url}: {e}", exc_info=True)
            return None
    
    def get_total_projects(self, html: str) -> int:
        """
        Extract total number of projects from listing page.
        
        Args:
            html: HTML source
            
        Returns:
            Total number of projects
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
            stats_elem = soup.find('div', class_='list-stats')
            if stats_elem:
                stats_text = stats_elem.get_text(strip=True)
                # Format: "Results 1-20 of 12504"
                if 'of' in stats_text:
                    total = stats_text.split('of')[-1].strip()
                    return int(total.replace(',', ''))
        except Exception as e:
            logger.error(f"Error extracting total projects: {e}")
        
        return 0
    
    def scrape_listing_page(self, page_number: int = 0) -> List[ProjectListing]:
        """
        Scrape a single listing page.
        
        Args:
            page_number: Page number to scrape (0-indexed)
            
        Returns:
            List of ProjectListing objects
        """
        url = f"{self.PROJECTS_URL}?page={page_number}"
        logger.info(f"Scraping listing page {page_number}: {url}")
        
        html = self.fetch_page(url)
        if not html:
            logger.error(f"Failed to fetch listing page {page_number}")
            return []
        
        # Extract total on first page
        if page_number == 0:
            self.total_projects = self.get_total_projects(html)
            logger.info(f"Total projects available: {self.total_projects}")
        
        return self.parse_listing_page(html)
    
    def scrape_detail_page(self, project_url: str) -> Optional[ProjectDetail]:
        """
        Scrape a single project detail page.
        
        Args:
            project_url: URL of the project detail page
            
        Returns:
            ProjectDetail object or None if failed
        """
        logger.info(f"Scraping detail page: {project_url}")
        
        html = self.fetch_page(project_url)
        if not html:
            logger.error(f"Failed to fetch detail page: {project_url}")
            return None
        
        return self.parse_detail_page(html, project_url)
