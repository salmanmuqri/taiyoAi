"""
ADB Projects Scraper - Main Entry Point
Command-line interface for scraping ADB project data.
"""

import argparse
import logging
import json
import csv
import os
import sys
from datetime import datetime
from typing import List
from pathlib import Path

from tqdm import tqdm
import colorama
from colorama import Fore, Style

from scraper import ADBProjectsScraper
from models import ProjectListing, ProjectDetail
from checkpoint_manager import CheckpointManager

# Initialize colorama for cross-platform colored output
colorama.init()


def setup_logging(log_file: str = "scraper.log", verbose: bool = False):
    """
    Configure logging for the application.
    
    Args:
        log_file: Path to log file
        verbose: Enable verbose logging
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


def save_to_json(projects: List, output_file: str, metadata: dict = None, append: bool = False):
    """
    Save projects to JSON file.
    
    Args:
        projects: List of project objects
        output_file: Output file path
        metadata: Optional metadata to include
        append: If True, append to existing file
    """
    if append and os.path.exists(output_file):
        # Load existing data and append
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            
            existing_projects = existing_data.get('projects', [])
            new_projects = [p.to_dict() for p in projects]
            
            # Merge projects (avoid duplicates by project_id)
            project_ids = {p.get('project_id') for p in existing_projects}
            for proj in new_projects:
                if proj.get('project_id') not in project_ids:
                    existing_projects.append(proj)
                    project_ids.add(proj.get('project_id'))
            
            data = {
                "metadata": {
                    "last_updated": datetime.now().isoformat(),
                    "total_projects": len(existing_projects)
                },
                "projects": existing_projects
            }
        except Exception as e:
            logging.error(f"Error reading existing file for append: {e}")
            # Fallback to new file
            data = {
                "metadata": metadata or {
                    "scraped_at": datetime.now().isoformat(),
                    "total_projects": len(projects)
                },
                "projects": [p.to_dict() for p in projects]
            }
    else:
        data = {
            "metadata": metadata or {
                "scraped_at": datetime.now().isoformat(),
                "total_projects": len(projects)
            },
            "projects": [p.to_dict() for p in projects]
        }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"{Fore.GREEN}✓ Saved {len(projects)} projects to {output_file}{Style.RESET_ALL}")


def save_to_csv(projects: List, output_file: str, append: bool = False):
    """
    Save projects to CSV file.
    
    Args:
        projects: List of project objects
        output_file: Output file path
        append: If True, append to existing file
    """
    if not projects:
        print(f"{Fore.YELLOW}⚠ No projects to save{Style.RESET_ALL}")
        return
    
    # Get all unique field names
    fieldnames = list(projects[0].to_dict().keys())
    
    mode = 'a' if (append and os.path.exists(output_file)) else 'w'
    write_header = mode == 'w'
    
    with open(output_file, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if write_header:
            writer.writeheader()
        
        for project in projects:
            row = project.to_dict()
            # Convert lists to semicolon-separated strings
            for key, value in row.items():
                if isinstance(value, list):
                    row[key] = '; '.join(str(v) for v in value if v)
            writer.writerow(row)
    
    action = "Appended" if append else "Saved"
    print(f"{Fore.GREEN}✓ {action} {len(projects)} projects to {output_file}{Style.RESET_ALL}")


def scrape_listings(args, scraper: ADBProjectsScraper, checkpoint: CheckpointManager):
    """
    Scrape project listings from multiple pages.
    
    Args:
        args: Command-line arguments
        scraper: ADBProjectsScraper instance
        checkpoint: CheckpointManager instance
        
    Returns:
        List of ProjectListing objects
    """
    all_projects = []
    
    # Determine page range
    start_page = args.start_page
    if args.resume:
        start_page = max(start_page, checkpoint.get_resume_page())
        print(f"{Fore.CYAN}ℹ Resuming from page {start_page}{Style.RESET_ALL}")
    
    end_page = args.end_page if args.end_page is not None else 625
    
    print(f"{Fore.CYAN}Scraping listing pages {start_page} to {end_page}...{Style.RESET_ALL}")
    
    # Scrape pages with progress bar
    for page_num in tqdm(range(start_page, end_page + 1), desc="Scraping listings"):
        try:
            projects = scraper.scrape_listing_page(page_num)
            
            if projects:
                all_projects.extend(projects)
                
                # Update checkpoint
                checkpoint.update_page_progress(page_num)
                for project in projects:
                    checkpoint.add_scraped_project(project.project_id)
                
                # Save checkpoint every 10 pages
                if (page_num + 1) % 10 == 0:
                    checkpoint.save_checkpoint()
                    logging.info(f"Checkpoint saved at page {page_num}")
            else:
                logging.warning(f"No projects found on page {page_num}")
            
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠ Interrupted by user. Saving progress...{Style.RESET_ALL}")
            checkpoint.save_checkpoint()
            break
            
        except Exception as e:
            logging.error(f"Error scraping page {page_num}: {e}")
            checkpoint.increment_errors()
            checkpoint.add_failed_url(f"{scraper.PROJECTS_URL}?page={page_num}", str(e))
    
    # Final checkpoint save
    checkpoint.save_checkpoint()
    
    return all_projects


def scrape_details(args, scraper: ADBProjectsScraper, checkpoint: CheckpointManager, project_urls: List[str], output_files: dict = None):
    """
    Scrape detailed information for specific projects with incremental saving.
    
    Args:
        args: Command-line arguments
        scraper: ADBProjectsScraper instance
        checkpoint: CheckpointManager instance
        project_urls: List of project URLs to scrape
        output_files: Dict with 'json' and 'csv' file paths for incremental saving
        
    Returns:
        List of ProjectDetail objects
    """
    all_details = []
    
    print(f"{Fore.CYAN}Scraping {len(project_urls)} project detail pages...{Style.RESET_ALL}")
    
    for idx, url in enumerate(tqdm(project_urls, desc="Scraping details"), 1):
        try:
            # Extract project ID from URL for checking
            project_id = url.split('/')[-2] if '/' in url else None
            
            # Skip if already scraped (unless force flag is set)
            if project_id and checkpoint.is_project_scraped(project_id) and not args.force:
                logging.debug(f"Skipping already scraped project: {project_id}")
                continue
            
            detail = scraper.scrape_detail_page(url)
            
            if detail:
                all_details.append(detail)
                checkpoint.increment_detail_pages()
                
                if project_id:
                    checkpoint.add_scraped_project(project_id)
                
                # Incremental save every 5 projects
                if output_files and len(all_details) % 5 == 0:
                    if args.format in ['json', 'both'] and 'json' in output_files:
                        save_to_json([detail], output_files['json'], append=True)
                    
                    if args.format in ['csv', 'both'] and 'csv' in output_files:
                        save_to_csv([detail], output_files['csv'], append=True)
                    
                    logging.info(f"Incremental save: {len(all_details)} projects saved so far")
                
                # Save checkpoint every 10 projects
                if len(all_details) % 10 == 0:
                    checkpoint.save_checkpoint()
            else:
                logging.warning(f"Failed to scrape detail page: {url}")
                checkpoint.add_failed_url(url, "Parsing failed")
            
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠ Interrupted by user. Saving progress...{Style.RESET_ALL}")
            checkpoint.save_checkpoint()
            # Final save of remaining data
            if output_files and all_details:
                if args.format in ['json', 'both'] and 'json' in output_files:
                    save_to_json(all_details, output_files['json'], append=True)
                if args.format in ['csv', 'both'] and 'csv' in output_files:
                    save_to_csv(all_details, output_files['csv'], append=True)
            break
            
        except Exception as e:
            logging.error(f"Error scraping detail page {url}: {e}")
            checkpoint.increment_errors()
            checkpoint.add_failed_url(url, str(e))
    
    # Final checkpoint save
    checkpoint.save_checkpoint()
    
    # Final save of any remaining unsaved data
    if output_files and all_details:
        remaining = len(all_details) % 5
        if remaining > 0:
            if args.format in ['json', 'both'] and 'json' in output_files:
                save_to_json(all_details[-remaining:], output_files['json'], append=True)
            if args.format in ['csv', 'both'] and 'csv' in output_files:
                save_to_csv(all_details[-remaining:], output_files['csv'], append=True)
    
    return all_details


def print_summary(checkpoint: CheckpointManager, start_time: datetime):
    """Print scraping summary statistics"""
    stats = checkpoint.get_statistics()
    duration = datetime.now() - start_time
    
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}Scraping Complete!{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"Total Projects Scraped: {Fore.YELLOW}{stats['total_projects']}{Style.RESET_ALL}")
    print(f"Listing Pages Scraped: {Fore.YELLOW}{stats['listing_pages']}{Style.RESET_ALL}")
    print(f"Detail Pages Scraped: {Fore.YELLOW}{stats['detail_pages']}{Style.RESET_ALL}")
    print(f"Errors Encountered: {Fore.RED}{stats['errors']}{Style.RESET_ALL}")
    print(f"Failed URLs: {Fore.RED}{stats['failed_urls']}{Style.RESET_ALL}")
    print(f"Duration: {Fore.YELLOW}{duration}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}\n")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="ADB Projects Web Scraper - Extract project data from ADB website",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape first 5 pages of listings only
  python main.py --mode listing --end-page 4 --format json
  
  # Scrape detail pages for specific projects
  python main.py --mode detail --urls urls.txt --format csv
  
  # Scrape both listings and details for pages 0-10
  python main.py --mode both --end-page 10 --format both
  
  # Resume interrupted scraping
  python main.py --mode listing --resume
        """
    )
    
    parser.add_argument('--mode', choices=['listing', 'detail', 'both'], default='listing',
                        help='Scraping mode: listing (list pages only), detail (detail pages only), or both')
    parser.add_argument('--output-dir', default='output',
                        help='Output directory for scraped data (default: output)')
    parser.add_argument('--format', choices=['json', 'csv', 'both'], default='both',
                        help='Output format (default: both)')
    parser.add_argument('--start-page', type=int, default=0,
                        help='Starting page number for listing scraping (default: 0)')
    parser.add_argument('--end-page', type=int, default=None,
                        help='Ending page number for listing scraping (default: all pages)')
    parser.add_argument('--urls', type=str,
                        help='File containing project URLs to scrape (one per line, for detail mode)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from last checkpoint')
    parser.add_argument('--force', action='store_true',
                        help='Force re-scraping of already scraped projects')
    parser.add_argument('--headless', action='store_true',
                        help='Run browser in headless mode')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('--reset-checkpoint', action='store_true',
                        help='Reset checkpoint and start fresh')
    
    args = parser.parse_args()
    
    # Setup
    setup_logging(verbose=args.verbose)
    start_time = datetime.now()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Initialize checkpoint manager
    checkpoint = CheckpointManager()
    if args.reset_checkpoint:
        checkpoint.reset()
        print(f"{Fore.YELLOW}Checkpoint reset{Style.RESET_ALL}")
    
    # Initialize scraper
    print(f"{Fore.CYAN}Initializing scraper...{Style.RESET_ALL}")
    scraper = ADBProjectsScraper(headless=args.headless)
    
    try:
        scraper.init_driver()
        
        # Prepare output filenames with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_files = {}
        
        # Execute scraping based on mode
        if args.mode in ['listing', 'both']:
            # Scrape listing pages
            listings = scrape_listings(args, scraper, checkpoint)
            
            if listings:
                # Save listings
                if args.format in ['json', 'both']:
                    json_file = output_dir / f"projects_listing_{timestamp}.json"
                    save_to_json(listings, str(json_file))
                
                if args.format in ['csv', 'both']:
                    csv_file = output_dir / f"projects_listing_{timestamp}.csv"
                    save_to_csv(listings, str(csv_file))
                
                # If mode is 'both', scrape details with incremental saving
                if args.mode == 'both':
                    project_urls = [p.url for p in listings]
                    
                    # Setup incremental save files
                    if args.format in ['json', 'both']:
                        output_files['json'] = str(output_dir / f"projects_detail_{timestamp}.json")
                    if args.format in ['csv', 'both']:
                        output_files['csv'] = str(output_dir / f"projects_detail_{timestamp}.csv")
                    
                    details = scrape_details(args, scraper, checkpoint, project_urls, output_files)
                    
                    print(f"{Fore.GREEN}✓ Scraped {len(details)} project details{Style.RESET_ALL}")
        
        elif args.mode == 'detail':
            # Load URLs from file
            if not args.urls:
                print(f"{Fore.RED}Error: --urls is required for detail mode{Style.RESET_ALL}")
                sys.exit(1)
            
            if not os.path.exists(args.urls):
                print(f"{Fore.RED}Error: URLs file not found: {args.urls}{Style.RESET_ALL}")
                sys.exit(1)
            
            with open(args.urls, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            
            # Setup incremental save files
            if args.format in ['json', 'both']:
                output_files['json'] = str(output_dir / f"projects_detail_{timestamp}.json")
            if args.format in ['csv', 'both']:
                output_files['csv'] = str(output_dir / f"projects_detail_{timestamp}.csv")
            
            details = scrape_details(args, scraper, checkpoint, urls, output_files)
            
            print(f"{Fore.GREEN}✓ Scraped {len(details)} project details{Style.RESET_ALL}")
        
        # Print summary
        print_summary(checkpoint, start_time)
        
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}⚠ Scraping interrupted by user{Style.RESET_ALL}")
        
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        print(f"{Fore.RED}✗ Fatal error: {e}{Style.RESET_ALL}")
        sys.exit(1)
        
    finally:
        # Cleanup
        scraper.close_driver()
        print(f"{Fore.GREEN}Done!{Style.RESET_ALL}")


if __name__ == '__main__':
    main()
