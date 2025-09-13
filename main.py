import asyncio
import json
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import logging
from loguru import logger

from scraper.example_scraper import ExampleDrivingSchoolScraper
from scraper.rijlessen_nl_scraper import RijlessenNLScraper
from scraper.base_scraper import ScrapedSchool
from database.models import DatabaseManager, DrivingSchool
from utils.validators import DataValidator, DataDeduplicator
from sqlalchemy.exc import IntegrityError

# Configure logging
logger.add("logs/scraper_{time:YYYY-MM-DD}.log", rotation="500 MB", level="INFO")

class EnhancedDataValidator:
    """Enhanced validation and cleaning for scraped data."""
    
    def __init__(self):
        self.validator = DataValidator()
        self.deduplicator = DataDeduplicator()
    
    def validate_school(self, school: ScrapedSchool) -> bool:
        """Validate a single school entry with enhanced rules."""
        if not school.name or not school.name.strip():
            return False
        
        # Validate phone if present
        if school.phone and not self.validator.validate_phone(school.phone):
            logger.warning(f"Invalid phone number for {school.name}: {school.phone}")
            school.phone = None
        
        # Validate email if present
        if school.email and not self.validator.validate_email_address(school.email):
            logger.warning(f"Invalid email for {school.name}: {school.email}")
            school.email = None
        
        # Validate website URL if present
        if school.website and not self.validator.validate_url(school.website):
            logger.warning(f"Invalid website URL for {school.name}: {school.website}")
            school.website = None
        
        # Validate rating
        if school.rating and not self.validator.validate_rating(school.rating):
            logger.warning(f"Invalid rating for {school.name}: {school.rating}")
            school.rating = None
        
        return True
    
    def clean_school(self, school: ScrapedSchool) -> ScrapedSchool:
        """Clean and normalize school data."""
        # Clean name
        if school.name:
            school.name = self.validator.clean_name(school.name)
        
        # Clean address
        if school.address:
            school.address = self.validator.clean_address(school.address)
        
        # Normalize phone
        if school.phone:
            school.phone = self.validator.normalize_phone(school.phone)
        
        # Clean URL
        if school.website and not school.website.startswith(('http://', 'https://')):
            school.website = f'https://{school.website}'
        
        return school

class DataStorage:
    """Handles storage of scraped data to both JSON and database."""
    
    def __init__(self, output_dir: str = "data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.db_manager = DatabaseManager()
        
        # Create database tables if they don't exist
        try:
            self.db_manager.create_tables()
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
    
    def save_to_json(self, schools: List[ScrapedSchool], filename: str = None) -> str:
        """Save schools to a JSON file."""
        if not filename:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"schools_{timestamp}.json"
            
        output_path = self.output_dir / filename
        
        # Convert ScrapedSchool objects to dictionaries
        school_dicts = [school.__dict__ for school in schools]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(school_dicts, f, indent=2, default=str)
            
        logger.info(f"Saved {len(school_dicts)} schools to {output_path}")
        return str(output_path)
    
    def save_to_database(self, schools: List[ScrapedSchool]) -> int:
        """Save schools to MariaDB database."""
        session = self.db_manager.get_session()
        saved_count = 0
        
        try:
            for school in schools:
                # Extract city from address or URL
                city = self._extract_city(school)
                
                # Convert courses to JSON string if present
                courses_json = None
                if school.courses:
                    courses_json = json.dumps(school.courses, default=str)
                
                # Extract success rate from courses if present
                success_rate = None
                if school.courses:
                    for course in school.courses:
                        if isinstance(course, dict) and course.get('type') == 'success_rate':
                            success_rate = course.get('value')
                            break
                
                # Create database record
                db_school = DrivingSchool(
                    name=school.name,
                    url=school.url,
                    address=school.address,
                    city=city,
                    phone=school.phone,
                    email=school.email,
                    website=school.website,
                    rating=school.rating,
                    review_count=school.review_count,
                    success_rate=success_rate,
                    price_range=school.price_range,
                    courses=courses_json,
                    source=school.source,
                    scraped_at=school.scraped_at
                )
                
                # Check for existing record to avoid duplicates
                existing = session.query(DrivingSchool).filter_by(
                    name=school.name,
                    address=school.address
                ).first()
                
                if existing:
                    # Update existing record
                    existing.phone = school.phone or existing.phone
                    existing.email = school.email or existing.email
                    existing.website = school.website or existing.website
                    existing.rating = school.rating or existing.rating
                    existing.review_count = school.review_count or existing.review_count
                    existing.success_rate = success_rate or existing.success_rate
                    existing.last_updated = datetime.utcnow()
                    logger.debug(f"Updated existing school: {school.name}")
                else:
                    # Add new record
                    session.add(db_school)
                    saved_count += 1
                    logger.debug(f"Added new school: {school.name}")
            
            session.commit()
            logger.info(f"Saved {saved_count} new schools to database")
            return saved_count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving to database: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def _extract_city(self, school: ScrapedSchool) -> str:
        """Extract city name from school data."""
        if school.address:
            # Try to extract city from address
            parts = school.address.split()
            if parts:
                return parts[-1]  # Assume last part is city
        
        if school.url and '/rijscholen/' in school.url:
            # Extract from URL
            city_part = school.url.split('/rijscholen/')[-1].split('/')[0]
            return city_part.replace('-', ' ').title()
        
        return "Unknown"

def run_scraper():
    """Run all configured scrapers with enhanced validation and database storage."""
    # Initialize components
    validator = EnhancedDataValidator()
    storage = DataStorage()
    
    # Initialize scrapers
    scrapers = [
        RijlessenNLScraper(),
        # ExampleDrivingSchoolScraper(),  # Disabled example scraper
    ]
    
    all_schools = []
    
    # Run all scrapers
    for scraper in scrapers:
        try:
            logger.info(f"Running {scraper.__class__.__name__}")
            schools = scraper.scrape()  # No limit - scrape all cities
            
            # Validate and clean each school
            valid_schools = []
            for school in schools:
                if validator.validate_school(school):
                    cleaned_school = validator.clean_school(school)
                    valid_schools.append(cleaned_school)
                else:
                    logger.warning(f"Invalid school data: {school.__dict__}")
            
            all_schools.extend(valid_schools)
            logger.info(f"Found {len(valid_schools)} valid schools from {scraper.__class__.__name__}")
            
        except Exception as e:
            logger.error(f"Error in {scraper.__class__.__name__}: {str(e)}", exc_info=True)
    
    # Deduplicate schools
    if all_schools:
        logger.info("Starting deduplication process...")
        school_dicts = [school.__dict__ for school in all_schools]
        duplicate_groups = validator.deduplicator.find_duplicates(school_dicts)
        
        if duplicate_groups:
            logger.info(f"Found {len(duplicate_groups)} duplicate groups")
            merged_dicts = validator.deduplicator.merge_duplicates(school_dicts, duplicate_groups)
            
            # Convert back to ScrapedSchool objects
            all_schools = []
            for school_dict in merged_dicts:
                school = ScrapedSchool(
                    name=school_dict.get('name'),
                    url=school_dict.get('url'),
                    address=school_dict.get('address'),
                    phone=school_dict.get('phone'),
                    email=school_dict.get('email'),
                    website=school_dict.get('website'),
                    rating=school_dict.get('rating'),
                    review_count=school_dict.get('review_count'),
                    price_range=school_dict.get('price_range'),
                    courses=school_dict.get('courses'),
                    source=school_dict.get('source'),
                    scraped_at=school_dict.get('scraped_at')
                )
                all_schools.append(school)
            
            logger.info(f"After deduplication: {len(all_schools)} unique schools")
    
    # Save results
    if all_schools:
        # Save to JSON (backup)
        storage.save_to_json(all_schools)
        
        # Save to database
        try:
            saved_count = storage.save_to_database(all_schools)
            logger.info(f"Scraping complete. Total schools found: {len(all_schools)}, Saved to DB: {saved_count}")
        except Exception as e:
            logger.error(f"Failed to save to database: {e}")
            logger.info(f"Scraping complete. Total schools found: {len(all_schools)} (JSON only)")
    else:
        logger.warning("No schools were scraped successfully.")

if __name__ == "__main__":
    run_scraper()
