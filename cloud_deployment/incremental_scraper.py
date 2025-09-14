import os
import json
import time
import signal
import sys
from pathlib import Path
from datetime import datetime
from loguru import logger
import psycopg2
from sqlalchemy import create_engine, text

# Import our scraper components
from scraper.rijlessen_nl_scraper import RijlessenNLScraper
from bs4 import BeautifulSoup

# Configure logging for production
logger.add("logs/incremental_scrape_{time:YYYY-MM-DD}.log", rotation="100 MB", level="INFO")

class IncrementalScraper:
    def __init__(self):
        self.engine = None
        self.scraper = None
        self.progress_file = "data/scrape_progress.json"
        self.should_stop = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.should_stop = True
    
    def create_database_schema(self):
        """Create production PostgreSQL database schema."""
        db_host = os.getenv('DB_HOST', 'db')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'driving_schools')
        db_user = os.getenv('DB_USER', 'scraper_user')
        db_password = os.getenv('DB_PASSWORD', '')
        
        database_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        self.engine = create_engine(database_url)
        
        # Create table schema
        schema_sql = """
        CREATE TABLE IF NOT EXISTS driving_schools (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            url TEXT,
            address TEXT,
            city VARCHAR(100),
            phone VARCHAR(50),
            email VARCHAR(100),
            website TEXT,
            rating DECIMAL(3,2),
            review_count INTEGER,
            success_rate INTEGER,
            source VARCHAR(100),
            scraped_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, address)
        );
        
        CREATE INDEX IF NOT EXISTS idx_city ON driving_schools(city);
        CREATE INDEX IF NOT EXISTS idx_name ON driving_schools(name);
        CREATE INDEX IF NOT EXISTS idx_rating ON driving_schools(rating);
        CREATE INDEX IF NOT EXISTS idx_success_rate ON driving_schools(success_rate);
        
        -- Progress tracking table
        CREATE TABLE IF NOT EXISTS scrape_progress (
            id SERIAL PRIMARY KEY,
            city_url TEXT UNIQUE,
            city_index INTEGER,
            total_cities INTEGER,
            schools_found INTEGER,
            completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(schema_sql))
            conn.commit()
        
        logger.info("Database schema created successfully")
        return self.engine
    
    def save_schools_batch(self, schools, city_url, city_index, total_cities):
        """Save a batch of schools to the database immediately."""
        if not schools:
            return 0
        
        insert_sql = """
        INSERT INTO driving_schools 
        (name, url, address, city, phone, email, website, rating, review_count, success_rate, source, scraped_at)
        VALUES (%(name)s, %(url)s, %(address)s, %(city)s, %(phone)s, %(email)s, %(website)s, 
                %(rating)s, %(review_count)s, %(success_rate)s, %(source)s, %(scraped_at)s)
        ON CONFLICT (name, address) DO UPDATE SET
            phone = EXCLUDED.phone,
            email = EXCLUDED.email,
            website = EXCLUDED.website,
            rating = EXCLUDED.rating,
            review_count = EXCLUDED.review_count,
            success_rate = EXCLUDED.success_rate,
            updated_at = CURRENT_TIMESTAMP
        """
        
        progress_sql = """
        INSERT INTO scrape_progress (city_url, city_index, total_cities, schools_found)
        VALUES (%(city_url)s, %(city_index)s, %(total_cities)s, %(schools_found)s)
        ON CONFLICT (city_url) DO UPDATE SET
            schools_found = EXCLUDED.schools_found,
            completed_at = CURRENT_TIMESTAMP
        """
        
        saved_count = 0
        
        try:
            with self.engine.connect() as conn:
                # Save schools
                batch_data = []
                for school in schools:
                    # Extract city from address or URL
                    city = school.address if school.address else "Unknown"
                    if len(city) > 100:
                        city = city[:100]
                    
                    batch_data.append({
                        'name': school.name,
                        'url': school.url,
                        'address': school.address,
                        'city': city,
                        'phone': school.phone,
                        'email': school.email,
                        'website': school.website,
                        'rating': school.rating,
                        'review_count': school.review_count,
                        'success_rate': school.success_rate,
                        'source': school.source,
                        'scraped_at': school.scraped_at
                    })
                
                if batch_data:
                    conn.execute(text(insert_sql), batch_data)
                    saved_count = len(batch_data)
                
                # Save progress
                conn.execute(text(progress_sql), {
                    'city_url': city_url,
                    'city_index': city_index,
                    'total_cities': total_cities,
                    'schools_found': len(schools)
                })
                
                conn.commit()
                
                logger.info(f"✅ Saved {saved_count} schools from city {city_index}/{total_cities}")
                
        except Exception as e:
            logger.error(f"❌ Error saving schools: {e}")
            
        return saved_count
    
    def get_completed_cities(self):
        """Get list of already completed cities to resume from where we left off."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT city_url FROM scrape_progress")).fetchall()
                return {row[0] for row in result}
        except:
            return set()
    
    def run_incremental_scrape(self):
        """Run incremental scraper that saves data as it goes."""
        logger.info("🚀 STARTING INCREMENTAL DRIVING SCHOOL SCRAPER")
        logger.info("=" * 60)
        
        # Create output directories
        Path("data").mkdir(exist_ok=True)
        Path("logs").mkdir(exist_ok=True)
        
        # Create database
        logger.info("📦 Setting up database...")
        self.create_database_schema()
        
        # Initialize scraper
        self.scraper = RijlessenNLScraper()
        
        logger.info("📡 Starting incremental scrape with real-time database saves...")
        
        start_time = datetime.now()
        total_saved = 0
        
        try:
            # Get city links
            main_url = f"{self.scraper.base_url}/rijscholen"
            html = self.scraper._fetch_page(main_url)
            if not html:
                logger.error("Could not fetch main rijscholen page")
                return False
            
            soup = BeautifulSoup(html, 'html.parser')
            city_links = self.scraper._parse_city_links(soup)
            
            if not city_links:
                logger.warning("No city links found")
                return False
            
            logger.info(f"Found {len(city_links)} cities to scrape")
            
            # Get already completed cities
            completed_cities = self.get_completed_cities()
            logger.info(f"Resuming: {len(completed_cities)} cities already completed")
            
            # Process each city
            for i, city_url in enumerate(city_links, 1):
                if self.should_stop:
                    logger.info("Graceful shutdown requested, stopping...")
                    break
                
                if city_url in completed_cities:
                    logger.debug(f"Skipping already completed city {i}/{len(city_links)}: {city_url}")
                    continue
                
                logger.info(f"Scraping city {i}/{len(city_links)}: {city_url}")
                
                # Scrape this city
                html = self.scraper._fetch_page(city_url)
                if not html:
                    continue
                
                soup = BeautifulSoup(html, 'html.parser')
                schools = self.scraper._parse_school_from_city_page(soup, city_url)
                
                if schools:
                    # Enhance schools with details
                    enhanced_schools = []
                    for school in schools:
                        if self.should_stop:
                            break
                            
                        if school.url and '/rijschool-' in school.url:
                            logger.debug(f"Fetching details for {school.name}")
                            enhanced_school = self.scraper._scrape_school_details(school)
                            enhanced_schools.append(enhanced_school)
                            time.sleep(0.5)  # Rate limit
                        else:
                            enhanced_schools.append(school)
                    
                    # Save immediately to database
                    saved_count = self.save_schools_batch(enhanced_schools, city_url, i, len(city_links))
                    total_saved += saved_count
                    
                    logger.info(f"City {i}/{len(city_links)} complete: {len(schools)} schools found, {saved_count} saved")
                else:
                    # Still record progress even if no schools found
                    self.save_schools_batch([], city_url, i, len(city_links))
                    logger.debug(f"No schools found in {city_url}")
                
                # Be nice to the server
                time.sleep(1)
                
                # Log progress every 10 cities
                if i % 10 == 0:
                    elapsed = datetime.now() - start_time
                    rate = i / elapsed.total_seconds() * 3600  # cities per hour
                    remaining_hours = (len(city_links) - i) / rate if rate > 0 else 0
                    
                    logger.info(f"📊 Progress: {i}/{len(city_links)} cities ({i/len(city_links)*100:.1f}%)")
                    logger.info(f"⏱️ Rate: {rate:.1f} cities/hour, ETA: {remaining_hours:.1f} hours")
                    logger.info(f"💾 Total schools saved: {total_saved:,}")
            
            # Final statistics
            end_time = datetime.now()
            duration = end_time - start_time
            
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM driving_schools")).fetchone()
                total_count = result[0]
                
                result = conn.execute(text("SELECT COUNT(DISTINCT city) FROM driving_schools")).fetchone()
                unique_cities = result[0]
                
                result = conn.execute(text("SELECT COUNT(*) FROM driving_schools WHERE phone IS NOT NULL")).fetchone()
                with_phones = result[0]
                
                result = conn.execute(text("SELECT COUNT(*) FROM driving_schools WHERE rating IS NOT NULL")).fetchone()
                with_ratings = result[0]
            
            logger.info("🎉 INCREMENTAL SCRAPE COMPLETED!")
            logger.info(f"⏱️ Duration: {duration}")
            logger.info(f"📊 FINAL STATISTICS:")
            logger.info(f"   Total schools in DB: {total_count:,}")
            logger.info(f"   Unique cities: {unique_cities:,}")
            logger.info(f"   With phones: {with_phones:,} ({with_phones/total_count*100:.1f}%)")
            logger.info(f"   With ratings: {with_ratings:,} ({with_ratings/total_count*100:.1f}%)")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during scraping: {e}", exc_info=True)
            return False

def main():
    scraper = IncrementalScraper()
    success = scraper.run_incremental_scrape()
    
    if success:
        logger.info("✅ Scraping completed successfully!")
    else:
        logger.error("❌ Scraping failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
