import os
import json
import time
from pathlib import Path
from datetime import datetime
from loguru import logger
import psycopg2
from sqlalchemy import create_engine, text

# Import our scraper components
from scraper.rijlessen_nl_scraper import RijlessenNLScraper

# Configure logging for production
logger.add("logs/production_scrape_{time:YYYY-MM-DD}.log", rotation="100 MB", level="INFO")

def create_production_database():
    """Create production PostgreSQL database schema."""
    database_url = os.getenv('DATABASE_URL', 'postgresql://scraper_user:password@localhost:5432/driving_schools')
    
    engine = create_engine(database_url)
    
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
    """
    
    with engine.connect() as conn:
        conn.execute(text(schema_sql))
        conn.commit()
    
    return engine

def save_to_database(schools, engine, batch_size=100):
    """Save schools to PostgreSQL database in batches."""
    saved_count = 0
    
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
    
    with engine.connect() as conn:
        for i in range(0, len(schools), batch_size):
            batch = schools[i:i + batch_size]
            
            batch_data = []
            for school in batch:
                # Extract city from address
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
            
            conn.execute(text(insert_sql), batch_data)
            conn.commit()
            saved_count += len(batch)
            
            logger.info(f"Saved batch {i//batch_size + 1}, total: {saved_count} schools")
    
    return saved_count

def run_production_scrape():
    """Run the full production scraper with cloud database storage."""
    logger.info("🚀 STARTING PRODUCTION DRIVING SCHOOL SCRAPER")
    logger.info("=" * 60)
    
    # Create output directories
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    # Create production database
    logger.info("📦 Setting up production database...")
    engine = create_production_database()
    
    # Initialize scraper
    scraper = RijlessenNLScraper()
    
    logger.info("📡 Starting FULL scrape of ALL 2,405+ cities...")
    logger.info("⏱️ Estimated time: 40-50 hours")
    
    start_time = datetime.now()
    
    try:
        # Run full scrape (no city limit)
        schools = scraper.scrape()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"✅ Scraping completed in {duration}")
        logger.info(f"📊 Found {len(schools)} schools total")
        
        if schools:
            # Save to JSON backup
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"driving_schools_production_{timestamp}.json"
            output_path = Path("data") / filename
            
            school_dicts = [school.__dict__ for school in schools]
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(school_dicts, f, indent=2, default=str)
            
            logger.info(f"💾 Saved JSON backup to: {output_path}")
            
            # Save to production database
            logger.info("🗄️ Saving to production database...")
            saved_count = save_to_database(schools, engine)
            logger.info(f"✅ Saved {saved_count} schools to database")
            
            # Get final statistics
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM driving_schools")).fetchone()
                total_count = result[0]
                
                result = conn.execute(text("SELECT COUNT(DISTINCT city) FROM driving_schools")).fetchone()
                unique_cities = result[0]
                
                result = conn.execute(text("SELECT COUNT(*) FROM driving_schools WHERE phone IS NOT NULL")).fetchone()
                with_phones = result[0]
                
                result = conn.execute(text("SELECT COUNT(*) FROM driving_schools WHERE rating IS NOT NULL")).fetchone()
                with_ratings = result[0]
            
            logger.info(f"📊 FINAL PRODUCTION STATISTICS:")
            logger.info(f"   Total schools: {total_count:,}")
            logger.info(f"   Unique cities: {unique_cities:,}")
            logger.info(f"   With phones: {with_phones:,} ({with_phones/total_count*100:.1f}%)")
            logger.info(f"   With ratings: {with_ratings:,} ({with_ratings/total_count*100:.1f}%)")
            
            return True
        else:
            logger.error("❌ No schools were found")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error during scraping: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = run_production_scrape()
    
    if success:
        logger.info("🎉 PRODUCTION SCRAPE COMPLETED SUCCESSFULLY!")
        logger.info("🌐 Database ready for API access")
    else:
        logger.error("❌ Production scrape failed")
        exit(1)
