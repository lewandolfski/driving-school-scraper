import json
import sqlite3
from pathlib import Path
from datetime import datetime
from loguru import logger

# Import our scraper components
from scraper.rijlessen_nl_scraper import RijlessenNLScraper
# from utils.validators import EnhancedDataValidator

# Configure logging
logger.add("logs/full_scrape_{time:YYYY-MM-DD}.log", rotation="500 MB", level="INFO")

def create_production_sqlite_db():
    """Create a production SQLite database for the full scrape."""
    db_path = Path("driving_schools_full.db")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table with all fields
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS driving_schools (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            url TEXT,
            address TEXT,
            city TEXT,
            phone TEXT,
            email TEXT,
            website TEXT,
            rating REAL,
            review_count INTEGER,
            success_rate INTEGER,
            price_range TEXT,
            courses TEXT,
            source TEXT,
            scraped_at TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, address) ON CONFLICT REPLACE
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_city ON driving_schools(city)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_name ON driving_schools(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rating ON driving_schools(rating)')
    
    conn.commit()
    return conn

def save_batch_to_sqlite(schools, conn, batch_size=100):
    """Save schools to SQLite database in batches for better performance."""
    cursor = conn.cursor()
    saved_count = 0
    
    for i in range(0, len(schools), batch_size):
        batch = schools[i:i + batch_size]
        
        for school in batch:
            # Extract city from address
            city = school.address if school.address else "Unknown"
            
            # Insert with REPLACE to handle duplicates
            cursor.execute('''
                INSERT OR REPLACE INTO driving_schools 
                (name, url, address, city, phone, email, website, rating, review_count, 
                 success_rate, price_range, courses, source, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                school.name, school.url, school.address, city, school.phone,
                school.email, school.website, school.rating, school.review_count,
                school.success_rate, school.price_range, school.courses,
                school.source, str(school.scraped_at)
            ))
            saved_count += 1
        
        conn.commit()
        print(f"💾 Saved batch {i//batch_size + 1}, total: {saved_count} schools")
    
    return saved_count

def get_database_stats(conn):
    """Get comprehensive database statistics."""
    cursor = conn.cursor()
    
    # Basic counts
    cursor.execute("SELECT COUNT(*) FROM driving_schools")
    total_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT city) FROM driving_schools")
    unique_cities = cursor.fetchone()[0]
    
    # Data quality metrics
    cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE phone IS NOT NULL AND phone != ''")
    with_phones = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE email IS NOT NULL AND email != ''")
    with_emails = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE website IS NOT NULL AND website != ''")
    with_websites = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE rating IS NOT NULL")
    with_ratings = cursor.fetchone()[0]
    
    return {
        'total_schools': total_count,
        'unique_cities': unique_cities,
        'with_phones': with_phones,
        'with_emails': with_emails,
        'with_websites': with_websites,
        'with_ratings': with_ratings
    }

def run_full_scrape_with_sqlite():
    """Run the full scraper and save everything to SQLite."""
    print("🚀 FULL DRIVING SCHOOL SCRAPER - SQLITE VERSION")
    print("=" * 60)
    
    # Create output directories
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    # Create production SQLite database
    print("📦 Creating production SQLite database...")
    conn = create_production_sqlite_db()
    
    # Initialize scraper
    scraper = RijlessenNLScraper()
    
    print("📡 Starting FULL scrape of ALL cities...")
    print("⏱️ This will take a while - scraping 2,405+ cities...")
    
    try:
        # Run full scrape (no city limit)
        schools = scraper.scrape()  # No max_cities parameter = scrape all
        
        print(f"\n✅ Scraping completed!")
        print(f"📊 Found {len(schools)} schools total")
        
        if schools:
            print(f"✅ Found {len(schools)} schools total")
            deduplicated_schools = schools
            
            # Save to JSON (backup)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"driving_schools_full_{timestamp}.json"
            output_path = Path("data") / filename
            
            school_dicts = [school.__dict__ for school in deduplicated_schools]
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(school_dicts, f, indent=2, default=str)
            
            print(f"💾 Saved JSON backup to: {output_path}")
            
            # Save to SQLite database
            print("🗄️ Saving to SQLite database...")
            saved_count = save_batch_to_sqlite(deduplicated_schools, conn)
            print(f"✅ Saved {saved_count} schools to database")
            
            # Get final statistics
            stats = get_database_stats(conn)
            
            print(f"\n📊 FINAL RESULTS")
            print("=" * 40)
            print(f"Total schools: {stats['total_schools']:,}")
            print(f"Unique cities: {stats['unique_cities']:,}")
            print(f"Schools with phones: {stats['with_phones']:,} ({stats['with_phones']/stats['total_schools']*100:.1f}%)")
            print(f"Schools with emails: {stats['with_emails']:,} ({stats['with_emails']/stats['total_schools']*100:.1f}%)")
            print(f"Schools with websites: {stats['with_websites']:,} ({stats['with_websites']/stats['total_schools']*100:.1f}%)")
            print(f"Schools with ratings: {stats['with_ratings']:,} ({stats['with_ratings']/stats['total_schools']*100:.1f}%)")
            
            return True
        else:
            print("❌ No schools were found")
            return False
            
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        logger.error(f"Scraping error: {e}", exc_info=True)
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    success = run_full_scrape_with_sqlite()
    
    if success:
        print(f"\n🎉 FULL SCRAPE COMPLETED SUCCESSFULLY!")
        print(f"💾 Database: driving_schools_full.db")
        print(f"📁 JSON backup in data/ folder")
        print(f"📊 Ready for integration with Windsurf backend!")
    else:
        print(f"\n❌ Full scrape failed. Check logs for details.")
