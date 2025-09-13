import json
import sqlite3
from pathlib import Path
from datetime import datetime
from loguru import logger

# Import our scraper components
from scraper.rijlessen_nl_scraper import RijlessenNLScraper
from scraper.base_scraper import ScrapedSchool

# Configure logging
logger.add("logs/test_sqlite_{time:YYYY-MM-DD}.log", rotation="500 MB", level="INFO")

def create_sqlite_db():
    """Create a simple SQLite database for testing."""
    db_path = Path("test_schools.db")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table
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
            source TEXT,
            scraped_at TEXT
        )
    ''')
    
    conn.commit()
    return conn

def save_to_sqlite(schools, conn):
    """Save schools to SQLite database."""
    cursor = conn.cursor()
    saved_count = 0
    
    for school in schools:
        # Extract city from address
        city = school.address if school.address else "Unknown"
        
        # Check for existing record
        cursor.execute(
            "SELECT id FROM driving_schools WHERE name = ? AND address = ?",
            (school.name, school.address)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Update existing
            cursor.execute('''
                UPDATE driving_schools 
                SET phone = ?, email = ?, website = ?, rating = ?, review_count = ?
                WHERE id = ?
            ''', (school.phone, school.email, school.website, school.rating, school.review_count, existing[0]))
        else:
            # Insert new
            cursor.execute('''
                INSERT INTO driving_schools 
                (name, url, address, city, phone, email, website, rating, review_count, source, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                school.name, school.url, school.address, city, school.phone,
                school.email, school.website, school.rating, school.review_count,
                school.source, str(school.scraped_at)
            ))
            saved_count += 1
    
    conn.commit()
    return saved_count

def view_sqlite_data(conn):
    """View data from SQLite database."""
    cursor = conn.cursor()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM driving_schools")
    total_count = cursor.fetchone()[0]
    
    print(f"\n🗄️ SQLITE DATABASE RESULTS")
    print("=" * 50)
    print(f"Total schools in database: {total_count}")
    
    if total_count == 0:
        print("❌ No schools found in database.")
        return
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE phone IS NOT NULL")
    with_phones = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE rating IS NOT NULL")
    with_ratings = cursor.fetchone()[0]
    
    print(f"\n📊 DATABASE STATISTICS")
    print("-" * 30)
    print(f"Schools with phone numbers: {with_phones}")
    print(f"Schools with ratings: {with_ratings}")
    
    # Top cities
    cursor.execute('''
        SELECT city, COUNT(*) as count 
        FROM driving_schools 
        GROUP BY city 
        ORDER BY count DESC 
        LIMIT 10
    ''')
    city_stats = cursor.fetchall()
    
    print(f"\n🏙️ TOP CITIES IN DATABASE")
    print("-" * 30)
    for city, count in city_stats:
        print(f"{city}: {count} schools")
    
    # Sample schools
    cursor.execute("SELECT * FROM driving_schools LIMIT 5")
    sample_schools = cursor.fetchall()
    
    print(f"\n📋 SAMPLE SCHOOLS FROM DATABASE")
    print("-" * 30)
    for i, school in enumerate(sample_schools, 1):
        print(f"\n{i}. {school[1]}")  # name
        print(f"   📍 City: {school[4]}")  # city
        print(f"   📞 Phone: {school[5] or 'N/A'}")  # phone
        print(f"   ⭐ Rating: {school[8] or 'N/A'}")  # rating
        print(f"   📅 Scraped: {school[12]}")  # scraped_at

def test_scraper_with_database():
    """Test the scraper and save to SQLite database."""
    print("🚀 TESTING SCRAPER WITH DATABASE STORAGE")
    print("=" * 50)
    
    # Create output directories
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    # Create SQLite database
    print("📦 Creating SQLite database...")
    conn = create_sqlite_db()
    
    # Initialize scraper
    scraper = RijlessenNLScraper()
    
    print("📡 Starting test scrape (limited to 3 cities)...")
    
    try:
        # Test with just 3 cities
        schools = scraper.scrape(max_cities=3)
        
        print(f"\n✅ Scraping completed!")
        print(f"📊 Found {len(schools)} schools")
        
        if schools:
            # Save to JSON (backup)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"test_schools_with_db_{timestamp}.json"
            output_path = Path("data") / filename
            
            school_dicts = [school.__dict__ for school in schools]
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(school_dicts, f, indent=2, default=str)
            
            print(f"💾 Saved JSON backup to: {output_path}")
            
            # Save to SQLite database
            print("🗄️ Saving to SQLite database...")
            saved_count = save_to_sqlite(schools, conn)
            print(f"✅ Saved {saved_count} new schools to database")
            
            # View database results
            view_sqlite_data(conn)
            
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
    success = test_scraper_with_database()
    
    if success:
        print(f"\n🎉 Test with database completed successfully!")
        print(f"💡 Database saved as: test_schools.db")
        print(f"💡 You can open this with any SQLite browser to view the data")
    else:
        print(f"\n❌ Test failed. Check logs for details.")
