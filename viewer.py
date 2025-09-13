import json
import os
from pathlib import Path
from datetime import datetime
import pandas as pd

def view_json_data():
    """View scraped data from JSON files."""
    data_dir = Path("data")
    
    if not data_dir.exists():
        print("❌ No data directory found. Run the scraper first.")
        return
    
    json_files = list(data_dir.glob("*.json"))
    
    if not json_files:
        print("❌ No JSON files found. Run the scraper first.")
        return
    
    # Get the most recent file
    latest_file = max(json_files, key=os.path.getctime)
    print(f"📁 Reading data from: {latest_file}")
    
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            schools = json.load(f)
        
        print(f"\n📊 SCRAPER RESULTS SUMMARY")
        print("=" * 50)
        print(f"Total schools found: {len(schools)}")
        
        if not schools:
            print("❌ No schools in the file.")
            return
        
        # Convert to DataFrame for better analysis
        df = pd.DataFrame(schools)
        
        # Basic statistics
        print(f"\n📈 DATA QUALITY METRICS")
        print("-" * 30)
        print(f"Schools with names: {df['name'].notna().sum()}")
        print(f"Schools with addresses: {df['address'].notna().sum()}")
        print(f"Schools with phones: {df['phone'].notna().sum()}")
        print(f"Schools with emails: {df['email'].notna().sum()}")
        print(f"Schools with websites: {df['website'].notna().sum()}")
        print(f"Schools with ratings: {df['rating'].notna().sum()}")
        
        # Show sample data
        print(f"\n📋 SAMPLE SCHOOLS")
        print("-" * 30)
        for i, school in enumerate(schools[:5]):
            print(f"\n{i+1}. {school.get('name', 'Unknown')}")
            print(f"   📍 Address: {school.get('address', 'N/A')}")
            print(f"   📞 Phone: {school.get('phone', 'N/A')}")
            print(f"   ⭐ Rating: {school.get('rating', 'N/A')}")
            print(f"   🌐 Website: {school.get('website', 'N/A')}")
        
        # City distribution
        if 'address' in df.columns:
            cities = df['address'].value_counts().head(10)
            print(f"\n🏙️ TOP 10 CITIES")
            print("-" * 30)
            for city, count in cities.items():
                print(f"{city}: {count} schools")
        
        print(f"\n✅ Data viewing complete!")
        
    except Exception as e:
        print(f"❌ Error reading JSON file: {e}")

def view_database_data():
    """View data from the database."""
    try:
        from database.models import DatabaseManager, DrivingSchool
        from sqlalchemy import func
        
        db = DatabaseManager()
        session = db.get_session()
        
        # Check if table exists and has data
        try:
            total_count = session.query(DrivingSchool).count()
            print(f"\n🗄️ DATABASE RESULTS")
            print("=" * 50)
            print(f"Total schools in database: {total_count}")
            
            if total_count == 0:
                print("❌ No schools found in database.")
                return
            
            # Get some statistics
            active_count = session.query(DrivingSchool).filter_by(is_active=True).count()
            with_ratings = session.query(DrivingSchool).filter(DrivingSchool.rating.isnot(None)).count()
            with_phones = session.query(DrivingSchool).filter(DrivingSchool.phone.isnot(None)).count()
            
            print(f"\n📊 DATABASE STATISTICS")
            print("-" * 30)
            print(f"Active schools: {active_count}")
            print(f"Schools with ratings: {with_ratings}")
            print(f"Schools with phone numbers: {with_phones}")
            
            # Top cities
            city_stats = session.query(
                DrivingSchool.city, 
                func.count(DrivingSchool.id).label('count')
            ).group_by(DrivingSchool.city).order_by(func.count(DrivingSchool.id).desc()).limit(10).all()
            
            print(f"\n🏙️ TOP 10 CITIES IN DATABASE")
            print("-" * 30)
            for city, count in city_stats:
                print(f"{city}: {count} schools")
            
            # Sample schools
            sample_schools = session.query(DrivingSchool).limit(5).all()
            print(f"\n📋 SAMPLE SCHOOLS FROM DATABASE")
            print("-" * 30)
            for i, school in enumerate(sample_schools, 1):
                print(f"\n{i}. {school.name}")
                print(f"   📍 City: {school.city}")
                print(f"   📞 Phone: {school.phone or 'N/A'}")
                print(f"   ⭐ Rating: {school.rating or 'N/A'}")
                print(f"   📅 Scraped: {school.scraped_at}")
            
            print(f"\n✅ Database viewing complete!")
            
        except Exception as e:
            print(f"❌ Error querying database: {e}")
            print("💡 Make sure the database is set up and the scraper has run.")
        
        finally:
            session.close()
            
    except ImportError as e:
        print(f"❌ Database modules not available: {e}")
        print("💡 Install dependencies: pip install pymysql sqlalchemy")
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        print("💡 Check your database configuration in .env file")

def main():
    """Main viewer function."""
    print("🔍 DRIVING SCHOOL SCRAPER DATA VIEWER")
    print("=" * 50)
    
    print("\n1. Viewing JSON data...")
    view_json_data()
    
    print("\n" + "="*50)
    print("2. Viewing database data...")
    view_database_data()

if __name__ == "__main__":
    main()
