import sqlite3
import json
from pathlib import Path

def verify_test_data():
    """Verify the current test data storage."""
    print("🔍 VERIFYING DATA STORAGE")
    print("=" * 50)
    
    # Check SQLite database
    if Path("test_schools.db").exists():
        conn = sqlite3.connect("test_schools.db")
        cursor = conn.cursor()
        
        # Get counts
        cursor.execute("SELECT COUNT(*) FROM driving_schools")
        total_schools = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT city) FROM driving_schools")
        unique_cities = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE name IS NOT NULL AND name != ''")
        schools_with_names = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE address IS NOT NULL AND address != ''")
        schools_with_addresses = cursor.fetchone()[0]
        
        print(f"📊 SQLite Database Results:")
        print(f"   Total schools: {total_schools}")
        print(f"   Unique cities: {unique_cities}")
        print(f"   Schools with names: {schools_with_names}")
        print(f"   Schools with addresses: {schools_with_addresses}")
        print(f"   Data integrity: {100 if schools_with_names == total_schools else 'ISSUES'}")
        
        # Sample data
        cursor.execute("SELECT name, city, address FROM driving_schools LIMIT 3")
        samples = cursor.fetchall()
        print(f"\n📋 Sample entries:")
        for i, (name, city, address) in enumerate(samples, 1):
            print(f"   {i}. {name} in {city}")
        
        conn.close()
        
        return total_schools > 0
    else:
        print("❌ No SQLite database found")
        return False

def verify_json_data():
    """Verify JSON backup files."""
    data_dir = Path("data")
    if not data_dir.exists():
        print("❌ No data directory found")
        return False
    
    json_files = list(data_dir.glob("*.json"))
    if not json_files:
        print("❌ No JSON files found")
        return False
    
    latest_file = max(json_files, key=lambda f: f.stat().st_mtime)
    print(f"\n📁 Latest JSON file: {latest_file.name}")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"   Schools in JSON: {len(data)}")
    print(f"   Sample school: {data[0]['name'] if data else 'None'}")
    
    return len(data) > 0

if __name__ == "__main__":
    db_ok = verify_test_data()
    json_ok = verify_json_data()
    
    if db_ok and json_ok:
        print(f"\n✅ Data storage verification PASSED")
        print(f"💡 Ready to run full scraper on all cities!")
    else:
        print(f"\n❌ Data storage verification FAILED")
