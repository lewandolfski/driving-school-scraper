import sqlite3
import json
from pathlib import Path

def export_data_for_deployment():
    """Export database data to JSON for online deployment."""
    print("📤 EXPORTING DATA FOR ONLINE DEPLOYMENT")
    print("=" * 50)
    
    # Try to find the database
    db_paths = [
        Path("../driving_schools_full.db"),
        Path("../test_schools.db"),
        Path("../enhanced_schools.db")
    ]
    
    db_path = None
    for path in db_paths:
        if path.exists():
            db_path = path
            break
    
    if not db_path:
        # Fallback to JSON files
        data_dir = Path("../data")
        if data_dir.exists():
            json_files = list(data_dir.glob("*.json"))
            if json_files:
                latest_file = max(json_files, key=lambda f: f.stat().st_mtime)
                print(f"📁 Using JSON file: {latest_file}")
                
                with open(latest_file, 'r', encoding='utf-8') as f:
                    schools = json.load(f)
                
                # Export for web deployment
                output_path = Path("static/schools_data.json")
                output_path.parent.mkdir(exist_ok=True)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(schools, f, indent=2, default=str)
                
                print(f"✅ Exported {len(schools)} schools to {output_path}")
                return len(schools)
        
        print("❌ No database or JSON files found")
        return 0
    
    print(f"🗄️ Using database: {db_path}")
    
    # Connect to database and export all data
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Get all schools
    cursor.execute("""
        SELECT name, address, city, phone, email, website, rating, review_count, 
               success_rate, source, scraped_at
        FROM driving_schools
        ORDER BY name
    """)
    
    schools = []
    for row in cursor.fetchall():
        school = {
            'name': row[0],
            'address': row[1],
            'city': row[2],
            'phone': row[3],
            'email': row[4],
            'website': row[5],
            'rating': row[6],
            'review_count': row[7],
            'success_rate': row[8],
            'source': row[9],
            'scraped_at': row[10]
        }
        schools.append(school)
    
    conn.close()
    
    # Create static directory and export data
    static_dir = Path("static")
    static_dir.mkdir(exist_ok=True)
    
    output_path = static_dir / "schools_data.json"
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(schools, f, indent=2, default=str)
    
    print(f"✅ Exported {len(schools)} schools to {output_path}")
    
    # Create statistics
    stats = {
        'total_schools': len(schools),
        'unique_cities': len(set(s['city'] for s in schools if s['city'])),
        'with_phones': len([s for s in schools if s['phone']]),
        'with_ratings': len([s for s in schools if s['rating']]),
        'with_success_rates': len([s for s in schools if s['success_rate']]),
        'with_emails': len([s for s in schools if s['email']]),
        'with_websites': len([s for s in schools if s['website']])
    }
    
    # Calculate percentages
    if stats['total_schools'] > 0:
        stats['phone_percentage'] = round((stats['with_phones'] / stats['total_schools']) * 100, 1)
        stats['rating_percentage'] = round((stats['with_ratings'] / stats['total_schools']) * 100, 1)
        stats['success_rate_percentage'] = round((stats['with_success_rates'] / stats['total_schools']) * 100, 1)
    
    # Top cities
    cities = {}
    for school in schools:
        city = school['city'] or 'Unknown'
        cities[city] = cities.get(city, 0) + 1
    
    stats['top_cities'] = [{'city': k, 'count': v} for k, v in sorted(cities.items(), key=lambda x: x[1], reverse=True)[:10]]
    
    stats_path = static_dir / "stats.json"
    with open(stats_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)
    
    print(f"✅ Exported statistics to {stats_path}")
    print(f"\n📊 EXPORT SUMMARY:")
    print(f"   Total schools: {stats['total_schools']:,}")
    print(f"   Cities: {stats['unique_cities']:,}")
    print(f"   With phones: {stats['with_phones']:,} ({stats.get('phone_percentage', 0)}%)")
    print(f"   With ratings: {stats['with_ratings']:,} ({stats.get('rating_percentage', 0)}%)")
    
    return len(schools)

if __name__ == "__main__":
    export_data_for_deployment()
