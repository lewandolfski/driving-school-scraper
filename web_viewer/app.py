from flask import Flask, render_template, jsonify, request
import sqlite3
import json
from pathlib import Path
import os

app = Flask(__name__)

def get_db_connection():
    """Get database connection."""
    db_path = Path("../driving_schools_full.db")
    if not db_path.exists():
        db_path = Path("../test_schools.db")
    if not db_path.exists():
        return None
    return sqlite3.connect(str(db_path))

def get_json_data():
    """Get data from JSON files as fallback."""
    data_dir = Path("../data")
    if not data_dir.exists():
        return []
    
    json_files = list(data_dir.glob("*.json"))
    if not json_files:
        return []
    
    latest_file = max(json_files, key=lambda f: f.stat().st_mtime)
    
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return []

@app.route('/')
def index():
    """Main page showing driving schools."""
    return render_template('index.html')

@app.route('/api/schools')
def get_schools():
    """API endpoint to get schools data."""
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    search = request.args.get('search', '').strip()
    
    conn = get_db_connection()
    
    if conn:
        # Database query
        cursor = conn.cursor()
        
        where_clause = ""
        params = []
        
        if search:
            where_clause = "WHERE name LIKE ? OR city LIKE ? OR address LIKE ?"
            params = [f"%{search}%", f"%{search}%", f"%{search}%"]
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM driving_schools {where_clause}"
        cursor.execute(count_query, params)
        total = cursor.fetchone()[0]
        
        # Get paginated data
        offset = (page - 1) * per_page
        query = f"""
            SELECT name, address, city, phone, email, website, rating, review_count, success_rate, scraped_at
            FROM driving_schools 
            {where_clause}
            ORDER BY name
            LIMIT ? OFFSET ?
        """
        cursor.execute(query, params + [per_page, offset])
        
        schools = []
        for row in cursor.fetchall():
            schools.append({
                'name': row[0],
                'address': row[1],
                'city': row[2],
                'phone': row[3],
                'email': row[4],
                'website': row[5],
                'rating': row[6],
                'review_count': row[7],
                'success_rate': row[8],
                'scraped_at': row[9]
            })
        
        conn.close()
        
        return jsonify({
            'schools': schools,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        })
    
    else:
        # Fallback to JSON data
        all_schools = get_json_data()
        
        if search:
            all_schools = [s for s in all_schools if 
                          search.lower() in s.get('name', '').lower() or
                          search.lower() in s.get('address', '').lower()]
        
        total = len(all_schools)
        start = (page - 1) * per_page
        end = start + per_page
        schools = all_schools[start:end]
        
        return jsonify({
            'schools': schools,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        })

@app.route('/api/stats')
def get_stats():
    """API endpoint to get statistics."""
    conn = get_db_connection()
    
    if conn:
        cursor = conn.cursor()
        
        # Basic stats
        cursor.execute("SELECT COUNT(*) FROM driving_schools")
        total_schools = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT city) FROM driving_schools")
        unique_cities = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE phone IS NOT NULL AND phone != ''")
        with_phones = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE rating IS NOT NULL")
        with_ratings = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM driving_schools WHERE success_rate IS NOT NULL")
        with_success_rates = cursor.fetchone()[0]
        
        # Top cities
        cursor.execute("""
            SELECT city, COUNT(*) as count 
            FROM driving_schools 
            GROUP BY city 
            ORDER BY count DESC 
            LIMIT 10
        """)
        top_cities = [{'city': row[0], 'count': row[1]} for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'total_schools': total_schools,
            'unique_cities': unique_cities,
            'with_phones': with_phones,
            'with_ratings': with_ratings,
            'with_success_rates': with_success_rates,
            'phone_percentage': round((with_phones / total_schools) * 100, 1) if total_schools > 0 else 0,
            'rating_percentage': round((with_ratings / total_schools) * 100, 1) if total_schools > 0 else 0,
            'success_rate_percentage': round((with_success_rates / total_schools) * 100, 1) if total_schools > 0 else 0,
            'top_cities': top_cities
        })
    
    else:
        # Fallback to JSON data
        all_schools = get_json_data()
        total_schools = len(all_schools)
        
        with_phones = len([s for s in all_schools if s.get('phone')])
        with_ratings = len([s for s in all_schools if s.get('rating')])
        with_success_rates = len([s for s in all_schools if s.get('success_rate')])
        
        cities = {}
        for school in all_schools:
            city = school.get('address', 'Unknown')
            cities[city] = cities.get(city, 0) + 1
        
        top_cities = [{'city': k, 'count': v} for k, v in sorted(cities.items(), key=lambda x: x[1], reverse=True)[:10]]
        
        return jsonify({
            'total_schools': total_schools,
            'unique_cities': len(cities),
            'with_phones': with_phones,
            'with_ratings': with_ratings,
            'with_success_rates': with_success_rates,
            'phone_percentage': round((with_phones / total_schools) * 100, 1) if total_schools > 0 else 0,
            'rating_percentage': round((with_ratings / total_schools) * 100, 1) if total_schools > 0 else 0,
            'success_rate_percentage': round((with_success_rates / total_schools) * 100, 1) if total_schools > 0 else 0,
            'top_cities': top_cities
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
