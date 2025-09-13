from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from typing import List, Optional
import os
from pydantic import BaseModel

app = FastAPI(title="Driving Schools API", version="1.0.0")

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://scraper_user:password@localhost:5432/driving_schools')
engine = create_engine(DATABASE_URL)

class DrivingSchool(BaseModel):
    id: int
    name: str
    address: Optional[str] = None
    city: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    success_rate: Optional[int] = None
    source: Optional[str] = None

class StatsResponse(BaseModel):
    total_schools: int
    unique_cities: int
    with_phones: int
    with_ratings: int
    with_success_rates: int
    phone_percentage: float
    rating_percentage: float
    success_rate_percentage: float

@app.get("/")
async def root():
    return {"message": "Driving Schools API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")

@app.get("/api/schools", response_model=List[DrivingSchool])
async def get_schools(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    search: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    min_rating: Optional[float] = Query(None, ge=0, le=5),
    min_success_rate: Optional[int] = Query(None, ge=0, le=100)
):
    """Get paginated list of driving schools with optional filters."""
    
    # Build WHERE clause
    where_conditions = []
    params = {}
    
    if search:
        where_conditions.append("(name ILIKE %(search)s OR address ILIKE %(search)s)")
        params['search'] = f"%{search}%"
    
    if city:
        where_conditions.append("city ILIKE %(city)s")
        params['city'] = f"%{city}%"
    
    if min_rating is not None:
        where_conditions.append("rating >= %(min_rating)s")
        params['min_rating'] = min_rating
    
    if min_success_rate is not None:
        where_conditions.append("success_rate >= %(min_success_rate)s")
        params['min_success_rate'] = min_success_rate
    
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    # Calculate offset
    offset = (page - 1) * per_page
    params['limit'] = per_page
    params['offset'] = offset
    
    query = f"""
        SELECT id, name, address, city, phone, email, website, rating, review_count, success_rate, source
        FROM driving_schools 
        {where_clause}
        ORDER BY name
        LIMIT %(limit)s OFFSET %(offset)s
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            schools = []
            for row in result:
                schools.append(DrivingSchool(
                    id=row[0],
                    name=row[1],
                    address=row[2],
                    city=row[3],
                    phone=row[4],
                    email=row[5],
                    website=row[6],
                    rating=row[7],
                    review_count=row[8],
                    success_rate=row[9],
                    source=row[10]
                ))
            return schools
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/api/schools/{school_id}", response_model=DrivingSchool)
async def get_school(school_id: int):
    """Get a specific driving school by ID."""
    
    query = """
        SELECT id, name, address, city, phone, email, website, rating, review_count, success_rate, source
        FROM driving_schools 
        WHERE id = %(school_id)s
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {'school_id': school_id})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="School not found")
            
            return DrivingSchool(
                id=row[0],
                name=row[1],
                address=row[2],
                city=row[3],
                phone=row[4],
                email=row[5],
                website=row[6],
                rating=row[7],
                review_count=row[8],
                success_rate=row[9],
                source=row[10]
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/api/stats", response_model=StatsResponse)
async def get_stats():
    """Get database statistics."""
    
    queries = {
        'total': "SELECT COUNT(*) FROM driving_schools",
        'cities': "SELECT COUNT(DISTINCT city) FROM driving_schools",
        'phones': "SELECT COUNT(*) FROM driving_schools WHERE phone IS NOT NULL",
        'ratings': "SELECT COUNT(*) FROM driving_schools WHERE rating IS NOT NULL",
        'success_rates': "SELECT COUNT(*) FROM driving_schools WHERE success_rate IS NOT NULL"
    }
    
    try:
        with engine.connect() as conn:
            stats = {}
            for key, query in queries.items():
                result = conn.execute(text(query))
                stats[key] = result.fetchone()[0]
            
            total = stats['total']
            
            return StatsResponse(
                total_schools=total,
                unique_cities=stats['cities'],
                with_phones=stats['phones'],
                with_ratings=stats['ratings'],
                with_success_rates=stats['success_rates'],
                phone_percentage=round((stats['phones'] / total) * 100, 1) if total > 0 else 0,
                rating_percentage=round((stats['ratings'] / total) * 100, 1) if total > 0 else 0,
                success_rate_percentage=round((stats['success_rates'] / total) * 100, 1) if total > 0 else 0
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/api/cities")
async def get_cities():
    """Get list of all cities with school counts."""
    
    query = """
        SELECT city, COUNT(*) as school_count
        FROM driving_schools 
        WHERE city IS NOT NULL
        GROUP BY city 
        ORDER BY school_count DESC, city
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            cities = []
            for row in result:
                cities.append({
                    'city': row[0],
                    'school_count': row[1]
                })
            return cities
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
