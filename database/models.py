from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class DrivingSchool(Base):
    __tablename__ = 'driving_schools'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    url = Column(String(500))
    address = Column(String(500))
    city = Column(String(100))
    phone = Column(String(50))
    email = Column(String(255))
    website = Column(String(500))
    rating = Column(Float)
    review_count = Column(Integer)
    success_rate = Column(Integer)  # Slagingspercentage
    price_range = Column(String(50))
    courses = Column(Text)  # JSON string of course data
    source = Column(String(255))
    scraped_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<DrivingSchool(name='{self.name}', city='{self.city}', rating={self.rating})>"

class DatabaseManager:
    def __init__(self):
        self.db_url = self._get_database_url()
        self.engine = create_engine(self.db_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
    def _get_database_url(self):
        """Construct database URL from environment variables."""
        db_host = os.getenv('DB_HOST', 'db')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'driving_schools')
        db_user = os.getenv('DB_USER', 'scraper_user')
        db_password = os.getenv('DB_PASSWORD', '')
        
        return f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    def create_tables(self):
        """Create all tables in the database."""
        Base.metadata.create_all(bind=self.engine)
        
    def get_session(self):
        """Get a database session."""
        return self.SessionLocal()
    
    def close_session(self, session):
        """Close a database session."""
        session.close()
