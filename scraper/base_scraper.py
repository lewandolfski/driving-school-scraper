import re
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import logging
from loguru import logger
import asyncio

@dataclass
class ScrapedSchool:
    """Data class representing a driving school entry."""
    name: str
    url: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    price_range: Optional[str] = None
    courses: Optional[List[Dict[str, Any]]] = None
    source: Optional[str] = None
    scraped_at: datetime = None
    
    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.utcnow()

class BaseScraper(ABC):
    """Abstract base class for all scrapers."""
    
    def __init__(self, base_url: str, headers: Optional[Dict] = None):
        self.base_url = base_url
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.logger = logger.bind(component=self.__class__.__name__)
    
    @abstractmethod
    async def scrape(self) -> List[ScrapedSchool]:
        """Main method to perform the scraping.
        
        Returns:
            List[ScrapedSchool]: List of scraped driving schools
        """
        pass
    
    def normalize_phone(self, phone: str) -> str:
        """Normalize phone number to a standard format."""
        if not phone:
            return ""
        # Remove all non-numeric characters
        digits = ''.join(c for c in phone if c.isdigit())
        
        # Format as +XX XXX XXX XXXX
        if len(digits) == 10:
            return f"+1 {digits[:3]} {digits[3:6]} {digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+1 {digits[1:4]} {digits[4:7]} {digits[7:]}"
        elif len(digits) == 12 and digits.startswith('+1'):
            return f"+{digits[1:3]} {digits[3:6]} {digits[6:9]} {digits[9:]}"
        return phone
    
    def log_scrape_result(self, result: List[ScrapedSchool]):
        """Log the result of a scrape operation."""
        self.logger.info(f"Scraped {len(result)} schools from {self.base_url}")
        if result:
            self.logger.debug(f"Sample entry: {result[0].__dict__}")
