import re
import requests
import time
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from .base_scraper import BaseScraper, ScrapedSchool

class RijlessenNLScraper(BaseScraper):
    """Scraper for rijlessen.nl driving schools directory."""
    
    def __init__(self):
        super().__init__(
            base_url="https://rijlessen.nl",
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'nl-NL,nl;q=0.8,en-US;q=0.5,en;q=0.3',
            }
        )
    
    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page and return its HTML content with error handling."""
        try:
            response = requests.get(url, headers=self.headers, timeout=30.0)
            response.raise_for_status()
            return response.text
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")
            return None
    
    def _parse_city_links(self, soup) -> List[str]:
        """Extract city links from the main rijscholen page."""
        city_links = []
        # Find all links that point to city pages
        links = soup.find_all('a', href=lambda x: x and '/rijscholen/' in x and x != '/rijscholen/')
        
        for link in links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                city_links.append(full_url)
        
        return list(set(city_links))  # Remove duplicates
    
    def _parse_school_from_city_page(self, soup, city_url: str) -> List[ScrapedSchool]:
        """Parse driving schools from a city page."""
        schools = []
        
        # Look for H3 headers which contain school names
        school_headers = soup.find_all('h3')
        
        for header in school_headers:
            try:
                school_name = header.get_text(strip=True)
                
                # Skip if it doesn't look like a school name
                if not school_name or len(school_name) < 3:
                    continue
                
                # Find the next link that contains school details
                school_link = None
                next_elem = header.find_next('a', href=lambda x: x and '/rijscholen/' in x and '/rijschool-' in x)
                if next_elem:
                    school_link = urljoin(self.base_url, next_elem['href'])
                
                # Extract information from the text around the header
                # Look for the content block after the header
                content_block = header.find_next_sibling() or header.parent
                if not content_block:
                    content_block = header.find_next(['div', 'p', 'section'])
                
                address = None
                rating = None
                review_count = None
                success_rate = None
                
                if content_block:
                    text = content_block.get_text()
                    
                    # Extract address (usually contains street name and city)
                    address_match = re.search(r'([A-Za-z\s]+\d+[A-Za-z\s]*(?:Amsterdam|Utrecht|Rotterdam|Den Haag|Eindhoven|Tilburg|Groningen|Almere|Breda|Nijmegen|[A-Z][a-z]+))', text)
                    if address_match:
                        address = address_match.group(1).strip()
                    
                    # Extract rating (format: X.X/5)
                    rating_match = re.search(r'(\d+\.?\d*)/5', text)
                    if rating_match:
                        try:
                            rating = float(rating_match.group(1))
                        except ValueError:
                            pass
                    
                    # Extract review count
                    review_match = re.search(r'\((\d+)\s*reviews?\)', text)
                    if review_match:
                        try:
                            review_count = int(review_match.group(1))
                        except ValueError:
                            pass
                    
                    # Extract success rate (slagingspercentage)
                    success_match = re.search(r'(\d+)%\s*slagingspercentage', text)
                    if success_match:
                        try:
                            success_rate = int(success_match.group(1))
                        except ValueError:
                            pass
                
                # Extract city name from URL
                city_name = city_url.split('/')[-1].replace('-', ' ').title()
                if not address:
                    address = city_name
                
                school = ScrapedSchool(
                    name=school_name,
                    url=school_link or city_url,
                    address=address,
                    rating=rating,
                    review_count=review_count,
                    source=self.base_url
                )
                
                # Add success rate as additional info
                if success_rate:
                    if not school.courses:
                        school.courses = []
                    school.courses.append({
                        'type': 'success_rate',
                        'value': success_rate,
                        'description': f'{success_rate}% slagingspercentage'
                    })
                
                schools.append(school)
                self.logger.debug(f"Found school: {school_name} in {city_name}")
                
            except Exception as e:
                self.logger.warning(f"Error parsing school from header: {str(e)}")
                continue
        
        return schools
    
    def _scrape_school_details(self, school: ScrapedSchool) -> ScrapedSchool:
        """Scrape detailed information from a school's individual page."""
        if not school.url or '/rijschool-' not in school.url:
            return school
            
        html = self._fetch_page(school.url)
        if not html:
            return school
            
        soup = BeautifulSoup(html, 'html.parser')
        
        try:
            # Extract phone numbers - look for phone patterns
            phone_patterns = [
                r'(\d{4}-\d{6})',  # 0522-244366
                r'(\d{2}-\d{8})',  # 06-57340906
                r'(\d{3}-\d{3}-\d{4})',  # 015-202-4021
                r'(\d{3}\s\d{3}\s\d{4})',  # 015 202 4021
                r'(\d{2}\s\d{2}\s\d{2}\s\d{2}\s\d{2})',  # 06 51 00 03 17
                r'(\d{4}\s\d{6})',  # 0522 244366
                r'(\d{2}\s\d{8})',  # 06 57340906
                r'(\d{10,11})',  # 0152024021
                r'(\+31\s?\d{1,3}\s?\d{3,4}\s?\d{4})'  # +31 15 202 4021
            ]
            
            page_text = soup.get_text()
            # Find ALL phone numbers on the page
            all_phones = []
            for pattern in phone_patterns:
                phone_matches = re.findall(pattern, page_text)
                for phone in phone_matches:
                    phone = phone.strip()
                    # Filter out obviously wrong numbers
                    if len(phone.replace('-', '').replace(' ', '')) >= 10:
                        all_phones.append(phone)
            
            # Use the first valid phone number found
            if all_phones:
                school.phone = all_phones[0]
                # If multiple phones, store them as a comma-separated string
                if len(all_phones) > 1:
                    school.phone = ', '.join(all_phones[:2])  # Limit to 2 phones
            
            # Extract email - look for mailto links or email patterns
            email_elem = soup.find('a', href=lambda x: x and x.startswith('mailto:'))
            if email_elem:
                school.email = email_elem['href'].replace('mailto:', '').strip()
            else:
                # Look for email patterns in text
                email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', page_text)
                if email_match:
                    school.email = email_match.group()
            
            # Extract full address - look for address patterns
            address_patterns = [
                r'([A-Za-z\s]+\d+[A-Za-z]?\s*\d{4}\s?[A-Z]{2}\s+[A-Za-z\s]+)',  # Street 123 1234AB City
                r'([A-Za-z\s]+\d+[A-Za-z]?\s+[A-Za-z\s]+)'  # Street 123A City
            ]
            
            for pattern in address_patterns:
                address_match = re.search(pattern, page_text)
                if address_match:
                    full_address = address_match.group(1).strip()
                    if len(full_address) > 10:  # Reasonable address length
                        school.address = full_address
                        break
            
            # Extract rating - look for rating patterns
            rating_patterns = [
                r'(\d+\.?\d*)/5',  # 4.9/5
                r'(\d+\.?\d*)\s*sterren',  # 4.9 sterren
                r'(\d+\.?\d*)\s*★'  # 4.9★
            ]
            
            for pattern in rating_patterns:
                rating_match = re.search(pattern, page_text)
                if rating_match:
                    try:
                        rating_value = float(rating_match.group(1))
                        if 0 <= rating_value <= 5:
                            school.rating = rating_value
                            break
                    except ValueError:
                        continue
            
            # Extract review count
            review_patterns = [
                r'(\d+)\s*reviews?',
                r'(\d+)\s*beoordelingen',
                r'op\s+basis\s+van\s+(\d+)\s+reviews?'
            ]
            
            for pattern in review_patterns:
                review_match = re.search(pattern, page_text, re.IGNORECASE)
                if review_match:
                    try:
                        school.review_count = int(review_match.group(1))
                        break
                    except ValueError:
                        continue
            
            # Extract success rate (slagingspercentage)
            success_patterns = [
                r'slagingspercentage\s*(\d+)%',
                r'(\d+)%\s*slagingspercentage',
                r'(\d+)%\s*op\s+basis\s+van\s+\d+\s+examens'
            ]
            
            for pattern in success_patterns:
                success_match = re.search(pattern, page_text, re.IGNORECASE)
                if success_match:
                    try:
                        success_rate = int(success_match.group(1))
                        if 0 <= success_rate <= 100:
                            school.success_rate = success_rate
                            break
                    except ValueError:
                        continue
            
            # Extract website - look for external links
            website_elem = soup.find('a', href=lambda x: x and x.startswith('http') and 'rijlessen.nl' not in x)
            if website_elem and 'href' in website_elem.attrs:
                school.website = website_elem['href'].strip()
            
            self.logger.debug(f"Enhanced school details for {school.name}: phone={school.phone}, rating={school.rating}, success_rate={school.success_rate}")
            
        except Exception as e:
            self.logger.warning(f"Error extracting details for {school.name}: {str(e)}")
            
        return school
    
    def scrape(self, max_cities: int = None) -> List[ScrapedSchool]:
        """Scrape driving schools from rijlessen.nl.
        
        Args:
            max_cities: Maximum number of cities to scrape
            
        Returns:
            List of ScrapedSchool objects
        """
        self.logger.info(f"Starting scrape of {self.base_url}")
        
        # First, get the main rijscholen page to extract city links
        main_url = f"{self.base_url}/rijscholen"
        html = self._fetch_page(main_url)
        if not html:
            self.logger.error("Could not fetch main rijscholen page")
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        city_links = self._parse_city_links(soup)
        
        if not city_links:
            self.logger.warning("No city links found")
            return []
        
        self.logger.info(f"Found {len(city_links)} city pages to scrape")
        
        # Limit the number of cities to scrape (if specified)
        if max_cities:
            city_links = city_links[:max_cities]
        
        all_schools = []
        
        for i, city_url in enumerate(city_links, 1):
            self.logger.info(f"Scraping city {i}/{len(city_links)}: {city_url}")
            
            html = self._fetch_page(city_url)
            if not html:
                continue
            
            soup = BeautifulSoup(html, 'html.parser')
            schools = self._parse_school_from_city_page(soup, city_url)
            
            if schools:
                # Enhance each school with detailed information from individual pages
                enhanced_schools = []
                for school in schools:
                    if school.url and '/rijschool-' in school.url:
                        self.logger.debug(f"Fetching details for {school.name}")
                        enhanced_school = self._scrape_school_details(school)
                        enhanced_schools.append(enhanced_school)
                        # Rate limit between individual school page requests
                        time.sleep(0.5)
                    else:
                        enhanced_schools.append(school)
                
                all_schools.extend(enhanced_schools)
                self.logger.info(f"Found {len(schools)} schools in {city_url}, enhanced {len([s for s in enhanced_schools if s.phone or s.rating])} with details")
            else:
                self.logger.debug(f"No schools found in {city_url}")
            
            # Be nice to the server
            time.sleep(1)
        
        self.log_scrape_result(all_schools)
        return all_schools
