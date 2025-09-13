import re
import json
from typing import Optional, Dict, Any, List
from email_validator import validate_email, EmailNotValidError
from loguru import logger

class DataValidator:
    """Enhanced data validation for driving school information."""
    
    @staticmethod
    def validate_phone(phone: str) -> bool:
        """Validate Dutch phone number format."""
        if not phone:
            return False
        
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', phone)
        
        # Dutch phone numbers: 10 digits starting with 06 (mobile) or area code
        # International format: +31 followed by 9 digits
        if len(digits) == 10 and digits.startswith(('06', '020', '030', '040', '050', '070', '010')):
            return True
        elif len(digits) == 11 and digits.startswith('316'):  # +31 6 format
            return True
        elif len(digits) == 12 and digits.startswith('31'):   # +31 format
            return True
        
        return False
    
    @staticmethod
    def validate_email_address(email: str) -> bool:
        """Validate email address format."""
        if not email:
            return False
        
        try:
            validate_email(email)
            return True
        except EmailNotValidError:
            return False
    
    @staticmethod
    def validate_url(url: str) -> bool:
        """Validate URL format."""
        if not url:
            return False
        
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        return url_pattern.match(url) is not None
    
    @staticmethod
    def validate_rating(rating: float) -> bool:
        """Validate rating is between 0 and 5."""
        if rating is None:
            return True  # Allow None ratings
        return 0 <= rating <= 5
    
    @staticmethod
    def validate_success_rate(rate: int) -> bool:
        """Validate success rate percentage."""
        if rate is None:
            return True  # Allow None rates
        return 0 <= rate <= 100
    
    @staticmethod
    def clean_name(name: str) -> str:
        """Clean and normalize school name."""
        if not name:
            return ""
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Remove common prefixes/suffixes that might be inconsistent
        name = re.sub(r'^(Rijschool|Autorijschool|Verkeersschool)\s+', '', name, flags=re.IGNORECASE)
        
        return name.strip()
    
    @staticmethod
    def clean_address(address: str) -> str:
        """Clean and normalize address."""
        if not address:
            return ""
        
        # Remove extra whitespace
        address = ' '.join(address.split())
        
        # Capitalize first letter of each word
        address = ' '.join(word.capitalize() for word in address.split())
        
        return address.strip()
    
    @staticmethod
    def normalize_phone(phone: str) -> str:
        """Normalize phone number to standard format."""
        if not phone:
            return ""
        
        # Remove all non-numeric characters
        digits = re.sub(r'\D', '', phone)
        
        # Format Dutch numbers
        if len(digits) == 10:
            if digits.startswith('06'):
                return f"+31 6 {digits[2:4]} {digits[4:6]} {digits[6:8]} {digits[8:]}"
            elif digits.startswith(('020', '030', '040', '050', '070', '010')):
                area_code = digits[:3]
                number = digits[3:]
                return f"+31 {area_code[1:]} {number[:3]} {number[3:]}"
        elif len(digits) == 11 and digits.startswith('316'):
            return f"+31 6 {digits[3:5]} {digits[5:7]} {digits[7:9]} {digits[9:]}"
        elif len(digits) == 12 and digits.startswith('31'):
            if digits[2] == '6':
                return f"+31 6 {digits[4:6]} {digits[6:8]} {digits[8:10]} {digits[10:]}"
            else:
                area = digits[2:4] if digits[2:4] in ['20', '30', '40', '50', '70', '10'] else digits[2:5]
                rest = digits[len(area)+2:]
                return f"+31 {area} {rest[:3]} {rest[3:]}"
        
        return phone  # Return original if can't normalize

class DataDeduplicator:
    """Handle deduplication of driving school data."""
    
    @staticmethod
    def calculate_similarity(school1: Dict[str, Any], school2: Dict[str, Any]) -> float:
        """Calculate similarity score between two schools (0-1)."""
        score = 0.0
        total_weight = 0.0
        
        # Name similarity (highest weight)
        if school1.get('name') and school2.get('name'):
            name1 = DataValidator.clean_name(school1['name']).lower()
            name2 = DataValidator.clean_name(school2['name']).lower()
            
            if name1 == name2:
                score += 0.5
            elif name1 in name2 or name2 in name1:
                score += 0.3
            total_weight += 0.5
        
        # Address similarity
        if school1.get('address') and school2.get('address'):
            addr1 = school1['address'].lower()
            addr2 = school2['address'].lower()
            
            if addr1 == addr2:
                score += 0.3
            elif any(word in addr2 for word in addr1.split() if len(word) > 3):
                score += 0.15
            total_weight += 0.3
        
        # Phone similarity
        if school1.get('phone') and school2.get('phone'):
            phone1 = re.sub(r'\D', '', school1['phone'])
            phone2 = re.sub(r'\D', '', school2['phone'])
            
            if phone1 == phone2:
                score += 0.2
            total_weight += 0.2
        
        return score / total_weight if total_weight > 0 else 0.0
    
    @staticmethod
    def find_duplicates(schools: List[Dict[str, Any]], threshold: float = 0.8) -> List[List[int]]:
        """Find duplicate schools based on similarity threshold."""
        duplicates = []
        processed = set()
        
        for i, school1 in enumerate(schools):
            if i in processed:
                continue
                
            duplicate_group = [i]
            
            for j, school2 in enumerate(schools[i+1:], i+1):
                if j in processed:
                    continue
                    
                similarity = DataDeduplicator.calculate_similarity(school1, school2)
                if similarity >= threshold:
                    duplicate_group.append(j)
                    processed.add(j)
            
            if len(duplicate_group) > 1:
                duplicates.append(duplicate_group)
                processed.update(duplicate_group)
        
        return duplicates
    
    @staticmethod
    def merge_duplicates(schools: List[Dict[str, Any]], duplicate_groups: List[List[int]]) -> List[Dict[str, Any]]:
        """Merge duplicate schools, keeping the most complete data."""
        merged_schools = []
        to_skip = set()
        
        for group in duplicate_groups:
            # Find the school with the most complete data
            best_school = None
            best_score = -1
            
            for idx in group:
                school = schools[idx]
                completeness_score = sum([
                    1 if school.get('name') else 0,
                    1 if school.get('address') else 0,
                    1 if school.get('phone') else 0,
                    1 if school.get('email') else 0,
                    1 if school.get('website') else 0,
                    1 if school.get('rating') else 0,
                    1 if school.get('review_count') else 0,
                ])
                
                if completeness_score > best_score:
                    best_score = completeness_score
                    best_school = school.copy()
            
            # Merge additional data from other schools in the group
            for idx in group:
                school = schools[idx]
                for key, value in school.items():
                    if not best_school.get(key) and value:
                        best_school[key] = value
            
            merged_schools.append(best_school)
            to_skip.update(group)
        
        # Add non-duplicate schools
        for i, school in enumerate(schools):
            if i not in to_skip:
                merged_schools.append(school)
        
        return merged_schools
