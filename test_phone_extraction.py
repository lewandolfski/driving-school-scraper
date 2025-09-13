import re
from scraper.rijlessen_nl_scraper import RijlessenNLScraper
from scraper.base_scraper import ScrapedSchool

def test_phone_patterns():
    """Test phone number extraction patterns."""
    print("🔍 TESTING PHONE NUMBER EXTRACTION")
    print("=" * 50)
    
    # Test text with various phone formats
    test_text = """
    Hoe kan je Autorijschool Nico ten Kate bereiken?
    Neem rechtstreeks contact op met Autorijschool Nico ten Kate en start met rijlessen!
    0522-244366 06-57340906
    
    Other formats:
    015 202 4021
    06 51 00 03 17
    +31 15 202 4021
    0152024021
    """
    
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
    
    all_phones = []
    for pattern in phone_patterns:
        phone_matches = re.findall(pattern, test_text)
        for phone in phone_matches:
            phone = phone.strip()
            if len(phone.replace('-', '').replace(' ', '')) >= 10:
                all_phones.append(phone)
    
    print(f"Found phones: {all_phones}")
    
    # Test with actual scraper
    scraper = RijlessenNLScraper()
    
    # Test specific school that had missing phone
    test_school = ScrapedSchool(
        name="Autorijschool Nico ten Kate",
        url="https://rijlessen.nl/rijscholen/meppel/rijschool-1234-autorijschool-nico-ten-kate",
        address="Meppel",
        source="https://rijlessen.nl"
    )
    
    print(f"\n🎯 Testing with real school page...")
    enhanced_school = scraper._scrape_school_details(test_school)
    
    print(f"Extracted phone: {enhanced_school.phone or 'NOT FOUND'}")
    
    return len(all_phones) > 0

if __name__ == "__main__":
    test_phone_patterns()
