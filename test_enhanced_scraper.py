import json
from pathlib import Path
from datetime import datetime
from loguru import logger

# Import our enhanced scraper
from scraper.rijlessen_nl_scraper import RijlessenNLScraper
from scraper.base_scraper import ScrapedSchool

# Configure logging
logger.add("logs/test_enhanced_{time:YYYY-MM-DD}.log", rotation="500 MB", level="DEBUG")

def test_single_school_details():
    """Test the enhanced scraper on a single school to verify detailed extraction."""
    print("🔍 TESTING ENHANCED SCRAPER - DETAILED EXTRACTION")
    print("=" * 60)
    
    # Create output directories
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    scraper = RijlessenNLScraper()
    
    # Test with a specific school URL that we know has detailed information
    test_school = ScrapedSchool(
        name="Test School",
        url="https://rijlessen.nl/rijscholen/den-haag/rijschool-10286-turbodrive-rijschool-den-haag",
        address="Den Haag",
        source="https://rijlessen.nl"
    )
    
    print(f"🎯 Testing detailed extraction for: {test_school.url}")
    
    # Extract detailed information
    enhanced_school = scraper._scrape_school_details(test_school)
    
    print(f"\n📊 EXTRACTION RESULTS:")
    print("-" * 40)
    print(f"Name: {enhanced_school.name}")
    print(f"Address: {enhanced_school.address}")
    print(f"Phone: {enhanced_school.phone or 'Not found'}")
    print(f"Email: {enhanced_school.email or 'Not found'}")
    print(f"Website: {enhanced_school.website or 'Not found'}")
    print(f"Rating: {enhanced_school.rating or 'Not found'}")
    print(f"Review Count: {enhanced_school.review_count or 'Not found'}")
    print(f"Success Rate: {enhanced_school.success_rate or 'Not found'}")
    
    # Test with a few cities to see the full pipeline
    print(f"\n🚀 Testing full pipeline with 2 cities...")
    schools = scraper.scrape(max_cities=2)
    
    if schools:
        print(f"\n✅ Found {len(schools)} schools total")
        
        # Count schools with detailed information
        with_phones = len([s for s in schools if s.phone])
        with_ratings = len([s for s in schools if s.rating])
        with_success_rates = len([s for s in schools if s.success_rate])
        with_emails = len([s for s in schools if s.email])
        
        print(f"\n📈 DATA QUALITY METRICS:")
        print("-" * 30)
        print(f"Schools with phone numbers: {with_phones}/{len(schools)} ({with_phones/len(schools)*100:.1f}%)")
        print(f"Schools with ratings: {with_ratings}/{len(schools)} ({with_ratings/len(schools)*100:.1f}%)")
        print(f"Schools with success rates: {with_success_rates}/{len(schools)} ({with_success_rates/len(schools)*100:.1f}%)")
        print(f"Schools with emails: {with_emails}/{len(schools)} ({with_emails/len(schools)*100:.1f}%)")
        
        # Show detailed examples
        print(f"\n📋 DETAILED EXAMPLES:")
        print("-" * 30)
        detailed_schools = [s for s in schools if s.phone or s.rating or s.success_rate][:3]
        
        for i, school in enumerate(detailed_schools, 1):
            print(f"\n{i}. {school.name}")
            print(f"   📍 Address: {school.address}")
            print(f"   📞 Phone: {school.phone or 'N/A'}")
            print(f"   📧 Email: {school.email or 'N/A'}")
            print(f"   ⭐ Rating: {school.rating or 'N/A'}")
            print(f"   📊 Reviews: {school.review_count or 'N/A'}")
            print(f"   🎯 Success Rate: {school.success_rate}%" if school.success_rate else "   🎯 Success Rate: N/A")
            print(f"   🌐 Website: {school.website or 'N/A'}")
        
        # Save enhanced results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"enhanced_schools_{timestamp}.json"
        output_path = Path("data") / filename
        
        school_dicts = [school.__dict__ for school in schools]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(school_dicts, f, indent=2, default=str)
        
        print(f"\n💾 Saved enhanced results to: {output_path}")
        
        return len(detailed_schools) > 0
    else:
        print("❌ No schools found")
        return False

if __name__ == "__main__":
    success = test_single_school_details()
    
    if success:
        print(f"\n🎉 Enhanced scraper test SUCCESSFUL!")
        print(f"✅ Detailed information extraction is working")
        print(f"💡 Ready to run full enhanced scraper")
    else:
        print(f"\n❌ Enhanced scraper test failed")
        print(f"🔧 Check logs for debugging information")
