import json
from pathlib import Path
from datetime import datetime
from loguru import logger

# Import our scraper components
from scraper.rijlessen_nl_scraper import RijlessenNLScraper
from scraper.base_scraper import ScrapedSchool

# Configure logging
logger.add("logs/test_scraper_{time:YYYY-MM-DD}.log", rotation="500 MB", level="INFO")

def test_scraper_basic():
    """Test the scraper with a small number of cities."""
    print("🚀 TESTING DRIVING SCHOOL SCRAPER")
    print("=" * 50)
    
    # Create output directories
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    # Initialize scraper
    scraper = RijlessenNLScraper()
    
    print("📡 Starting test scrape (limited to 3 cities)...")
    
    try:
        # Test with just 3 cities
        schools = scraper.scrape(max_cities=3)
        
        print(f"\n✅ Scraping completed!")
        print(f"📊 Found {len(schools)} schools")
        
        if schools:
            # Save to JSON for viewing
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"test_schools_{timestamp}.json"
            output_path = Path("data") / filename
            
            # Convert to dictionaries
            school_dicts = [school.__dict__ for school in schools]
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(school_dicts, f, indent=2, default=str)
            
            print(f"💾 Saved results to: {output_path}")
            
            # Show sample results
            print(f"\n📋 SAMPLE RESULTS:")
            print("-" * 30)
            for i, school in enumerate(schools[:3], 1):
                print(f"\n{i}. {school.name}")
                print(f"   📍 Address: {school.address or 'N/A'}")
                print(f"   📞 Phone: {school.phone or 'N/A'}")
                print(f"   ⭐ Rating: {school.rating or 'N/A'}")
                print(f"   🌐 Website: {school.website or 'N/A'}")
                print(f"   🔗 URL: {school.url or 'N/A'}")
            
            return True
        else:
            print("❌ No schools were found")
            return False
            
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        logger.error(f"Scraping error: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = test_scraper_basic()
    
    if success:
        print(f"\n🎉 Test completed successfully!")
        print(f"💡 Run 'python viewer.py' to see detailed results")
    else:
        print(f"\n❌ Test failed. Check logs for details.")
