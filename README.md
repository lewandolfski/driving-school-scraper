# Driving School Scraper

A modular web scraping system for collecting driving school data from various sources.

## Features

- **Modular Architecture**: Easily add new scrapers for different data sources
- **Data Validation**: Clean and validate scraped data
- **Flexible Storage**: Save results to JSON files
- **Asynchronous**: Built with asyncio for efficient scraping
- **Logging**: Comprehensive logging for monitoring and debugging

## Project Structure

```
driving-school-scraper/
├── data/                   # Output directory for scraped data
├── logs/                   # Log files
├── scraper/                # Scraper modules
│   ├── __init__.py
│   ├── base_scraper.py     # Base scraper class
│   └── example_scraper.py  # Example scraper implementation
├── .env.example           # Example environment variables
├── main.py                # Main orchestration script
├── README.md              # This file
└── requirements.txt       # Python dependencies
```

## Getting Started

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd driving-school-scraper
   ```

2. **Set up a virtual environment** (recommended)
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   Copy `.env.example` to `.env` and update the values as needed.

5. **Run the scraper**
   ```bash
   python main.py
   ```

## Adding a New Scraper

1. Create a new file in the `scraper` directory (e.g., `my_scraper.py`)
2. Create a class that inherits from `BaseScraper`
3. Implement the `scrape()` method
4. Add your scraper to the `scrapers` list in `main.py`

## Configuration

Create a `.env` file with the following variables:

```
# API keys and credentials
API_KEY=your_api_key_here

# Request settings
REQUEST_TIMEOUT=30
MAX_RETRIES=3

# Output settings
OUTPUT_DIR=data
LOG_LEVEL=INFO
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
