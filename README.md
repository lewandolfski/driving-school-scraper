# Driving School Scraper

A comprehensive web scraping system for collecting and analyzing driving school data from rijlessen.nl and other sources.
## Features

- **Complete Data Collection**: Scrape detailed information including contact details, ratings, and success rates
- **Cloud-Ready**: Deploy to GCP, AWS, or DigitalOcean with Docker
- **Web Viewer**: Built-in web interface for data visualization
- **REST API**: JSON API for integration with other applications
- **Modular Architecture**: Easily extensible for new data sources
- **Data Validation**: Clean and validate scraped data
- **Asynchronous**: Built with asyncio for efficient scraping

## Project Structure

```
driving-school-scraper/
├── cloud_deployment/      # Docker and cloud deployment files
├── database/              # Database models and migrations
├── scraper/              # Scraper modules
│   ├── __init__.py
│   ├── base_scraper.py   # Base scraper class
│   └── rijlessen_nl_scraper.py  # Rijlessen.nl scraper
├── web_viewer/           # Web interface and API
├── .env.example          # Example environment variables
├── docker-compose.yml    # Local development
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/lewandolfski/driving-school-scraper.git
   cd driving-school-scraper
   ```

2. **Set up environment**
   ```bash
   # Copy and configure environment variables
   cp .env.example .env
   
   # Install dependencies
   pip install -r requirements.txt
   ```

3. **Run the scraper locally**
   ```bash
   python main.py
   ```

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed instructions on deploying to:
- Google Cloud Platform (GCP)
- AWS
- DigitalOcean
- Local development with Docker

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
=======
# driving-school-scraper
>>>>>>> c389d5dacda88709a8bb5d837bcbdc0f3d9f8890
