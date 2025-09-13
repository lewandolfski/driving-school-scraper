# Driving School Scraper - Deployment Guide

## Overview

This system scrapes driving school data from rijlessen.nl and stores it in a MariaDB database with comprehensive validation and deduplication.

## Features

- **Complete Coverage**: Scrapes all 2400+ cities in the Netherlands
- **Database Storage**: MariaDB/MySQL integration with automatic table creation
- **Data Validation**: Phone numbers, emails, URLs, ratings validation
- **Deduplication**: Intelligent duplicate detection and merging
- **Docker Ready**: Complete containerization for easy deployment
- **Backup Storage**: JSON files as backup alongside database storage

## Quick Start with Docker

### 1. Clone and Setup

```bash
git clone <repository-url>
cd driving-school-scraper
cp .env.example .env
```

### 2. Start Database

```bash
docker-compose up -d mariadb
```

Wait for database to initialize (check logs: `docker-compose logs mariadb`)

### 3. Run Scraper

```bash
# One-time run
docker-compose run --rm scraper

# Or build and run manually
docker-compose up --build scraper
```

## Manual Installation

### Prerequisites

- Python 3.11+
- MariaDB/MySQL server
- Git

### 1. Install Dependencies

```bash
pip install -r requirements-simple.txt
```

### 2. Database Setup

Create database and user:

```sql
CREATE DATABASE driving_schools CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'scraper_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON driving_schools.* TO 'scraper_user'@'localhost';
FLUSH PRIVILEGES;
```

### 3. Configure Environment

Copy `.env.example` to `.env` and update:

```env
DB_HOST=localhost
DB_PORT=3306
DB_NAME=driving_schools
DB_USER=scraper_user
DB_PASSWORD=your_password
```

### 4. Run Scraper

```bash
python main.py
```

## Database Schema

The system creates the following table:

```sql
CREATE TABLE driving_schools (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url VARCHAR(500),
    address VARCHAR(500),
    city VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(255),
    website VARCHAR(500),
    rating FLOAT,
    review_count INT,
    success_rate INT,
    price_range VARCHAR(50),
    courses TEXT,
    source VARCHAR(255),
    scraped_at DATETIME,
    is_active BOOLEAN DEFAULT TRUE,
    last_updated DATETIME
);
```

## Scheduling

### Linux/Mac (Cron)

Add to crontab for daily runs:

```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/scraper && docker-compose run --rm scraper
```

### Windows (Task Scheduler)

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (e.g., daily at 2 AM)
4. Set action: Start a program
5. Program: `docker-compose`
6. Arguments: `run --rm scraper`
7. Start in: `C:\path\to\scraper`

## API Integration

To integrate with your Windsurf system, query the database:

```python
from database.models import DatabaseManager, DrivingSchool

db = DatabaseManager()
session = db.get_session()

# Get all active schools
schools = session.query(DrivingSchool).filter_by(is_active=True).all()

# Get schools by city
amsterdam_schools = session.query(DrivingSchool).filter_by(city='Amsterdam').all()

# Get top-rated schools
top_schools = session.query(DrivingSchool).filter(
    DrivingSchool.rating >= 4.5
).order_by(DrivingSchool.rating.desc()).limit(10).all()
```

## Performance

- **Full scrape**: ~2400 cities, ~20,000+ schools
- **Runtime**: 2-4 hours depending on network speed
- **Storage**: ~50MB database, ~10MB JSON backup
- **Rate limiting**: 1 second between requests (respectful scraping)

## Monitoring

Check logs in:
- `logs/scraper_YYYY-MM-DD.log` (application logs)
- `docker-compose logs scraper` (container logs)

## Troubleshooting

### Database Connection Issues

```bash
# Test database connection
docker-compose exec mariadb mysql -u scraper_user -p driving_schools
```

### Memory Issues

For large scrapes, increase Docker memory:

```yaml
# In docker-compose.yml
services:
  scraper:
    mem_limit: 2g
```

### Network Issues

If scraping fails, check:
1. Internet connection
2. rijlessen.nl website status
3. Rate limiting (increase delays in scraper)

## Data Quality

The system includes:
- **Phone validation**: Dutch number format validation
- **Email validation**: RFC compliant email checking
- **URL validation**: Proper URL format verification
- **Deduplication**: 80% similarity threshold for merging
- **Data cleaning**: Name/address normalization

## Security

- Database credentials in environment variables
- No hardcoded secrets
- Minimal Docker image with only required dependencies
- Non-root container execution

## Scaling

For production deployment:
- Use managed database (AWS RDS, Google Cloud SQL)
- Deploy on container orchestration (Kubernetes, Docker Swarm)
- Add monitoring (Prometheus, Grafana)
- Implement alerting for failed scrapes
- Add data backup strategies

## License

MIT License - see LICENSE file for details.
