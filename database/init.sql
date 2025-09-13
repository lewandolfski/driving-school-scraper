-- Initialize database for driving schools scraper
CREATE DATABASE IF NOT EXISTS driving_schools CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE driving_schools;

-- Create user for the scraper application
CREATE USER IF NOT EXISTS 'scraper_user'@'%' IDENTIFIED BY 'scraper_pass_2024';
GRANT ALL PRIVILEGES ON driving_schools.* TO 'scraper_user'@'%';
FLUSH PRIVILEGES;

-- The tables will be created by SQLAlchemy when the application runs
