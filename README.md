# Social Media Follower Scraper

A collection of Python scripts to scrape follower counts from various social media platforms (Facebook, Instagram, Twitter, and YouTube) and store the data in Airtable.

## Features

- Scrapes follower counts from:
  - Facebook
  - Instagram
  - Twitter
  - YouTube
- Automatic ChromeDriver setup
- Data storage in Airtable
- Environment variable configuration for secure credential management
- Error handling and retry mechanisms

## Setup

1. Install Python 3.7+ if not already installed

2. Clone this repository:
   ```bash
   git clone <repository-url>
   cd SocialMediaScraper
   ```

3. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file in the root directory with your Airtable credentials:
   ```
   AIRTABLE_PAT=your_airtable_pat_here
   AIRTABLE_BASE_ID=your_base_id_here
   AIRTABLE_TABLE_NAME=your_table_name_here
   ```

## Usage

### Running All Scrapers

To run all scrapers at once:
```bash
python run_all_scrapers.py
```

### Running Individual Scrapers

You can also run each scraper individually:
```bash
python facebook_follower_scraper.py
python instagram_follower_scraper.py
python twitter_follower_scraper.py
python youtube_follower_scraper.py
```

## Files Description

- `run_all_scrapers.py`: Main script to run all scrapers sequentially
- `setup_chromedriver.py`: Handles ChromeDriver setup for Selenium
- `facebook_follower_scraper.py`: Facebook scraper
- `instagram_follower_scraper.py`: Instagram scraper
- `twitter_follower_scraper.py`: Twitter scraper
- `youtube_follower_scraper.py`: YouTube scraper
- `requirements.txt`: Python package dependencies
- `.env`: Environment variables (not tracked in git)
- `.gitignore`: Git ignore rules

## Notes

- Make sure you have Chrome browser installed
- The ChromeDriver will be automatically installed when needed
- Each scraper includes retry mechanisms for reliability
- Credentials are stored securely in the `.env` file
