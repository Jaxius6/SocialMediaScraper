import subprocess
import sys
from setup_chromedriver import setup_chromedriver
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_env_variables():
    """Check if all required environment variables are set."""
    required_vars = ['AIRTABLE_PAT', 'AIRTABLE_BASE_ID', 'AIRTABLE_TABLE_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"- {var}")
        return False
    return True

def run_scraper(script_name):
    """Run a scraper script and handle any errors."""
    print(f"\nRunning {script_name}...")
    try:
        subprocess.run([sys.executable, script_name], check=True)
        print(f"Successfully completed {script_name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {str(e)}")
        return False

def main():
    # First check environment variables
    if not check_env_variables():
        return

    # Setup ChromeDriver
    if not setup_chromedriver():
        print("Failed to setup ChromeDriver. Exiting...")
        return

    # List of scraper scripts to run
    scrapers = [
        'facebook_follower_scraper.py',
        'instagram_follower_scraper.py',
        'twitter_follower_scraper.py',
        'youtube_follower_scraper.py'
    ]

    # Run each scraper
    results = []
    for scraper in scrapers:
        success = run_scraper(scraper)
        results.append((scraper, success))

    # Print summary
    print("\nScraping Summary:")
    print("-" * 50)
    for scraper, success in results:
        status = "✓ Success" if success else "✗ Failed"
        print(f"{scraper}: {status}")

if __name__ == "__main__":
    main()
