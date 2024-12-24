import sys
import subprocess
from datetime import datetime
import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import time
from functools import wraps

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging
def setup_logging():
    """Set up logging to both file and console with proper formatting"""
    log_filename = f'logs/facebook_scraper_{datetime.now().strftime("%Y%m%d")}.log'
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup file handler
    file_handler = RotatingFileHandler(
        log_filename,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Setup logger
    logger = logging.getLogger('facebook_scraper')
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger
logger = setup_logging()

# Load environment variables
load_dotenv()

def check_environment():
    """Check if all required environment variables are set"""
    required_vars = ['AIRTABLE_PAT', 'AIRTABLE_BASE_ID', 'AIRTABLE_TABLE_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    return True

def install_requirements():
    """Install required packages if they're missing."""
    required_packages = [
        'selenium',
        'requests',
        'webdriver-manager',
        'pandas',
        'python-dotenv'
    ]
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            logger.info(f"Installing {package}...")
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
                logger.info(f"Successfully installed {package}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to install {package}: {str(e)}")
                return False
    return True

# Install requirements if needed
if __name__ == '__main__':
    if not check_environment():
        sys.exit(1)
    if not install_requirements():
        sys.exit(1)

# Now import all required packages
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import time
import random
import requests
import json
import re

# Airtable configuration
AIRTABLE_PAT = os.getenv('AIRTABLE_PAT')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME')

def wait_random():
    time.sleep(random.uniform(1, 2))  # Slightly longer wait for Facebook

def parse_follower_count(text):
    try:
        # Remove any non-numeric characters except commas, decimal points, and K/M/B
        text = text.strip()
        # Extract just the number part
        number_match = re.search(r'([\d,\.]+\s*[KMBkmb]?)\s*(?:people follow this|followers?)?', text, re.IGNORECASE)
        if number_match:
            number_str = number_match.group(1).strip()
            
            # Handle K, M, B suffixes
            multiplier = 1
            if number_str[-1].upper() == 'K':
                multiplier = 1000
                number_str = number_str[:-1]
            elif number_str[-1].upper() == 'M':
                multiplier = 1000000
                number_str = number_str[:-1]
            elif number_str[-1].upper() == 'B':
                multiplier = 1000000000
                number_str = number_str[:-1]
            
            # Remove commas and convert to float
            number_str = number_str.replace(',', '')
            return float(number_str) * multiplier
    except Exception as e:
        logger.error(f"Error parsing follower count '{text}': {str(e)}")
    return None

def get_follower_counts(usernames, max_retries=2):
    # Setup Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    # Add these options to suppress WebGL warnings
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-webgl')
    options.add_argument('--disable-webgl2')
    # Suppress logging
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    results = []
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    driver = None
    total_users = len(usernames)
    
    try:
        logger.info("Initializing Chrome driver...")
        try:
            service = Service()
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logger.error(f"Error with default service, trying ChromeDriverManager: {str(e)}")
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
            except Exception as e:
                logger.error(f"Error with ChromeDriverManager: {str(e)}")
                service = Service("chromedriver.exe")
                driver = webdriver.Chrome(service=service, options=options)

        for index, username in enumerate(usernames, 1):
            if not username:
                continue
                
            retries = 0
            follower_count = None
            error_message = None
            
            while retries < max_retries and follower_count is None:
                try:
                    logger.info(f"\n{index}/{total_users} @{username} (Attempt {retries + 1}/{max_retries})")
                    url = f"https://www.facebook.com/{username}"
                    driver.get(url)
                    wait_random()
                    
                    # Wait for and close the login popup if it appears
                    try:
                        wait = WebDriverWait(driver, 5)
                        close_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'div[aria-label="Close"]')))
                        close_button.click()
                        logger.info("Closed login popup")
                        wait_random()
                    except:
                        logger.info("No login popup found or couldn't close it")
                    
                    # Try to find follower count using multiple possible selectors
                    wait = WebDriverWait(driver, 10)
                    
                    # List of possible selectors for follower count
                    selectors = [
                        "a[href*='followers'] span",
                        "a[href*='followers']",
                        "div[role='main'] span:contains('followers')",
                        "div[role='main'] span:contains('people follow')"
                    ]
                    
                    for selector in selectors:
                        try:
                            element = driver.find_element(By.CSS_SELECTOR, selector)
                            follower_text = element.text
                            logger.info(f"Found text: {follower_text}")
                            follower_count = parse_follower_count(follower_text)
                            if follower_count is not None:
                                break
                        except:
                            continue
                    
                    if follower_count is None:
                        error_message = "Could not find or parse follower count"
                        retries += 1
                        if retries < max_retries:
                            logger.info(f"Retrying... ({retries}/{max_retries})")
                            wait_random()
                        
                except TimeoutException as e:
                    error_message = f"Timeout: {str(e)}"
                    logger.error(f"Timeout while processing {username}")
                    retries += 1
                    if retries < max_retries:
                        logger.info(f"Retrying... ({retries}/{max_retries})")
                        wait_random()
                except Exception as e:
                    error_message = str(e)
                    logger.error(f"Error processing {username}: {str(e)}")
                    retries += 1
                    if retries < max_retries:
                        logger.info(f"Retrying... ({retries}/{max_retries})")
                        wait_random()
            
            # Add result whether successful or not
            results.append({
                'username': username,
                'follower_count': follower_count,
                'timestamp': timestamp,
                'error': error_message if follower_count is None else None
            })
            
            if follower_count is not None:
                logger.info(f"Successfully retrieved follower count for {username}: {follower_count:,.0f}")
            else:
                logger.error(f"Failed to process {username} after {max_retries} attempts: {error_message}")
            
            wait_random()
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        if driver:
            driver.quit()
            
    return results

def retry_with_backoff(retries=3, backoff_in_seconds=1):
    """Retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            x = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if x == retries:
                        logger.error(f"Failed after {retries} attempts: {str(e)}")
                        raise
                    wait = (backoff_in_seconds * 2 ** x)
                    logger.info(f"Attempt {x + 1} failed: {str(e)}")
                    logger.info(f"Retrying in {wait} seconds...")
                    time.sleep(wait)
                    x += 1
        return wrapper
    return decorator

@retry_with_backoff(retries=3, backoff_in_seconds=1)
def get_airtable_records():
    """Fetch records from Airtable with retry logic."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        'Authorization': f'Bearer {AIRTABLE_PAT}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        records = response.json().get('records', [])
        return [{
            'id': record.get('id'),
            'facebook_user': record.get('fields', {}).get('facebook_user', ''),
        } for record in records if record.get('fields', {}).get('facebook_user')]
    else:
        logger.error(f"Error fetching Airtable records: {response.status_code}")
        logger.error(f"Response: {response.text}")
        raise Exception(f"Airtable API error: {response.status_code}")

@retry_with_backoff(retries=3, backoff_in_seconds=1)
def update_airtable_batch(updates):
    """Update multiple records in Airtable in a single request."""
    if not updates:
        return True

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        'Authorization': f'Bearer {AIRTABLE_PAT}',
        'Content-Type': 'application/json'
    }

    # Process in batches of 10 (Airtable limit)
    batch_size = 10
    success = True

    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        
        # Prepare the payload
        payload = {
            'records': [{
                'id': update['id'],
                'fields': {
                    'facebook_followers': update['follower_count'],
                    'facebook_last_updated': update['timestamp']
                }
            } for update in batch]
        }
        
        response = requests.patch(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            logger.info(f"Successfully updated batch of {len(batch)} records in Airtable")
        else:
            logger.error(f"Error updating Airtable records: {response.status_code}")
            logger.error(f"Response: {response.text}")
            raise Exception(f"Airtable API error: {response.status_code}")
            
    return success

if __name__ == "__main__":
    logger.info("Fetching Facebook usernames from Airtable...")
    airtable_records = get_airtable_records()
    
    if not airtable_records:
        logger.error("No Facebook usernames found in Airtable")
        sys.exit(1)
        
    logger.info(f"Found {len(airtable_records)} Facebook usernames")
    
    # Get follower counts
    usernames = [record['facebook_user'] for record in airtable_records]
    results = get_follower_counts(usernames)
    
    if not results:
        logger.error("No follower data retrieved")
        sys.exit(1)
        
    # Prepare updates for Airtable
    updates = []
    success_count = 0
    
    for data in results:
        for record in airtable_records:
            if record['facebook_user'] == data['username']:
                if data['follower_count'] is not None:
                    updates.append({
                        'id': record['id'],
                        'follower_count': data['follower_count'],
                        'timestamp': data['timestamp']
                    })
                break
    
    # Update Airtable in batches
    if updates:
        logger.info(f"Updating {len(updates)} records in Airtable...")
        if update_airtable_batch(updates):
            success_count = len(updates)
    
    # Separate successful and failed results
    successful_results = []
    failed_results = []
    
    for result in results:
        if result['follower_count'] is not None:
            successful_results.append(result)
        else:
            failed_results.append(result)
    
    # Print results in a nice format
    logger.info("\nFinal Results:")
    logger.info("-" * 50)
    
    if successful_results:
        logger.info("\nSuccessful Updates:")
        for result in successful_results:
            count = result['follower_count']
            logger.info(f"{result['username']}: {count:,.0f} followers")
    
    if failed_results:
        logger.info("\nFailed Updates:")
        for result in failed_results:
            logger.error(f"{result['username']}: Not found")
    
    if results:
        logger.info(f"\nTimestamp: {results[0]['timestamp']}")
        logger.info(f"Successfully updated {success_count} out of {len(results)} records in Airtable")
    if failed_results:
        logger.error(f"Failed to get follower counts for {len(failed_results)} accounts")
