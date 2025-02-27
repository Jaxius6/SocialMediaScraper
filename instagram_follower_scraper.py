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
import random

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging
def setup_logging():
    """Set up logging to both file and console with proper formatting"""
    log_filename = f'logs/instagram_scraper_{datetime.now().strftime("%Y%m%d")}.log'
    
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
    logger = logging.getLogger('instagram_scraper')
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger
logger = setup_logging()

# Load environment variables
load_dotenv()

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

def login_to_instagram(driver):
    """Log into Instagram"""
    try:
        logger.info("Logging into Instagram...")
        driver.get("https://www.instagram.com/accounts/login/")
        time.sleep(random.uniform(3, 5))  # Wait for page load
        
        # Find and fill username
        username_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "username"))
        )
        username_input.send_keys(os.getenv('INSTAGRAM_USERNAME'))
        time.sleep(random.uniform(0.5, 1.5))
        
        # Find and fill password
        password_input = driver.find_element(By.NAME, "password")
        password_input.send_keys(os.getenv('INSTAGRAM_PASSWORD'))
        time.sleep(random.uniform(0.5, 1.5))
        
        # Click login button
        login_button = driver.find_element(By.XPATH, "//button[@type='submit']")
        login_button.click()
        
        # Wait for login to complete
        time.sleep(random.uniform(4, 6))
        
        # Check for successful login
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='dialog']"))
            )
            # If we find a dialog, click "Not Now" if it exists
            try:
                not_now_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Not Now')]")
                not_now_button.click()
            except Exception:
                pass
        except TimeoutException:
            pass
        
        logger.info("Successfully logged into Instagram")
        return True
        
    except Exception as e:
        logger.error(f"Failed to log in to Instagram: {str(e)}")
        return False

def check_environment():
    """Check if all required environment variables are set"""
    required_vars = [
        'AIRTABLE_PAT', 
        'AIRTABLE_BASE_ID', 
        'AIRTABLE_TABLE_NAME',
        'INSTAGRAM_USERNAME',
        'INSTAGRAM_PASSWORD'
    ]
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
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            logger.info(f"Successfully installed {package}")

# Install requirements if needed
if __name__ == '__main__':
    install_requirements()

# Now import all required packages
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import time
import requests
import json
import re

# Airtable configuration
AIRTABLE_PAT = os.getenv('AIRTABLE_PAT')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME')

def wait_random():
    time.sleep(random.uniform(1, 2))  # Slightly longer wait for Instagram

def parse_follower_count(text):
    try:
        # Remove any non-numeric characters except commas, decimal points, and K/M/B
        text = text.strip()
        # Extract just the number part if it's in a format like "1.2M followers" or "2,771 followers"
        number_match = re.search(r'([\d,\.]+\s*[KMBkmb]?)\s*(?:followers?)?', text, re.IGNORECASE)
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

@retry_with_backoff(retries=3, backoff_in_seconds=1)
def get_follower_counts(usernames, max_retries=2):
    # Setup Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    # Add more random user agents
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
    ]
    options.add_argument(f'--user-agent={random.choice(user_agents)}')
    
    # Add these to make detection harder
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
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

        # Add login step here
        if not login_to_instagram(driver):
            raise Exception("Failed to log in to Instagram")
        
        for index, username in enumerate(usernames, 1):  
            if not username:
                continue
                
            retries = 0
            follower_count = None
            error_message = None
            
            while retries < max_retries:
                try:
                    logger.info(f"\n{index}/{total_users} @{username} (Attempt {retries + 1}/{max_retries})")
                    url = f"https://www.instagram.com/{username}/"
                    driver.get(url)
                    
                    # Add check for "Page Not Found"
                    try:
                        error_text = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Sorry, this page isn')]"))
                        )
                        logger.error(f"Page not found for {username}")
                        error_message = "Page not found"
                        break  # Exit retry loop if page doesn't exist
                    except TimeoutException:
                        pass  # Page exists, continue normally
                    
                    def get_follower_count(driver, username):
                        try:
                            # Wait for initial page load with randomization
                            wait_time = random.uniform(3, 5)
                            WebDriverWait(driver, wait_time).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                            
                            # Add random scrolling to simulate human behavior
                            driver.execute_script(f"window.scrollTo(0, {random.randint(100, 300)});")
                            time.sleep(random.uniform(0.5, 1.5))
                            driver.execute_script("window.scrollTo(0, 0);")
                            
                            # Updated selectors for follower count
                            methods = [
                                # Method 1: New meta tag format
                                lambda: driver.find_element(By.CSS_SELECTOR, 'meta[property="og:description"]')
                                    .get_attribute("content").split("Followers")[0].strip().split()[-1],
                                
                                # Method 2: Updated class names for stats section
                                lambda: driver.find_element(By.CSS_SELECTOR, 'span[class*="_aacl"][class*="_aaco"]').text,
                                
                                # Method 3: New XPath for follower count
                                lambda: driver.find_element(
                                    By.XPATH,
                                    "//section//ul//li[2]//span//span[contains(@class, '_ac2a') or contains(@class, '_aacl')]"
                                ).text,
                                
                                # Method 4: Backup method using aria-label
                                lambda: driver.find_element(
                                    By.CSS_SELECTOR,
                                    'a[href*="/followers/"] span[aria-label]'
                                ).get_attribute('aria-label').split()[0]
                            ]
                            
                            # Try each method with proper error handling
                            for method in methods:
                                try:
                                    count_text = method()
                                    if count_text:
                                        parsed_count = parse_follower_count(count_text)
                                        if parsed_count:
                                            return parsed_count
                                except Exception:
                                    continue
                            
                            # If no method worked, try one last time after a longer wait
                            time.sleep(random.uniform(2, 3))
                            elements = driver.find_elements(
                                By.XPATH,
                                "//*[contains(text(),'followers') or contains(text(),'Followers')]"
                            )
                            for elem in elements:
                                text = elem.text
                                if text and any(c.isdigit() for c in text):
                                    parsed_count = parse_follower_count(text)
                                    if parsed_count:
                                        return parsed_count
                            
                            raise ValueError("Could not find follower count")
                            
                        except Exception as e:
                            logger.error(f"Error getting follower count for {username}: {str(e)}")
                            return None
                    
                    follower_count = get_follower_count(driver, username)
                    
                    if follower_count is not None:
                        logger.info(f"Successfully found follower count: {follower_count}")
                        break
                        
                    retries += 1
                    if retries < max_retries:
                        logger.info(f"Retrying... ({retries}/{max_retries})")
                        time.sleep(random.uniform(2, 3))
                
                except Exception as e:
                    error_message = str(e)
                    logger.error(f"Error processing {username}: {str(e)}")
                    retries += 1
                    if retries < max_retries:
                        logger.info(f"Retrying... ({retries}/{max_retries})")
                        time.sleep(random.uniform(2, 3))
            
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
            
            # Add longer delay between accounts to avoid rate limiting
            time.sleep(random.uniform(2, 4))
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        if driver:
            driver.quit()
            
    return results

def get_airtable_records():
    """Fetch records from Airtable."""
    headers = {
        'Authorization': f'Bearer {AIRTABLE_PAT}',
        'Content-Type': 'application/json',
    }
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        records = response.json().get('records', [])
        return [{
            'id': record['id'],
            'ig_user': record.get('fields', {}).get('ig_user', ''),
        } for record in records if record.get('fields', {}).get('ig_user')]
    else:
        logger.error(f"Error fetching Airtable records: {response.status_code}")
        return []

def update_airtable_batch(updates):
    """Update multiple records in Airtable in a single request."""
    if not updates:
        return True
        
    headers = {
        'Authorization': f'Bearer {AIRTABLE_PAT}',
        'Content-Type': 'application/json',
    }
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}'
    
    # Process updates in batches of 10
    batch_size = 10
    success = True
    
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        
        # Prepare the records for batch update
        payload = {
            'records': [{
                'id': update['id'],
                'fields': {
                    'ig_followers': update['follower_count']
                }
            } for update in batch]
        }
        
        response = requests.patch(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            logger.info(f"Successfully updated batch of {len(batch)} records in Airtable")
        else:
            logger.error(f"Error updating Airtable records: {response.status_code}")
            logger.error(response.text)
            success = False
            
    return success

if __name__ == "__main__":
    if not check_environment():
        sys.exit(1)
        
    logger.info("Fetching Instagram usernames from Airtable...")
    airtable_records = get_airtable_records()
    
    if not airtable_records:
        logger.error("No Instagram usernames found in Airtable")
        sys.exit(1)
        
    logger.info(f"Found {len(airtable_records)} Instagram usernames")
    
    # Get follower counts
    usernames = [record['ig_user'] for record in airtable_records]
    results = get_follower_counts(usernames)
    
    if not results:
        logger.error("No follower data retrieved")
        sys.exit(1)
        
    # Prepare updates for Airtable
    updates = []
    success_count = 0
    
    for data in results:
        for record in airtable_records:
            if record['ig_user'] == data['username']:
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
