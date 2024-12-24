import sys
import subprocess
import datetime
import pandas as pd

def install_requirements():
    """Install required packages if they're missing."""
    required_packages = [
        'selenium',
        'requests',
        'webdriver-manager',
        'pandas'
    ]
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            print(f"Successfully installed {package}")

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
import random
import requests
import json
import re

# Airtable configuration
AIRTABLE_PAT = "patC3CJ296jcbbAMd.e12065cdff24c5b8e6ed1e9315fa4f5a9233ee9803efd602a7a2a74ff14c5057"
AIRTABLE_BASE_ID = "appdeZcAttBaG5oVI"
AIRTABLE_TABLE_NAME = "Politicians"

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
        print(f"Error parsing follower count '{text}': {str(e)}")
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
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    driver = None
    total_users = len(usernames)
    
    try:
        print("Initializing Chrome driver...")
        try:
            service = Service()
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            print(f"Error with default service, trying ChromeDriverManager: {str(e)}")
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
            except Exception as e:
                print(f"Error with ChromeDriverManager: {str(e)}")
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
                    print(f"\n{index}/{total_users} @{username} (Attempt {retries + 1}/{max_retries})")
                    url = f"https://www.facebook.com/{username}"
                    driver.get(url)
                    wait_random()
                    
                    # Wait for and close the login popup if it appears
                    try:
                        wait = WebDriverWait(driver, 5)
                        close_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'div[aria-label="Close"]')))
                        close_button.click()
                        print("Closed login popup")
                        wait_random()
                    except:
                        print("No login popup found or couldn't close it")
                    
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
                            print(f"Found text: {follower_text}")
                            follower_count = parse_follower_count(follower_text)
                            if follower_count is not None:
                                break
                        except:
                            continue
                    
                    if follower_count is None:
                        error_message = "Could not find or parse follower count"
                        retries += 1
                        if retries < max_retries:
                            print(f"Retrying... ({retries}/{max_retries})")
                            wait_random()
                        
                except TimeoutException as e:
                    error_message = f"Timeout: {str(e)}"
                    print(f"Timeout while processing {username}")
                    retries += 1
                    if retries < max_retries:
                        print(f"Retrying... ({retries}/{max_retries})")
                        wait_random()
                except Exception as e:
                    error_message = str(e)
                    print(f"Error processing {username}: {str(e)}")
                    retries += 1
                    if retries < max_retries:
                        print(f"Retrying... ({retries}/{max_retries})")
                        wait_random()
            
            # Add result whether successful or not
            results.append({
                'username': username,
                'follower_count': follower_count,
                'timestamp': timestamp,
                'error': error_message if follower_count is None else None
            })
            
            if follower_count is not None:
                print(f"Successfully retrieved follower count for {username}: {follower_count:,.0f}")
            else:
                print(f"Failed to process {username} after {max_retries} attempts: {error_message}")
            
            wait_random()
            
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
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
            'facebook_user': record.get('fields', {}).get('facebook_user', ''),
        } for record in records if record.get('fields', {}).get('facebook_user')]
    else:
        print(f"Error fetching Airtable records: {response.status_code}")
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
                    'facebook_followers': update['follower_count']
                }
            } for update in batch]
        }
        
        response = requests.patch(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            print(f"Successfully updated batch of {len(batch)} records in Airtable")
        else:
            print(f"Error updating Airtable records: {response.status_code}")
            print(response.text)
            success = False
            
    return success

if __name__ == "__main__":
    print("Fetching Facebook usernames from Airtable...")
    airtable_records = get_airtable_records()
    
    if not airtable_records:
        print("No Facebook usernames found in Airtable")
        sys.exit(1)
        
    print(f"Found {len(airtable_records)} Facebook usernames")
    
    # Get follower counts
    usernames = [record['facebook_user'] for record in airtable_records]
    results = get_follower_counts(usernames)
    
    if not results:
        print("No follower data retrieved")
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
        print(f"Updating {len(updates)} records in Airtable...")
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
    print("\nFinal Results:")
    print("-" * 50)
    
    if successful_results:
        print("\nSuccessful Updates:")
        for result in successful_results:
            count = result['follower_count']
            print(f"{result['username']}: {count:,.0f} followers")
    
    if failed_results:
        print("\nFailed Updates:")
        for result in failed_results:
            print(f"{result['username']}: Not found")
    
    if results:
        print(f"\nTimestamp: {results[0]['timestamp']}")
        print(f"Successfully updated {success_count} out of {len(results)} records in Airtable")
    if failed_results:
        print(f"Failed to get follower counts for {len(failed_results)} accounts")
