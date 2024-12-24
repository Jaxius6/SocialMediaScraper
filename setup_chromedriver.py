import sys
import subprocess
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

def setup_chromedriver():
    print("Setting up ChromeDriver...")
    try:
        # Configure ChromeDriver with specific options for Windows
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        
        # Install ChromeDriver and get the correct path
        driver_manager = ChromeDriverManager()
        driver_path = driver_manager.install()
        
        # Make sure we're using the actual chromedriver executable
        if 'THIRD_PARTY_NOTICES' in driver_path:
            driver_dir = os.path.dirname(driver_path)
            driver_path = os.path.join(driver_dir, 'chromedriver.exe')
        
        print(f"ChromeDriver path: {driver_path}")
        
        if not os.path.exists(driver_path):
            print(f"Error: ChromeDriver not found at {driver_path}")
            return False
            
        # Create service with the driver path
        service = Service(executable_path=driver_path)
        
        # Test the installation
        print("Testing ChromeDriver installation...")
        driver = webdriver.Chrome(service=service, options=options)
        driver.quit()
        
        print("ChromeDriver setup completed successfully!")
        return True
    except Exception as e:
        print(f"Error setting up ChromeDriver: {str(e)}")
        print(f"Python version: {sys.version}")
        print(f"Selenium version: {webdriver.__version__}")
        return False

if __name__ == '__main__':
    setup_chromedriver()
