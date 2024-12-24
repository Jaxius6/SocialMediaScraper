import sys
import subprocess
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def setup_chromedriver():
    print("Setting up ChromeDriver...")
    try:
        # Try to install ChromeDriver
        driver_path = ChromeDriverManager().install()
        print(f"ChromeDriver installed successfully at: {driver_path}")
        
        # Test the installation
        service = Service(driver_path)
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Run in headless mode for testing
        driver = webdriver.Chrome(service=service, options=options)
        driver.quit()
        print("ChromeDriver setup completed successfully!")
        return True
    except Exception as e:
        print(f"Error setting up ChromeDriver: {str(e)}")
        return False

if __name__ == '__main__':
    setup_chromedriver()
