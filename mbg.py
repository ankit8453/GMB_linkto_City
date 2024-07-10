import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Current version of the script
CURRENT_VERSION = "1.0.0"
VERSION_CHECK_URL = "https://raw.githubusercontent.com/ankit8453/GMB_linkto_City/main/latest_version.txt?token=GHSAT0AAAAAACUKHZBZY3DLUXXTXLUAKJ4YZUO4KPA"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Determine if running in a packaged environment
is_packaged = hasattr(sys, 'frozen')

# Function to check for the latest version
def check_latest_version():
    try:
        logging.info(f"Checking for the latest version from {VERSION_CHECK_URL}")
        response = requests.get(VERSION_CHECK_URL)
        response.raise_for_status()
        latest_version = response.text.strip()
        logging.info(f"Latest version from server: {latest_version}")
        return latest_version
    except requests.RequestException as e:
        logging.error(f"Failed to check for the latest version: {e}")
        return None

# Check for updates only if running as a packaged executable
if is_packaged:
    latest_version = check_latest_version()
    logging.info(f"Current version: {CURRENT_VERSION}, Latest version: {latest_version}")
    if latest_version and latest_version != CURRENT_VERSION:
        logging.error(f"Outdated version detected. Please update to the latest version {latest_version}.")
        sys.exit(1)

# Get the Excel file path from command line arguments or default to 'mbg.xlsx'
excel_file = sys.argv[1] if len(sys.argv) > 1 else 'mbg.xlsx'

# Constants
gmb_column = 'GMB Link'
city_column = 'City'
driver_path = 'path_to_your_webdriver/chromedriver'

# Read the Excel sheet
data = pd.read_excel(excel_file)

# Ensure the 'GMB Link' column exists
if gmb_column not in data.columns:
    raise KeyError(f"Column '{gmb_column}' not found in the Excel file. Available columns: {data.columns}")

# Add City column if it doesn't exist
if city_column not in data.columns:
    data[city_column] = ''

# Function to initialize a Chrome driver
def init_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")  # Run headless Chrome to avoid opening the browser window
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome()

# Function to scrape city name from a GMB link
def get_city_name(index, gmb_link):
    driver = init_driver()
    city_name = None
    try:
        driver.get(gmb_link)
        # Wait until the city name element is present
        city_name_element = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'kR99db'))
        )
        city_name = city_name_element.text
        logging.info(f"Captured city name for row {index + 1}: {city_name}")
    except (TimeoutException, NoSuchElementException) as e:
        logging.error(f"Error while processing row {index + 1}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while processing row {index + 1}: {e}")
    finally:
        driver.quit()
    return index, city_name

# Use ThreadPoolExecutor to process rows in parallel
max_workers = 5  # Adjust this based on your system capabilities
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(get_city_name, index, row[gmb_column]) for index, row in data.iterrows()]
    for future in as_completed(futures):
        index, city_name = future.result()
        data.at[index, city_column] = city_name

# Save the updated DataFrame to the same Excel sheet
data.to_excel(excel_file, index=False)
logging.info(f"Process completed and Excel file saved to {excel_file}")
