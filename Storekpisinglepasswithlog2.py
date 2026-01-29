import json
import csv
import sqlite3
from datetime import datetime
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException
import pandas as pd
import time
import logging
import os
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Keywords untuk filter toko yang harus di-skip
SKIP_KEYWORDS = ['tutup', 'renovasi', 'maintenance', 'pindah', 'closed', 'relokasi','Tutup','(Tutup)','(tutup)']

def should_skip_store(store_name):
    """Check if store should be skipped based on keywords"""
    for keyword in SKIP_KEYWORDS:
        if re.search(rf'\b{keyword}\b', store_name, re.IGNORECASE):
            logger.info(f"⏭️  SKIPPED ({keyword.title()}): {store_name}")
            return True
    return False

class DataStorage:
    """Class to handle multiple storage formats"""
    
    def __init__(self, base_filename=None):
        self.base_filename = base_filename or f"pmo_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.all_data = []
        
    def add_store_data(self, store_data):
        """Add store data to storage"""
        self.all_data.append(store_data)
    
    def save_to_csv(self, data=None):
        """Save data to CSV file"""
        data_to_save = data if data is not None else self.all_data
        
        if not data_to_save:
            logger.warning("No data to save to CSV")
            return None
        
        filename = f"{self.base_filename}.csv"
        try:
            df = pd.DataFrame(data_to_save)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            logger.info(f"✓ Data saved to CSV: {filename}")
            logger.info(f"  Total records: {len(data_to_save)}")
            return filename
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return None
    
    def save_to_json(self, data=None):
        """Save data to JSON file with structured format"""
        data_to_save = data if data is not None else self.all_data
        
        if not data_to_save:
            logger.warning("No data to save to JSON")
            return None
        
        filename = f"{self.base_filename}.json"
        try:
            # Create structured JSON format
            json_data = {
                "metadata": {
                    "extraction_date": datetime.now().isoformat(),
                    "total_stores": len(data_to_save),
                    "data_format": "structured"
                },
                "stores": data_to_save
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✓ Data saved to JSON: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            return None
    
    def save_to_sqlite(self, data=None):
        """Save data to SQLite database"""
        data_to_save = data if data is not None else self.all_data
        
        if not data_to_save:
            logger.warning("No data to save to SQLite")
            return None
        
        filename = f"{self.base_filename}.db"
        try:
            conn = sqlite3.connect(filename)
            cursor = conn.cursor()
            
            # Create table for store information
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    regional TEXT,
                    store_name TEXT,
                    year INTEGER,
                    month INTEGER,
                    extraction_type TEXT,
                    extraction_datetime TEXT,
                    error_message TEXT
                )
            ''')
            
            # Create table for scores
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_id INTEGER,
                    score_type TEXT,
                    score_value REAL,
                    FOREIGN KEY (store_id) REFERENCES stores (id)
                )
            ''')
            
            # Create table for KPIs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS kpis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_id INTEGER,
                    kpi_number TEXT,
                    kpi_name TEXT,
                    kpi_value REAL,
                    achievement_value TEXT,
                    FOREIGN KEY (store_id) REFERENCES stores (id)
                )
            ''')
            
            # Insert data
            for store_data in data_to_save:
                # Insert store info
                cursor.execute('''
                    INSERT INTO stores (regional, store_name, year, month, extraction_type, 
                                      extraction_datetime, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    store_data.get('Regional'),
                    store_data.get('Store'),
                    store_data.get('Year'),
                    store_data.get('Month'),
                    store_data.get('Extraction_Type'),
                    store_data.get('Extraction_DateTime'),
                    store_data.get('Error_Message', 'None')
                ))
                
                store_id = cursor.lastrowid
                
                # Insert scores
                score_types = ['Financial', 'Customer', 'Internal_Business_Process', 
                             'Learning_and_Growth', 'Total']
                for score_type in score_types:
                    score_key = f"{score_type}_Score"
                    if score_key in store_data:
                        cursor.execute('''
                            INSERT INTO scores (store_id, score_type, score_value)
                            VALUES (?, ?, ?)
                        ''', (store_id, score_type, store_data[score_key]))
                
                # Insert KPIs
                for key, value in store_data.items():
                    if key.startswith('KPI_') and key.endswith('_Name'):
                        kpi_num = key.replace('_Name', '').replace('KPI_', '')
                        kpi_name = value
                        kpi_value_key = f"KPI_{kpi_num}_Value"
                        kpi_value = store_data.get(kpi_value_key, 0.0)
                        
                        cursor.execute('''
                            INSERT INTO kpis (store_id, kpi_number, kpi_name, kpi_value)
                            VALUES (?, ?, ?, ?)
                        ''', (store_id, kpi_num, kpi_name, kpi_value))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✓ Data saved to SQLite database: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Error saving to SQLite: {e}")
            return None
    
    def save_to_text(self, data=None):
        """Save data to human-readable text file"""
        data_to_save = data if data is not None else self.all_data
        
        if not data_to_save:
            logger.warning("No data to save to text")
            return None
        
        filename = f"{self.base_filename}_report.txt"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("PMO DATA EXTRACTION REPORT\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Extraction Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Stores: {len(data_to_save)}\n\n")
                
                for i, store_data in enumerate(data_to_save, 1):
                    f.write(f"\n{'='*60}\n")
                    f.write(f"STORE {i}: {store_data.get('Store', 'Unknown')}\n")
                    f.write(f"{'='*60}\n")
                    f.write(f"Regional: {store_data.get('Regional', 'N/A')}\n")
                    f.write(f"Year: {store_data.get('Year', 'N/A')}\n")
                    f.write(f"Month: {store_data.get('Month', 'N/A')}\n")
                    f.write(f"Extraction Type: {store_data.get('Extraction_Type', 'N/A')}\n")
                    
                    # Write scores
                    f.write("\nSCORES:\n")
                    f.write("-" * 40 + "\n")
                    score_keys = [k for k in store_data.keys() if k.endswith('_Score')]
                    for key in sorted(score_keys):
                        score_name = key.replace('_', ' ').replace('Score', '').strip()
                        f.write(f"{score_name}: {store_data[key]:.2f}\n")
                    
                    # Write KPIs
                    f.write("\nKPIs:\n")
                    f.write("-" * 40 + "\n")
                    
                    # Group KPIs by their numbers
                    kpi_numbers = set()
                    for key in store_data.keys():
                        if key.startswith('KPI_') and '_Name' in key:
                            kpi_num = key.split('_')[1]
                            kpi_numbers.add(kpi_num)
                    
                    for kpi_num in sorted(kpi_numbers, key=lambda x: int(x)):
                        name_key = f"KPI_{kpi_num}_Name"
                        value_key = f"KPI_{kpi_num}_Value"
                        
                        if name_key in store_data and value_key in store_data:
                            kpi_name = store_data[name_key]
                            kpi_value = store_data[value_key]
                            f.write(f"Control {kpi_num}: {kpi_name}\n")
                            f.write(f"  YTD Achievement: {kpi_value:,.2f}\n")
                    
                    # Write extracted KPI count
                    total_key = 'Total_KPIs_Extracted' if 'Total_KPIs_Extracted' in store_data else 'Financial_KPIs_Extracted'
                    if total_key in store_data:
                        f.write(f"\nTotal KPIs Extracted: {store_data[total_key]}\n")
                    
                    if store_data.get('Error_Message') not in ['None', None]:
                        f.write(f"\n⚠️  ERROR: {store_data.get('Error_Message')}\n")
            
            logger.info(f"✓ Report saved to text file: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Error saving to text file: {e}")
            return None
    
    def save_all_formats(self):
        """Save data in all available formats"""
        if not self.all_data:
            logger.warning("No data to save")
            return []
        
        saved_files = []
        
        # Save to CSV
        csv_file = self.save_to_csv()
        if csv_file:
            saved_files.append(("CSV", csv_file))
        
        # Save to JSON
        json_file = self.save_to_json()
        if json_file:
            saved_files.append(("JSON", json_file))
        
        # Save to SQLite
        sqlite_file = self.save_to_sqlite()
        if sqlite_file:
            saved_files.append(("SQLite", sqlite_file))
        
        # Save to Text
        text_file = self.save_to_text()
        if text_file:
            saved_files.append(("Text Report", text_file))
        
        return saved_files


class PMOFastDataExtractor:
    def __init__(self, username, password, year=None, month=None, target_regionals=None, 
                 headless=False, extract_type="all", storage_formats=None):
        """
        Initialize the PMO Data Extractor - FAST VERSION
        
        Args:
            username (str): PMO username
            password (str): PMO password
            year (int): Year to extract
            month (int): Month to extract (1-12)
            target_regionals (list): List of regional letters to extract
            headless (bool): Run in headless mode
            extract_type (str): Type of data to extract. Options:
                - "all": All perspectives (FAST - single pass extraction)
                - "financial": Only financial metrics
                - "scores": Only score metrics
            storage_formats (list): List of storage formats. Options:
                - "csv": CSV format
                - "json": JSON format
                - "sqlite": SQLite database
                - "text": Text report
                - "all": All formats (default)
        """
        self.username = username
        self.password = password
        self.driver = None
        self.wait = None
        self.setup_driver(headless)
        
        self.target_regionals = target_regionals or ['E']
        self.extract_type = extract_type  # "all", "financial", or "scores"
        
        if year and month:
            self.current_year = str(year)
            self.current_month = month
        else:
            current_date = datetime.now()
            self.current_year = str(current_date.year)
            self.current_month = current_date.month
        
        # Initialize data storage
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        regional_str = '_'.join(self.target_regionals)
        
        # Set base filename based on extraction type
        if self.extract_type == "financial":
            base_name = f"pmo_financial_{regional_str}_{self.current_year}_{self.current_month:02d}_{timestamp}"
        elif self.extract_type == "scores":
            base_name = f"pmo_scores_{regional_str}_{self.current_year}_{self.current_month:02d}_{timestamp}"
        else:  # "all"
            base_name = f"pmo_all_kpis_{regional_str}_{self.current_year}_{self.current_month:02d}_{timestamp}"
        
        self.storage = DataStorage(base_name)
        
        # Set storage formats
        if storage_formats is None or "all" in storage_formats:
            self.storage_formats = ["csv", "json", "sqlite", "text"]
        else:
            self.storage_formats = storage_formats
    
    def setup_driver(self, headless=False):
        """Initialize Chrome driver with options"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.maximize_window()
            self.wait = WebDriverWait(self.driver, 30)
            logger.info("Chrome driver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize driver: {e}")
            raise
    
    def login(self):
        """Handle login process"""
        try:
            logger.info("Navigating to login page")
            self.driver.get("https://pmo.mykg.id/Systems/Login.aspx ")
            
            username_field = self.wait.until(
                EC.presence_of_element_located((By.ID, "txt_UserID"))
            )
            username_field.clear()
            username_field.send_keys(self.username)
            
            password_field = self.driver.find_element(By.ID, "txt_Password")
            password_field.clear()
            password_field.send_keys(self.password)
            
            sign_in_btn = self.driver.find_element(By.ID, "robLogin")
            sign_in_btn.click()
            
            logger.info("Login credentials submitted")
            
            try:
                next_btn = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "btnSaveInputRole"))
                )
                next_btn.click()
                logger.info("Popup modal handled")
            except TimeoutException:
                logger.warning("No popup modal found or timeout")
            
            self.wait.until(EC.url_contains("Home/Home.aspx"))
            logger.info("Login successful")
            
        except Exception as e:
            logger.error(f"Login failed: {e}")
            raise
    
    def navigate_to_dashboard(self):
        """Navigate to Performance Review Dashboard"""
        try:
            logger.info("Navigating to Performance Review Dashboard")
            
            performance_menu = self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//a[text()='Performance Review']"))
            )
            
            actions = ActionChains(self.driver)
            actions.move_to_element(performance_menu).perform()
            time.sleep(3)
            
            submenu = self.wait.until(
                EC.visibility_of_element_located((By.ID, "ctl00_MenuControlHorizontal1_NavigationMenu:submenu:16"))
            )
            
            dashboard_link = submenu.find_element(By.XPATH, ".//a[@href='Dashboard.aspx']")
            
            try:
                dashboard_link.click()
            except ElementClickInterceptedException:
                self.driver.execute_script("arguments[0].click();", dashboard_link)
            
            self.wait.until(EC.url_contains("Dashboard.aspx"))
            logger.info("Successfully navigated to dashboard")
            
        except Exception as e:
            logger.error(f"Failed to navigate to dashboard: {e}")
            try:
                logger.info("Trying direct navigation to dashboard")
                current_url = self.driver.current_url
                base_url = current_url.split('/Home/')[0]
                dashboard_url = f"{base_url}/Performance%20Review/Dashboard.aspx"
                self.driver.get(dashboard_url)
                
                self.wait.until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_ddlPeriod"))
                )
                logger.info("Direct navigation to dashboard successful")
                
            except Exception as e2:
                logger.error(f"Direct navigation also failed: {e2}")
                raise
    
    def select_year_and_month(self):
        """Select specified year and month"""
        try:
            logger.info(f"Selecting year {self.current_year} and month {self.current_month}")
            
            year_dropdown = Select(self.wait.until(
                EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_ddlPeriod"))
            ))
            year_dropdown.select_by_visible_text(self.current_year)
            time.sleep(3)
            
            month_dropdown = Select(self.driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddlMonth"))
            month_names = ["January", "February", "March", "April", "May", "June",
                          "July", "August", "September", "October", "November", "December"]
            
            selected_month_name = month_names[self.current_month - 1]
            month_dropdown.select_by_visible_text(selected_month_name)
            time.sleep(3)
            
            logger.info(f"Year and month selected successfully: {self.current_year} - {selected_month_name}")
            
        except Exception as e:
            logger.error(f"Failed to select year/month: {e}")
            raise
    
    def click_view_other_scorecard(self, max_attempts=5):
        """Click the View Other Scorecard button with retry logic"""
        for attempt in range(max_attempts):
            try:
                logger.info(f"Attempting to click View Other Scorecard (attempt {attempt + 1}/{max_attempts})")
                
                time.sleep(5)
                
                view_btn = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_btnViewOtherSCO"))
                )
                
                try:
                    view_btn.click()
                    logger.info("View Other Scorecard button clicked successfully")
                    time.sleep(5)
                    return True
                except ElementClickInterceptedException:
                    logger.warning("Regular click intercepted, trying JavaScript click")
                    self.driver.execute_script("arguments[0].click();", view_btn)
                    time.sleep(5)
                    return True
                    
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"All {max_attempts} attempts failed to click View Other Scorecard")
                    return False
        return False
    
    def get_stores_by_regional_fresh(self, regional_letter):
        """Get all stores for a specific regional with fresh element discovery and filtering"""
        stores = []
        skipped_stores = []
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                logger.info(f"Getting stores for Regional {regional_letter} (attempt {attempt + 1})")
                
                time.sleep(5)
                
                regional_divs = {
                    'A': 'ctl00_ContentPlaceHolder1_OrganizationTreeView1_tvHierarchyn22Nodes',
                    'B': 'ctl00_ContentPlaceHolder1_OrganizationTreeView1_tvHierarchyn43Nodes', 
                    'C': 'ctl00_ContentPlaceHolder1_OrganizationTreeView1_tvHierarchyn61Nodes',
                    'D': 'ctl00_ContentPlaceHolder1_OrganizationTreeView1_tvHierarchyn84Nodes',
                    'E': 'ctl00_ContentPlaceHolder1_OrganizationTreeView1_tvHierarchyn107Nodes',
                    'F': 'ctl00_ContentPlaceHolder1_OrganizationTreeView1_tvHierarchyn128Nodes',
                    'G': 'ctl00_ContentPlaceHolder1_OrganizationTreeView1_tvHierarchyn156Nodes'
                }
                
                if regional_letter not in regional_divs:
                    logger.error(f"Regional {regional_letter} not found in mapping")
                    return stores
                
                regional_div = self.wait.until(
                    EC.presence_of_element_located((By.ID, regional_divs[regional_letter]))
                )
                
                store_links = regional_div.find_elements(By.XPATH, ".//a[contains(@class, 'NodeStyle')]")
                
                for i, link in enumerate(store_links):
                    try:
                        store_name = link.text.strip()
                        
                        # Skip regional manager entries
                        if store_name.startswith('RM -'):
                            continue
                        
                        # Filter: Skip toko dengan keyword tutup/renovasi/dll
                        if should_skip_store(store_name):
                            skipped_stores.append(store_name)
                            continue
                        
                        # Toko valid, tambahkan ke list
                        if store_name:
                            stores.append({
                                'name': store_name,
                                'element': link,
                                'regional': regional_letter,
                                'index': i
                            })
                    except Exception as e:
                        logger.warning(f"Error processing store link {i}: {e}")
                        continue
                
                if stores or skipped_stores:
                    logger.info(f"✓ Found {len(stores)} active stores in Regional {regional_letter}")
                    if skipped_stores:
                        logger.info(f"⏭️  Skipped {len(skipped_stores)} closed/inactive stores")
                    return stores
                else:
                    logger.warning(f"No stores found in Regional {regional_letter} on attempt {attempt + 1}")
                    if attempt < max_attempts - 1:
                        time.sleep(3)
                        continue
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to get stores for regional {regional_letter}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(3)
                    continue
        
        logger.error(f"Failed to get stores for Regional {regional_letter} after all attempts")
        return stores
    
    def wait_for_data_refresh_improved(self, store_name, max_wait_time=45):
        """Wait for data to refresh with improved verification"""
        try:
            logger.info(f"Waiting for data refresh for store: {store_name}")
            
            start_time = time.time()
            
            # Wait for revenue element
            target_id = f"ctl00_ContentPlaceHolder1_grvScorecard_ctl02_lblYTDAchievement{self.current_month}"
            
            try:
                self.wait.until(EC.presence_of_element_located((By.ID, target_id)))
                logger.info(f"Element found for data verification")
            except TimeoutException:
                logger.error(f"Data element not found within timeout")
                return False
            
            stable_count = 0
            required_stable_count = 3
            last_value = None
            
            while time.time() - start_time < max_wait_time:
                try:
                    target_element = self.driver.find_element(By.ID, target_id)
                    current_value = target_element.text.strip()
                    
                    if current_value == last_value and current_value != "" and current_value != "-":
                        stable_count += 1
                        logger.info(f"Value stable ({stable_count}/{required_stable_count}): {current_value}")
                        
                        if stable_count >= required_stable_count:
                            logger.info(f"Data has stabilized for {store_name}")
                            
                            time.sleep(2)
                            return True
                    else:
                        if last_value is not None and current_value != last_value:
                            logger.info(f"Value changed from '{last_value}' to '{current_value}' - resetting stability counter")
                        stable_count = 0
                        last_value = current_value
                    
                    time.sleep(1.5)
                    
                except StaleElementReferenceException:
                    logger.warning("Stale element during data refresh wait")
                    stable_count = 0
                    time.sleep(2)
                    continue
                except Exception as e:
                    logger.warning(f"Error during stability check: {e}")
                    time.sleep(2)
                    continue
            
            logger.warning(f"Data refresh timeout for {store_name} after {max_wait_time}s")
            return False
            
        except Exception as e:
            logger.error(f"Error waiting for data refresh: {e}")
            return False
    
    def select_store_robust(self, store_info, max_attempts=5):
        """Select a specific store with robust error handling"""
        store_name = store_info['name']
        
        for attempt in range(max_attempts):
            try:
                logger.info(f"Attempting to select store '{store_name}' (attempt {attempt + 1}/{max_attempts})")
                
                regional_letter = store_info['regional']
                fresh_stores = self.get_stores_by_regional_fresh(regional_letter)
                
                target_store = None
                for fresh_store in fresh_stores:
                    if fresh_store['name'] == store_name:
                        target_store = fresh_store
                        break
                
                if not target_store:
                    logger.error(f"Could not find store '{store_name}' in fresh store list")
                    if attempt < max_attempts - 1:
                        time.sleep(3)
                        continue
                    else:
                        return False
                
                store_element = target_store['element']
                
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", store_element)
                time.sleep(2)
                
                try:
                    store_element.click()
                    logger.info(f"Store '{store_name}' clicked successfully")
                except ElementClickInterceptedException:
                    self.driver.execute_script("arguments[0].click();", store_element)
                    logger.info(f"Store '{store_name}' selected with JavaScript click")
                
                if self.wait_for_data_refresh_improved(store_name):
                    logger.info(f"Data successfully refreshed for {store_name}")
                    return True
                else:
                    logger.warning(f"Data refresh verification failed for {store_name}")
                    if attempt < max_attempts - 1:
                        time.sleep(5)
                        continue
                    else:
                        return False
                    
            except StaleElementReferenceException:
                logger.warning(f"Stale element reference for store '{store_name}' on attempt {attempt + 1}")
                if attempt < max_attempts - 1:
                    time.sleep(3)
                    continue
            except Exception as e:
                logger.error(f"Error selecting store '{store_name}' on attempt {attempt + 1}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(3)
                    continue
        
        logger.error(f"Failed to select store '{store_name}' after all attempts")
        return False
    
    def extract_all_data_fast_single_pass(self):
        """
        Extract ALL data in ONE FAST PASS - from ctl02 to ctl22
        This is the efficient version that reads everything at once
        """
        all_data = {}
        kpi_count = 0
        
        try:
            logger.info("Starting FAST single-pass extraction...")
            
            # First, extract scores if we want them
            if self.extract_type == "all" or self.extract_type == "scores":
                scores = self.extract_score_data_fast()
                all_data.update(scores)
            
            # Now extract all KPIs from ctl02 to ctl22 in one pass
            current_perspective = "Financial"
            
            for i in range(2, 23):  # ctl02 to ctl22
                try:
                    # Get KPI name
                    kpi_id = f"ctl00_ContentPlaceHolder1_grvScorecard_ctl{i:02d}_lblKPI"
                    kpi_element = self.driver.find_element(By.ID, kpi_id)
                    kpi_name = kpi_element.text.strip()
                    
                    if not kpi_name:
                        continue
                    
                    # Get YTD achievement
                    achievement_id = f"ctl00_ContentPlaceHolder1_grvScorecard_ctl{i:02d}_lblYTDAchievement{self.current_month}"
                    achievement_element = self.driver.find_element(By.ID, achievement_id)
                    achievement_value = achievement_element.text.strip()
                    
                    # Convert achievement value to float
                    numeric_value = 0.0
                    if achievement_value not in ["-", "", None]:
                        try:
                            numeric_value = float(achievement_value.replace(",", ""))
                        except ValueError:
                            numeric_value = 0.0
                    
                    # Clean KPI name for column name
                    clean_kpi_name = re.sub(r'[^\w\s]', '', kpi_name)
                    clean_kpi_name = re.sub(r'\s+', '_', clean_kpi_name.strip())
                    
                    # Determine perspective based on control number and KPI name
                    if self.extract_type == "all":
                        # For "all" extraction, include perspective in column name
                        if i <= 6 or "revenue" in kpi_name.lower() or "cogs" in kpi_name.lower() or "profit" in kpi_name.lower() or "expense" in kpi_name.lower():
                            perspective = "Financial"
                        elif "customer" in kpi_name.lower() or "satisfaction" in kpi_name.lower():
                            perspective = "Customer"
                        elif "stock" in kpi_name.lower() or "fulfillment" in kpi_name.lower() or "sales" in kpi_name.lower():
                            perspective = "Customer"
                        elif "productivity" in kpi_name.lower() or "conversion" in kpi_name.lower() or "fraud" in kpi_name.lower():
                            perspective = "Internal_Business_Process"
                        elif "learning" in kpi_name.lower() or "growth" in kpi_name.lower() or "hr" in kpi_name.lower():
                            perspective = "Learning_and_Growth"
                        else:
                            perspective = "Other"
                        
                        # Create column name with perspective
                        col_name = f"{perspective}_{clean_kpi_name}_ACH"
                    else:
                        # For "financial" extraction, just use Financial prefix
                        col_name = f"Financial_{clean_kpi_name}_ACH"
                    
                    # Store in data dictionary
                    all_data[col_name] = numeric_value
                    
                    # Also store with simple KPI number for reference
                    all_data[f"KPI_{i:02d}_Value"] = numeric_value
                    all_data[f"KPI_{i:02d}_Name"] = kpi_name
                    
                    # Log the extraction (this is the FAST log you see)
                    logger.info(f"Control {i:02d}: {kpi_name}")
                    logger.info(f"  YTD Achievement: {achievement_value}")
                    
                    kpi_count += 1
                    
                except NoSuchElementException:
                    # If we can't find ctl22, we've reached the end
                    if i == 22:
                        logger.info(f"Control 22 not found. Total KPIs extracted: {kpi_count}")
                        break
                    else:
                        # Some controls might be missing, continue
                        continue
                except Exception as e:
                    logger.warning(f"Error extracting control {i:02d}: {e}")
                    continue
            
            # Add summary info
            all_data['Total_KPIs_Extracted'] = kpi_count
            
            logger.info(f"✓ FAST extraction complete: {kpi_count} KPIs extracted")
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error in fast single-pass extraction: {e}")
            return all_data
    
    def extract_score_data_fast(self):
        """Extract score metrics quickly"""
        scores = {}
        
        # Mapping of score types to their element IDs
        score_mapping = {
            'Financial_Score': 'ctl00_ContentPlaceHolder1_lblAchievementYTD_F',
            'Customer_Score': 'ctl00_ContentPlaceHolder1_lblAchievementYTD_CS',
            'Internal_Business_Process_Score': 'ctl00_ContentPlaceHolder1_lblAchievementYTD_IBP',
            'Learning_and_Growth_Score': 'ctl00_ContentPlaceHolder1_lblAchievementYTD_LG',
            'Total_Score': 'ctl00_ContentPlaceHolder1_lblAchievementYTD_Total'
        }
        
        for score_name, element_id in score_mapping.items():
            try:
                score_element = self.driver.find_element(By.ID, element_id)
                score_value = score_element.text.strip()
                
                # Clean and convert the score value
                if score_value == "-" or score_value == "":
                    scores[score_name] = 0.0
                else:
                    try:
                        scores[score_name] = float(score_value)
                    except ValueError:
                        scores[score_name] = 0.0
                
                logger.info(f"{score_name.replace('_', ' ')}: {scores[score_name]}")
                
            except NoSuchElementException:
                scores[score_name] = 0.0
            except Exception as e:
                logger.warning(f"Error extracting {score_name}: {e}")
                scores[score_name] = 0.0
        
        return scores
    
    def extract_financial_data_fast(self):
        """Extract only financial data quickly"""
        all_data = {}
        kpi_count = 0
        
        try:
            logger.info("Extracting financial data only (fast)...")
            
            # Extract only financial KPIs from ctl02 to ctl07
            for i in range(2, 8):  # ctl02 to ctl07
                try:
                    # Get KPI name
                    kpi_id = f"ctl00_ContentPlaceHolder1_grvScorecard_ctl{i:02d}_lblKPI"
                    kpi_element = self.driver.find_element(By.ID, kpi_id)
                    kpi_name = kpi_element.text.strip()
                    
                    if not kpi_name:
                        continue
                    
                    # Get YTD achievement
                    achievement_id = f"ctl00_ContentPlaceHolder1_grvScorecard_ctl{i:02d}_lblYTDAchievement{self.current_month}"
                    achievement_element = self.driver.find_element(By.ID, achievement_id)
                    achievement_value = achievement_element.text.strip()
                    
                    # Convert to float
                    numeric_value = 0.0
                    if achievement_value not in ["-", "", None]:
                        try:
                            numeric_value = float(achievement_value.replace(",", ""))
                        except ValueError:
                            numeric_value = 0.0
                    
                    # Clean KPI name for column name
                    clean_kpi_name = re.sub(r'[^\w\s]', '', kpi_name)
                    clean_kpi_name = re.sub(r'\s+', '_', clean_kpi_name.strip())
                    
                    # Create column name
                    col_name = f"Financial_{clean_kpi_name}_ACH"
                    
                    # Store in data dictionary
                    all_data[col_name] = numeric_value
                    
                    # Log the extraction
                    logger.info(f"Financial KPI {i:02d}: {kpi_name}")
                    logger.info(f"  YTD Achievement: {achievement_value}")
                    
                    kpi_count += 1
                    
                except Exception as e:
                    # Stop if we can't find more financial KPIs
                    if i == 2:
                        logger.error(f"No revenue data found: {e}")
                        break
                    continue
            
            all_data['Financial_KPIs_Extracted'] = kpi_count
            logger.info(f"✓ Financial extraction complete: {kpi_count} KPIs extracted")
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error extracting financial data: {e}")
            return all_data
    
    def extract_scores_data_fast(self):
        """Extract ONLY score metrics quickly - optimized version"""
        all_data = {}
        
        try:
            logger.info("Extracting ONLY score metrics (fast)...")
            
            # Extract scores using the same method as in extract_score_data_fast
            scores = self.extract_score_data_fast()
            all_data.update(scores)
            
            # Add score metrics count
            all_data['Score_Metrics_Extracted'] = len(scores)
            
            logger.info(f"✓ Score extraction complete: {len(scores)} scores extracted")
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error extracting score data: {e}")
            return all_data
    
    def extract_store_data_fast(self, store_info):
        """Extract data for a store - FAST VERSION (single pass)"""
        try:
            store_name = store_info['name']
            regional = store_info['regional']
            
            logger.info(f"Extracting data for {store_name} (Regional {regional})")
            
            time.sleep(3)  # Small wait for page to settle
            
            # Base result structure
            result = {
                'Regional': regional,
                'Store': store_name,
                'Year': self.current_year,
                'Month': self.current_month,
                'Extraction_Type': self.extract_type,
                'Error_Message': 'None',
                'Extraction_DateTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Extraction_Method': 'Single-Pass-Fast'
            }
            
            if self.extract_type == "financial":
                # Extract only financial data (fast)
                financial_data = self.extract_financial_data_fast()
                result.update(financial_data)
                
            elif self.extract_type == "scores":
                # Extract only score metrics (fast)
                scores_data = self.extract_scores_data_fast()
                result.update(scores_data)
                
            else:  # "all"
                # Extract ALL data in ONE FAST PASS
                all_data = self.extract_all_data_fast_single_pass()
                result.update(all_data)
            
            # Add to storage
            self.storage.add_store_data(result)
            
            logger.info(f"✓ FAST extraction complete for {store_name}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting data for {store_info['name']}: {e}")
            
            # Create error record
            result = {
                'Regional': store_info['regional'],
                'Store': store_info['name'],
                'Year': self.current_year,
                'Month': self.current_month,
                'Extraction_Type': self.extract_type,
                'Error_Message': str(e),
                'Extraction_DateTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Extraction_Method': 'Error'
            }
            
            # Fill with zeros/default values based on extraction type
            if self.extract_type == "financial":
                result.update({
                    'Financial_KPIs_Extracted': 0
                })
            elif self.extract_type == "scores":
                result.update({
                    'Score_Metrics_Extracted': 0
                })
            else:  # "all"
                result.update({
                    'Total_KPIs_Extracted': 0
                })
            
            # Add to storage
            self.storage.add_store_data(result)
            return result
    
    def close_modal_if_open(self, max_attempts=3):
        """Check if modal is open and close it if needed"""
        for attempt in range(max_attempts):
            try:
                time.sleep(3)
                
                close_buttons = self.driver.find_elements(By.XPATH, "//input[@value='Close']")
                
                for close_btn in close_buttons:
                    if close_btn.is_displayed():
                        try:
                            close_btn.click()
                            time.sleep(3)
                            logger.info("Modal closed successfully")
                            return True
                        except:
                            self.driver.execute_script("arguments[0].click();", close_btn)
                            time.sleep(3)
                            logger.info("Modal closed with JavaScript click")
                            return True
                
                logger.info("No open modal found")
                return True
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} to close modal failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(3)
                    continue
                    
        logger.warning("Could not close modal after all attempts")
        return False
    
    def run_extraction(self):
        """Main extraction process - FAST VERSION"""
        try:
            logger.info("=" * 60)
            logger.info("Starting PMO Data Extractor - FAST VERSION")
            logger.info(f"Extraction Type: {self.extract_type}")
            logger.info(f"Year: {self.current_year}, Month: {self.current_month}")
            logger.info(f"Target Regionals: {self.target_regionals}")
            logger.info(f"Storage Formats: {', '.join(self.storage_formats)}")
            logger.info("=" * 60)
            
            # Step 1: Login
            self.login()
            
            # Step 2: Navigate to dashboard
            self.navigate_to_dashboard()
            
            # Step 3: Select year and month
            self.select_year_and_month()
            
            # Step 4: Process each regional
            for regional in self.target_regionals:
                try:
                    logger.info(f"\n{'='*50}")
                    logger.info(f"Processing Regional {regional}")
                    logger.info(f"{'='*50}")
                    
                    # Step 4a: Click View Other Scorecard
                    if not self.click_view_other_scorecard():
                        logger.error(f"Failed to open modal for Regional {regional}")
                        continue
                    
                    # Step 4b: Get all stores for this regional
                    stores = self.get_stores_by_regional_fresh(regional)
                    
                    if not stores:
                        logger.warning(f"No active stores found in Regional {regional}")
                        continue
                    
                    logger.info(f"Found {len(stores)} active stores to process")
                    
                    # Step 4c: Process each store
                    for i, store in enumerate(stores, 1):
                        logger.info(f"\n[{i}/{len(stores)}] Processing store: {store['name']}")
                        
                        # Select the store
                        if self.select_store_robust(store):
                            # Extract data using FAST single-pass method
                            self.extract_store_data_fast(store)
                        else:
                            logger.error(f"Failed to select store: {store['name']}")
                            # Add error record
                            error_result = {
                                'Regional': regional,
                                'Store': store['name'],
                                'Year': self.current_year,
                                'Month': self.current_month,
                                'Extraction_Type': self.extract_type,
                                'Error_Message': 'Failed to select store',
                                'Extraction_DateTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'Extraction_Method': 'Error'
                            }
                            self.storage.add_store_data(error_result)
                        
                        # Prepare for next store
                        if i < len(stores):
                            logger.info("Preparing for next store...")
                            self.close_modal_if_open()
                            time.sleep(3)
                            
                            if not self.click_view_other_scorecard():
                                logger.error("Failed to reopen modal for next store")
                                break
                    
                except Exception as e:
                    logger.error(f"Error processing Regional {regional}: {e}")
                    continue
            
            # Step 5: Save results in multiple formats
            saved_files = []
            
            if "csv" in self.storage_formats:
                csv_file = self.storage.save_to_csv()
                if csv_file:
                    saved_files.append(("CSV", csv_file))
            
            if "json" in self.storage_formats:
                json_file = self.storage.save_to_json()
                if json_file:
                    saved_files.append(("JSON", json_file))
            
            if "sqlite" in self.storage_formats:
                sqlite_file = self.storage.save_to_sqlite()
                if sqlite_file:
                    saved_files.append(("SQLite Database", sqlite_file))
            
            if "text" in self.storage_formats:
                text_file = self.storage.save_to_text()
                if text_file:
                    saved_files.append(("Text Report", text_file))
            
            # Display summary
            if saved_files:
                logger.info(f"\n{'='*60}")
                logger.info("FAST EXTRACTION COMPLETED!")
                logger.info(f"Total stores processed: {len(self.storage.all_data)}")
                logger.info(f"Data saved to {len(saved_files)} format(s):")
                for format_name, file_path in saved_files:
                    logger.info(f"  • {format_name}: {os.path.basename(file_path)}")
                logger.info(f"{'='*60}")
            else:
                logger.warning("No data was extracted or saved")
            
            # Step 6: Close driver
            self.driver.quit()
            return True
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            if self.driver:
                self.driver.quit()
            return False


def get_user_input_fast():
    """Get user input for fast extraction"""
    while True:
        try:
            print("\n" + "="*60)
            print("PMO Data Extractor - FAST VERSION")
            print("="*60)
            
            print("\nWhat type of data would you like to extract?")
            print("1. Financial Metrics Only (Revenue, COGS, Operating Profit, etc.)")
            print("2. ALL Data (All KPIs from ctl02 to ctl22 - FAST SINGLE PASS)")
            print("3. Score Metrics Only (Financial, Customer, IBP, L&G, Total Scores)")
            
            data_type = input("\nEnter choice (1, 2, or 3): ").strip()
            
            if data_type not in ['1', '2', '3']:
                print("Please enter a valid choice (1, 2, or 3).")
                continue
            
            # Map choice to extraction type
            extract_type_map = {
                '1': "financial",
                '2': "all",
                '3': "scores"
            }
            
            extract_type = extract_type_map[data_type]
            
            print("\n" + "="*60)
            print("Enter the year and month for data extraction:")
            
            year = input("Enter year (e.g., 2024): ").strip()
            if not year.isdigit() or len(year) != 4:
                print("Please enter a valid 4-digit year.")
                continue
            year = int(year)
            
            month = input("Enter month (1-12): ").strip()
            if not month.isdigit() or int(month) < 1 or int(month) > 12:
                print("Please enter a valid month (1-12).")
                continue
            month = int(month)
            
            print("\nAvailable Regionals:")
            print("A, B, C, D, E, F, G")
            print("Or enter 'ALL' to extract all regionals at once")
            
            regional_input = input("Enter regional letters (comma-separated, e.g., 'E', 'A,B,C', or 'ALL'): ").strip().upper()
            
            if regional_input == 'ALL':
                target_regionals = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
            else:
                if not regional_input:
                    print("Please enter at least one regional or 'ALL'.")
                    continue
                
                target_regionals = []
                valid_regionals = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
                
                for regional in regional_input.split(','):
                    regional = regional.strip()
                    if regional in valid_regionals:
                        target_regionals.append(regional)
                    else:
                        print(f"Invalid regional: {regional}. Valid options: {', '.join(valid_regionals)} or 'ALL'")
                        break
                else:
                    if not target_regionals:
                        print("Please enter at least one valid regional.")
                        continue
            
            print("\nChoose storage formats (comma-separated):")
            print("1. CSV (Excel-compatible)")
            print("2. JSON (Structured data format)")
            print("3. SQLite (Database format)")
            print("4. Text (Human-readable report)")
            print("5. ALL formats")
            
            storage_input = input("\nEnter format numbers (e.g., '1,3,4' or '5' for all): ").strip()
            
            format_mapping = {
                '1': 'csv',
                '2': 'json', 
                '3': 'sqlite',
                '4': 'text',
                '5': 'all'
            }
            
            storage_formats = []
            for fmt in storage_input.split(','):
                fmt = fmt.strip()
                if fmt in format_mapping:
                    if fmt == '5':
                        storage_formats = ['all']
                        break
                    else:
                        storage_formats.append(format_mapping[fmt])
                else:
                    print(f"Invalid format: {fmt}")
                    break
            else:
                if not storage_formats:
                    storage_formats = ['csv']  # Default to CSV
            
            month_names = ["January", "February", "March", "April", "May", "June",
                          "July", "August", "September", "October", "November", "December"]
            
            data_type_text = {
                '1': 'Financial Metrics Only (FAST)',
                '2': 'ALL Data (FAST SINGLE PASS)',
                '3': 'Score Metrics Only'
            }[data_type]
            
            print(f"\n{'='*60}")
            print(f"You selected:")
            print(f"  Data Type: {data_type_text}")
            print(f"  Period: {month_names[month-1]} {year}")
            print(f"  Regionals: {', '.join(target_regionals)}")
            print(f"  Storage Formats: {', '.join(storage_formats)}")
            print(f"{'='*60}")
            
            confirm = input("Is this correct? (y/n): ").strip().lower()
            
            if confirm in ['y', 'yes']:
                return year, month, target_regionals, extract_type, storage_formats
            else:
                print("Let's try again...\n")
                continue
                
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            exit(1)
        except Exception as e:
            print(f"Error: {e}. Please try again.")


def main_fast():
    """Main function for fast version"""
    try:
        year, month, target_regionals, extract_type, storage_formats = get_user_input_fast()
        
        print("\n" + "="*60)
        print("Login Credentials")
        print("="*60)
        
        # Check for environment variables
        username = os.getenv('PMO_USERNAME')
        password = os.getenv('PMO_PASSWORD')
        
        if not username:
            username = input("Enter username: ")
        else:
            print(f"Using username from environment variable")
        
        if not password:
            password = input("Enter password: ")
        else:
            print(f"Using password from environment variable")
        
        headless_input = input("\nRun in headless mode (no browser window)? (y/n): ").strip().lower()
        headless = headless_input in ['y', 'yes']
        
        print(f"\n{'='*60}")
        month_names = ["January", "February", "March", "April", "May", "June",
                      "July", "August", "September", "October", "November", "December"]
        
        data_type_text = {
            'financial': 'Financial Metrics Only (FAST)',
            'all': 'ALL Data (FAST SINGLE PASS)',
            'scores': 'Score Metrics Only'
        }[extract_type]
        
        print(f"Starting FAST extraction:")
        print(f"  Data Type: {data_type_text}")
        print(f"  Period: {month_names[month-1]} {year}")
        print(f"  Regionals: {', '.join(target_regionals)}")
        print(f"  Storage Formats: {', '.join(storage_formats)}")
        print(f"{'='*60}\n")
        
        # Create and run the extractor
        extractor = PMOFastDataExtractor(
            username=username,
            password=password,
            year=year,
            month=month,
            target_regionals=target_regionals,
            headless=headless,
            extract_type=extract_type,
            storage_formats=storage_formats
        )
        
        success = extractor.run_extraction()
        
        if success:
            print("\n" + "="*60)
            print("✓ FAST data extraction completed successfully!")
            print("="*60)
            print("\nThe extraction used SINGLE-PASS method for maximum speed.")
            print("Data has been stored in the selected formats.")
        else:
            print("\n" + "="*60)
            print("✗ FAST data extraction failed or was incomplete")
            print("="*60)
        
        input("\nPress Enter to exit...")
        
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"✗ FAST data extraction failed: {e}")
        print(f"{'='*60}")
        input("\nPress Enter to exit...")


if __name__ == "__main__":
    main_fast()