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
from datetime import datetime
import logging
import os
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 🆕 Keywords untuk filter toko yang harus di-skip
SKIP_KEYWORDS = ['tutup', 'renovasi', 'maintenance', 'pindah', 'closed', 'relokasi','Tutup','(Tutup)','(tutup)']

def should_skip_store(store_name):
    """Check if store should be skipped based on keywords"""
    for keyword in SKIP_KEYWORDS:
        if re.search(rf'\b{keyword}\b', store_name, re.IGNORECASE):
            logger.info(f"⏭️  SKIPPED ({keyword.title()}): {store_name}")
            return True
    return False

class PMODataExtractor:
    def __init__(self, username, password, year=None, month=None, target_regionals=None, 
                 headless=False, extract_scores=False):
        self.username = username
        self.password = password
        self.driver = None
        self.wait = None
        self.setup_driver(headless)
        
        self.target_regionals = target_regionals or ['E']
        self.extract_scores = extract_scores  # 🆕 Flag untuk memilih antara score atau financial data
        
        if year and month:
            self.current_year = str(year)
            self.current_month = month
        else:
            current_date = datetime.now()
            self.current_year = str(current_date.year)
            self.current_month = current_date.month
        
        self.results = []
        self.last_extracted_values = {}
    
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
            self.driver.get("https://pmo.mykg.id/Systems/Login.aspx")
            
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
                        
                        # 🆕 FILTER: Skip toko dengan keyword tutup/renovasi/dll
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
        """Wait for data to refresh with improved verification - CRITICAL FIX"""
        try:
            logger.info(f"Waiting for data refresh for store: {store_name}")
            
            start_time = time.time()
            
            # Different element IDs based on what we're extracting
            if self.extract_scores:
                # 🆕 If extracting scores, wait for the total score element
                target_id = "ctl00_ContentPlaceHolder1_lblAchievementYTD_Total"
            else:
                # Original financial data element
                target_id = f"ctl00_ContentPlaceHolder1_grvScorecard_ctl02_lblYTDAchievement{self.current_month}"
            
            try:
                self.wait.until(EC.presence_of_element_located((By.ID, target_id)))
                logger.info(f"{'Score' if self.extract_scores else 'Revenue'} element found")
            except TimeoutException:
                logger.error(f"{'Score' if self.extract_scores else 'Revenue'} element not found within timeout")
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
                            logger.info(f"Data has stabilized for {store_name}: {'Total Score' if self.extract_scores else 'Revenue'} = {current_value}")
                            
                            time.sleep(2)
                            
                            # Additional verification based on extraction type
                            if self.extract_scores:
                                # Verify at least one score is loaded
                                try:
                                    financial_score = self.driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblAchievementYTD_F").text.strip()
                                    logger.info(f"Financial score also loaded: {financial_score}")
                                except:
                                    logger.warning("Financial score element not found, but proceeding")
                            else:
                                # Original COGS verification
                                cogs_id = f"ctl00_ContentPlaceHolder1_grvScorecard_ctl03_lblYTDAchievement{self.current_month}"
                                try:
                                    cogs_element = self.driver.find_element(By.ID, cogs_id)
                                    cogs_value = cogs_element.text.strip()
                                    logger.info(f"COGS also loaded: {cogs_value}")
                                except:
                                    logger.warning("COGS element not found, but proceeding")
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
    
    def extract_score_data(self):
        """Extract all score metrics from the page"""
        scores = {}
        
        # 🆕 Mapping of score types to their element IDs
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
                        logger.warning(f"Could not convert {score_name} value to float: '{score_value}'")
                
                logger.info(f"{score_name.replace('_', ' ')}: {scores[score_name]}")
                
            except NoSuchElementException:
                logger.warning(f"Score element not found: {element_id}")
                scores[score_name] = 0.0
            except Exception as e:
                logger.error(f"Error extracting {score_name}: {e}")
                scores[score_name] = 0.0
        
        return scores
    
    def extract_metric_by_id(self, metric_name, control_number, max_attempts=3):
        """Extract achievement data with improved error handling and retry"""
        for attempt in range(max_attempts):
            try:
                element_id = f"ctl00_ContentPlaceHolder1_grvScorecard_ctl0{control_number}_lblYTDAchievement{self.current_month}"
                
                logger.info(f"Extracting {metric_name} (ID: {element_id}, attempt {attempt + 1})")
                
                achievement_element = self.wait.until(
                    EC.presence_of_element_located((By.ID, element_id))
                )
                
                time.sleep(1.5)
                
                achievement_element = self.driver.find_element(By.ID, element_id)
                value = achievement_element.text.strip()
                
                logger.info(f"{metric_name} raw value: '{value}'")
                
                if value == "-" or value == "" or value is None:
                    if attempt < max_attempts - 1 and metric_name == "Revenue":
                        logger.warning(f"Revenue empty on attempt {attempt + 1}, retrying...")
                        time.sleep(3)
                        continue
                    return 0.0
                else:
                    clean_value = value.replace(",", "")
                    try:
                        return float(clean_value)
                    except ValueError:
                        logger.warning(f"Could not convert {metric_name} value to float: '{value}'")
                        if attempt < max_attempts - 1:
                            time.sleep(2)
                            continue
                        return 0.0
                    
            except TimeoutException:
                logger.warning(f"Timeout for {metric_name} (attempt {attempt + 1})")
                if attempt < max_attempts - 1:
                    time.sleep(2)
                    continue
                return 0.0
            except NoSuchElementException:
                logger.warning(f"Element not found for {metric_name}")
                if attempt < max_attempts - 1:
                    time.sleep(2)
                    continue
                return 0.0
            except Exception as e:
                logger.error(f"Error extracting {metric_name}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(2)
                    continue
                return 0.0
        
        return 0.0
    
    def detect_store_structure_prioritize_operating_profit(self):
        """
        Detect store structure by checking which control has 'Operating Profit' label.
        - Control 06 has 'Operating Profit' label → NO EBITDA
        - Control 07 has 'Operating Profit' label → HAS EBITDA (ctl06 is EBITDA)
        """
        try:
            logger.info("Detecting store structure by checking Operating Profit label position")
            
            time.sleep(2)
            
            # Check control 06 label
            label_06_id = "ctl00_ContentPlaceHolder1_grvScorecard_ctl06_lblKPI"
            label_07_id = "ctl00_ContentPlaceHolder1_grvScorecard_ctl07_lblKPI"
            
            label_06_text = None
            label_07_text = None
            
            # Try to get label text from control 06
            try:
                label_06_element = self.driver.find_element(By.ID, label_06_id)
                label_06_text = label_06_element.text.strip()
                logger.info(f"Control 06 label: '{label_06_text}'")
            except Exception as e:
                logger.warning(f"Could not read control 06 label: {e}")
            
            # Try to get label text from control 07
            try:
                label_07_element = self.driver.find_element(By.ID, label_07_id)
                label_07_text = label_07_element.text.strip()
                logger.info(f"Control 07 label: '{label_07_text}'")
            except Exception as e:
                logger.warning(f"Could not read control 07 label: {e}")
            
            # Decision logic: Check which control has "Operating Profit" in the label
            
            # Case 1: Control 06 has "Operating Profit" → NO EBITDA
            if label_06_text and "Operating Profit" in label_06_text:
                logger.info("=" * 60)
                logger.info("✓ Control 06 label contains 'Operating Profit'")
                logger.info("  → Store has NO EBITDA")
                logger.info("  → Operating Profit at control 06")
                logger.info("=" * 60)
                
                return {
                    'operating_profit_position': 6,
                    'ebitda_position': None,
                    'has_ebitda': False,
                    'structure_type': 'no_ebitda_op_at_06'
                }
            
            # Case 2: Control 07 has "Operating Profit" → HAS EBITDA
            if label_07_text and "Operating Profit" in label_07_text:
                logger.info("=" * 60)
                logger.info("✓ Control 07 label contains 'Operating Profit'")
                logger.info("  → Store HAS EBITDA")
                logger.info("  → EBITDA at control 06")
                logger.info("  → Operating Profit at control 07")
                logger.info("=" * 60)
                
                return {
                    'operating_profit_position': 7,
                    'ebitda_position': 6,
                    'has_ebitda': True,
                    'structure_type': 'has_ebitda_op_at_07'
                }
            
            # Fallback: If we can't read labels, assume NO EBITDA (safest default)
            logger.warning("Could not determine structure from labels, defaulting to NO EBITDA")
            logger.info("=" * 60)
            logger.info("⚠ Fallback: Assuming NO EBITDA structure")
            logger.info("  → Operating Profit at control 06")
            logger.info("=" * 60)
            
            return {
                'operating_profit_position': 6,
                'ebitda_position': None,
                'has_ebitda': False,
                'structure_type': 'fallback_no_ebitda'
            }
                
        except Exception as e:
            logger.error(f"Error detecting store structure: {e}")
            return {
                'operating_profit_position': 6,
                'ebitda_position': None,
                'has_ebitda': False,
                'structure_type': 'error_fallback'
            }
    
    def extract_store_data(self, store_info):
        """Extract all metrics data for a store"""
        try:
            store_name = store_info['name']
            regional = store_info['regional']
            
            logger.info(f"Extracting data for {store_name} (Regional {regional})")
            
            time.sleep(10)
            
            if self.extract_scores:
                # 🆕 Extract score data
                scores = self.extract_score_data()
                
                result = {
                    'Regional': regional,
                    'Store': store_name,
                    'Year': self.current_year,
                    'Month': self.current_month,
                    'Financial_Score': scores['Financial_Score'],
                    'Customer_Score': scores['Customer_Score'],
                    'Internal_Business_Process_Score': scores['Internal_Business_Process_Score'],
                    'Learning_and_Growth_Score': scores['Learning_and_Growth_Score'],
                    'Total_Score': scores['Total_Score'],
                    'Error_Message': 'None',
                    'Extraction_DateTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            else:
                # Original financial data extraction
                revenue = self.extract_metric_by_id("Revenue", 2, max_attempts=3)
                cogs = self.extract_metric_by_id("COGS", 3)
                cogs_to_revenue = self.extract_metric_by_id("COGS to Revenue", 4)
                operating_expense = self.extract_metric_by_id("Operating Expense", 5)
                
                structure_info = self.detect_store_structure_prioritize_operating_profit()
                
                operating_profit = self.extract_metric_by_id("Operating Profit", 
                                                             structure_info['operating_profit_position'])
                
                ebitda = 0.0
                if structure_info['has_ebitda'] and structure_info['ebitda_position']:
                    ebitda = self.extract_metric_by_id("EBITDA", structure_info['ebitda_position'])
                
                logger.info(f"Final extracted values - OP: {operating_profit}, EBITDA: {ebitda}, Revenue: {revenue}")
                
                result = {
                    'Regional': regional,
                    'Store': store_name,
                    'Year': self.current_year,
                    'Month': self.current_month,
                    'Revenue_ACH': revenue,
                    'COGS_ACH': cogs,
                    'COGS_to_Revenue_ACH': cogs_to_revenue,
                    'Operating_Expense_ACH': operating_expense,
                    'EBITDA_ACH': ebitda,
                    'Operating_Profit_ACH': operating_profit,
                    'Has_EBITDA': structure_info['has_ebitda'],
                    'Structure_Type': structure_info['structure_type'],
                    'OP_Position': structure_info['operating_profit_position'],
                    'EBITDA_Position': structure_info.get('ebitda_position', 'N/A'),
                    'Error_Message': 'None',
                    'Extraction_DateTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            self.results.append(result)
            logger.info(f"✓ Data extracted successfully for {store_name}")
            
        except Exception as e:
            logger.error(f"Error extracting data for {store_info['name']}: {e}")
            
            # Create error record
            result = {
                'Regional': store_info['regional'],
                'Store': store_info['name'],
                'Year': self.current_year,
                'Month': self.current_month,
                'Error_Message': str(e),
                'Extraction_DateTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Fill with zeros for all metrics if error
            if self.extract_scores:
                result.update({
                    'Financial_Score': 0.0,
                    'Customer_Score': 0.0,
                    'Internal_Business_Process_Score': 0.0,
                    'Learning_and_Growth_Score': 0.0,
                    'Total_Score': 0.0
                })
            else:
                result.update({
                    'Revenue_ACH': 0.0,
                    'COGS_ACH': 0.0,
                    'COGS_to_Revenue_ACH': 0.0,
                    'Operating_Expense_ACH': 0.0,
                    'EBITDA_ACH': 0.0,
                    'Operating_Profit_ACH': 0.0,
                    'Has_EBITDA': False,
                    'Structure_Type': 'error',
                    'OP_Position': 0,
                    'EBITDA_Position': 'N/A'
                })
            
            self.results.append(result)
    
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
    
    def process_regional(self, regional_letter):
        """Process all stores in a regional with improved error handling"""
        try:
            logger.info(f"Processing Regional {regional_letter}")
            
            if not self.click_view_other_scorecard():
                logger.error(f"Failed to open modal for Regional {regional_letter}")
                return
            
            stores = self.get_stores_by_regional_fresh(regional_letter)
            
            if not stores:
                logger.warning(f"No stores found for Regional {regional_letter}")
                return
            
            logger.info(f"Found {len(stores)} stores in Regional {regional_letter}")
            
            successful_stores = 0
            failed_stores = 0
            
            for i, store_info in enumerate(stores):
                store_name = store_info['name']
                
                try:
                    logger.info(f"\n{'='*60}")
                    logger.info(f"Processing store {i+1}/{len(stores)}: {store_name}")
                    logger.info(f"{'='*60}\n")
                    
                    if self.select_store_robust(store_info):
                        self.extract_store_data(store_info)
                        successful_stores += 1
                        
                        time.sleep(5)
                    else:
                        logger.error(f"Failed to select store: {store_name}")
                        self.add_error_record(store_info, "Failed to select store")
                        failed_stores += 1
                    
                    if i < len(stores) - 1:
                        logger.info("Preparing for next store selection...")
                        
                        self.close_modal_if_open()
                        time.sleep(6)
                        
                        if not self.click_view_other_scorecard():
                            logger.error("Failed to reopen modal for next store")
                            time.sleep(3)
                        
                except Exception as e:
                    logger.error(f"Error processing store {store_name}: {e}")
                    self.add_error_record(store_info, f"Processing error: {str(e)}")
                    failed_stores += 1
                    
                    if i < len(stores) - 1:
                        try:
                            logger.info("Attempting recovery for next store...")
                            self.close_modal_if_open()
                            time.sleep(5)
                            if not self.click_view_other_scorecard():
                                logger.warning("Recovery attempt failed, but continuing...")
                        except:
                            logger.warning("Recovery attempt had errors, but continuing...")
                    
                    continue
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Regional {regional_letter} processing complete:")
            logger.info(f"  ✓ Successful: {successful_stores}")
            logger.info(f"  ✗ Failed: {failed_stores}")
            logger.info(f"{'='*60}\n")
                        
        except Exception as e:
            logger.error(f"Critical error processing Regional {regional_letter}: {e}")
        finally:
            try:
                self.close_modal_if_open()
            except:
                pass
    
    def add_error_record(self, store_info, error_message):
        """Add an error record for a failed store extraction"""
        try:
            if self.extract_scores:
                result = {
                    'Regional': store_info['regional'],
                    'Store': store_info['name'],
                    'Year': self.current_year,
                    'Month': self.current_month,
                    'Financial_Score': 0,
                    'Customer_Score': 0,
                    'Internal_Business_Process_Score': 0,
                    'Learning_and_Growth_Score': 0,
                    'Total_Score': 0,
                    'Error_Message': error_message,
                    'Extraction_DateTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            else:
                result = {
                    'Regional': store_info['regional'],
                    'Store': store_info['name'],
                    'Year': self.current_year,
                    'Month': self.current_month,
                    'Revenue_ACH': 0,
                    'COGS_ACH': 0,
                    'COGS_to_Revenue_ACH': 0,
                    'Operating_Expense_ACH': 0,
                    'EBITDA_ACH': 0,
                    'Operating_Profit_ACH': 0,
                    'Has_EBITDA': False,
                    'Structure_Type': 'error',
                    'OP_Position': 'N/A',
                    'EBITDA_Position': 'N/A',
                    'Error_Message': error_message,
                    'Extraction_DateTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            self.results.append(result)
            logger.info(f"Added error record for {store_info['name']}: {error_message}")
        except Exception as e:
            logger.error(f"Failed to add error record: {e}")
    
    def run_extraction(self):
        """Main extraction process"""
        try:
            self.login()
            self.navigate_to_dashboard()
            self.select_year_and_month()
            
            for regional in self.target_regionals:
                try:
                    self.last_extracted_values = {}
                    self.process_regional(regional)
                except Exception as e:
                    logger.error(f"Failed to process Regional {regional}: {e}")
                    continue
            
            self.save_results()
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed")

    def save_to_csv(self, filename=None):
        """Save results to CSV file"""
        if not self.results:
            logger.warning("No results to save")
            return False
        
        try:
            if filename is None:
                # Create default filename based on extraction type
                if self.extract_scores:
                    prefix = "scores"
                else:
                    prefix = "financial"
                
                filename = f"{prefix}_data_{self.current_year}_{self.current_month:02d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            df = pd.DataFrame(self.results)
            df.to_csv(filename, index=False, encoding='utf-8')
            
            logger.info(f"✓ Data saved to {filename}")
            logger.info(f"  Total records: {len(self.results)}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
            return False

    def run(self):
        """Main execution method"""
        try:
            logger.info("=" * 60)
            logger.info("Starting PMO Data Extractor")
            logger.info(f"Year: {self.current_year}, Month: {self.current_month}")
            logger.info(f"Target Regionals: {self.target_regionals}")
            logger.info(f"Extracting: {'Scores' if self.extract_scores else 'Financial Data'}")
            logger.info("=" * 60)
            
            # Step 1: Login
            self.login()
            
            # Step 2: Navigate to dashboard
            self.navigate_to_dashboard()
            
            # Step 3: Select year and month
            self.select_year_and_month()
            
            # Step 4: Click View Other Scorecard
            if not self.click_view_other_scorecard():
                logger.error("Failed to click View Other Scorecard. Exiting.")
                return False
            
            # Step 5: Process each regional
            for regional in self.target_regionals:
                logger.info(f"\n{'='*50}")
                logger.info(f"Processing Regional {regional}")
                logger.info(f"{'='*50}")
                
                # Get all stores for this regional
                stores = self.get_stores_by_regional_fresh(regional)
                
                if not stores:
                    logger.warning(f"No active stores found in Regional {regional}")
                    continue
                
                logger.info(f"Found {len(stores)} active stores to process")
                
                # Process each store
                for i, store in enumerate(stores, 1):
                    logger.info(f"\n[{i}/{len(stores)}] Processing store: {store['name']}")
                    
                    # Select the store
                    if self.select_store_robust(store):
                        # Extract data
                        self.extract_store_data(store)
                    else:
                        logger.error(f"Failed to select store: {store['name']}")
                        
                        # Add error record
                        error_result = {
                            'Regional': regional,
                            'Store': store['name'],
                            'Year': self.current_year,
                            'Month': self.current_month,
                            'Error_Message': 'Failed to select store',
                            'Extraction_DateTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        
                        # Add zeros for all metrics
                        if self.extract_scores:
                            error_result.update({
                                'Financial_Score': 0.0,
                                'Customer_Score': 0.0,
                                'Internal_Business_Process_Score': 0.0,
                                'Learning_and_Growth_Score': 0.0,
                                'Total_Score': 0.0
                            })
                        else:
                            error_result.update({
                                'Revenue_ACH': 0.0,
                                'COGS_ACH': 0.0,
                                'COGS_to_Revenue_ACH': 0.0,
                                'Operating_Expense_ACH': 0.0,
                                'EBITDA_ACH': 0.0,
                                'Operating_Profit_ACH': 0.0,
                                'Has_EBITDA': False,
                                'Structure_Type': 'error',
                                'OP_Position': 0,
                                'EBITDA_Position': 'N/A'
                            })
                        
                        self.results.append(error_result)
                    
                    # Small delay between stores
                    time.sleep(2)
            
            # Step 6: Save results to CSV
            if self.results:
                self.save_to_csv()
                logger.info(f"\n{'='*60}")
                logger.info(f"EXTRACTION COMPLETE!")
                logger.info(f"Total stores processed: {len(self.results)}")
                logger.info(f"{'='*60}")
            else:
                logger.warning("No data was extracted")
            
            # Step 7: Close driver
            self.driver.quit()
            return True
            
        except Exception as e:
            logger.error(f"Critical error in run method: {e}")
            if self.driver:
                self.driver.quit()
            return False

    def save_results(self):
        """Save extracted data to CSV with summary"""
        try:
            if not self.results:
                logger.warning("No data to save")
                return
            
            df = pd.DataFrame(self.results)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            regional_str = '_'.join(self.target_regionals)
            
            # Different filename based on extraction type
            if self.extract_scores:
                filename = f"pmo_scores_extract_regional_{regional_str}_{self.current_year}_{self.current_month:02d}_{timestamp}.csv"
            else:
                filename = f"pmo_financial_extract_regional_{regional_str}_{self.current_year}_{self.current_month:02d}_{timestamp}.csv"
                
                # Format COGS to Revenue dengan simbol % (hanya untuk financial data)
                if 'COGS_to_Revenue_ACH' in df.columns:
                    df['COGS_to_Revenue_ACH'] = df['COGS_to_Revenue_ACH'].apply(
                        lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "0.00%"
                    )
                    logger.info("COGS to Revenue formatted with percentage symbol")
            
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            logger.info(f"\n{'='*60}")
            logger.info(f"Data saved to {filename}")
            logger.info(f"Total records extracted: {len(self.results)}")
            
            successful_extractions = len(df[df['Error_Message'] == 'None'])
            failed_extractions = len(df[df['Error_Message'] != 'None'])
            
            logger.info(f"\nSummary Statistics:")
            logger.info(f"  ✓ Successful extractions: {successful_extractions}")
            logger.info(f"  ✗ Failed extractions: {failed_extractions}")
            
            if successful_extractions > 0:
                if self.extract_scores:
                    valid_data = df[(df['Total_Score'] > 0) & (df['Error_Message'] == 'None')]
                    if len(valid_data) > 0:
                        avg_total_score = valid_data['Total_Score'].mean()
                        avg_financial = valid_data['Financial_Score'].mean()
                        avg_customer = valid_data['Customer_Score'].mean()
                        
                        logger.info(f"\nScore Metrics Summary:")
                        logger.info(f"  Valid stores (Total Score > 0): {len(valid_data)}")
                        logger.info(f"  Average Total Score: {avg_total_score:.2f}")
                        logger.info(f"  Average Financial Score: {avg_financial:.2f}")
                        logger.info(f"  Average Customer Score: {avg_customer:.2f}")
                else:
                    valid_data = df[(df['Revenue_ACH'] > 0) & (df['Error_Message'] == 'None')]
                    if len(valid_data) > 0:
                        avg_revenue = valid_data['Revenue_ACH'].mean()
                        total_revenue = valid_data['Revenue_ACH'].sum()
                        avg_op_profit = valid_data['Operating_Profit_ACH'].mean()
                        
                        logger.info(f"\nFinancial Metrics Summary:")
                        logger.info(f"  Valid stores (Revenue > 0): {len(valid_data)}")
                        logger.info(f"  Average revenue: {avg_revenue:,.0f}")
                        logger.info(f"  Total revenue: {total_revenue:,.0f}")
                        logger.info(f"  Average operating profit: {avg_op_profit:,.0f}")
            
            logger.info(f"{'='*60}\n")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")



def get_user_input():
    """Get year, month, regional selection, and data type from user"""
    while True:
        try:
            print("\n" + "="*60)
            print("PMO Data Extractor - Enhanced Version")
            print("="*60)
            
            # 🆕 Ask user what type of data to extract
            print("\nWhat type of data would you like to extract?")
            print("1. Financial Metrics (Revenue, COGS, Operating Profit, etc.)")
            print("2. Score Metrics (Financial, Customer, Internal Process, Learning, Total)")
            print("3. Both (extract all available data)")
            
            data_type = input("\nEnter choice (1, 2, or 3): ").strip()
            
            if data_type not in ['1', '2', '3']:
                print("Please enter a valid choice (1, 2, or 3).")
                continue
            
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
            
            month_names = ["January", "February", "March", "April", "May", "June",
                          "July", "August", "September", "October", "November", "December"]
            
            data_type_text = {
                '1': 'Financial Metrics',
                '2': 'Score Metrics',
                '3': 'Both Financial and Score Metrics'
            }[data_type]
            
            print(f"\n{'='*60}")
            print(f"You selected:")
            print(f"  Data Type: {data_type_text}")
            print(f"  Period: {month_names[month-1]} {year}")
            print(f"  Regionals: {', '.join(target_regionals)}")
            print(f"{'='*60}")
            
            confirm = input("Is this correct? (y/n): ").strip().lower()
            
            if confirm in ['y', 'yes']:
                return year, month, target_regionals, data_type
            else:
                print("Let's try again...\n")
                continue
                
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            exit(1)
        except Exception as e:
            print(f"Error: {e}. Please try again.")

def extract_both_data_types(username, password, year, month, target_regionals, headless):
    """Extract both financial and score data in sequence"""
    print("\n" + "="*60)
    print("Starting BOTH data extraction process")
    print("="*60)
    
    # First extract financial data
    print("\n--- EXTRACTING FINANCIAL METRICS ---")
    financial_extractor = PMODataExtractor(
        username=username,
        password=password,
        year=year,
        month=month,
        target_regionals=target_regionals,
        headless=headless,
        extract_scores=False
    )
    
    try:
        financial_extractor.run_extraction()
    except Exception as e:
        print(f"Financial data extraction failed: {e}")
    
    print("\n" + "="*60)
    print("Financial metrics extraction completed")
    print("="*60)
    
    # Then extract score data
    print("\n--- EXTRACTING SCORE METRICS ---")
    score_extractor = PMODataExtractor(
        username=username,
        password=password,
        year=year,
        month=month,
        target_regionals=target_regionals,
        headless=headless,
        extract_scores=True
    )
    
    try:
        score_extractor.run_extraction()
    except Exception as e:
        print(f"Score data extraction failed: {e}")
    
    print("\n" + "="*60)
    print("Both data extractions completed!")
    print("="*60)

def main():
    """Main function"""
    try:
        year, month, target_regionals, data_type = get_user_input()
        
        print("\n" + "="*60)
        print("Login Credentials")
        print("="*60)
        username = input("Enter username: ") if not os.getenv('PMO_USERNAME') else os.getenv('PMO_USERNAME')
        password = input("Enter password: ") if not os.getenv('PMO_PASSWORD') else os.getenv('PMO_PASSWORD')
        
        headless_input = input("Run in headless mode (no browser window)? (y/n): ").strip().lower()
        headless = headless_input in ['y', 'yes']
        
        print(f"\n{'='*60}")
        month_names = ["January", "February", "March", "April", "May", "June",
                      "July", "August", "September", "October", "November", "December"]
        
        data_type_text = {
            '1': 'Financial Metrics',
            '2': 'Score Metrics',
            '3': 'Both Financial and Score Metrics'
        }[data_type]
        
        print(f"Starting extraction:")
        print(f"  Data Type: {data_type_text}")
        print(f"  Period: {month_names[month-1]} {year}")
        print(f"  Regionals: {', '.join(target_regionals)}")
        print(f"{'='*60}\n")
        
        if data_type == '3':
            # Extract both
            extract_both_data_types(username, password, year, month, target_regionals, headless)
        else:
            # Extract single type
            extract_scores = (data_type == '2')
            
            extractor = PMODataExtractor(
                username=username,
                password=password,
                year=year,
                month=month,
                target_regionals=target_regionals,
                headless=headless,
                extract_scores=extract_scores
            )
            
            extractor.run_extraction()
        
        print("\n" + "="*60)
        print("✓ Data extraction completed successfully!")
        print("="*60)
        print("\nPlease check the generated CSV file(s) for results.")
        print("Review the log for any warnings or errors.")
        
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"✗ Data extraction failed: {e}")
        print(f"{'='*60}")
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()