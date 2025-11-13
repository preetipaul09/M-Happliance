import os
import csv
import requests
import mysql.connector
import logging
from bs4 import BeautifulSoup
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import FirefoxOptions
from datetime import datetime, timedelta, date
from webdriver_manager.chrome import ChromeDriverManager
from random import randint ,uniform
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from seleniumwire import webdriver as wire_webdriver 
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
# import pyautogui
import undetected_chromedriver as uc
import time
import random
import json
from modules.runTimeSecrets import HOST, DB, USER, PASS, HOST2, DB2, USER2, PASS2, HOST3, DB3, USER3, PASS3
from modules.saveRanks import commence as evalRanking
# ------------------------------------------------------------
# HOST, DB, USER, PASS = "162.243.170.201", "wfnpyvxqtp", "wfnpyvxqtp", "tvv2kGXHjE"
# HOST2, DB2, USER2, PASS2 = "162.243.170.201","wfnpyvxqtp","wfnpyvxqtp","tvv2kGXHjE"
# HOST3, DB3, USER3, PASS3 = "162.243.170.201", "wfnpyvxqtp", "wfnpyvxqtp", "tvv2kGXHjE"


# logger
# ------------------------------------------------------------
def loggerInit(logFileName):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler(f'logs/{logFileName}')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    return logger
logger = loggerInit(logFileName="ProdDetails.log")
# ---------------------------------------------------------------

# ---------------------- WEBDRIVER SETUP ----------------------

def simulate_mouse_movement():
    width, height = pyautogui.size()
    for _ in range(random.randint(3, 6)):
        x = random.randint(0, width)
        y = random.randint(0, height)
        pyautogui.moveTo(x, y, duration=random.uniform(0.3, 1.2))
        time.sleep(random.uniform(0.1, 0.4))

def human_scroll(driver):
    total_scrolls = random.randint(2, 5)
    for i in range(total_scrolls):
        scroll_by = random.randint(200, 600)
        driver.execute_script(f"window.scrollBy(0, {scroll_by});")
        time.sleep(random.uniform(0.5, 1.5))

def triggerSelenium(useVPN=False, checkIP=False):
    logger.debug("Selenium triggered")

    geckoPath = f"driver/geckodriver.exe"
    if not os.path.exists(geckoPath):
        raise FileNotFoundError(f"GeckoDriver not found at: {geckoPath}")

    with open("vpn.config.json") as json_data_file:
        configs = json.load(json_data_file)

    attempts = 0
    while attempts < 3:
        try:
            VPN_IP_PORT = random.choice(configs.get("VPN_IP_PORT", []))

            options = FirefoxOptions()
            # options.add_argument('-headless')  # Uncomment if needed
            # options.add_argument('-private')    # Uncomment if needed

            service = Service(executable_path=geckoPath)

            if useVPN:
                from seleniumwire import webdriver as wire_webdriver  # Import only if VPN is used
                seleniumwire_options = {
                    'proxy': {
                        "http": f"http://{VPN_IP_PORT}",
                        "https": f"http://{VPN_IP_PORT}",
                        'no_proxy': 'localhost,127.0.0.1'
                    }
                }
                driver = wire_webdriver.Firefox(
                    service=service,
                    options=options,
                    seleniumwire_options=seleniumwire_options
                )
            else:
                from selenium import webdriver as vanilla_webdriver
                driver = vanilla_webdriver.Firefox(
                    service=service,
                    options=options
                )

            if checkIP:
                time.sleep(random.uniform(1, 3))
                driver.get("https://ip.me/")
                time.sleep(random.uniform(0.5, 1.5))
                driver.refresh()
                ip_value = driver.find_element(By.CSS_SELECTOR, 'input#ip-lookup').get_attribute('value')
                logger.debug(f"New Rotated IP: {ip_value}")

            return driver

        except Exception as e:
            logger.debug(f"Attempt {attempts + 1}/3 failed with error: {e}")
            attempts += 1
            if attempts == 3:
                logger.error("triggerSelenium() failed after 3 attempts")
                raise e

# VgFTsBvymAWgFJx

def try_press_and_hold_captcha(driver):
    try:
        logger.info("Checking for 'Press & Hold' CAPTCHA...")
        time.sleep(2)

        # Save snapshot of page for debugging
        driver.save_screenshot("captcha_detected.png")
        with open("captcha_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)

        # Detect and log all iframes
        from selenium.webdriver.remote.webelement import WebElement

        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        logger.info(f"Found {len(iframes)} iframes on the page.")

        for idx, frame in enumerate(iframes):
            logger.info(f"Iframe {idx} is WebElement: {isinstance(frame, WebElement)}")
            if not isinstance(frame, WebElement):
                logger.warning(f"Iframe {idx} is not a WebElement. Type: {type(frame)}")
                continue

            try:
                title = frame.get_attribute("title")
                src = frame.get_attribute("src")
                logger.info(f"Iframe {idx}: title='{title}', src='{src}'")
            except Exception as frame_exc:
                logger.warning(f"Issue accessing iframe {idx}: {frame_exc}")

        # Check if CAPTCHA is not present
        page_source = driver.page_source
        if "Press and hold" not in page_source and "px-captcha" not in page_source:
            logger.info("No CAPTCHA prompt detected — continuing scrape.")
            driver.switch_to.default_content()
            return True

        # Wait for CAPTCHA container
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "px-captcha"))
            )
            logger.info("CAPTCHA container found.")
        except:
            logger.warning("CAPTCHA container not found. Possibly already passed.")
            driver.switch_to.default_content()
            return True

        # Try multiple selectors to find the CAPTCHA press-and-hold button
        selectors = [
            ".px-captcha-error-button",
            ".px-captcha-btn",
            "div[data-testid='px-captcha-button']",
            ".button-holder > div",
            "#px-captcha button",
        ]

        button = None
        for selector in selectors:
            try:
                button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                logger.info(f"Found CAPTCHA button using selector: {selector}")
                break
            except:
                continue

        if not button:
            driver.save_screenshot("captcha_button_not_found.png")
            logger.error("CAPTCHA button could not be located.")
            driver.switch_to.default_content()
            return False

        # Simulate press and hold
        ActionChains(driver).move_to_element_with_offset(button, 5, 5).click_and_hold().pause(10).release().perform()
        logger.info("Simulated press-and-hold action.")
        time.sleep(5)

        # Switch back to main content
        driver.switch_to.default_content()

        # Verify if CAPTCHA is still present
        if "Press and hold" in driver.page_source or "px-captcha" in driver.page_source:
            driver.save_screenshot("captcha_still_present.png")
            logger.warning("CAPTCHA still detected after interaction.")
            return False

        logger.info("CAPTCHA passed successfully.")
        return True

    except Exception as e:
        logger.exception(f"Exception while handling CAPTCHA: {e}")
        driver.switch_to.default_content()
        return False


def dump_debug(driver):
    try:
        with open("debug_page_source.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        driver.save_screenshot("debug_screenshot.png")
    except Exception as e:
        logger.error(f"Error during debug dump: {e}")

def random_pause(min_time=2, max_time=5):
    """
    Add a random pause to simulate human thinking or waiting.
    """
    time.sleep(uniform(min_time, max_time))

def getAllProUrl(category_url):
    product_urls = set()
    new_category_url = category_url
    pageNumber = 1 
    try:
        # trigger driver funtion call here
        driver = triggerSelenium(useVPN=False,checkIP=True)
        while True:
            url = new_category_url if pageNumber == 1 else f'{new_category_url}?page={pageNumber}'
            driver.get(url)

            random_pause(2,100)

            driver.save_full_page_screenshot("ss.png")
            
            human_scroll(driver)

            # simulate_mouse_movement()

            success = try_press_and_hold_captcha(driver)
            if not success:
                raise Exception("CAPTCHA could not be bypassed.")

            try:
                WebDriverWait(driver, 80).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#__next"))
                )
                time.sleep(random.uniform(1, 3))  # Additional delay
                human_scroll(driver)  # Additional human mimicry
            except TimeoutException:
                if "Press and hold" in driver.page_source:
                    logger.warning("CAPTCHA still present after waiting.")
                else:
                    logger.error("Timeout: Product elements did not load.")
                driver.save_screenshot("captcha_or_timeout.png")
                raise

            # dump_debug(driver)

            products = driver.find_elements(By.CSS_SELECTOR, '#__next article a')
            if not products:
                logger.debug(f"No products found on page {pageNumber} for {url}.")
                break

            for product in products:
                product_url = product.get_attribute('href')
                if product_url and product_url not in product_urls:
                    product_urls.add(product_url)
                    with open("productUrls(preeti)1.txt", "a", encoding="utf-8") as f:
                        f.write(product_url + '\n')

            print(f"Total unique product URLs found so far: {len(product_urls)}")
            pageNumber += 1
            if pageNumber == 2:
                break

        return product_urls
    except Exception as e :
        logger.debug(f"Driver not found")
    finally:
        driver.quit()

def scraper_unit(vendor_product_id, product_id, given_product_mpn, product_url, vendor_url, vendor_id):
    try:
        # driver = triggerSelenium(checkIP=False,useVPN=False)
        options = ChromeOptions()
        # options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--remote-debugging-port=9222")
        driver = uc.Chrome(version_main=141, options=options)

        temp = {}
        url = product_url
        driver.get(url)
        logger.debug(url)
        random_pause(20,30)
        soup = BeautifulSoup(driver.page_source,'html.parser')
        if 'Press & Hold' in soup.text:
            print("Captcha")
            return 
        else:
            scraped_product_mpn_tag = soup.select_one('meta[property="product:retailer_item_id"]')
            if scraped_product_mpn_tag:
                scraped_product_mpn = scraped_product_mpn_tag.get('content')
                temp["product_mpn"] = scraped_product_mpn
            else:
                scraped_product_mpn = None
                temp["product_mpn"] = None
            
            print(scraped_product_mpn)
            if given_product_mpn != scraped_product_mpn:
                logger.warning(f"MPN mismatch for {url}: given_product_mpn:{given_product_mpn}, scraped_product_mpn:{scraped_product_mpn}")
                with open("OldMpnNotMatched.txt", mode="a", encoding="utf-8") as file:
                    file.write(f"{url} | given_product_mpn: {given_product_mpn} | scraped_product_mpn: {scraped_product_mpn}\n")
                return
            else:
                logger.warning(f"MPN matched for {url}: given_product_mpn:{given_product_mpn}, scraped_product_mpn:{scraped_product_mpn}")
                product_msrp = None
                productMsrp = soup.select_one('p[data-uw-rm-sr="price"]')
                msrp_text = productMsrp.text.strip() if productMsrp and productMsrp.text.strip() else ""
                if '-' in msrp_text:
                    print(f"Skipping product due to MSRP range: {msrp_text}")
                    return None, None
                elif msrp_text:
                    product_msrp = (
                        msrp_text
                        .replace("Rs.", "")
                        .replace("$", "")
                        .replace(",", "")
                        .replace(r"\ea", "")
                        .strip()
                    )
                else:
                    product_msrp = None

                temp["msrp"] = product_msrp
                temp["product_url"] = url
                temp['url'] = url

                base_price = None
                basePice = soup.select_one('div#product-content')
                price_text = basePice.text.strip() if basePice else ""
                if '-' in price_text:
                    print(f"Skipping product due to base price range: {price_text}")
                    return None, None
                elif price_text:
                    base_price = price_text.replace("$", "").replace(",", "").replace("Sale", "").replace("Rs.", "").replace(r"\ea", "").strip()
                    temp['vendorprice_price'] = base_price
                    temp["vendorprice_finalprice"] = base_price
                else:
                    temp['vendorprice_price'] = None
                    temp["vendorprice_finalprice"] = None
                    
                temp['scraped_by_system'] = "Preeti pc"
                temp['source'] = "direct_from_website"
                temp['product_condition'] = 'New'

                print(temp, product_id)

                if temp['vendorprice_price'] == None:
                        logger.warning(f"No price found for product ID {product_id}")
                        with open("priceNotFound.txt", "a") as file:
                            file.write(f"{vendor_product_id}\n")
                        return
                else:
                    vendorTempPricing(vendor_product_id,temp)
                    # vendorZPricing(temp, vendor_id)
                    evalRanking(vendor_id , product_id)
                if temp["msrp"]:
                    productMsrpUpdate(product_id, temp)
                    productVendorMsrpUpdate(vendor_product_id,temp)
    except Exception as e:
        logger.error(f"An error occurred while fetching product data: {e}")
    finally:
        if driver:
            driver.quit()

# Saving data to the MSP
def insertIntoMsp(row, vendor_id):
    product_id = vendor_product_id = None  # Initialize to None
    try:
        print("**************************")
        brand_id = checkInsertBrand(vendor_id, row['brand_name'])
        product_id = checkInsertProduct(vendor_id, brand_id, row['product_mpn'], row['product_name'], row['msrp'], row['product_image'])
        vendor_product_id = checkInsertProductVendor(vendor_id, product_id, row['vendor_sku'], row['product_name'], row['product_url'], row['msrp'])
        checkInsertProductVendorURL(vendor_id, vendor_product_id, row['product_url'])
    except Exception as e:
        logger.error(f"Error in insertIntoMsp: {e}")
    return product_id, vendor_product_id


def getBrandRawName(brand_name):
    letters, numbers, spaces = [], [], []
    for character in brand_name:
        if character.isalpha():
            letters.append(character)
        elif character.isnumeric():
            numbers.append(character)
        elif character.isspace():
            spaces.append(character)
    if len(letters) > 0: raw_name = "".join(spaces + letters)
    else: raw_name = "".join(spaces + numbers)
    return raw_name


# Add brand if doesn't exists
def checkInsertBrand(vendor_id,brand_name):
    try:
        print("Insert in checkInsertBrand")
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            print("Connected db in checkInsertBrand")
            this = conn.cursor(buffered=True)
            this.execute("SELECT brand_id FROM BrandSynonyms WHERE brand_synonym = %s", (brand_name,))
            brand_id = this.fetchone()
            if brand_id:
                logger.info(f"{vendor_id} >> Found brand synonym: {brand_name} ({brand_id[0]})")
                return brand_id[0]
            else:
                brandRawNname = getBrandRawName(brand_name)
                brandRaw = brandRawNname.lower().strip()
                this.execute("SELECT brand_id, brand_name FROM Brand WHERE brand_raw_name = %s",(brandRaw,))
                records = this.fetchone()
                if records:
                    fetchedBrandId = records[0]
                    fetchedBrandName = records[1]
                    if fetchedBrandName != brand_name:
                        insertBrandSynonymsQuery = "INSERT INTO BrandSynonyms (brand_id,brand_synonym) VALUES (%s,%s);"
                        this.execute(insertBrandSynonymsQuery,(fetchedBrandId,brand_name))
                        conn.commit()
                        logger.info(f"Inserted {brandRawNname} as a synonym for {fetchedBrandName}.")
                    else:
                        logger.info(f"{brandRaw} Brand Name Matched")
                        return fetchedBrandId
                else:
                    insertBrandQuery = "INSERT INTO Brand (brand_name,brand_key,brand_raw_name) VALUES (%s,%s,%s);"
                    this.execute(insertBrandQuery,(brand_name,brand_name.replace(" ", "-").lower(),brandRaw))
                    conn.commit()
                    logger.info(f'{vendor_id} >> Added new brand "{brand_name} ({this.lastrowid})".')
                    return this.lastrowid
        else:
            logger.debug(f"Error in DB connection:{e}")           
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertBrand() >> {e}")
        logger.warning(f"{vendor_id}, {brand_name}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Add product if doesn't exists
def checkInsertProduct(vendor_id, brand_id, mpn, name, msrp, image):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkProductQuery = "SELECT product_id FROM Product WHERE brand_id = %s AND product_mpn = %s"
            this.execute(checkProductQuery, [brand_id,mpn])
            records = this.fetchone()
            # Change this section
            if records is None:  # If no record found
                # Insert new product
                if msrp != '':
                    insertProductQuery = "INSERT INTO Product (brand_id,product_name,product_mpn,msrp,product_image) VALUES (%s,%s,%s,%s,%s)"
                    this.execute(insertProductQuery, (brand_id,name,mpn,msrp,image))
                else:
                    insertProductQuery = "INSERT INTO Product (brand_id,product_name,product_mpn,product_image) VALUES (%s,%s,%s,%s)"
                    this.execute(insertProductQuery, (brand_id,name,mpn,image))
                conn.commit()
                logger.info(f'{vendor_id} >> Added new product with mpn "{mpn} ({this.lastrowid})".')
                return this.lastrowid
            else:
                product_id = int(records[0])
                this.execute("UPDATE Product SET product_name = %s, product_image = %s WHERE product_id = %s", [name,image,product_id])
                conn.commit()
                if msrp != '':
                    this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s AND msrp IS NULL", [msrp,product_id])
                    conn.commit()
                logger.info(f'{vendor_id} >> Updated details for product with mpn "{mpn} ({product_id})".')
                return product_id
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertProduct() >> {e}")
        logger.warning(f"{vendor_id}, {brand_id}, {mpn}, {name}, {msrp}, {image}")
        return None
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Add product vendor if doesn't exists
def checkInsertProductVendor(vendor_id, product_id, sku, name, product_url, msrp):
    try:
        # First check if we have valid input
        if product_id is None:
            logger.warning(f"{vendor_id} >> Cannot insert vendor product: product_id is None")
            return None
            
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            if msrp == '' or msrp is None:
                msrp = None  # or set to 0.0 if you prefer a default value

            checkProductVendorQuery = "SELECT vendor_product_id FROM ProductVendor WHERE vendor_id = %s AND product_id = %s LIMIT 1"
            this.execute(checkProductVendorQuery, [vendor_id, product_id])
            records = this.fetchone()
            
            # Handle case where no records found
            if records is None:
                # Insert new record
                insertProductVendorQuery = "INSERT INTO ProductVendor (vendor_id, product_id, product_name, vendor_sku, msrp) VALUES (%s, %s, %s, %s, %s)"
                this.execute(insertProductVendorQuery, (vendor_id, product_id, name, sku, msrp))
                conn.commit()
                logger.info(f'{vendor_id} >> Added new product in ProductVendor "{vendor_id} x {product_id}".')
                return this.lastrowid
            else:
                # Update existing record
                vp_id = int(records[0])
                updateProductDetailQuery = "UPDATE ProductVendor SET vendor_sku = %s, product_name = %s, msrp = %s WHERE vendor_product_id = %s"
                this.execute(updateProductDetailQuery, [sku, name, msrp, vp_id])
                conn.commit()
                if this.rowcount == 1:
                    logger.info(f'{vendor_id} >> Updated details for vendor_product_id ({vp_id}).')
                logger.info(f'{vendor_id} >> Returned vendor_product_id ({vp_id}).')
                return vp_id
    except mysql.connector.Error as e:
        logger.error(f"{vendor_id} >> MySQL ERROR checkInsertProductVendor() >> {e}")
        return None
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Add product vendor url if doesn't exists
def checkInsertProductVendorURL(vendor_id, vendor_product_id, product_url):
    url = product_url.split('&')[0]
    try:
        if not vendor_product_id:
            logger.warning(f"{vendor_id} >> Invalid vendor_product_id: {vendor_product_id}")
            return  # Exit the function early
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkProductVendorURLQuery = "SELECT vendor_product_id FROM VendorURL WHERE vendor_product_id = %s"
            this.execute(checkProductVendorURLQuery, [vendor_product_id,])
            records = this.fetchall()
            if len(records) == 0:
                insertProductVendorURLQuery = "INSERT INTO VendorURL (vendor_product_id, vendor_raw_url, vendor_url) VALUES (%s, %s, %s)"
                this.execute(insertProductVendorURLQuery, [vendor_product_id, product_url, url])
                conn.commit()
                logger.info(f'{vendor_id} >> Added product vendor URL for vendor_product_id "{vendor_product_id}".')
                return this.lastrowid
            else:
                # fatchquary = "SELECT vendor_url_id, vendor_raw_url, vendor_url FROM VendorURL WHERE vendor_product_id = %s"
                # this.execute(fatchquary, [vendor_product_id])
                # results = this.fetchall()
                # if results[0][2] != url:
                # Update the existing record
                updateProductVendorURLQuery = """UPDATE VendorURL SET vendor_raw_url = %s, vendor_url = %s WHERE vendor_product_id = %s"""
                this.execute(updateProductVendorURLQuery, [product_url, url, vendor_product_id])
                conn.commit()
                logger.info(f'{vendor_id} >> Updated product vendor URL for vendor_product_id "{vendor_product_id}".')
                # else:
                #     logger.info(f'{vendor_id} >> Same Product vendor URL already exists for vendor_product_id "{vendor_product_id}".')
                # try:
                #     vendor_url_id, vendor_raw_url, vendor_url = results[0][0], results[0][1], results[0][2]
                #     checkProductVendorURLQuery = "SELECT vendor_bakup_url_id FROM BuilddotcomeDirectScraping_VendorURLBackup WHERE vendor_product_id = %s"
                #     this.execute(checkProductVendorURLQuery, [vendor_product_id,])
                #     Record = this.fetchone()
                #     if Record is None or len(Record) == 0:
                #         insertProductVendorURLQuery = "INSERT INTO BuilddotcomeDirectScraping_VendorURLBackup (vendor_url_id, vendor_product_id, vendor_raw_url, vendor_url) VALUES (%s, %s, %s, %s)"
                #         this.execute(insertProductVendorURLQuery, [vendor_url_id, vendor_product_id, vendor_raw_url, vendor_url])
                #         conn.commit()
                #         logger.info(f'Added product vendor_url for vendor_product_id "{vendor_product_id}" for vendor_bakup_url_id {this.lastrowid}.')
                #     else:
                #         if Record[0] is not None:
                #             fatchquary = "SELECT vendor_url_id, vendor_raw_url, vendor_url FROM BuilddotcomeDirectScraping_VendorURLBackup WHERE vendor_bakup_url_id = %s"
                #             this.execute(fatchquary, [Record[0],])
                #             Records = this.fetchone()
                #             if Records and Records[2] != vendor_url:
                #                 # Update the existing record
                #                 updateProductVendorURLQuery = """UPDATE BuilddotcomeDirectScraping_VendorURLBackup SET vendor_raw_url = %s, vendor_url = %s WHERE vendor_bakup_url_id = %s"""
                #                 this.execute(updateProductVendorURLQuery, [vendor_raw_url, vendor_url, Record[0]])
                #                 conn.commit()
                #                 logger.info(f'Updated vendor_raw_url, vendor_url for vendor_bakup_url_id "{Record[0]}".')
                #             else:
                #                 logger.info(f'Same Product vendor URL already exists for vendor_bakup_url_id "{Record[0]}".')
                # except mysql.connector.Error as e:
                #     logger.warning(f"MySQL ERROR checkInsertProductVendorURL() >> {e}")
                # results.append(Records)
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertProductVendorURL() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# call all function into this function
def insertall(product_id, vendor_product_id, temp, vendor_id):
    try:
        vendorTempPricing(vendor_product_id, temp)
        rpVendorPricingHistory(vendor_product_id, temp, vendor_id)
        productMsrpUpdate(product_id, temp)
        productVendorMsrpUpdate(vendor_product_id, temp)
    except Exception as e:
        logger.error(f"Error in insertall(): {e}")

def getDatetime():
    currentDatetime = datetime.now()
    return currentDatetime.strftime("%Y-%m-%d %H:%M:%S")

# Temp vnendor pricing data
def vendorTempPricing(vendor_product_id, temp):
    dateTime = getDatetime()
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkQuery = "SELECT vendor_product_id FROM TempVendorPricing WHERE vendor_product_id = %s AND source = %s LIMIT 1"
            this.execute(checkQuery, (vendor_product_id, temp['source']))
            records = this.fetchone()
            if records:
                getPricequary = "SELECT * FROM TempVendorPricing WHERE vendor_product_id = %s AND source = 'direct_from_website'"
                this.execute(getPricequary, (records[0],))
                result = this.fetchone()
                savedprice = str(result[2]).strip()
                scrapedprice = str(temp['vendorprice_price']).strip()
                if savedprice == scrapedprice:
                    logger.info(f"Same vendor price already exists for vendor_product_id {vendor_product_id}")
                else:
                    updateQuery = """UPDATE TempVendorPricing SET is_price_changed = %s, price_changed_date = %s WHERE vendor_product_id = %s AND source = %s"""
                    values = ("1", dateTime, vendor_product_id, temp['source'])
                    this.execute(updateQuery, values)
                    conn.commit()
                    logger.info(f"is_price_changed set 1 for vendor_product_id ({vendor_product_id}).")
                updateQuery = """UPDATE TempVendorPricing SET vendorprice_price = %s, vendorprice_finalprice = %s, vendorprice_date = %s, product_condition = %s, is_rp_calculated = %s, is_member = %s, scraped_by_system = %s
                    WHERE vendor_product_id = %s AND source = %s"""
                values = (temp['vendorprice_price'], temp['vendorprice_finalprice'], dateTime, temp['product_condition'], '2', '0', temp['scraped_by_system'], vendor_product_id, temp['source'])
                this.execute(updateQuery, values)
                conn.commit()
                logger.info(f"Record Updated for vendor_product_id ({vendor_product_id}) and source ({temp['source']})")
            else:
                insertQuery = """INSERT INTO TempVendorPricing (vendor_product_id, vendorprice_price, vendorprice_finalprice, vendorprice_date, product_condition, source, is_rp_calculated, is_member, scraped_by_system) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                values = (vendor_product_id, temp['vendorprice_price'], temp['vendorprice_finalprice'], dateTime, temp['product_condition'], temp['source'], '2', '0', temp['scraped_by_system'])
                this.execute(insertQuery, values)
                conn.commit()
                logger.info(f"Record Inserted for vendor_product_id ({vendor_product_id}) and source ({temp['source']})")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR vendorTempPricing() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close() 

def get_table_structure(host, db, user, password, table_name):
    """Retrieve column details from a table, preserving the column order."""
    try:
        conn = mysql.connector.connect(host=host, database=db, user=user, password=password)
        cursor = conn.cursor()            
        cursor.execute(f"DESCRIBE {table_name}")
        structure = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in cursor.fetchall()]  
        # (Column Name, Column Type, NULL, Key, Default, Extra)
    except Exception as e:
        logger.error(f"Error fetching table structure for {table_name}: {e}")
        structure = []
    finally:
        cursor.close()
        conn.close()
    return structure

def match_table_structure(source_structure, target_structure):
    """Find missing columns with full definitions and their correct positions."""
    target_columns = {col[0]: col for col in target_structure}  # {Column Name: Column Details}
    missing_columns = []

    for index, column in enumerate(source_structure):
        col_name, col_type, is_null, key, default, extra = column
        if col_name not in target_columns:
            after_column = source_structure[index - 1][0] if index > 0 else None
            missing_columns.append((col_name, col_type, is_null, key, default, extra, after_column))
    if missing_columns and len(missing_columns) > 0:
        logger.info(f"Missing columns: {missing_columns}")
    logger.info(f"History Table is up-to-date.")
    return missing_columns

def rpVendorPricingHistory(vendor_product_id, temp, vendor_id):
    dateTime = getDatetime()
    try:
        # save to AF/HP if vendor_id is one of them
        if vendor_id == 10021 or vendor_id == 10024: conn = mysql.connector.connect(host=HOST2, database=DB2, user=USER2, password=PASS2)
        else: conn = mysql.connector.connect(host=HOST3, database=DB3, user=USER3, password=PASS3)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            # check if vendor specific vendorPricing table exists or not
            vendor_pricing_table = f"z_{vendor_id}_VendorPricing"
            this.execute(f"""SELECT * 
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{vendor_pricing_table}'
            LIMIT 1""")
            result = this.fetchone()
            source_structure = get_table_structure(HOST, DB, USER, PASS, 'TempVendorPricing')
            if not result:
                logger.info(f"Table {vendor_pricing_table} does not exist. Creating table...")
                column_definitions = []
                primary_key = None  # Store primary key column if exists
                for col_name, col_type, is_null, key, default, extra in source_structure:
                    null_option = "NULL" if is_null == "YES" else "NOT NULL"
                    # Handle default values properly
                    if default is not None:
                        if "timestamp" in col_type.lower() or "datetime" in col_type.lower():
                            default_option = "DEFAULT CURRENT_TIMESTAMP" if default.lower() == "current_timestamp()" else ""
                        else:
                            default_option = f"DEFAULT {repr(default)}"
                    else:
                        default_option = ""
                    extra_option = extra if extra else ""
                    # Ensure AUTO_INCREMENT is properly handled
                    if "auto_increment" in extra.lower():
                        extra_option = "AUTO_INCREMENT"
                        primary_key = col_name  # Store primary key
                    column_definitions.append(f"`{col_name}` {col_type} {null_option} {default_option} {extra_option}")
                create_table_query = f"""
                    CREATE TABLE `{vendor_pricing_table}` (
                        {', '.join(column_definitions)}
                        {f", PRIMARY KEY (`{primary_key}`)" if primary_key else ""}
                    );
                """.strip()
                this.execute(create_table_query)
                conn.commit()
                logger.info(f"Table {vendor_pricing_table} created successfully.")
                logger.info(f"==========================================")
            else:
                if vendor_id == 10021 or vendor_id == 10024:
                    target_structure = get_table_structure(HOST2, DB2, USER2, PASS2, vendor_pricing_table)
                else:
                    target_structure = get_table_structure(HOST3, DB3, USER3, PASS3, vendor_pricing_table)
                missing_columns = match_table_structure(source_structure, target_structure)
                if missing_columns and len(missing_columns) > 0:
                    # Add missing columns if table exists
                    for col_name, col_type, is_null, key, default, extra, after_column in missing_columns:
                        null_option = "NULL" if is_null == "YES" else "NOT NULL"
                        # Handle default values properly
                        if default is not None:
                            if "timestamp" in col_type.lower() or "datetime" in col_type.lower():
                                default_option = "DEFAULT CURRENT_TIMESTAMP" if default.lower() == "current_timestamp()" else ""
                            else:
                                default_option = f"DEFAULT {repr(default)}"
                        else:
                            default_option = ""
                        extra_option = extra if extra else ""
                        after_option = f"AFTER `{after_column}`" if after_column else "FIRST"
                        # Prevent adding AUTO_INCREMENT column incorrectly
                        if "auto_increment" in extra.lower():
                            logger.warning(f"Skipping column `{col_name}` because it has AUTO_INCREMENT.")
                            continue  # Do not add AUTO_INCREMENT column
                        alter_query = f"""
                            ALTER TABLE `{vendor_pricing_table}`
                            ADD COLUMN `{col_name}` {col_type} {null_option} {default_option} {extra_option} {after_option};
                        """.strip()
                        this.execute(alter_query)
                    conn.commit()
                    logger.info(f"Table {vendor_pricing_table} altered successfully.")
                    logger.info(f"==========================================")

            insertQuery = f"""INSERT INTO {vendor_pricing_table} (vendor_product_id, vendorprice_price, vendorprice_finalprice, vendorprice_date, 
                product_condition, source, is_rp_calculated, is_member, scraped_by_system) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (vendor_product_id, temp['vendorprice_price'], temp['vendorprice_finalprice'], dateTime, temp['product_condition'], temp['source'], '2', '0', temp['scraped_by_system'])
            this.execute(insertQuery, values)
            conn.commit()
            logger.info(f"Record Inserted for vendor_product_id ({vendor_product_id}) and source ({temp['source']}) In {vendor_pricing_table} history table.")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR {vendor_pricing_table} >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRF in Product table
def productMsrpUpdate(product_id, temp):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM Product WHERE product_id = %s", (product_id,))
            result = this.fetchone()
            if result:
                # Update MSRP
                if temp['msrp']:
                    this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s", (temp['msrp'], product_id))
                    conn.commit()
                    logger.info(f"Record Updated for product_id ({product_id}).")
    except mysql.connector.Error as e:
        logger.warning(f"{product_id} >> MySQL ERROR productMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRF in ProductVendor table
def productVendorMsrpUpdate(vendor_product_id, temp):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM ProductVendor WHERE vendor_product_id = %s", (vendor_product_id,))
            result = this.fetchone()
            if result:
                # Update MSRP
                if temp['msrp']:
                    this.execute("UPDATE ProductVendor SET msrp = %s WHERE vendor_product_id = %s", (temp['msrp'], vendor_product_id))
                    conn.commit()
                    logger.info(f"Record Updated for vendor_product_id ({vendor_product_id}).")
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_product_id} >> MySQL ERROR productVendorMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

def read_product_urls_from_file(filepath):
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def focus_browser_window(window_title_contains="Opera"):
    windows = gw.getWindowsWithTitle(window_title_contains)
    if windows:
        win = windows[0]
        win.activate()
        time.sleep(1)  # Give it a moment to focus

def open_url_human_like(url, window_title_contains="Opera"):
    focus_browser_window(window_title_contains)
    pyautogui.hotkey('ctrl', 't')
    time.sleep(1.5)  # Wait for new tab to open
    pyautogui.typewrite(url, interval=0.07)  # Simulate human typing
    time.sleep(0.5)
    pyautogui.press('enter')
    time.sleep(3)  # Wait for page to load (adjust as needed)

def getUrls(vendor_id, vendor_url):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor()
            getVendorURLQuery = """
                SELECT 
                    ProductVendor.vendor_product_id,
                    Product.product_id,
                    Product.product_mpn,
                    VendorURL.vendor_url 
                FROM VendorURL
                INNER JOIN ProductVendor ON ProductVendor.vendor_product_id = VendorURL.vendor_product_id
                INNER JOIN Product ON Product.product_id = ProductVendor.product_id
                WHERE ProductVendor.vendor_id = %s AND VendorURL.vendor_url NOT IN ("https://www.applianceandelectronics.com/product/kitchenaid-36-scorched-orange-commercial-style-freestanding-dual-fuel-range-kfdc506jsc-384338","https://www.applianceandelectronics.com/product/dcs-series-7-3-burner-stainless-steel-built-in-natural-gas-grill-bh1-36r-n-82870","https://www.applianceandelectronics.com/product/dcs-series-7-3-burner-stainless-steel-built-in-natural-gas-grill-bh1-36r-n-82870","https://www.applianceandelectronics.com/product/kitchenaid-36-scorched-orange-commercial-style-freestanding-dual-fuel-range-kfdc506jsc-384338","https://www.applianceandelectronics.com/product/kitchenaid-36-scorched-orange-commercial-style-freestanding-dual-fuel-range-kfdc506jsc-384338","https://www.applianceandelectronics.com/product/whirlpool-commercial-33-cu-ft-white-agitator-top-load-washer-cae2795fq-114034")
            """
            this.execute(getVendorURLQuery, [vendor_id,])
            url_list = this.fetchall()
            if url_list:
                logger.info(f"Found {len(url_list)} URLs to process")
                # Process URLs sequentially instead of in parallel for better logging
                for value in url_list:
                    vendor_product_id, product_id, product_mpn, url = value[0], value[1], value[2], value[3].strip()
                    if "html&" in url: 
                        url = url.split("html&")[0] + "html"
                    logger.info(f"Processing URL: {url}")
                    try:
                        scraper_unit(vendor_product_id, product_id, product_mpn, url, vendor_url, vendor_id)
                    except Exception as e:
                        logger.error(f"Error processing URL {url}: {e}")
                        continue
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR getUrls() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

if __name__ == "__main__":  
    start = time.perf_counter() 
    # catUrllist = [
       
    # ]            
    vendor_url = "https://www.applianceandelectronics.com/"
    domain = "https://www.applianceandelectronics.com"
    vendor_id = 90077
    
    getUrls(vendor_id, vendor_url)

    # product_urls = read_product_urls_from_file("productUrls(preeti)1.txt")
    # if not product_urls:
    #     logger.debug("No products found in productUrls(preeti).txt.")
    # else:
    #     logger.debug(f"Total {len(product_urls)} products found in productUrls(preeti).txt.")
    #     for product_url in product_urls:
    #         fetch_product_data(product_url,vendor_id)
    # finally:
    #     if driver:
    #         driver.quit()
    finish = time.perf_counter()
    logger.debug(f'Finished ThreadMain in {round(finish - start, 2)} second(s)')