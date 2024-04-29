import os
import sys
here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
import requests
from datetime import datetime
import time
import undetected_chromedriver as uc
from scrapy import Selector
import json
from bson.objectid import ObjectId
import xmltodict
from pymongo import MongoClient
from concurrent.futures import ProcessPoolExecutor
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from iteration_utilities import unique_everseen
import unicodedata
import regex as re
import pandas as pd
import random

def checkPriceUpdate(js1, js2):
    if 'priceChange' in js2:
        js1['priceChange'].extend(js2['priceChange'])
    js1['priceChange'].append(
        {'price': js2['amount'], 'date': js2['updatedAt']})
    dupes = list(unique_everseen(js1['priceChange']))
    js1['priceChange'] = dupes
    return js1

def get_cleaned_cat(url):
    if url:
        cleaned_url = unicodedata.normalize("NFD", url)
        cleaned_url = re.sub(r"[\u0300-\u036f]", "", cleaned_url)
        cleaned_url = re.sub(r"[^\w\s]", "", cleaned_url)
        cleaned_url = re.sub(r"\s+", "-", cleaned_url)
        cleaned_url = cleaned_url.lower()
        cleaned_url = cleaned_url.strip("-")
        return cleaned_url


def get_proxies():
    df = pd.read_csv('proxies.csv')
    df = df['proxy'].to_list()
    pr = random.choice(df)
    return f'http://{pr}'




def get_driver():
    proxy = get_proxies()
    driver = getattr(ProcessPoolExecutor, 'driver', None)
    if driver is None:
        options = uc.ChromeOptions()
        options.add_argument(f'--proxy-server={proxy}')
        options.add_argument(f'--incognito')
        driver = uc.Chrome(driver_executable_path='./chromedriver.exe',options=options,use_subprocess=False)
        setattr(ProcessPoolExecutor, 'driver', driver)
    return driver


def getUsdCurrency():
    req = requests.get('http://api.exchangeratesapi.io/v1/convert?access_key=36f7810dfb9321e6f169ea28682c25e9&from=USD&to=MXN&amount=1')
    print(req.json())
    return req.json()['result']



client = MongoClient('mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net')
db = client['development']
collection = db['pharmacies']
currency = getUsdCurrency()


def main_scraper(url):
    global collection,currency
    pharmacy_ids = {
        "Rite Aid":  ObjectId("65a1298dfe6a93b6b5258163") ,
        "Walgreens":  ObjectId("65a129d2fe6a93b6b5258164") , 
        "CVS Pharmacy":  ObjectId("65a12a06fe6a93b6b5258165") ,
        "Target (CVS)":  ObjectId("65a12a28fe6a93b6b5258166") ,
        "Safeway":  ObjectId("65a12a92fe6a93b6b5258167") ,
        "Albertsons":  ObjectId("65a12aa6fe6a93b6b5258168") ,
        "Walmart":  ObjectId("65a12abafe6a93b6b5258169") ,
        "Kroger Pharmacy":  ObjectId("65a12accfe6a93b6b525816a") ,
        "Costco*":  ObjectId("65a12ae1fe6a93b6b525816b"),
        "ShopRite":  ObjectId("65ae83dcfe6a93b6b5258181"), 
        "Acme Markets Pharmacy":  ObjectId("65ae83b9fe6a93b6b525817f"),  
        "Duane Reade":  ObjectId("65ae83cbfe6a93b6b5258180"),   
        "Wegmans":  ObjectId("65b1472cfe6a93b6b5258183"),   
    }
    try:
        driver = get_driver()
        print(url)
        driver.get(url)
        time.sleep(2)
        resp = Selector(text=driver.page_source)
        if resp.xpath('//h1[contains(text(),"Please verify you are a human")]').get():
            driver.close()
            setattr(ProcessPoolExecutor, 'driver', None)
            time.sleep(1)
            driver = get_driver()
            driver.get(url)
            time.sleep(2)
        
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//button[@data-qa="default-coupon-first-location-button"]')))
        except: pass
        
        resp = Selector(text=driver.page_source)
        if resp.xpath('//button[@data-qa="default-coupon-first-location-button"]').get():
            if resp.xpath('//button[contains(text(),"San Diego")]').get() == None:
                print('setting up zipcode')
                driver.find_element(By.XPATH,'//button[@data-qa="default-coupon-first-location-button"]').click()
                time.sleep(2)
                driver.find_element(By.XPATH,'//input[@aria-describedby="zip-related-content"]').send_keys('92110')
                time.sleep(1)
                driver.find_element(By.XPATH,'//button[@aria-label="Set location"]').click()
        resp = Selector(text=driver.page_source)
        
        used_for = resp.xpath('//span[contains(text(),"Used for ")]/text()').get()
        
        if used_for:
            used_for = used_for.replace("Used for ",'').strip()
        presentation = resp.xpath('//span[contains(text(),"Prescription")]/following-sibling::span/@title').get()
        drugSchemaScript = json.loads(resp.xpath('//script[@data-qa="drugSchemaScript"]/text()').get())
        try:Category = drugSchemaScript['administrationRoute']
        except: Category = None
        for sr in resp.xpath('//div[@aria-label="List of pharmacy prices"]/div'):
            try:
                a = sr.xpath('.//div[@color="$text-primary"]/span/text()').get()
                b = sr.xpath('.//span[@data-qa="pharmacy-row-price"]/text()').get()
                retail = sr.xpath('.//div[@data-qa="pharmacy-row-savings"]/div/span[1]/text()').get()
                if retail: retail = retail.replace('$','').replace('$','').replace(',','')
                if a:
                    b = b.replace('$','').replace(',','').strip()
                    cutPrice = float(b)
                    cutPrice = round(cutPrice * currency,2)
                    print(a)
                    if retail:
                        retailPrice = round(float((retail)) * currency,2)
                    else: retailPrice = None
                    fnl = {
                        "Pharmacyname": a,
                        "Subpharmacyname": 'Goodrx',
                        "URL": url,
                        "pharmacyStoreId": pharmacy_ids[a],
                        "Category": used_for,
                        "Product": drugSchemaScript['name'],
                        # "Price": str(price),
                        # "CutPrice": "",
                        "startedAt": "",
                        "Image": [drugSchemaScript['image']['contentUrl']],
                        "amount": cutPrice,
                        "updatedAt": "",
                        "categorySlug": get_cleaned_cat(used_for),
                        "medicineSlug": get_cleaned_cat(str(a) + ' ' + drugSchemaScript['name']),
                        "priceChange": [],
                        "country": "USA",
                    }
                    if retailPrice != '':
                        fnl["Price"] = retailPrice
                        fnl['CutPrice'] = cutPrice
                    else:
                        fnl["Price"] = cutPrice
                        fnl['CutPrice'] = ""

                    extra_info = {
                        'Ingredientes':[drugSchemaScript['alternateName']],
                        "description": drugSchemaScript['description'],
                        "drugClass": drugSchemaScript['drugClass']['name'],
                        "availableStrength": drugSchemaScript['availableStrength'],
                        "nonProprietaryName": drugSchemaScript['nonProprietaryName'],
                        "administrationRoute": Category,
                        "Enfermedad": [used_for],
                        "retailPrice":str(retailPrice),
                        'retailPriceUSD': retail,
                        'cutPriceUSD': b,
                        "presentation":presentation,
                        "labelDetails": drugSchemaScript['labelDetails'],
                        "legalStatus": drugSchemaScript['legalStatus'],
                        "warning": drugSchemaScript['warning'],
                        "drugUnit": drugSchemaScript['drugUnit'],
                        "dosageForm": drugSchemaScript['dosageForm'],
                        "scrappedWebsite": "GoodRX",
                        
                    }
                    
                    fnl["ExtraInfo"] = extra_info
                    find_rec = collection.find_one({"URL": url,"Pharmacyname":a})
                    if find_rec:
                        fnl["startedAt"] = find_rec["startedAt"]
                        fnl["updatedAt"] = datetime.now()
                        fnl = checkPriceUpdate(fnl, find_rec)
                            
                        fnl = fnl.copy()
                        data = {
                            "fnl": fnl,
                            "is_updated": True
                        }


                    else:
                        fnl["startedAt"] = datetime.now()
                        fnl["updatedAt"] = datetime.now()
                        fnl = fnl.copy()
                        fnl['priceChange'].append({'price': fnl['amount'], 'date': fnl['updatedAt']})
                        data = {
                            "fnl": fnl,
                            "is_updated": False
                        }
                    
                    fnl = data["fnl"]
                    is_updated = data["is_updated"]
                    link = fnl["URL"]
                    if is_updated:
                        collection.update_one({"URL": link,"Pharmacyname":a}, {"$set": fnl})
                        print('data updated on db')
                    else:
                        collection.insert_one(fnl)
                        print('data uploaded on db')
                
            except Exception as e:
                print(e)
    except Exception as e:
        print(e) 
        driver.close()
        time.sleep(1)
        setattr(ProcessPoolExecutor, 'driver', None)
        



if __name__ == '__main__':
    # driver = get_driver()
    # driver.get('https://www.goodrx.com/acetohydroxamic-acid')
    # time.sleep(5)
    # resp = Selector(text=driver.page_source)
    
    req = requests.get('https://www.goodrx.com/sitemaps/drug-price.xml')
    if req.status_code == 200:
        all_urls = []
        for r in xmltodict.parse(req.text)['urlset']['url']:
            all_urls.append(r['loc'])
        print(len(all_urls))
        with ProcessPoolExecutor(max_workers=2)  as executor:
            executor.map(main_scraper,all_urls[:])

        # for au in all_urls:
        #     main_scraper(au)


