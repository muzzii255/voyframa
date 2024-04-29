import os
import sys
here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
from datetime import datetime
import time
import undetected_chromedriver as uc
from scrapy import Selector
import pandas as pd
import json
from bson.objectid import ObjectId
from pymongo import MongoClient
from concurrent.futures import ProcessPoolExecutor
from other_func.price_compare import checkPriceUpdate,get_cleaned_cat,get_proxies


client = MongoClient('mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net/')
db = client['development']
collection = db['pharmacies']

def get_driver():
    proxy = get_proxies()
    driver = getattr(ProcessPoolExecutor, 'driver', None)
    if driver is None:
        options = uc.ChromeOptions()
        options.add_argument(f'--proxy-server={proxy}')
        options.add_argument('--incognito')
        driver = uc.Chrome(driver_executable_path='./chromedriver.exe',options=options,use_subprocess=True)
        driver.set_window_size(300, 400)
        setattr(ProcessPoolExecutor, 'driver', driver)
    return driver



def main_scraper(row):
    global collection
    try:
        driver = get_driver()
        url = row['url']
        print(url)
        driver.get(url)
        time.sleep(5)
        response = Selector(text=driver.page_source)
        name = " ".join(response.xpath("//h1[contains(@class, 'page-title')]//text()").getall()).strip()
        if name == None and response.xpath('//div[contains(text(),"This request was blocked by our security service")]').get():

            driver.close()
            setattr(ProcessPoolExecutor, 'driver', None)
            time.sleep(2)
            driver = get_driver()
            driver.get(url)
            time.sleep(2)
        response = Selector(text=driver.page_source)
        name = " ".join(response.xpath("//h1[contains(@class, 'page-title')]//text()").getall()).strip()
        category = row['cat']
        if response.xpath('//div[@class="product-info-price"]//span[@data-price-type="oldPrice"]/span/text()'):
            orig_price = response.xpath(
                '//div[@class="product-info-price"]//span[@data-price-type="oldPrice"]/span/text()').get()
            discounted_price = response.xpath(
                '//div[@class="product-info-price"]//span[@data-price-type="finalPrice"]/span/text()').get()
        else:
            orig_price = response.xpath('//div[@class="product-info-price"]//span[@data-price-type="finalPrice"]/span/text()').get()
            discounted_price = ""
        print(discounted_price,orig_price)
        
        if orig_price:
            orig_price = orig_price.replace('$','').replace(",", "").strip()
        if discounted_price:
            discounted_price = discounted_price.replace('$','').replace(",", "").strip()
        if discounted_price:    
            amount = float(discounted_price)
        else:
            if orig_price:
                amount = float(orig_price)
            else:amount = 0
            
            
        dscr = " ".join(response.xpath("//div[@id='description']//div//p/text()").getall()).strip()
        images = []
        json_data = response.xpath("//script[contains(text(), 'mage/gallery/gallery')]/text()").get()
        if json_data:
            json_resp = json.loads(json_data)
            for _, value in json_resp.items():
                data = value.get("mage/gallery/gallery")
                if data:
                    if data.get("data"):
                        for item in data.get("data"):
                            if item.get("full"):
                                images.append(item.get("full"))
                break
            
            
        if len(images) == 0:
            images = response.xpath('//div[@class="fotorama__stage"]/div/div/@href').getall()
        if name != '' and name:
        
            fnl = {
                "Pharmacyname": "Farmacia del Ahorro",
                "URL": url,
                "pharmacyStoreId": ObjectId("63d4eebd096c24a0283eba8e"),
                "Category": category,
                "Product": name,
                # "Price": orig_price,
                # "CutPrice": discounted_price,
                "startedAt": "",
                "Image": images,
                "amount": amount,
                "updatedAt": "",
                "categorySlug": get_cleaned_cat(category),
                "medicineSlug": get_cleaned_cat(name),
                "priceChange": [],
            }
            if discounted_price != '':
                fnl["Price"] = discounted_price
                fnl['CutPrice'] = orig_price
            else:
                fnl["Price"] = orig_price
                fnl['CutPrice'] = ""
            
            extra_info = {
                "description": dscr,
                'isDeleted':False,
            }
        
        
            for item in response.xpath("//table[contains(@id, 'specs-table')]//tr"):
                label = " ".join(item.xpath(".//th//text()").getall()).strip()
                value = " ".join(item.xpath(".//td//text()").getall()).strip()
                if "Ingredientes" in label:
                    extra_info[label] = value.split(",")
                else:
                    extra_info[label] = value
            fnl["ExtraInfo"] = extra_info
            find_rec = collection.find_one({"URL": url})
        
        
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
                data = {
                    "fnl": fnl,
                    "is_updated": False
                }
            
            fnl = data["fnl"]
            is_updated = data["is_updated"]
            link = fnl["URL"]
            if is_updated:
                collection.update_one({"URL": link}, {"$set": fnl})
            else:
                collection.insert_one(fnl)
            print('data uploaded on db')
    
        else:
            find_rec = collection.find_one({"URL": url})
        
            if find_rec:
                find_rec["updatedAt"] = datetime.now()
                    
                find_rec = find_rec.copy()
                find_rec['ExtraInfo']['isDeleted'] = True
                data = {
                    "fnl": find_rec,
                    "is_updated": True
                }

            fnl = data["fnl"]
            is_updated = data["is_updated"]
            link = fnl["URL"]
            if is_updated:
                collection.update_one({"URL": link}, {"$set": fnl})
            else:
                collection.insert_one(fnl)
    except Exception as e:
        print(e)    



if __name__ == '__main__':
    cmp = []
    df = pd.read_csv(f'{os.getcwd()}\\fahorro\\fahorro_urls.csv')
    df = df[df['url'] != 'url']
    with ProcessPoolExecutor(max_workers=2)  as executor:
        executor.map(main_scraper,df.to_dict('records'))
