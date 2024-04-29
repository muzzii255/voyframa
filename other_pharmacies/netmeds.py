import os
import sys
here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
import pandas as pd
import json
import scraper_helper
import requests
from scrapy import Selector
from bson.objectid import ObjectId
from pymongo import MongoClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from other_func.price_compare import checkPriceUpdate,get_cleaned_cat
import xmltodict
import random
import logging
os.makedirs('logs',exist_ok=True)
cwd = os.getcwd()
log_filename = datetime.now().strftime('%d-%m-%Y')
log_level = logging.INFO
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m-%d-%Y %H:%M:%S',
                    filename=f'{cwd}\\logs\\{log_filename}.logs',
                    level=log_level
                    )





def get_proxies():
    df = pd.read_csv('proxies.csv')
    df = df['proxy'].to_list()
    pr = random.choice(df)
    proxy = {
        'http': f'http://{pr}',
        'https': f'http://{pr}',
    }
    return proxy

def getCurrency(cur):
    req = requests.get(f'http://api.exchangeratesapi.io/v1/convert?access_key=36f7810dfb9321e6f169ea28682c25e9&from=MXN&to={cur}&amount=1')
    print(req.json())
    return req.json()['result']

def cleanList(ls):
    ls = [l.strip() for l in ls]
    ls = [l for l in ls if l != '' and l]
    return ls

def fetchExtraInfo(js,resp):
    ingredients = resp.xpath('//b[contains(text(),"Ingredients: ")]/following-sibling::text()').get()
    if ingredients:
        ingredients = ingredients.split(',')
        ingredients = [x.strip() for x in ingredients]
    else:
        ingredients = []
    extraInfo = {}
    extraInfo['country'] = 'India'
    extraInfo['ingredients'] = ingredients
    extraInfo['safety_information'] = cleanList(resp.xpath('//div[@id="np_tab4"]/div//ul/li/text() | //div[@id="np_tab4"]//div/text()').getall())
    extraInfo['direction_for_use_dosage'] = cleanList(resp.xpath('//div[@id="np_tab3"]/div//ul/li/text() | //div[@id="np_tab3"]//div/text()').getall())
    extraInfo['key_benefits'] = cleanList(resp.xpath('//div[@id="np_tab2"]/div//ul/li/text() | //div[@id="np_tab2"]//div/text()').getall())
    extraInfo['description'] = resp.xpath('//div[@id="np_tab1"]/div//p/text()').get()
    extraInfo['pack_size'] = js['pack_size']
    extraInfo['type_of_package'] = js['type_of_package']
    extraInfo['formulation_type'] = js['formulation_type']
    extraInfo['brand_name'] = js['brand_name']
    extraInfo['search_keywords'] = js['search_keywords']
    try:extraInfo['CountryOrigin'] = js['json_ext']['CountryOrigin']
    except:pass
    try:extraInfo['MarketterName'] = js['json_ext']['MarketterName']
    except:pass
    try:extraInfo['MarketterAddress'] = js['json_ext']['MarketterAddress']
    except:pass
    extraInfo['stock_qty'] = js['stock_qty']
    keys_to_delete = []
    for key, item in extraInfo.items():
        if item == None:
            keys_to_delete.append(key)
    for kd in keys_to_delete:
        extraInfo.pop(kd)
    return extraInfo


def scraper(url):
    try:
        req = sess.get(url,headers=headers,proxies=get_proxies())
        
        logging.info(f'{url},{req.status_code}')
        resp = Selector(text=req.text)
        raw_text = resp.xpath('//script[contains(text(),"Sub-category Id")]/text()').get()
        a = raw_text.find("['Product details array'] = ")
        raw_text = raw_text[a:].replace("['Product details array'] = ",'')
        b = raw_text.find("obj['Quantity']")
        raw_text = raw_text[:b]
        raw_text = scraper_helper.cleanup(raw_text)
        raw_text = raw_text[:-1]
        
        js = json.loads(raw_text)
        category = ' - '.join([x['name'] for x in js['categories'][0]['bread_crumbs']])
        name = js['display_name']
        
        images = [f'https://www.netmeds.com/images/product-v1/600x600/{x}' for x in js['image_paths']]
        
        discounted_price = js['best_price']
        orig_price = js['mrp']
        if discounted_price == orig_price:
            discounted_price = ''
        if discounted_price != '':
            discounted_price = round(currency * discounted_price,2)
        orig_price = round(currency * orig_price,2)
        discounted_price = str(discounted_price)
        orig_price = str(orig_price)
        if discounted_price != '':
            amount = float(discounted_price)
        else:
            amount = float(orig_price)
            
        fnl = {
                "Pharmacyname": "Netmeds",
                "URL": url,
                "pharmacyStoreId": ObjectId("64d11360ba8edf325ffe5f06"),
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
                "priceChange": []
            }
        if discounted_price != '':
            fnl["Price"] = discounted_price
            fnl['CutPrice'] = orig_price
        else:
            fnl["Price"] = orig_price
            fnl['CutPrice'] = ""
        fnl["ExtraInfo"] = fetchExtraInfo(js,resp)
        find_rec = collection.find_one({"URL": url})
        if find_rec:
            fnl["startedAt"] = find_rec["startedAt"]
            fnl["updatedAt"] = datetime.now()
            fnl = checkPriceUpdate(fnl,find_rec)
            fnl = fnl.copy()
            item = {
                "fnl": fnl,
                "is_updated": True
            }
        else:
            fnl["startedAt"] = datetime.now()
            fnl["updatedAt"] = datetime.now()
            fnl = fnl.copy()
            item = {
                "fnl": fnl,
                "is_updated": False
            }
            
        # print(fnl)
        fnl = item.get("fnl")
        is_updated = item.get("is_updated")
        link = fnl.get("URL")
        if is_updated:
            collection.update_one({"URL": link}, {"$set": fnl})
        else:
            fnl['priceChange'].append(
            {'price': fnl['amount'], 'date': fnl['updatedAt']})
            collection.insert_one(fnl)
    except Exception as e:
        logging.exception(e)


headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en;q=0.9
Sec-Ch-Ua: "Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: none
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"""
headers = scraper_helper.get_dict(headers)
sess = requests.Session()
client = MongoClient('mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net')
db = client['development']
collection = db['pharmacies']
currency = getCurrency('INR')
currency = 1 / currency
currency = round(currency,4)

all_links = []
sitemaps = ['https://www.netmeds.com/feeds/sitemap-1-1.xml','https://www.netmeds.com/feeds/sitemap-1-2.xml','https://www.netmeds.com/feeds/sitemap-1-3.xml']
for sitemap in sitemaps:
    req = requests.get(sitemap,headers=headers)
    print(req.status_code)
    try:
        for ur in xmltodict.parse(req.text[3:])['urlset']['url']:
            all_links.append(ur['loc'])
    except:
        for ur in xmltodict.parse(req.text)['urlset']['url']:
            all_links.append(ur['loc'])
    

if __name__ == '__main__':
    print('Started Netmed')
    logging.info('Started Netmed')
    with ThreadPoolExecutor(max_workers=12) as executor:
        executor.map(scraper,all_links)

    print('Completed Netmed')
    logging.info('Completed Netmed')