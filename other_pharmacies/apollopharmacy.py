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
from other_func.price_compare import checkPriceUpdate
import html
import logging
os.makedirs('logs',exist_ok=True)

cwd = os.getcwd()
log_filename = datetime.now().strftime('%d-%m-%Y')
log_level = logging.INFO
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m-%d-%Y %H:%M:%S',
                    filename=f'{cwd}\\logs\\{log_filename}.logs',
                    level=log_level
                    )

def getCurrency(cur):
    req = requests.get(f'http://api.exchangeratesapi.io/v1/convert?access_key=36f7810dfb9321e6f169ea28682c25e9&from=MXN&to={cur}&amount=1')
    print(req.json())
    return req.json()['result']

def getToken(text):
    a = text.find('"Bearer')
    text = text[a:].replace('"Bearer','')
    b = text.find('"')
    text = text[:b]
    text = text.strip()
    return text
    
def htmlDecode(txt):
    if txt:
        txt = txt.encode('unicode_escape').decode('utf-8')
        txt = html.unescape(txt)
        return txt
        
def extractList(text):
    data = []
    if text:
        resp = Selector(text=text)
        for li in resp.xpath('//li'):
            t = ' '.join(li.xpath('.//text()').getall())
            data.append(t)
    return data
        
def fetchExtraInfo(ks):
    ingredients = ks['key_ingredient']
    if ingredients:
        ingredients = ingredients.split(',')
        ingredients = [x.strip() for x in ingredients]
    else:
        ingredients = []
    extraInfo = {}
    extraInfo['country'] = 'India'
    extraInfo['sku'] = ks['sku']
    extraInfo['type_id'] = ks['type_id']
    extraInfo['consume_type'] = ks['consume_type']
    extraInfo['ingredients'] = ingredients
    extraInfo['size'] = ks['size']
    extraInfo['strength'] = ks['strength']
    extraInfo['flavour_fragrance'] = ks['flavour_fragrance']
    extraInfo['colour'] = ks['colour']
    extraInfo['uses'] = ks['uses']
    extraInfo['pack_size'] = ks['pack_size']
    extraInfo['cold_chain_product'] = ks['cold_chain_product']
    extraInfo['marketer_address'] = ks['marketer_address']
    extraInfo['product_information'] = htmlDecode(ks['product_information'])
    extraInfo['key_benefits'] = extractList(htmlDecode(ks['key_benefits']))
    extraInfo['safety_information'] =extractList(htmlDecode( ks['safety_information']))
    extraInfo['direction_for_use_dosage'] = extractList(htmlDecode(ks['direction_for_use_dosage']))
    extraInfo['pack_form'] = ks['pack_form']
    extraInfo['product_form'] = ks['product_form']
    extraInfo['age'] = ks['age']
    extraInfo['gender'] = ks['gender']
    extraInfo['vegetarian'] = ks['vegetarian']
    extraInfo['manufacturer'] = ks['manufacturer']
    extraInfo['country_of_origin'] = ks['country_of_origin']
    extraInfo['expiry_date'] = ks['expiry_date']
    keys_to_delete = []
    for key, item in extraInfo.items():
        if item == None:
            keys_to_delete.append(key)
    for kd in keys_to_delete:
        extraInfo.pop(kd)
    return extraInfo
    
    
def parseProduct(url,cat1,cat2):
    url = f'https://www.apollopharmacy.in/otc/{url}'
    req = sess.get(url,headers=headers)
    logging.info(f'{url},{req.status_code}')
    response = Selector(text=req.text)
    raw_js = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
    if raw_js:
        raw_js = scraper_helper.cleanup(raw_js)
        js = json.loads(raw_js)
        category = f'{cat1} - {cat2}'
        name = js['props']['pageProps']['productDetails']['productdp'][0]['name']
        
        images = [f'https://images.apollo247.in/pub/media{x}' for x in js['props']['pageProps']['productDetails']['productdp'][0]['image']]
        
        try:discounted_price = js['props']['pageProps']['productDetails']['productdp'][0]['special_price']
        except:discounted_price = ''
        orig_price = js['props']['pageProps']['productDetails']['productdp'][0]['price']
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
                "Pharmacyname": "Apollo Pharmacy",
                "URL": url,
                "pharmacyStoreId": ObjectId("64d1130cba8edf325ffe5f05"),
                "Category": category,
                "Product": name,
                # "Price": orig_price,
                # "CutPrice": discounted_price,
                "startedAt": "",
                "Image": images,
                "amount": amount,
                "updatedAt": "",
                "priceChange": []
            }
        if discounted_price != '':
            fnl["Price"] = discounted_price
            fnl['CutPrice'] = orig_price
        else:
            fnl["Price"] = orig_price
            fnl['CutPrice'] = ""
        fnl["ExtraInfo"] = fetchExtraInfo(js['props']['pageProps']['productDetails']['productdp'][0])
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
            
        fnl = item.get("fnl")
        is_updated = item.get("is_updated")
        link = fnl.get("URL")
        if is_updated:
            collection.update_one({"URL": link}, {"$set": fnl})
        else:
            fnl['priceChange'].append(
            {'price': fnl['amount'], 'date': fnl['updatedAt']})
            collection.insert_one(fnl)
            

def scraper(row):
    payload = {"category_id":row['id'],"page_id":1,"filters":{},"size":24}
    req = sess.post('https://magento.apollo247.com/v1/CategoryProducts',headers=headers1,data=json.dumps(payload))
    # all_products.extend([x['url_key'] for x in req.json()['products']])
    for x in req.json()['products']:
        parseProduct(x['url_key'],row['parent_cat'],row['sub_cat'])
    total_products = req.json()['count']
    print(total_products)
    pages = total_products // 24
    pages = pages + 2
    for x in range(2,pages):
        payload['page_id'] = x 
        req = sess.post('https://magento.apollo247.com/v1/CategoryProducts',headers=headers1,data=json.dumps(payload))
        # all_products.extend([x['url_key'] for x in req.json()['products']])
        for x in req.json()['products']:
            parseProduct(x['url_key'],row['parent_cat'],row['sub_cat'])


    
    
    
headers1 = """Accept: */*
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Authorization: Bearer 2o1kd4bjapqifpb27fy7tnbivu8bqo1d
Content-Length: 57
Content-Type: application/json
Origin: https://www.apollopharmacy.in
Referer: https://www.apollopharmacy.in/
Sec-Ch-Ua: "Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: cross-site
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"""
headers1 = scraper_helper.get_dict(headers1)
    
headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Cache-Control: max-age=0
Cookie: _gcl_au=1.1.1972611513.1691295920; _gid=GA1.2.2000636564.1691295920; WZRK_G=ad14bf0f706f4d458986b2601aeba38e; ln_or=eyIzMDkzODAxIjoiZCJ9; _clck=1epu3c0|2|fdx|0|1313; _uetsid=410fcfe0341111eeb16a039fa3167eaf; _uetvid=410fef30341111eead951ff78f9b41a3; _ga=GA1.2.1656884075.1691295920; cto_bundle=CFyR9F9uZ2tnY2c0USUyQmhNbVc1a2J4NlczZjZ0WE16ZWNCZSUyRnJtSHFKR25kaERBTnpTTmRrMk5PJTJGM1RYMGJndkNWTDJzSjYxbkRGZHE5eHJRYnF5TXVtZ3NrZm9GVHc5SFZwJTJGMjVQZnlpaWpOdkk5NldUWDNQd3lzZjZVdklLVFp1d3VpeG1EczZXOWlpT1g0UUU4QzZKYVFJVWRveERoNU9UJTJGT3NteWlySWY0T3I0JTNE; _ga_9CWW1XZBKT=GS1.1.1691359449.5.1.1691360002.58.0.0; _ga_GT0RTB03JM=GS1.1.1691359449.5.1.1691360002.0.0.0; _clsk=3biv3i|1691362494927|33|0|r.clarity.ms/collect
If-None-Match: "16wsqrh4tbw2ek0"
Sec-Ch-Ua: "Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: same-origin
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


if __name__ == '__main__':    
    print('Starting Apollopharmacy')
    logging.info('Starting Apollopharmacy')
    req = sess.get('https://assets.apollo247.in/production/aph-pharmacy-frontend/_next/static/chunks/9862-8748d7b39f156a4a.js')
    token = getToken(req.text)
    headers1['Authorization'] = f'Bearer {token}'
    df = pd.read_csv(f'{os.getcwd()}\\other_pharmacies\\categories.csv',dtype=str)
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(scraper,df.to_dict('records'))
    print('Completed Apollopharmacy')
    logging.info('Completed Apollopharmacy')
    

