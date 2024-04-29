import os
import sys
here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
import json
import xmltodict
from scrapy import Selector
import requests
import scraper_helper
from bson.objectid import ObjectId
from pymongo import MongoClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from other_func.price_compare import checkPriceUpdate
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
    return req.json()['result']


headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Cache-Control: max-age=0
Sec-Ch-Ua: "Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: none
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36
"""
headers = scraper_helper.get_dict(headers,strip_cookie=False)

sess = requests.Session()
currency = getCurrency('CRC')
print(currency)
currency = 1 / currency
currency = round(currency,4)
all_links = []
req = sess.get('https://www.fischelenlinea.com/sitemap-productos.xml')

for row in xmltodict.parse(req.text)['urlset']['url']:
	all_links.append(row['loc'])



        

client = MongoClient('mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net')
db = client['development']
collection = db['pharmacies']



def data_scraper(url):
    try:
        req = sess.get(url,headers=headers)
        
        logging.info(f'{url},{req.status_code}')
        response = Selector(text=req.text)
        raw_js = response.xpath('//script[contains(text(),"URLWhatsapp")]/text()').get()
        if raw_js:
            raw_js = scraper_helper.cleanup(raw_js)
            a = raw_js.find('Product =')
            raw_js = raw_js[a:].replace('Product =','')
            b = raw_js.find('; var itsUserAvailable')
            raw_js = raw_js[:b]
            raw_js = scraper_helper.cleanup(raw_js)
            js = json.loads(raw_js)
            category = js['product']['productsList']['Categories'][-1]['Name']
            name = response.xpath('//meta[@property="og:title"]/@content').get()
            try:discounted_price = js['product']['productsList']['ProductAttributes'][0]['PriceWithDiscount']
            except:discounted_price = 0
            try:orig_price = js['product']['productsList']['ProductAttributes'][0]['PriceWithTaxes']
            except:
                orig_price = js['product']['productsList']['PriceWithTaxes']
            if discounted_price == 0:
                try:
                    orig_price = str(round(js['product']['productsList']['ProductAttributes'][0]['PriceWithTaxes'] * currency,2))
                except:orig_price = str(round(js['product']['productsList']['PriceWithTaxes'] * currency,2))
                discounted_price = ''
            else:
                orig_price = str(round(orig_price * currency,2))
                discounted_price = str(round(discounted_price * currency,2))
            images = response.xpath('//meta[@property="og:image"]/@content').getall()
            if discounted_price != '' and discounted_price != None:
                amount = float(discounted_price)
            else:
                amount = float(orig_price)
            fnl = {
                    "Pharmacyname": "Fischel",
                    "URL": url,
                    "pharmacyStoreId": ObjectId("64a5b460b7b9a720e5aacb34"),
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
            extra_info = {
                "description": scraper_helper.cleanup(js['product']['productsList']['LargeDescription']),
                "Country": 'Costa Rico'
            }
            ingredients = scraper_helper.cleanup(js['product']['productsList']['Components'])
            if ingredients:
                extra_info['ingredients'] = ingredients.split(',')
            fnl["ExtraInfo"] = extra_info
            
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
                collection.insert_one(fnl)
            
    except Exception as e:
        logging.exception(e)    
        
        
if __name__ == '__main__':
    print('Started Fischelenlinea')
    logging.info('Started Fischelenlinea')
    req = sess.get(all_links[0])
    cookies = ''
    for c in sess.cookies:
        a = c.name
        b = c.value
        cookie = f'{a}={b}; '
        cookies += cookie
    headers['Cookie'] = cookies
    print('products to scrape: ',len(all_links))
    with ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(data_scraper,all_links)
        
    print('Completed Fischelenlinea')
    logging.info('Completed Fischelenlinea')