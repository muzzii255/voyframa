import os
import sys
here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
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

def fetchExtraInfo(js,resp):
    extraInfo = {}
    extraInfo['country'] = 'India'
    extraInfo['consumerBrandName'] = js['consumerBrandName']
    extraInfo['measurementUnit'] = js['measurementUnit']
    extraInfo['manufacturer'] = js['manufacturer']
    extraInfo['packForm'] = js['packform']
    extraInfo['therapy'] = js['therapy']
    try:extraInfo['expiryDate'] = js['expiryDate']
    except:pass
    extraInfo['keyHighlights'] = keyhighligts(resp,'Key Highlights')
    extraInfo['benefits'] = keyhighligts(resp,'Benefits')
    extraInfo['ingredients'] = keyhighligts(resp,'Ingredients')
    extraInfo['uses'] = keyhighligts(resp,'Uses')
    extraInfo['howToUse'] = keyhighligts(resp,'How to Use')
    extraInfo['sideEffects'] = keyhighligts(resp,'Side Effects')
    extraInfo['safetyInformation'] = keyhighligts(resp,'Safety Information')
    extraInfo['description'] = ' '.join(resp.xpath('//strong[contains(text(),"Description")]/parent::h2/following-sibling::div//text()').getall())
    extraInfo['countryOfOrigin'] = resp.xpath('//div[contains(text(),"Country of Origin")]/following-sibling::div[1]/text()').get()
    keys_to_delete = []
    for key, item in extraInfo.items():
        if item == None:
            keys_to_delete.append(key)
    for kd in keys_to_delete:
        extraInfo.pop(kd)
    return extraInfo
        
def keyhighligts(resp,keyword):
    h = []
    for r in resp.xpath(f'//strong[contains(text(),"{keyword}")]/parent::h2/following-sibling::div/ul/li'):
        a = scraper_helper.cleanup(' '.join(r.xpath('.//text()').getall()))
        h.append(a)
    return h

    
    
def parseProduct(url,cat1,cat2):
    req = sess.get(url,headers=headers1)
    
    logging.info(f'{url},{req.status_code}')
    response = Selector(text=req.text)
    raw_js = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
    if raw_js:
        raw_js = scraper_helper.cleanup(raw_js)
        js = json.loads(raw_js)
        if 'productDetails' in js['props']['pageProps']:
            category = f'{cat1} - {cat2}'
            name = js['props']['pageProps']['productDetails']['name']
            
            images = [x['url'] for x in js['props']['pageProps']['productDetails']['damImages']]
            
            discounted_price = float(js['props']['pageProps']['productDetails']['salePrice'])
            orig_price = float(js['props']['pageProps']['productDetails']['costPrice'])
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
                    "Pharmacyname": "PharmEasy",
                    "URL": url,
                    "pharmacyStoreId": ObjectId("64d113adba8edf325ffe5f07"),
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
            fnl["ExtraInfo"] = fetchExtraInfo(js['props']['pageProps']['productDetails'],response)
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
                

def scraper_main(cat_link):
    cat_id = cat_link.split('-')[-1]
    cat1 = cat_link.split('/')[-2].replace('-',' ').title()
    cat2 = ' '.join(cat_link.split('/')[-1].split('-')[:-1]).title()
    page = 1
    while True:
        url = f'https://pharmeasy.in/api/otc/getCategoryProducts?categoryId={cat_id}&page={page}'
        req = sess.get(url,headers=headers)
        
        logging.info(f'{url},{req.status_code}')
        for row in req.json()['data']['products']:
            url = f'https://pharmeasy.in/health-care/products/{row["slug"]}'
            parseProduct(url,cat1,cat2)
        if len(req.json()['data']['products']) > 0:
            page +=1
        else:
            break
    
    
headers = """Accept: application/json, text/plain, */*
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Referer: https://pharmeasy.in/health-care/personal-care-877
Sec-Ch-Ua: "Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"""
headers = scraper_helper.get_dict(headers)
    
    
headers1 = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Referer: https://pharmeasy.in/health-care/personal-care-877
Sec-Ch-Ua: "Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"""
headers1 = scraper_helper.get_dict(headers1)
    


sess = requests.Session()
client = MongoClient('mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net')
db = client['development']
collection = db['pharmacies']
currency = getCurrency('INR')
currency = 1 / currency
currency = round(currency,4)
print(currency)





if __name__ == '__main__':
    print('Started Pharmeasy')
    logging.info('Started Pharmeasy')
    cat_links = []
    req = sess.get('https://pharmeasy.in/sitemaps/sitemap-categories.xml')
    for ur in xmltodict.parse(req.text)['urlset']['url']:
        cat_links.append(ur['loc'])
    print(len(cat_links))
    req = sess.get('https://pharmeasy.in',headers=headers1)
    print(req.status_code)
    cookies = ''
    for c in sess.cookies:
        a = c.name
        b = c.value
        cookie = f'{a}={b}; '
        cookies += cookie
    headers1['Cookie'] = cookies
    headers['Cookie'] = cookies
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(scraper_main,cat_links)
    print('Completed Pharmeasy')
    logging.info('Completed Pharmeasy')