import os
import sys
here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
import scraper_helper
import requests
from scrapy import Selector
from bson.objectid import ObjectId
from pymongo import MongoClient
from datetime import datetime
from other_func.price_compare import checkPriceUpdate,get_cleaned_cat
import logging

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



headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Cache-Control: max-age=0
Referer: https://everyone.org/shop
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
currency = getCurrency('EUR')
currency = 1 / currency
currency = round(currency,4)
categories = [
    ['https://everyone.org/shop/cardiology','Cardiology'],
    ['https://everyone.org/shop/skin-diseases','Dermatology'],
    ['https://everyone.org/shop/endocrinology','Endocrinology'],
    ['https://everyone.org/shop/women-s-health','Gynaecology'],
    ['https://everyone.org/shop/haematology','Haematology'],
    ['https://everyone.org/shop/metabolic-diseases','Hepatology'],
    ['https://everyone.org/shop/infectious-diseases','Infectiology'],
    ['https://everyone.org/shop/inflammatory-disease','Inflammatory Disease'],
    ['https://everyone.org/shop/neurological-diseases','Neurology'],
    ['https://everyone.org/shop/cancer','Oncology'],
    ['https://everyone.org/shop/opthalmology','Opthalmology'],
    ['https://everyone.org/shop/other-diseases','Pulmonology'],
    ['https://everyone.org/shop/bone-diseases-osteoporosis','Rheumatology'],
]

def fetchExtraInfo(resp):
    data = {}
    data['Country'] = 'Netherlands'
    data['description'] = scraper_helper.cleanup('\n'.join(resp.xpath('//div[@class="product attribute overview"]//text()').getall()))
    data['diseaseIndication'] = resp.xpath('//h4[contains(text(),"Disease Indications")]/following-sibling::p/text()').get()
    data['manufacturer'] = resp.xpath('//h4[contains(text(),"Manufacturer")]/following-sibling::p/text()').get()
    data['marca'] = resp.xpath('//h4[contains(text(),"Manufacturer")]/following-sibling::p/text()').get()
    data['usage'] = resp.xpath('//h4[contains(text(),"Usage")]/following-sibling::p/text()').get()
    data['medicineApprovedBy'] = resp.xpath('//h4[contains(text(),"Medicine approved by")]/following-sibling::ul/li/span/text()').getall()
    data['detailsHtml'] = scraper_helper.cleanup(resp.xpath('//div[@data-customtab="details"]').get())
    data['clincalTrialsHtml'] = scraper_helper.cleanup(resp.xpath('//div[@data-customtab="clinical_trials"]').get())
    data['otherPrices'] = []
    for sr in resp.xpath('//div[@data-customtab="price_&_costs"]//table//tr[@class="currency-row"]'):
        a = sr.xpath('./td[1]/text()').get()
        b = sr.xpath('./td[2]/text()').get()
        if b:
            b = b.replace('EUR','').strip()
            b = round(float(b)* currency,2)
            c = {}
            c['size'] = a
            c['price'] = b
            data['otherPrices'].append(c)
    return data

def scraper(url,cat):
    req = sess.get(url,headers=headers)
    
    logging.info(f'{url},{req.status_code}')
    resp = Selector(text=req.text)
    links = resp.xpath('//strong[@class="product name product-item-name"]/a/@href').getall()
    for link in links:
        req = sess.get(link,headers=headers)
        
        logging.info(f'{link},{req.status_code}')
        resp = Selector(text=req.text)
        images = resp.xpath('//img[@class="main-product-image loading"]/@src | //img[@class="main-product-image"]/@src').getall()
        category = cat
        
        name = resp.xpath('//h1/span/text()').get()
        discounted_price = resp.xpath('//span[@data-price-type="finalPrice"]/@data-price-amount').get()
        if discounted_price:
            discounted_price = float(discounted_price)
            
            discounted_price = round(currency * discounted_price,2)
            discounted_price = str(discounted_price)
            amount = float(discounted_price)
        else:
            discounted_price = ''
            amount = ''
        fnl = {
                "Pharmacyname": "Farmacias Everyone",
                "URL": link,
                "pharmacyStoreId": ObjectId("64d7b3c52f6771f2f8e28b20"),
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
        
        fnl["Price"] = discounted_price
        fnl['CutPrice'] = ""
        fnl["ExtraInfo"] = fetchExtraInfo(resp)
        find_rec = collection.find_one({"URL": link})
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
            
req = sess.get('https://everyone.org/shop')
print(req.status_code)
print(sess.cookies)
cookies = ''
for c in sess.cookies:
    a = c.name
    b = c.value
    cookie = f'{a}={b}; '
    cookies += cookie
headers['Cookie'] = cookies  
print('Started Everyone')
logging.info('Started Everyone')
for link,cat in categories:
    print(link,cat)
    scraper(link,cat)
print('Completed Everyone')
logging.info('Completed Everyone')