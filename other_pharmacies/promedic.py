import os
import sys
here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
from scrapy import Selector
import requests
import scraper_helper
from bson.objectid import ObjectId
from pymongo import MongoClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from other_func.price_compare import checkPriceUpdate,get_cleaned_cat
import logging
import pandas as pd
import random
os.makedirs('logs',exist_ok=True)

cwd = os.getcwd()
log_filename = datetime.now().strftime('%d-%m-%Y')
log_level = logging.INFO
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m-%d-%Y %H:%M:%S',
                    filename=f'{cwd}\\logs\\{log_filename}.logs',
                    level=log_level
                    )

headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Referer: https://www.probemedic.mx/
Sec-Ch-Ua: "Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: same-origin
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36
"""
headers = scraper_helper.get_dict(headers,strip_cookie=False)
sess = requests.Session()


client = MongoClient('mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net')
db = client['development']
collection = db['pharmacies']

def get_proxies():
    df = pd.read_csv('proxies.csv')
    df = df['proxy'].to_list()
    pr = random.choice(df)
    proxy = {
        'http': f'http://{pr}',
        'https': f'http://{pr}',
    }
    return proxy


def data_scraper(row):
    try:
        url,category = row
        req = sess.get(url,headers=headers,proxies=get_proxies())
        
        logging.info(f'{url},{req.status_code}')
        response = Selector(text=req.text)
        # category = response.xpath('//div[@class="breadcrumbs"]/ul/li[3]/a/text() | //div[@class="breadcrumbs"]/ul/li[2]/a/text()').get()
        name = response.xpath('//span[@itemprop="name"]/text()').get()
        discounted_price = response.xpath('//span[@class="special-price"]//span[@class="price"]/text()').get()
        orig_price = response.xpath('//span[@class="old-price"]//span[@class="price"]/text()').get()
        if orig_price and discounted_price:
            discounted_price = discounted_price.replace('$','').replace(',','')
            orig_price = orig_price.replace('$','').replace(',','')
        if orig_price == None:
            orig_price = response.xpath('//span[@class="price"]/text()').get()
            if orig_price: orig_price = orig_price.replace('$','').replace(',','')
            discounted_price = ''
        images = response.xpath('//img[@alt="main product photo"]/@src').getall()
        if discounted_price and discounted_price != '':
            amount = float(discounted_price)
        else:
            amount = float(orig_price)
        fnl = {
                "Pharmacyname": "Farmacias Probemedic",
                "URL": url,
                "pharmacyStoreId": ObjectId("64a71905b7b9a720e5aacb3b"),
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
        extra_info = {
            
        }
        ingredients = response.xpath('//th[contains(text(),"Sustancia Activa")]/following-sibling::td[1]/text()').get()
        if ingredients:
            extra_info['ingredients'] = ingredients.split(',')
        for tr in response.xpath('//table[@class="data table additional-attributes"]//tr'):
            a = tr.xpath('./th/text()').get()
            b = tr.xpath('./td/text()').get()
            extra_info[a] = b
        fnl["ExtraInfo"] = extra_info
        
        find_rec = collection.find_one({"URL": url})
        if find_rec:
            fnl["startedAt"] = find_rec["startedAt"]
            fnl["updatedAt"] = datetime.now()
            fnl = checkPriceUpdate(fnl, find_rec)
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
        print(e)


if __name__ == '__main__':
    print('Starting promedic')
    logging.info('Starting promedic')
    all_categories = []
    req = sess.get('https://www.probemedic.mx/farmacias.html',proxies=get_proxies())
    cookies = ''
    for c in sess.cookies:
        a = c.name
        b = c.value
        cookie = f'{a}={b}; '
        cookies += cookie
    headers['Cookie'] = cookies
    req = sess.get('https://www.probemedic.mx/farmacias.html',headers=headers,proxies=get_proxies())
    print(req.status_code)
    resp = Selector(text=req.text)
    for url in resp.xpath('//ul[@id="rw-menutop"]/li/a/following-sibling::div[1]/div[1]//ul[contains(@class,"level2-popup")]/li/a/@href | //ul[@id="rw-menutop"]/li/a/following-sibling::div[1]/div[1]//ul[contains(@class,"vertical-menu")]/li/a/@href').getall():
        all_categories.append(url)
    print('scraping urls from categories')
    print(len(all_categories))
    all_links = []
    for url in all_categories:
        print(url)
        if '-nivel-' not in url:
            category = url.split('/')[-1].replace('.html','').replace('-',' ').title()
        else:
            category = url.split('/')[-2].replace('.html','').replace('-',' ').title()
        main_url = url
        while True:
            print(main_url)
            try:
                req = sess.get(main_url,headers=headers,proxies=get_proxies(),timeout=100)
                resp = Selector(text=req.text)
                all_links.extend([[i,category] for i in resp.xpath('//a[@class="product-item-link"]/@href').getall()])
                next_page = resp.xpath('//a[@class="action  next"]/@href').get()
                if next_page:
                    main_url = next_page
                else:
                    break
            except: break
    print(f'total urls to scrape: {len(all_links)}')
    logging.info(f'total urls to scrape: {len(all_links)}')

    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(data_scraper,all_links)
    
    print('Completed promedic')
    logging.info('Completed promedic')