import os
import sys
here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
from scrapy import Selector
import requests
import scraper_helper
import pandas as pd
import random
from bson.objectid import ObjectId
from pymongo import MongoClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from other_func.price_compare import checkPriceUpdate,get_cleaned_cat
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


headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Cache-Control: max-age=0
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
headers = scraper_helper.get_dict(headers, strip_cookie=False)

sess = requests.Session()
client = MongoClient(
    'mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net')
db = client['development']
collection = db['pharmacies']


def data_scraper(url_row):
    try:
        url = url_row['url']
        print(url)
        category = url_row['cat']
        req = sess.get(url, headers=headers,proxies=get_proxies())
        
        logging.info(f'{url},{req.status_code}')
        if req.status_code == 200:
            response = Selector(text=req.text)
            
            name = response.xpath('//h1/text()').get()
            discounted_price = scraper_helper.cleanup(response.xpath(
                '//div[@class="price-listing-pdp pricelst"]/span[@class="price"]/text()').get())
            orig_price = scraper_helper.cleanup(response.xpath(
                '//div[@class="price-listing-pdp pricelst"]/span[@class="old_price"]/text()').get())
            if orig_price and discounted_price:
                discounted_price = discounted_price.replace(
                    '$', '').replace(',', '')
                orig_price = orig_price.replace('$', '').replace(',', '')
            if orig_price == None:
                orig_price = scraper_helper.cleanup(
                    response.xpath('//span[@class="price"]/text()').get())
                if orig_price:
                    orig_price = orig_price.replace('$', '').replace(',', '')
                discounted_price = ''
            images = response.xpath(
                '//ul[@id="ProductAngleProdImagesAreaProdList"]/li/a/img/@data-zoom-image').getall()
            if discounted_price and discounted_price != '':
                amount = float(discounted_price)
            else:
                amount = float(orig_price)

            fnl = {
                "Pharmacyname": "Farmacias Guadalajara",
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
                "priceChange": []
            }
            if discounted_price != '':
                fnl["Price"] = discounted_price
                fnl['CutPrice'] = orig_price
            else:
                fnl["Price"] = orig_price
                fnl['CutPrice'] = ""
            extra_info = {
                "description": scraper_helper.cleanup(str(' '.join(response.xpath('(//div[@class="panel-collapse collapse"])[1]/div[1]//text()').getall()))),
                # 'short description': response.xpath('//p[@itemprop="description"]/text()').get()
            }
            ingredients = response.xpath(
                '//meta[@name="description"]/@content').get()
            if ingredients:
                ingredients = scraper_helper.cleanup(str(ingredients))
                a = ingredients.find('(')
                ingredients = ingredients[a:].replace('(', '')
                ingredients = ingredients[:-1]
                extra_info['ingredients'] = [ingredients]
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
        
        logging.info(f'{url} scraped')

    except Exception as e:
        logging.info(url_row['url'])
        logging.exception(e)


if __name__ == '__main__':
    try:
        print('Starting FarmaciasGuadalajara')
        logging.info('Starting FarmaciasGuadalajara')
        req = sess.get('https://www.farmaciasguadalajara.com/salud-es',headers=headers,proxies=get_proxies())
        print(req.status_code)
        cookies = ''
        for c in sess.cookies:
            a = c.name
            b = c.value
            cookie = f'{a}={b}; '
            cookies += cookie
        headers['Cookie'] = cookies
        
        df = pd.read_csv(f'{os.getcwd()}\\guadalajara\\farmaciasguadalajara_urls.csv', names=['url','cat'])
        df = df[df['url'] != 'url']
        df = df.drop_duplicates()
        df['test'] = df['url'].apply(lambda x: x.split('/')[-1])
        df = df.drop_duplicates('test')
        all_links = df.to_dict('records')
        all_links = [x for x in all_links if 'ProductDisplay' not in x['test']]
        df = df.drop('test',axis=1)
        df.to_csv(f'{os.getcwd()}\\guadalajara\\farmaciasguadalajara_urls.csv',index=False)
        with ThreadPoolExecutor(max_workers=6) as executor:
            executor.map(data_scraper,all_links)
        # for link in all_links:
        #     data_scraper(link)
        print('Completed FarmaciasGuadalajara.')
        logging.info('Completed FarmaciasGuadalajara.')
    except Exception as e:
        logging.exception(e)
    
