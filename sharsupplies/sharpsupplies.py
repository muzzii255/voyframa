import json
import requests
import scraper_helper
from bson.objectid import ObjectId
from pymongo import MongoClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging
import os
os.makedirs('logs',exist_ok=True)

cwd = os.getcwd()
log_filename = datetime.now().strftime('%d-%m-%Y')
log_level = logging.INFO
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m-%d-%Y %H:%M:%S',
                    filename=f'{cwd}\\logs\\{log_filename}.logs',
                    level=log_level
                    )

client = MongoClient('mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net')
db = client['development']
collection = db['pharmacies']
order_collection = db['sharpsupplies_order']
def genCookies():
    cookies = ''
    for c in sess.cookies:
        a = c.name
        b = c.value
        cookie = f'{a}={b}'
        cookies += cookie
    return cookies


def getUsdCurrency():
    req = requests.get('http://api.exchangeratesapi.io/v1/convert?access_key=36f7810dfb9321e6f169ea28682c25e9&from=MXN&to=USD&amount=1')
    print(req.json())
    return req.json()['result']

    
def checkPriceUpdate(js1, js2):
    if 'priceChange' in js2:
        js1['priceChange'].extend(js2['priceChange'])
    js1['priceChange'].append({'Price': js2['Price'],'date':js2['updatedAt']})
    dupes = js1['priceChange']
    js1['priceChange'] = dupes
    return js1

def processPrice(row,updatedAt):
    price_list = []
    if 'shipModes' in row:
        a = row['shipModes'].split('<br/>')
        b = row['stock'].split('<br/>')
        c = row['prices'].split('<br/>')
        abc = list(zip(a,b,c))
        for s in abc:
            if s[0] != '':
                d = {}
                d['shipMode'] = s[0] 
                d['stock'] = s[1]
                try:d['stockInteger'] = int(s[1])
                except:d['stockInteger'] = 0
                d['price'] = round(currency * float(s[2]),2)
                d['date'] = updatedAt
                price_list.append(d) 
    return price_list

def scraper(srow):
    try:
        row1,row = srow
        name = row['itemName'].replace(' ','+')
        img_req_url = f'https://sharp.supplies/mysql/hotItems/getPics?itemName={name}&page=0'
        img_req = sess.get(img_req_url,headers=headers2)
        logging.info(f'{img_req_url},{img_req.status_code}')
        images = [x['fullUrl'] for x in img_req.json()['data']]
        
        price_list = processPrice(row,datetime.now())
        price = row1['price']
        price = round(price,2)
        price = round(currency * price,2)
        stock = row['stock'].split('<br/>')
        stock = stock[0]
        try:stock = int(stock)
        except:stock = 0
        fnl = {
            "Pharmacyname": "SharpSupplies",
            "URL": '',
            "pharmacyStoreId": ObjectId("656f83ba7e5a5c1bcbc848b7"),
            "Category": '',
            "Product": row['itemName'],
            "Image": images,
            "Price": price,
            "CutPrice": '',
            "startedAt": "",
            "updatedAt": "",
            "amount": price,
            "priceChange": [],    
        }
        extra_info = {
            "class": row['class'],
            "composition": row['composition'],
            'Ingredients': row['composition'].split('+'),
            "tempRange": row['tempRange'],
            "marketedBy": row['marketedBy'],
            'hotItem': row['hotitem'],
            "priceAndShipment": price_list,
        }
        for key,item in row1.items():
            if key != 'itemName' and key != 'price':
                extra_info[key] = item
        fnl["ExtraInfo"] = extra_info
        find_rec = collection.find_one({"Product": row['itemName']})
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
        if is_updated:
            collection.update_one({"Product": row['itemName']}, {"$set": fnl})
        else:
            fnl['priceChange'].append({'Price': fnl['Price'],'date':fnl['updatedAt']})
            collection.insert_one(fnl)
    except Exception as e:
        logging.exception(e)

def orderScraper(row):
    try:
        if row['album'] != '' and row['album']:
            img_req = sess.get(f'https://sharp.supplies/mysql/tRetailEx/viewFiles/{row["album"]}',headers=headers2)
            images = [x['fileUrl'] for x in img_req.json()]
        else:
            images = []
        # price = 
        fnl = {
                "Product": row['items'],
                "Image": images,
                "clientOrderId": row['clientOrderId'],
                "startedAt": "",
                "updatedAt": "",  
                "extraInfo": row
            }
        find_rec = collection.find_one({"clientOrderId": row['clientOrderId']})
        if find_rec:
            fnl["startedAt"] = find_rec["startedAt"]
            fnl["updatedAt"] = datetime.now()
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
        if is_updated:
            order_collection.update_one({"clientOrderId": row['clientOrderId']}, {"$set": fnl})
        else:
            order_collection.insert_one(fnl)
    except Exception as e:
        print(e)



headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Connection: keep-alive
Host: sharp.supplies
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: none
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36
sec-ch-ua: "Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
"""
headers = scraper_helper.get_dict(headers,strip_cookie=False)

headers1 = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Cache-Control: max-age=0
Connection: keep-alive
Content-Length: 45
Content-Type: application/x-www-form-urlencoded
Host: sharp.supplies
Origin: https://sharp.supplies
Referer: https://sharp.supplies/login
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: same-origin
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36
sec-ch-ua: "Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
"""
headers1 = scraper_helper.get_dict(headers1,strip_cookie=False)

headers2 = """Accept: application/json, text/plain, */*
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Connection: keep-alive
Cookie: sharp_session_id=s%3AdZ8tW7FaPBOF1q8UmyuFhuKEYGcCuG5W.jjznXsuP%2BX7GqPb0TEXT8JDpYOlK2hbDvEFV%2Fl2WnYY
Host: sharp.supplies
Referer: https://sharp.supplies/sharp
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36
X-Requested-With: XMLHttpRequest
sec-ch-ua: "Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
"""
headers2 = scraper_helper.get_dict(headers2,strip_cookie=False)

currency = getUsdCurrency()
currency = 1 / currency
currency = round(currency,4)
print(currency)

sess = requests.Session()


if __name__ == '__main__':
    print('Started Sharpsupplies')
    logging.info('Started Sharpsupplies')
    req1 = sess.get('https://sharp.supplies/login',headers=headers)
    headers['Cookie'] = genCookies()
    headers1['Cookie'] = genCookies()
    req = sess.post('https://sharp.supplies/mysql/users/login',data='username=1.HugoMx&password=Ma5%25rtha19&hash=',headers=headers1)
    headers2['Cookie'] = genCookies()
    req2 = sess.get('https://sharp.supplies/mysql/vUltimate/inventory?account=1.HugoMX&item1=USD',headers=headers2)
    req3 = sess.get('https://sharp.supplies/mysql/mItems/productGallery?account=1.HugoMX&item1=USD',headers=headers2)
    req4 = sess.get('https://sharp.supplies/mysql/hotItems/latest',headers=headers2)
    data_js = {}
    for r in req3.json()['data']:
        data_js[r['itemName']] = r
    
    data = []
    for ro in req2.json()['data']:
        r1 = data_js[ro['itemName']]
        ro['hotitem'] = False
        r2 = [r1,ro]
        data.append(r2)
    for ro in req4.json()['data']:
        r1 = data_js[ro['itemName']]
        ro['hotitem'] = True
        r2 = [r1,ro]
        data.append(r2)
        
        
    end_date_ = datetime.now().strftime('%Y-%m-%d')
    orders_req = sess.get(f'https://sharp.supplies/mysql/tRetailEx/clientOrdersBetween?account=1.HugoMX&endDate={end_date_}&startDate=2015-01-01',headers=headers2)
    print(orders_req.status_code)
    orders_data = orders_req.json()['data']
    
    
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(scraper,data)
    logging.info('scraping orders from sharpsupplies')
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(orderScraper,orders_data)
    


    print('Completed Sharpsupplies')
    logging.info('Completed Sharpsupplies')