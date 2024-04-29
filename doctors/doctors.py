import pandas as pd
from scrapy import Selector
import requests
import scraper_helper
from bson.objectid import ObjectId
from pymongo import MongoClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from other_func.price_compare import get_proxies
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
collection = db['doctors']

headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Cache-Control: max-age=0
Cookie: _gcl_au=1.1.1777925443.1703030184; _fbp=fb.1.1703030185224.1721772466; _gid=GA1.2.638503434.1703199468; _ga=GA1.1.1176280417.1703030183; _ga_4795QW0FCT=GS1.1.1703199468.3.0.1703199837.60.0.0
Dnt: 1
Sec-Ch-Ua: "Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: cross-site
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"""
headers = scraper_helper.get_dict(headers,strip_cookie=False)
sess = requests.Session()

def doctorScraper(link):
    try:
        req = sess.get(link,headers=headers,proxies=get_proxies())
        print(req.status_code)
        resp = Selector(text=req.text)
        cedula = resp.xpath('//h2[contains(text(),"Mi licencia")]/following-sibling::ul[@class="DoctorProfile__summaryDetails__list"]/li[@class="DoctorProfile__summaryDetails__list__item"]/text()').getall()
        cedula = [x.strip() for x in cedula if x]
        cedula = [x.split(' ')[0] for x in cedula]
        sobre = []
        sobre.append(resp.xpath('//div/article[@data-section-id="about"]//div[@class="DoctorProfile__sectionModule"]').get())
        sobre.append(resp.xpath('//div/article[@data-section-id="about"]//h2[contains(text(),"Mis estudios")]/following-sibling::div[1]').get())
        sobre = [x for x in sobre if x]
        sobre = ''.join(sobre)
        locations = []
        for li in resp.xpath('//ul[@class="DoctorProfile__clinicsTabs clearfix"]/li'):
            a = {
                'name': li.xpath('.//span[@class="DoctorProfile__clinicInfoText DoctorProfile__clinicName"]/@title').get(),
                'address': li.xpath('.//span[@class="DoctorProfile__clinicInfoText DoctorProfile__clinicLocation"]/@title').get(),
                'address2': li.xpath('.//div[@class="DoctorProfile__colContent"]/text()').get(),
                # '': li.xpath('.//span[@class="DoctorProfile__clinicInfoText DoctorProfile__clinicName"]/@title').get(),
            }
            locations.append(a)
        if len(locations) > 0:
            location = locations[0]['address']
            if len(location.split(',')) > 1:
                state = location.split(',')[-2]
                country = location.split(',')[-1]
            else:
                state = None
                country = None
        else:
            state = None
            country = None
        fnl = {
            "doctorWebsitesId": {
            "$oid": "657ebfd83e8881bb4ce25f7d"
            },
            "sourceName": "hulihealth",
            "URL": link,
            "name": resp.xpath('//h1[@class="DoctorProfile__nameText"]/text()').get(),
            "phoneNumber": [],
            "description":sobre,
            "photo": resp.xpath('//figure[@class="DoctorProfile__picture"]/a/@href').get(),
            "expertoEn": resp.xpath('//div[@class="table DoctorProfile__tableTreatment clearfix"]//div[@itemprop="description"]/text()').getall(),
            "cedulaProfesional": cedula,
            "location": locations,
            "city": state,
            "country": country,
            "speciality": ','.join(resp.xpath('//div[@class="DoctorProfile__specialtyWrapper"]/span/span/text()').getall()),
            "socialMediaProfiles": [],
            "tags": [],
            "isDeleted": False
            }
        find_rec = collection.find_one({"URL": link})
        
        
        if find_rec:
            fnl["startedAt"] = find_rec["startedAt"]
            fnl["updatedAt"] = datetime.now()
                
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
    except Exception as e:
        print(e)


if __name__ == '__main__':
    req = sess.get('https://www.hulihealth.com/es/doctor',headers=headers)
    print(req.status_code)
    resp = Selector(text=req.text)
    all_links = resp.xpath('//div[@class="item"]/a/@href').getall()
    all_links = list(map(lambda x: f'https://www.hulihealth.com{x}',all_links))
    df = pd.DataFrame({'urls':all_links},index=list(range(len(all_links)))).to_csv(f'{os.getcwd()}\\doctors\\doctors.csv',index=False,mode='a',header=False)
    for link in all_links:
        doctorScraper(link)
    
    df = pd.read_csv(f'{os.getcwd()}\\doctors\\doctors.csv',names=['url'])
    df = df.drop_duplicates()
    df.to_csv(f'{os.getcwd()}\\doctors\\doctors.csv',index=False)  
    