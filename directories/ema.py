from concurrent.futures import ThreadPoolExecutor
import os
import sys
here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
import scraper_helper
import requests
from datetime import datetime
from scrapy import Selector
from pymongo import MongoClient
import pandas as  pd
import random

client = MongoClient('mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net')
db = client['development']
collection = db['fda_data']

headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Cookie: cck1=%7B%22cm%22%3Afalse%2C%22all1st%22%3Afalse%2C%22closed%22%3Afalse%7D
Dnt: 1
Referer: https://www.ema.europa.eu/en/search?f%5B0%5D=ema_medicine_bundle%3Aema_medicine&f%5B1%5D=ema_search_categories%3A83&landing_from=73303&page=12
Sec-Ch-Ua: "Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: same-origin
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"""
headers = scraper_helper.get_dict(headers,strip_cookie=False)

sess = requests.Session()

def get_proxies():
    df = pd.read_csv('proxies.csv')
    df = df['proxy'].to_list()
    pr = random.choice(df)
    proxy = {
        'http': f'http://{pr}',
        'https': f'http://{pr}',
    }
    return proxy

def scraper(url):
    # try:
    req = sess.get(url,headers=headers,proxies=get_proxies())
    resp = Selector(text=req.text)
    faqs = []
    for f in resp.xpath('//div[@id="accordion-bcl-accordion"]/div'):
        a = f.xpath('./h2/button/span/text()').get()
        b = ' '.join(f.xpath('./div//div[@class="ema-faq__body"]/p/text()').getall())
        faqs.append({'q':a,'a':b})

    status = resp.xpath('//div[@data-medicine-color="status-success"]/@data-medicine-color').get()
    if status:
        status = 'Approved'
    else:
        status = 'Rejected'
    fnl = {
        "URL": url,
        'ndaCode': '',
        'andaCode': '',
        'emaCode': resp.xpath('//dt[contains(text(),"EMA product number")]/parent::div/following-sibling::dd/div/text()').get()
    }
    fnl['sourceName'] = 'ema'
    fnl['ProductName'] = resp.xpath('//h1[@class="content-banner-title card-title bcl-heading"]/span/text()').get()
    fnl['strength'] = ''
    fnl['dosage'] = ''
    fnl['marketingStatus'] = ''
    fnl['teCode'] = ''
    fnl['rld'] = ''
    fnl['rs'] = ''
    fnl["startedAt"]= ""
    fnl["updatedAt"]= ""
    fnl['summary'] = ''
    fnl['importantInformation'] = ''
    fnl['Country'] = 'EU'
    fnl['originalApprovalsOrTentativeApprovals'] = []
    fnl['supplements'] = []
    fnl['labels'] = []
    fnl['products'] = []
    fnl['therapueticEquivalents'] = []
    fnl['approvalDate']= resp.xpath('//dt[contains(text(),"Marketing authorisation issued")]/parent::div/following-sibling::dd/div/text()').get()
    fnl['updateDate'] = resp.xpath('//strong[contains(text(),"This page was last updated on")]/following-sibling::time/text()').get()
    fnl['activeIngredients']= str(resp.xpath('//span[contains(@data-bs-original-title,"Active substance")]/parent::dt/parent::div/following-sibling::dd[1]/div/text() | //div[@class="card-content-block"]/div[1]//span/text()').get()).split(';')
    fnl['company']= resp.xpath('//span[contains(text(),"Marketing authorisation holder")]/parent::dt/parent::div/following-sibling::dd/div/text()').get()
    fnl['submissionStatus'] = status
    fnl['ingredients'] = str(resp.xpath('//span[contains(@data-bs-original-title,"Active substance")]/parent::dt/parent::div/following-sibling::dd[1]/div/text()').get()).split(';')
    fnl['companyAddress'] = scraper_helper.cleanup(' '.join(resp.xpath('//span[contains(text(),"Marketing authorisation holder")]/parent::dt/parent::div/following-sibling::dd/div/p/text()').getall()))
    fnl['iNN'] = resp.xpath('//span[contains(text(),"International non-proprietary name")]/parent::dt/parent::div/following-sibling::dd/div/text()').get()
    fnl['mesh'] = resp.xpath('//dt[contains(text(),"MeSH")]/parent::div/following-sibling::dd/div/text()').get()
    fnl['atcCode'] = resp.xpath('//dt[contains(text(),"ATC")]/parent::div/following-sibling::dd/div/text()').get()
    fnl['opinionAdopted'] = resp.xpath('//dt[contains(text(),"Opinion adopted")]/parent::div/following-sibling::dd/div/text()').get()
    fnl['marketingAuthorisationIssued'] = resp.xpath('//dt[contains(text(),"issued")]/parent::div/following-sibling::dd[1]/div/text()').get()
    fnl['revision'] = resp.xpath('//dt[contains(text(),"Revision")]/parent::div/following-sibling::dd[1]/div/text()').get()
    fnl['overview'] = scraper_helper.cleanup(' '.join((resp.xpath('//h2[contains(text(),"Overview")]/following-sibling::p/text()').getall())))
    fnl['faqs'] = faqs
    fnl['pharmacotherapeuticGroup'] = resp.xpath('//h3[contains(text(),"Pharmacotherapeutic group")]/following-sibling::text() | //h3[contains(text(),"Pharmacotherapeutic group")]/following-sibling::ul/li/text()').getall()
    fnl['therapeuticIndication'] = resp.xpath('//h3[contains(text(),"Therapeutic")]/following-sibling::p//text() | //h3[contains(text(),"Therapeutic")]/following-sibling::ul/li/text()').getall()

    if fnl['approvalDate']:
        fnl['approvalDate'] = fnl['approvalDate'].strip()
        fnl['approvalDate'] = datetime.strptime(fnl['approvalDate'].strip(),'%d/%m/%Y')
        
    if fnl['updateDate']:
        fnl['updateDate'] = fnl['updateDate'].strip()
        fnl['updateDate'] = datetime.strptime(fnl['updateDate'].strip(),'%d/%m/%Y')
    fnl['files'] = []
    for key,item in fnl.items():
        if item and isinstance(item,str):
            fnl[key] = scraper_helper.cleanup(item)
            
    for srow in resp.xpath('//div[@class="px-3-5 py-3 ema-file-container"]'):
        file = {}
        file['fileName'] = ' '.join(srow.xpath('./div[1]/div[2]//p[1]//text()').getall())
        file['firstPublished'] = srow.xpath('./div[1]/div[2]//div/small[1]/span[2]/time/text()').get()
        file['lastUpdated'] = srow.xpath('./div[1]/div[2]//div/small[2]/span[2]/time/text()').get()
        file['referenceNumber'] = srow.xpath('./div[1]/div[2]//div/small[2]/span[3]/text()').get()
        file['fileUrl'] = srow.xpath('./following-sibling::div[1]//a/@href').get()
        fnl['files'].append(file)

    find_rec = collection.find_one({"URL": url})
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
    # except Exception as e:
    #     print(e)
    



if __name__ == '__main__':
    all_links = []
    main_url = 'https://www.ema.europa.eu/en/search?f%5B0%5D=ema_medicine_bundle%3Aema_medicine&f%5B1%5D=ema_search_categories%3A83&landing_from=73303&page=1'
    while True:
        req = sess.get(main_url,headers=headers,proxies=get_proxies())
        print(req.status_code)
        resp = Selector(text=req.text)

        urls = resp.xpath('//div[@class="teaser-title card-title bcl-heading"]/a[@class="standalone"]/@href').getall()
        urls = list(map(lambda x: f'https://www.ema.europa.eu{x}',urls))
        all_links.extend(urls)
        next_page = resp.xpath('//a[@aria-label="Next"]/@href').get()
        if next_page:
            next_page = f'https://www.ema.europa.eu/en/search{next_page}'
            print(next_page)
            main_url = next_page
        else:
            break
        
    print(len(all_links),' links to scrape.')
    for url in all_links:
        scraper(url)
        # break