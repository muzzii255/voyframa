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
Cache-Control: max-age=0
Cookie: AWSALB=cFrvrUGjK8SeyPX2K7vCrIOhwxnN/HoY3pjehGGRifyBHofzjfSu8cX6YfmYQ7LVw8FYcCQ7TTT2fIg8TMv0AMOnkBjkx0uFOG8qWLZVj0hoa9sEDNjtSjlugcx2; _ga=GA1.1.358978922.1706198905
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
    try:
        req = sess.get(url,headers=headers,proxies=get_proxies())
        resp = Selector(text=req.text)
        fnl = {
            "URL": url,
            'ndaCode': resp.xpath('//strong[contains(text(),"New Drug Application (NDA)")]/parent::span/following-sibling::span[1]/text()').get(),
            'andaCode': resp.xpath('//strong[contains(text(),"ANDA")]/parent::span/following-sibling::span[1]/text()').get(),
        }
        fnl['sourceName'] = 'fda'
        fnl['ProductName'] = resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[1]/text()').get()
        fnl['strength'] = resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[3]/text()').getall()
        fnl['dosage'] = resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[4]/text()').get()
        fnl['marketingStatus'] = resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[5]/text()').get()
        fnl['teCode'] = scraper_helper.cleanup(resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[6]/text()').get())
        fnl['rld'] = resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[7]/text()').get()
        fnl['rs'] = resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[8]/text()').get()
        fnl["startedAt"]= ""
        fnl["updatedAt"]= ""
        fnl['summary'] = resp.xpath('//a[@title="Summary Review"]/@href').get()
        fnl['importantInformation'] = resp.xpath('//a[contains(@title,"Important Information")]/@href').get()
        fnl['Country'] = 'USA'
        fnl['originalApprovalsOrTentativeApprovals'] = []
        fnl['supplements'] = []
        fnl['labels'] = []
        fnl['products'] = []
        fnl['therapueticEquivalents'] = []
        fnl['approvalDate']= resp.xpath('//table[@summary="Original Approvals or Tentative Approvals"]/tbody/tr/td[1]/text()').get() 
        fnl['updateDate'] = resp.xpath('//table[@summary="Supplements"]/tbody/tr[1]/td[1]/text()').get() 
        fnl['activeIngredients'] = []
        if resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[2]/text()').get():
            fnl['activeIngredients'] = resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[2]/text()').get().split(';')
        fnl['company'] = resp.xpath('//span[contains(text(),"Company")]/following-sibling::span[1]/text()').get()
        fnl['submissionStatus'] = 'Approved'
        fnl['ingredients'] = []
        if resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[2]/text()').get():
            fnl['ingredients'] = resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr/td[2]/text()').get().split(';')
        fnl['companyAddress'] = ''
        fnl['iNN'] = ''
        fnl['mesh'] = ''
        fnl['atcCode'] = ''
        fnl['opinionAdopted'] = ''
        fnl['marketingAuthorisationIssued'] = ''
        fnl['revision'] = ''
        fnl['overview'] = ''
        fnl['faqs'] = ''
        fnl['pharmacotherapeuticGroup'] = ''
        fnl['therapeuticIndication'] = ''
        fnl['files'] = []
        if fnl['approvalDate']:
            fnl['approvalDate'] = datetime.strptime(fnl['approvalDate'],'%m/%d/%Y')
        if fnl['updateDate']:
            fnl['updateDate'] = datetime.strptime(fnl['updateDate'],'%m/%d/%Y')

        for key,item in fnl.items():
            if item and isinstance(item,str):
                fnl[key] = scraper_helper.cleanup(item)

        for row in resp.xpath('//table[@summary="FDA Approved Drug Products"]/tbody/tr'):
            d = {}
            d['drugName'] = row.xpath('./td[1]/text()').get()
            d['ingredients'] = []
            if  row.xpath('./td[2]/text()').get():
                d['ingredients'] =  row.xpath('./td[2]/text()').get().split(';')
            d['strength'] = row.xpath('./td[3]/text()').get()
            d['dosage'] = row.xpath('./td[4]/text()').get()
            d['marketingStatus'] = row.xpath('./td[5]/text()').get()
            d['teCode'] = row.xpath('./td[6]/text()').get()
            d['rld'] = row.xpath('./td[7]/text()').get()
            d['rs'] = row.xpath('./td[8]/text()').get()
            for key,item in d.items():
                if item and isinstance(item,list) == False:
                    d[key] = scraper_helper.cleanup(item)
            fnl['products'].append(d)

        for row in resp.xpath('//table[@summary="Original Approvals or Tentative Approvals"]/tbody/tr'):
            d = {}
            d['actionDate'] = row.xpath('./td[1]/text()').get()
            d['submission'] = row.xpath('./td[2]/text()').get()
            d['actionType'] = row.xpath('./td[3]/text()').get()
            d['submissionClassification'] = row.xpath('./td[4]/text()').get()
            d['reviewPriority'] = row.xpath('./td[5]/text()').get()
            d['label'] = row.xpath('./td[6]/span/a[@title="Links to Label"]/@href').get()
            d['letter'] = row.xpath('./td[6]/span/a[@title="Links to Letter"]/@href').get()
            d['review'] = row.xpath('./td[6]/span/a[@title="Links to Review"]/@href').get()
            d['summaryReview'] = row.xpath('./td[6]/span/a[@title="Links to Summary Review"]/@href').get()
            for key,item in d.items():
                if item:
                    d[key] = scraper_helper.cleanup(item)
            fnl['originalApprovalsOrTentativeApprovals'].append(d)

        for row in resp.xpath('//table[@summary="Supplements"]/tbody/tr'):
            d = {}
            d['actionDate'] = row.xpath('./td[1]/text()').get()
            d['submission'] = row.xpath('./td[2]/text()').get()
            d['supplementCategoriesOrApprovalType'] = row.xpath('./td[3]/text()').get()
            d['label'] = row.xpath('./td[4]/a[@title="Links to Label"]/@href').get()
            d['letter'] = row.xpath('./td[4]/a[@title="Links to Letter"]/@href').get()
            for key,item in d.items():
                if item:
                    d[key] = scraper_helper.cleanup(item)
            fnl['supplements'].append(d)

        for row in resp.xpath('//table[@summary="Labels for the selected Application"]/tbody/tr'):
            d = {}
            d['actionDate'] = row.xpath('./td[1]/text()').get()
            d['submission'] = row.xpath('./td[2]/text()').get()
            d['supplementCategoriesOrApprovalType'] = row.xpath('./td[3]/text()').get()
            d['label'] = row.xpath('./td[4]/a/@href').get()
            d['notes'] = row.xpath('./td[5]/a/@href').get()
            for key,item in d.items():
                if item:
                    d[key] = scraper_helper.cleanup(item)
            fnl['labels'].append(d)

        for row in resp.xpath('//table[contains(@summary,"Therapeutic Equivalents")]/tbody/tr'):
            d = {}
            d['drugName'] = row.xpath('./td[1]/text()').get()
            d['ingredients'] = []
            if row.xpath('./td[2]//text()').get():
                d['ingredients'] =  row.xpath('./td[2]//text()').get().split(';')
            d['strength'] = row.xpath('./td[3]/text()').get()
            d['dosage'] = row.xpath('./td[4]/text()').get()
            d['marketingStatus'] = row.xpath('./td[5]/text()').get()
            d['teCode'] = row.xpath('./td[7]/text()').get()
            d['rld'] = row.xpath('./td[6]/text()').get()
            d['applicationNumber'] = row.xpath('./td[8]/a/@href').get()
            d['company'] = row.xpath('./td[9]/text()').get()
            for key,item in d.items():
                if item and isinstance(item,list) == False:
                    d[key] = scraper_helper.cleanup(item)
            fnl['therapueticEquivalents'].append(d)



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
    except Exception as e:
                exception_type, exception_object, exception_traceback = sys.exc_info()
                line_number = exception_traceback.tb_lineno
                print("Exception type: ", exception_type)
                print("exception_object: ", exception_object)
                print("Line number: ", line_number)


if __name__ == '__main__':
    all_links = []
    alphabets = 'abcdefghijklmnopqrstuvwxyz'
    # alphabets = 'a'
    for alp in alphabets:
        req = sess.get(f'https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=browseByLetter.page&productLetter={alp.upper()}&ai=0',headers=headers,proxies=get_proxies())
        resp = Selector(text=req.text)
        urls = resp.xpath('//table[@summary="Layout table showing Drug Names"]/tbody/tr/td[1]//ul/li/a/@href').getall()
        urls = list(map(lambda x: f'https://www.accessdata.fda.gov{x}',urls))
        all_links.extend(urls)
    print(len(all_links),' links to scrape.')
    # with ThreadPoolExecutor(max_workers=6) as executor:
    #     executor.map(scraper,all_links)
    for url in all_links:
        scraper(url)
    # scraper('https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=009175')
