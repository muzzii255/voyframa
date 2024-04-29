from scrapy import Selector
import requests
import scraper_helper
import pandas as pd
import random
import os

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
if __name__ == '__main__':
    print('generating cookies')
    req = sess.get('https://www.farmaciasguadalajara.com/salud-es',headers=headers,proxies=get_proxies())
    print(req.status_code)
    cookies = ''
    for c in sess.cookies:
        a = c.name
        b = c.value
        cookie = f'{a}={b}; '
        cookies += cookie
    headers['Cookie'] = cookies
    req = sess.get('https://www.farmaciasguadalajara.com/',headers=headers,proxies=get_proxies())
    print(req.status_code)
    resp = Selector(text=req.text)
    for cat_row in resp.xpath('//ul[@id="departmentsMenu"]/li//ul/li/a'):
        try:
            cat_url = cat_row.xpath('./@href').get()
            cat_id = cat_row.xpath('./@id').get()
            cat_id_ = cat_id.split('_')[-1]
            print(cat_url,cat_id,cat_id_)
            cat_name = cat_row.xpath('./text()').get()
            cat_name = scraper_helper.cleanup(cat_name)
            parent_category = cat_row.xpath('./ancestor::div[2]/@aria-label').get()
            cat_name = ' - '.join([parent_category,cat_name])
            req = sess.get(cat_url,headers=headers,proxies=get_proxies())
            print(req.status_code)
            resp = Selector(text=req.text)
            x = 0
            while True:
                cat_url2 = f'https://www.farmaciasguadalajara.com/ProductListingView?top_category2=&top_category3=&facet=&searchTermScope=&top_category4=&top_category5=&searchType=&filterFacet=&resultCatEntryType=&sType=SimpleSearch&top_category=&gridPosition=&ddkey=ProductListingView_6_-2011_3074457345618263054&metaData=&ajaxStoreImageDir=%2Fwcsstore%2FFGSAS%2F&advancedSearch=&categoryId={cat_id_}&categoryFacetHierarchyPath=&searchTerm=&emsName=&filterTerm=&manufacturer=&resultsPerPage=20&disableProductCompare=ture&parent_category_rn=&catalogId=10052&langId=-24&enableSKUListView=false&storeId=10151&contentBeginIndex=0&beginIndex={x}&productBeginIndex={x}&orderBy=&x_listOnly=true&pageSize=20&x_pageType=SLP&x_noDropdown=true'
                req = sess.get(cat_url2,headers=headers,proxies=get_proxies())
                print(req.status_code)
                resp = Selector(text=req.text)
                for product_url in resp.xpath('//a[@class="plp-product-link-thumbnail"]/@href').getall():
                    print(product_url)
                    pd.DataFrame({'url': product_url,'cat': cat_name},index=[0]).to_csv(f'{os.getcwd()}\\guadalajara\\farmaciasguadalajara_urls.csv',index=False,mode='a',header=False)
                if resp.xpath('//button[@class="plp_loadmore_btn"]').get():
                    x += 20
                else:
                    break
        except Exception as e:
            print(e)