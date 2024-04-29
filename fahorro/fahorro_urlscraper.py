import time
import undetected_chromedriver as uc
from scrapy import Selector
import requests
import pandas as pd
import requests
import random
import os



switch = True
def exporter(row,file_name):
    global switch 
    if switch:
        switch = False
        pd.DataFrame(row,index=[0]).to_csv(file_name,index=False,mode='a')
    else:
        pd.DataFrame(row,index=[0]).to_csv(file_name,index=False,mode='a',header=False)

sess = requests.Session()

def get_proxies():
    df = pd.read_csv('proxies.csv')
    df = df['proxy'].to_list()
    pr = random.choice(df)
    return f'http://{pr}'


def get_driver():
    proxy = get_proxies()
    options = uc.ChromeOptions()
    options.add_argument(f'--proxy-server={proxy}')
    # options.add_argument('--incognito')
    driver = uc.Chrome(driver_executable_path='./chromedriver.exe',options=options,use_subprocess=False)
    print('driver started successfully')
    return driver



if __name__ == '__main__':
    driver = get_driver()
    driver.get('https://www.fahorro.com/categorias-fahorro')
    time.sleep(2)
    resp = Selector(text=driver.page_source)
    for catrow in resp.xpath('//div[@data-element="main"]/a[@data-link-type="category"]'):
        category = catrow.xpath('./@href').get()
        catname = catrow.xpath('./span/text()').get()
        print(category,catname)
        main_cat_url = category
        while True:
            driver.get(main_cat_url)
            time.sleep(2)
            resp =Selector(text=driver.page_source)
            if len(resp.xpath('//a[@class="product-item-link"]/@href').getall()) == 0:
                driver.close()
                driver = get_driver()
                driver.get(main_cat_url)
                time.sleep(5)
                resp= Selector(text=driver.page_source)
            resp= Selector(text=driver.page_source)
            for u in resp.xpath('//a[@class="product-item-link"]/@href').getall():
                exporter({'url': u,'cat': catname},f'{os.getcwd()}\\fahorro\\fahorro_urls.csv')
            next_page_url = resp.xpath('//a[@class="action  next"]/@href').get()
            print(next_page_url)
            if next_page_url:
                main_cat_url = next_page_url
            else:break

    df = pd.read_csv(f'{os.getcwd()}\\fahorro\\fahorro_urls.csv')
    df = df.drop_duplicates()
    df.to_csv(f'{os.getcwd()}\\fahorro\\fahorro_urls.csv',index=False)
