from iteration_utilities import unique_everseen
import unicodedata
import regex as re
import pandas as pd
import random

def checkPriceUpdate(js1, js2):
    if 'priceChange' in js2:
        js1['priceChange'].extend(js2['priceChange'])
    js1['priceChange'].append(
        {'price': js2['amount'], 'date': js2['updatedAt']})
    dupes = list(unique_everseen(js1['priceChange']))
    js1['priceChange'] = dupes
    return js1

def get_cleaned_cat(url):
    if url:
        cleaned_url = unicodedata.normalize("NFD", url)
        cleaned_url = re.sub(r"[\u0300-\u036f]", "", cleaned_url)
        cleaned_url = re.sub(r"[^\w\s]", "", cleaned_url)
        cleaned_url = re.sub(r"\s+", "-", cleaned_url)
        cleaned_url = cleaned_url.lower()
        cleaned_url = cleaned_url.strip("-")
        return cleaned_url


def get_proxies():
    df = pd.read_csv('proxies.csv')
    df = df['proxy'].to_list()
    pr = random.choice(df)
    return f'http://{pr}'


