from pymongo import MongoClient
from datetime import datetime,timedelta
import logging
import os
os.makedirs('logs',exist_ok=True)

cwd = os.getcwd()
log_filename = 'Database_delete_records'
log_level = logging.INFO
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m-%d-%Y %H:%M:%S',
                    filename=f'{cwd}\\logs\\{log_filename}.logs',
                    level=log_level
                    )



client = MongoClient('mongodb+srv://hugogarcia:mfS8AsZEKVNmj!Y@scrap.czefu97.mongodb.net')
db = client['development']
collection = db['pharmacies']
#change this
target_days = 6

target_date = datetime.now() - timedelta(days=target_days)

print(target_date)

all_data = collection.find()
for ad in all_data:
    if 'updatedAt' in ad:
        updated_date = ad['updatedAt']
        if updated_date < target_date:
            try:
                collection.delete_one({'URL': ad['URL']})
                logging.info(f'{updated_date}')
                logging.info(f'deleted: {ad["URL"]}')
            except Exception as e:
                logging.exception(e)