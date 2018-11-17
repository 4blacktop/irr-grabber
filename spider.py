import urllib
import csv
import logging
from grab import Grab
from grab.spider import Spider, Task
import pymongo
from pymongo import MongoClient
from bson import json_util
import time

class ExampleSpider(Spider):
    def task_generator(self):
##        for id in range(698260999, 698269999): ## 9k
##        for id in range(698550000, 698660000): ## 110k started 1830 th=5
##        for id in range(697800000, 697900000): ##  697853738 th=10 100k st1208 --- 5678.99 seconds ---        
        for id in range(697700000, 697800000): ##  697853738 th=10 100k st1556 --- 5427.14 seconds ---      
            url = 'http://irr.ru/mobile_api/1.2/advertisements/advert/%s' % id
            g = Grab()
            g.proxylist.load_file('res/proxy-http.txt', proxy_type='http')
            g.setup(url=url)
            yield Task('search', grab=g)
            
    def task_search(self, grab, task):
        data = json_util.loads(grab.doc.body)
       
        if data['error'] == None:
            client = MongoClient('mongodb://localhost:27017/')
            db = client.irr 
            ads = db.ads
            result = ads.find({"_id": data['advertisement']["id"]}, {"_id": 1}).limit(1)
            
            if result.count() > 0:    
                pass
            else:
                data['_id'] = data['advertisement']["id"]
                post_id = ads.insert_one(data).inserted_id

        else:
            pass

if __name__ == '__main__':
    start_time = time.time()
    bot = ExampleSpider(thread_number=10)
    
    bot.run()
    print("--- %s seconds ---" % round(time.time() - start_time, 2))