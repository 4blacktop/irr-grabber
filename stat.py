import pymongo
from pymongo import MongoClient
import time

start_time = time.time()
start_id = 697000000
stop_id =  699000000
part_count = 100000;

print("From %s" % start_id + " to %s" % stop_id)

client = MongoClient('mongodb://localhost:27017/')
db = client.irr 
ads = db.ads

for id_now in range(start_id, stop_id, part_count):
    min_now = str(id_now)
    max_now = str(id_now + part_count)
    posts = ads.find({"_id": {"$gt": min_now, "$lte": max_now}})
    print ("%s - " % id_now + "%s" % max_now + " %s" % round(100 * posts.count() / part_count, 2) + '%: ' + "%s" % posts.count())

total = ads.find()
print("Total: %s" % total.count())
print("--- %s seconds ---" % round(time.time() - start_time, 2))
