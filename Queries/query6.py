# Importing pymongo package and datetime package
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://34.82.18.173:27017")
# client = MongoClient("mongodb://127.0.0.1:27017")
start = datetime.now()
with client:
    db = client.test
    db.stations.update({'locationtext': 'Foster NB'},{ '$set' :{'milepost' : 22.6}})
    end = datetime.now()
    print("Total time to execute the query",end-start)
	