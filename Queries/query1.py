from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://127.0.0.1:27017")
# client = MongoClient("mongodb://34.82.18.173:27017")
start = datetime.now()
print("connected to db")
db = client.dataset # Opening Dataset
# Counting the number of records that have high speed greater than 80 and less than 5 from loopdata collection
data = db.loopdata.count_documents({"$or" : [{'speed':{"$gt":80}}, {'speed':{"$lt":5}}]}) 
print("Total number of records with given speed :",data)
end = datetime.now()
# print("Total time to execute the query",end-start)