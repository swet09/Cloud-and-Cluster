from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://34.83.143.197:27017")
# client = MongoClient("mongodb://127.0.0.1:27017")
start = datetime.now()
detector=[] #initialize a detectors list
volume_numbers=[] # initialize volume list for counting volume
sum_of_volume=0 # initialize vaiable to calculate sum of all volumes
with client:
	db = client.dataset # Opening Database
	data = db.stations.find({'locationtext':'Foster NB'}) # fetch the documents with loactiontext fosterNB
	for station in data:
		r=station['detectors'] # get detectors from the fetched documents
		for i in r:
			detector.append(i['detectorid'])
	for i in detector:
		aggregate_docs = [{'$sort': {'starttime':1}},{ '$match': {'$and': [ { 'detectorid':i }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-15T00:00:00.070+00:00'), "$lt":datetime.fromisoformat('2011-09-16T00:00:00.070+00:00')}}] }},
				  { '$group': {'_id': "null", 'sumofvolume': { '$sum': "$volume" } }}]
		print("aggregate_data", aggregate_docs)
		volume = list(db.loopdata.aggregate(aggregate_docs)) #get volume from loopdata with matching detector id and date
		print("vol", volume)
		# adding the volume to a list
		volume_numbers.append(volume) 
	for i in volume_numbers:
    		# add all volumes to return total volume
    		sum_of_volume +=i[0]['sumofvolume']
	
	print("Total volumeon Foster NB on 2011-09-15 is :",sum_of_volume)
	end = datetime.now()
	print("Total time to execute the query",end-start)