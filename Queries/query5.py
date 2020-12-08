# import pymongo and datetime packages
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://34.83.143.197:27017")
# create an empty list route
start = datetime.now()
route=[] 
with client:
	db = client.dataset # Opening dataset
	# fetch documents with 'locationtext':'Johnson Cr NB' and "highway_name":"I-205" from stations collection
	docs = db.stations.find({'locationtext':'Johnson Cr NB',"highway_name":"I-205"})
	print("documents",docs)
	for station in docs:	
		route.append(station['locationtext']) 
		# get the downstream station id to Johnson Cr NB
		downstream = station['downstream'] 
	# fetch documents with 'locationtext': 'Columbia to I-205 NB',"highway_name":"I-205" from stations collection
	d1 = db.stations.find({'locationtext': 'Columbia to I-205 NB',"highway_name":"I-205"}) 
	for station in d1:
    	# get the downstream station of Columbia to I-205 NB
		stationid = station['downstream']
	while(downstream != stationid):
		d_data = db.stations.find({'stationid':downstream,"highwayid":"3"}) 
		for station in d_data:
			downstream=station['downstream']
			print("downstream",downstream)
			# get the location text using downstream and append the loaction text to the route
			route.append(station['locationtext'])
			print("loaction",station['locationtext'])
	print("Route Finding :")
	print("Route from Johnson Cr NB to Columbia to I-205 NB - ",route)
	end = datetime.now()
	print("Total time to execute the query",end-start)