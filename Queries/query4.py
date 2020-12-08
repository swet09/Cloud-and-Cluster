# Importing pymongo and datetime package
from pymongo import MongoClient
from datetime import datetime
 
# Passing MongoClient a host name and a port number.
# client = MongoClient("mongodb://127.0.0.1:27017")
client = MongoClient("mongodb://35.199.184.186:27017")
start = datetime.now()
detectors=[] 
length=[] 
# List to store the peak period travel times of all stations at 7 to 9 am
list7to9 = [] 
# List to store the peak period travel times of all stations at 4 to 6 pm
list4to6=[] 
with client:
	db = client.dataset # Opening Database project
	documents = db.stations.find({'locationtext':'Foster NB'})  # fetch documents with locationtext-Foster NB
	for station in documents:
		length.append(station['length']) 
		# get detectorid in stations documnet
		r=station['detectors'] 
		detector=[]
		for i in r:
			detector.append(i['detectorid'])
		detectors.append(detector)
	ct=0 # Index used to fetch length of each station
	for i in detectors:
		sum7to9 = 0 
		sum4to6 = 0 
		for j in i:
    		# fetch average speeds from loopdata between 7 to 9
			aggregate_data7to9 = [{'$sort': {'starttime':1}},{ '$match': {'$and': [ { 'detectorid':j }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T07:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T09:00:00.070+00:00')}}] }},
					   { '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
			avg_speed7to9 = list(db.loopdata.aggregate(aggregate_data7to9)) 
			if(avg_speed7to9[0]['Avgspeed']==None):
				avg_speed7to9[0]['Avgspeed']=0
			sum7to9+=avg_speed7to9[0]['Avgspeed']
			 # fetch average speeds from loopdata between 4 to 6
			aggregate_data4to6 = [{'$sort': {'starttime':1}},{ '$match': {'$and': [ { 'detectorid':j }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T16:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T18:00:00.070+00:00')}}] }},
					   { '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
			avg_speed4to6 = list(db.loopdata.aggregate(aggregate_data4to6))
			if(avg_speed4to6[0]['Avgspeed']==None):
				avg_speed4to6[0]['Avgspeed']=0
			sum4to6+=avg_speed4to6[0]['Avgspeed']
		# find total average speed from 7 to 9
		avgspeed_total=sum7to9/len(i) 
		# travel time in minutes for time period 7 to 9 am for each stations
		if(avgspeed_total == 0):
			timetaken = 0
		else:
			timetaken=(length[ct]/avgspeed_total)*60
		list7to9.append(timetaken)
		# find total avg speed from 4 to 6
		avgspeed_total=sum4to6/len(i)
		# travel time in minutes for time period 4 to 6 pm
		if(avgspeed_total == 0):
			timetaken = 0
		else:
			timetaken=(length[ct]/avgspeed_total)*60
		list4to6.append(timetaken)
		ct=ct+1
	sum_7to9 =0 # initialize final sum variable
	sum_4to6 = 0 
	for t in range(len(list7to9)):
		sum_7to9+=list7to9[t]
		sum_4to6+=list4to6[t]
	print("The final Peak Period Travel Times on 2011-09-22 :")
	print("Total travel time from 7 to 9 is - ",sum_7to9)
	print("Total travel time from 4 to 6 is - ", sum_4to6)
	end = datetime.now()
	print("Total time to execute the query",end-start)