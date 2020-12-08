from pymongo import MongoClient
import csv

def fetch_stations(csvfile):
    stationids=[]
    with open(csvfile, 'r',encoding='utf-8-sig') as file:
        csv_reader = csv.reader(file, delimiter=',')
        for row in csv_reader:
            stationid = int(row[0])
            milepost = float(row[2])
            locationtext = row[3]
            upstream = int(row[4])
            downstream = int(row[5])
            numberlanes = int(row[7])
            latlon = row[8].replace(" ", ",", 1)
            length = float(row[9])
            highwayid = row[1]
            stationclass = row[6]
            highway_name = row[10]

            station_document = {'stationid':stationid,'highwayid':highwayid,'milepost':milepost,'locationtext':locationtext,'upstream':upstream,
                                'downstream':downstream,'stationclass':stationclass,'numberlanes':numberlanes,'latlon':latlon,'length':length, 'highway_name':highway_name}
            stationids.append(station_document)
    return stationids

def fetch_detectors(csvfile):
    detectors = {}
    with open(csvfile, 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.reader(file, delimiter=',')
        for eachrow in csv_reader:

            detectorid = int(eachrow[0])
            lanenumber = int(eachrow[5])
            stationid = int(eachrow[6])

            document = {
                'detectorid': detectorid, 'lanenumber': lanenumber}
            if stationid not in detectors:
                detectors[stationid] = []
            detectors[stationid].append(document)
    return detectors

    			
# client = MongoClient("mongodb://34.82.18.173:27017")
client = MongoClient("mongodb://127.0.0.1:27017")
db=client.test 
stations = fetch_stations('C:/Users/prane/Desktop/Likhitha/Fall_2020/Cloud and Cluster/CNC-Project/Project_Code/Data/freeway_stations.csv') # Fetch documents from freeway_stations csv
detectors = fetch_detectors('C:/Users/prane/Desktop/Likhitha/Fall_2020/Cloud and Cluster/CNC-Project/Project_Code/Data/freeway_detectors.csv') # Fetch documents from freeway_detectors csv
for station in stations: 
    stationid = station['stationid'] 
    if stationid in detectors:
        station['detectors'] = detectors[stationid]    
    else:
        station['detectors'] = []
    result = db.stations.insert_one(station)
