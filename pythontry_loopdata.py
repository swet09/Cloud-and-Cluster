from pymongo import MongoClient
from pprint import pprint
from datetime import datetime

def insert_loopdata(db, stationidinDetectors, csvfile):
    	loopdata = []
	count = 0
	error = 0
	fileHandler = open(csvfile, "r")
	next(fileHandler)
	
	for single_line in fileHandler:
		count += 1
		single_line = single_line.encode('ascii', 'ignore').decode('ascii')
		single_line = single_line.split(',')
		detectorid = int(single_line[0])
		starttime = None if not single_line[1] else datetime.strptime(single_line[1], "%Y-%m-%d %H:%M:%S-%f")
		volume = None if not single_line[2] else int(single_line[2])
		speed = None if not single_line[3] else int(single_line[3])
		occupancy = None if not single_line[4] else int(single_line[4])
		status = None if not single_line[5] else int(single_line[5])
		dqflags = None if not single_line[6] else int(single_line[6].strip())
		if detectorid in stationidinDetectors.keys():
			stationid = stationidinDetectors[detectorid]

		row = {
			'stationid':stationid,
			'detectorid':detectorid,
			'starttime': starttime,
			'volume':volume,
			'speed':speed,
			'occupancy':occupancy,
			'status':status,
			'dqflags':dqflags
		}
		result = db.loopdata.insert_one(row)
	fileHandler.close()
	print("completed")
	print('count', count)
	print('error', error)

def fetch_detectors(csvfile):
	stationidinDetectors = {}
	row_count = 0
	error = 0
	fileHandler = open(csvfile, "r") #, encoding="utf-8")
	for row in fileHandler:
		try:
			row_count += 1
			row = row.encode('ascii', 'ignore').decode('ascii')
			row = row.split(',')
			detectorid = int(row[0])
			stationid = int(row[6].strip())
			stationidinDetectors[detectorid] = stationid
		except:
			error += 1
	fileHandler.close()
	print('row count', row_count)
	print('error', error)
	return stationidinDetectors

if __name__ == "__main__":
	client = MongoClient("mongodb://34.82.18.173:27017")
	db=client.dataset
	serverstatus = db.command("serverStatus")
	stationidinDetectors = fetch_detectors('C:/Users/prane/Desktop/Likhitha/Fall_2020/Cloud and Cluster/CNC-Project/Project_Code/Data/freeway_detectors.csv')
	insert_loopdata(db, stationidinDetectors, 'C:/Users/prane/Desktop/Likhitha/Fall_2020/Cloud and Cluster/CNC-Project/Project_Code/Data/freeway_loopdata.csv')