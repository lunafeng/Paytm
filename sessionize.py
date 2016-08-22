from pyspark import SparkContext
from itertools import combinations
import dateutil.parser
import shlex
import ast

def userInfo(data):
	try:
		data_list = shlex.split(data)
		timestamp = dateutil.parser.parse(data_list[field_index["timestamp"]])
		clientip = data_list[field_index["client"]]
		url = data_list[field_index["request"]]
		url = "".join(url.split(" ")[1:-1])
		browser = data_list[field_index["user_agent"]]
		return (clientip + " " + browser, (timestamp, url))
	except:
		pass

def getSession(times, time_url):
	urls = []
	start_time = times[0]
	end_time = None
	urls.append(time_url[times[0]])
	index = 0
	if len(times) > 1:
		for i in range(len(times)-1):
			index = i
			t = times[i+1]	
			if float((t - start_time).seconds)/60 < 10:
				urls.append(time_url[t])
				end_time = t
			else:
				break
	if index == 0:
		if len(times) > 2:
			end_time = start_time
		else:
			if len(times) > 1:
				end_time = times[1]
			else:
				end_time = times[0]
			return (start_time, end_time, urls, times[index+2:])
	return (start_time, end_time, urls, times[index+1:])

def sessionInfo(data):
	client = data[0]
	requests = data[1]
	time_url = {}
	for re in requests:
		time = re[0]
		url = re[1]
		time_url[time] = url
	times = time_url.keys()
	times.sort()
	sessions = []
	index = 0
	while times != []:
		session_results = getSession(times, time_url)
		times = session_results[3]
		session = session_results[:3]
		sessions.append(session)
	return (client, sessions)

def timeCount(data):
	time_all = 0
	count = 0
	sessions = data[1]
	for session in sessions:
		start_time = session[0]
		end_time = session[1]
		time = (end_time - start_time).seconds
		time_all += time
		count += 1
	return (time_all, count)

def clientTime(data):
	time_all = 0
	sessions = data[1]
	for session in sessions:
		start_time = session[0]
		end_time = session[1]
		time = (end_time - start_time).seconds
		time_all += time
	return (client, time_all)

sc = SparkContext("local[16]")
log = sc.textFile("2015_07_22_mktplace_shop_web_log_sample.log")
global field_index
field_index = {"timestamp":0, "client":2, "request":11, "user_agent":12}
data = log.map(userInfo).filter(lambda x: x is not None)
data_group = data.groupByKey().mapValues(list)
client_session = data_group.map(sessionInfo)
print client_session.take(15)

with open("PageHits.csv", "w+") as f:
	f.write("visitor,session,hits,unique_hits\n")
	for data in client_session.toLocalIterator():
		client = str(data[0])
		sessions = data[1]
		count = 0
		for session in sessions:
			f.write(client + "," + str(count) + "," + str(len(session[2])) + "," + str(len(list(set(session[2])))) + "\n")
			count += 1
			f.flush()

session_time_rdd = client_session.map(timeCount)
session_time_dict = session_time_rdd.collectAsMap()
average_session_time = float(sum(session_time_dict.keys()))/sum(session_time_dict.values())
print "Average Session Time:", average_session_time

client_time = client_session.map(clientTime).max(key = lambda x: x[1])
print "Most Engaged User:", client_time


