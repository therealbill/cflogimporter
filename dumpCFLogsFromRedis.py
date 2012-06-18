#!/usr/bin/python

import os
import os.path
import redis
import datetime
import syslog
import urllib
try:
	import json
except ImportError:
	import simplejson as json
from cStringIO import StringIO

RDS_HOST = ""
RDS_DB = 0


logit = syslog.syslog

syslog.openlog("dumpCFLogsFromRedis")

def getAvailableLogs(includeToday=False):
	logit("Getting available Logs")
	available_logs =  rds.keys("aws:cflogs:*")
	if not includeToday:
		today = datetime.datetime.today().strftime("%Y-%m-%d")
		print today
		try:
			logit("Removing today (%s) from log list" % today)
			available_logs.remove(today)
		except ValueError:
			logit("%s not in list" % today)
	logit("Found %d available logs" % len(available_logs))
	return available_logs


def dumpLog(logdata,dstring):
	logs = StringIO()
	logdate=dstring
	logit("Generating logfile for %s" %dstring)
	for logentry in logdata:
		entry = json.loads(logentry)
		try:
			print >>logs, "%(humanstamp)s\t%(timestamp)s\t%(cs_Host)s\t%(x-edge-location)s\taws.cloudfront\t%(c-ip)s\t%(cs-method)s\t-\t%(sc-status)s\t%(sc-bytes)s\t%(cs-uri-stem)s\t%(cs-uri-query)s\t%(cs_Referer)s\t%(cs_User-Agent)s" % entry
		except UnicodeEncodeError:
			logit("BADENTRY| %s" %logentry.encode('utf-8'))
			entry['cs_User-Agent'] = entry['cs_User-Agent'].encode('ascii','ignore')
			rec =  "%(humanstamp)s\t%(timestamp)s\t%(cs_Host)s\t%(x-edge-location)s\taws.cloudfront\t%(c-ip)s\t%(cs-method)s\t-\t%(sc-status)s\t%(sc-bytes)s\t%(cs-uri-stem)s\t%(cs-uri-query)s\t%(cs_Referer)s\t%(cs_User-Agent)s" % entry
			print >>logs, rec.encode('utf-16')
	dmpname = "/var/log/%s/cloudfront/cf-access.log" %  logdate
	logs.seek(0)
	if not os.path.exists("/var/log/%s/cloudfront" % logdate):
		os.mkdir("/var/log/%s/cloudfront" % logdate)
		logit("Dumping log data to file %s" % dmpname)
		print >>open(dmpname,'w'), logs.read()
		logit("Finished dumping to %s" % dmpname)
	else:
		logit("Skipping existing file")





logit("Connecting to redis. RDS_HOST=%s, RDS_DB=%s" %(RDS_HOST, RDS_DB) )
rds = redis.Redis(RDS_HOST,db=RDS_DB)
logit("Connected")

logs = getAvailableLogs()
logs.sort()

if logs:
	for log in logs:
		dstring = log.split(":")[2]
		if not os.path.exists("/var/log/%s/cloudfront" % dstring):
			logit("Retrieving records for %s" % dstring)
			logdata = rds.zrange(log,0,-1)
			dumpLog(logdata,dstring)
		else:
			logit("Skipping existing log file")

