#!/usr/bin/env python
import boto
import datetime
import getopt
import sys, os
import gzip
import urllib
import syslog
import redis
import time

try:
	import json
except ImportError:
	import simplejson as json

"""
Download and transform log files for AWS S3 / CloudFront

Usage: python CloutFrontLogPuller.py [options]

Options:
  -b ..., --bucket=...	  AWS Bucket
  -p ..., --prefix=...	  AWS Key Prefix
  -a ..., --access=...	  AWS Access Key ID
  -s ..., --secret=...	  AWS Secret Access Key
  -h, --help			  Show this help
  -d					  Show debugging information while parsing

Examples:
  get-aws-logs.py -b eqxlogs
  get-aws-logs.py --bucket=eqxlogs
  get-aws-logs.py -p logs/cdn.example.com/
  get-aws-logs.py --prefix=logs/cdn.example.com/


This script will retreive the given logs, and convert them to a structure and field
sequence  that can also be configured in apache. It is a tab delineated format.

This program requires the boto module for Python to be installed.
Based on work by  "Johan Steen (http://www.artstorm.net/)"
"""



RDS_HOST=""
RDS_DB = 0

_debug = 0
class GzipConsumer:

	def __init__(self, consumer):
		self.__consumer = consumer
		self.__decoder = None
		self.__data = ""

	def __getattr__(self, key):
		# delegate unknown methods/attributes
		return getattr(self.__consumer, key)

	def feed(self, data):
		if self.__decoder is None:
			# check if we have a full gzip header
			data = self.__data + data
			try:
				i = 10
				flag = ord(data[3])
				if flag & 4: # extra
					x = ord(data[i]) + 256*ord(data[i+1])
					i = i + 2 + x
				if flag & 8: # filename
					while ord(data[i]):
						i = i + 1
					i = i + 1
				if flag & 16: # comment
					while ord(data[i]):
						i = i + 1
					i = i + 1
				if flag & 2: # crc
					i = i + 2
				if len(data) < i:
					raise IndexError("not enough data")
				if data[:3] != "\x1f\x8b\x08":
					raise IOError("invalid gzip data")
				data = data[i:]
			except IndexError:
				self.__data = data
				return # need more data
			import zlib
			self.__data = ""
			self.__decoder = zlib.decompressobj(-zlib.MAX_WBITS)
		data = self.__decoder.decompress(data)
		if data:
			self.__consumer.feed(data)

	def close(self):
		if self.__decoder:
			data = self.__decoder.flush()
			if data:
				self.__consumer.feed(data)
		self.__consumer.close()


class stupid_gzip_consumer:
	 def __init__(self): self.data = []
	 def feed(self, data): self.data.append(data)
	 def close(self): return

def gunzip(data):
	c = stupid_gzip_consumer()
	gzc = GzipConsumer(c)
	gzc.feed(data)
	gzc.close()
	return "".join(c.data)


AggLogData={}

class CFlogConnector:
	"""Download log files from the specified bucket and path and then delete them from the bucket.
	Uses: http://boto.s3.amazonaws.com/index.html
	"""
	# Set default values
	AWS_BUCKET_NAME = ''
	AWS_KEY_PREFIX = ''
	AWS_ACCESS_KEY_ID = ''
	AWS_SECRET_ACCESS_KEY = ''
	LOCAL_PATH = 'logs/'
	# Don't change below here
	s3_conn = None
	bucket_list = None

	def __init__(self):
		s3_conn = None
		bucket_list = None
		self.r = redis.Redis(RDS_HOST,db=RDS_DB)
		self.pipe = self.r.pipeline()

	def addEntryToRedis(self,datestring,entrydict):
		basekey = "aws:cflogs:%s" % datestring
		self.pipe.zadd(basekey, json.dumps(entrydict),  entrydict['timestamp'])


	def start(self):
		"""Connect, get file list, copy and delete the logs"""
		self.s3Connect()
		self.getList()
		self.copyFiles(deleteAfter=True)

	def s3Connect(self):
		"""Creates a S3 Connection Object"""
		self.s3_conn = boto.connect_s3(self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY)

	def getList(self):
		"""Connects to the bucket and then gets a list of all keys available with the chosen prefix"""
		syslog.syslog("Obtaining List of available logs for bucket '%s'" % self.AWS_BUCKET_NAME)
		s = time.time()
		bucket = self.s3_conn.get_bucket(self.AWS_BUCKET_NAME)
		self.bucket_list = bucket.list(self.AWS_KEY_PREFIX)
		elapsed = time.time() - s
		syslog.syslog("Got List in %0.2f seconds" % elapsed)

	def copyFiles(self,deleteAfter=False):
		"""Creates a local folder if not already exists and then download all keys and deletes them from the bucket"""
		fields = []
		lastDate = False
		syslog.syslog("Iterating over detected log files in bucket '%s'" % self.AWS_BUCKET_NAME)
		for key_list in self.bucket_list:
			key = str(key_list.key)
			filename = key.split('/')[-1]
			fdata = filename.split(".",3)
			dstring,hour = fdata[1].rsplit("-",1)
			if not lastDate:
				lastDate = dstring
			else:
				if lastDate != dstring:
					syslog.syslog("Completed entries for  date:%s" % lastDate)
					syslog.syslog("%d entries for %s " % (self.r.zcard('aws:cflogs:%s' %lastDate), lastDate) )
					syslog.syslog("Working on new date:%s" % dstring)
			lastDate = dstring
			yy,MM,dd = [int(x) for x in dstring.split("-")]
			data = key_list.get_contents_as_string()
			unzipped_data = gunzip(data)
			for line in unzipped_data.splitlines():
				ldata = line.split("\t")
				if len(ldata) > 1:
					edate = ldata.pop(0)
					etime = ldata.pop(0)
					hh,mm,ss = [int(x) for x in etime.split(":")]
					et = datetime.datetime(yy,MM,dd,hh,mm,ss)
					ldata.insert(0,et.strftime("%s"))
					ldata.insert(0,"%d-%02d-%02dT%02d:%02d:%02d-0000" %(yy,MM,dd,hh,mm,ss))
					keyed =  dict(zip(fields,ldata))
					keyed['sc-status'] = int(keyed['sc-status'])
					keyed['timestamp'] = int(keyed['timestamp'])
					keyed['sc-bytes'] = int(keyed['sc-bytes'])
					keyed['cs_User-Agent'] = urllib.unquote_plus(keyed['cs_User-Agent'])
					self.addEntryToRedis(dstring,keyed)
				else:
					if ldata[0].startswith("#Fields"):
						if not fields:
							fields = line.split()[1:]
							fields[0] = "humantstamp"
							fields[1] = "timestamp"
							fields = [x.replace("(","_").replace(")","") for x in fields]
			self.pipe.execute()
			if deleteAfter:
				syslog.syslog("%s processed, deleting original" % filename)
				key_list.delete()

def usage():
	print __doc__

def main(argv):
	try:
		opts, args = getopt.getopt(argv, "hb:p:l:a:s:d", ["help", "bucket=", "prefix=", "local=", "access=", "secret="])
	except getopt.GetoptError:
		usage()
		sys.exit(2)
	syslog.openlog("CloudFrontLogPuller")
	syslog.syslog("Initializing")
	logs = CFlogConnector()
	if logs:
		syslog.syslog("Connection to Amazon S3 complete")
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
			sys.exit()
		elif opt == '-d':
			global _debug
			_debug = 1
		elif opt in ("-b", "--bucket"):
			logs.AWS_BUCKET_NAME = arg
		elif opt in ("-p", "--prefix"):
			logs.AWS_KEY_PREFIX = arg
		elif opt in ("-a", "--access"):
			logs.AWS_ACCESS_KEY_ID = arg
		elif opt in ("-s", "--secret"):
			logs.AWS_SECRET_ACCESS_KEY = arg
	logs.start()

if __name__ == "__main__":
	main(sys.argv[1:])
