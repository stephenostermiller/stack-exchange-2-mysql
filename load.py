#!/usr/bin/python3

from lib import db
from lib import xmlline
import sys
import re
import subprocess
import requests

def string2Bool(s):
	return s.lower() in ['true', '1', 't', 'y', 'yes']

def mungeBadges(data, context):
	addSiteId(data, context)
	data['TagBased'] = string2Bool(data['TagBased'])

def mungeTags(data, context):
	addSiteId(data, context)
	if 'IsModeratorOnly' in data:
		data['IsModeratorOnly'] = string2Bool(data['IsModeratorOnly'])
	if 'IsRequired' in data:
		data['IsRequired'] = string2Bool(data['IsRequired'])

def addSiteId(data, context):
	data['SiteId'] = context['site']['Id']

tables={
	"sites":{
		"name": "Sites"
	},
	"badges":{
		"name": "Badges",
		"munge": mungeBadges
	},
	"comments":{
		"name": "Comments",
		"munge": addSiteId
	},
	"posthistory":{
		"name": "PostHistory",
		"munge": addSiteId
	},
	"postlinks":{
		"name": "PostLinks",
		"munge": addSiteId
	},
	"posts":{
		"name": "Posts",
		"munge": addSiteId
	},
	"tags":{
		"name": "Tags",
		"munge": mungeTags
	},
	"users":{
		"name": "Users",
		"munge": addSiteId
	},
	"votes":{
		"name": "Votes",
		"munge": addSiteId
	}
}

def fileNameToUrl(file):
	return 'https://' + re.sub(r'-.*','',re.sub(r'(\.(xml|7z))+','',re.sub(r'.*/','',file)))

def lastSummary(context, last):
	lastSummary = "Last:"
	if context["site"] and 'TinyName' in context['site']:
		lastSummary = lastSummary + " " +  context['site']['TinyName']
	if context[last]:
		lastSummary = lastSummary + " " +  context[last]['table']['name']
		data = context[last]['data']
		if 'Id' in data:
			lastSummary = lastSummary + " " +  data['Id']
		if 'CreationDate' in data:
			lastSummary = lastSummary + " " + data['CreationDate']
		elif 'Date' in data:
			lastSummary = lastSummary + " " + data['Date']
	return lastSummary

def logSkipped(context):
	print("Skipped " + str(context['skipped']) + " records. " + lastSummary(context, "lastSkipped"))
	context['skipped'] = 0

def logAndCommit(context):
	print("Committing " + str(context['count']) + " records. " + lastSummary(context, "lastDone"))
	db.cnx.commit()
	context['count'] = 0

def processResume(context, data):
	if 'Id' in data:
		resume = str(data['Id'])
		if context['resume'] == resume:
			context['resume'] = None
		if context['table']:
			resume = context['table']['name'].lower() + " " + resume	
		if context['resume'] == resume:
			context['resume'] = None
		if context['site']:
			resume = context['site']['TinyName'].lower() + " " + resume
		if context['resume'] == resume:
			context['resume'] = None
	return True

def loadLine(line, context):
	line = line.strip()
	if (not line or re.search(r'^.?<\?xml', line, re.IGNORECASE)):
		pass
	elif (re.match(r'^\s*</', line)):
		if "onLoad" in context['table']:
			context['table']["onLoad"]()
		context['table'] = None
	elif (m := re.match(r'^\s*<([a-z]+)>', line)):
		if m[1] in tables:
			context['table']=tables[m[1]]
		else:
			raise Exception ("Unknown table: " + m[1])
	elif (re.match(r'^\s*<row ', line)):
		if (not context['table']):
			raise Exception ("Row found with no table")
		data = xmlline.getAttributes(line)
		if ('munge' in context['table']):
			context['table']['munge'](data, context)
		skipTable = len(context['tableSet']) != 0 and context['table']['name'].lower() not in context['tableSet']
		skipResume = context['resume'] and processResume(context, data)
		if skipTable or skipResume:
			context['skipped'] = context['skipped'] + 1
			context['lastSkipped'] = {
				"data": data,
				"table": context['table']
			}
		else:
			db.upsert(context['table']['name'], data)
			context['count'] = context['count'] + 1
			context['lastDone'] = {
				"data": data,
				"table": context['table']
			}
		if (context['count'] >= 2048):
			logAndCommit(context)
		elif (context['skipped'] >= 10000):
			logSkipped(context)
	else:
		raise Exception ("Unknown line: `" + line + "`")

def getDefaultContext():
	return {
		"count": 0,
		"table": None,
		"site": None,
		"lastDone": None,
		"lastSkipped": None,
		"tableSet": {},
		"skipped": 0,
		"resume": None
	}

def setSiteContext(context, fileName):
	if (re.match(r'^(.*/)?sites\.[a-zA-Z0-9]+$', fileName, re.IGNORECASE)):
		context['site'] = None
	else:
		context['site'] = db.querySite(fileNameToUrl(fileName))
		assert context['site']['Id'], "Could not find site id for " + fileName + "(Load Sites.xml first.)"

def loadXml(context, fileName):
	print("Loading XML: " + fileName)
	setSiteContext(context, fileName)
	fh = open(fileName, mode="r", encoding="utf-8")
	try:
		while line := fh.readline():
			try:
				loadLine(line.strip(), context)
			except:
				print(line)
				raise
	finally:
		fh.close()

def loadXmlUrl(context, url):
	print("Loading XML: " + url)
	setSiteContext(context, url)
	req = requests.get(url, stream=True)

	if req.encoding is None:
		req.encoding = 'utf-8'

	try:
		for line in req.iter_lines(decode_unicode=True):
			try:
				loadLine(line.strip(), context)
			except:
				print(line)
				raise
	finally:
		req.close()

def loadXml7z(context, fileName):
	print("Loading zipped XML: " + fileName)
	setSiteContext(context, fileName)
	process = subprocess.Popen(["7z", "e", "-so", "-bd", fileName], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
	try:
		while line := process.stdout.readline():
			try:
				loadLine(line.decode('utf-8').strip(), context)
			except:
				print(line)
				raise
	finally:
		process.stdout.close()


def loadXml7zUrl(context, url):
	print("Loading zipped XML: " + url)
	setSiteContext(context, url)
	process = subprocess.Popen(["7z", "e", "-so", "-bd", fileName], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
	try:
		while line := process.stdout.readline():
			try:
				loadLine(line.decode('utf-8').strip(), context)
			except:
				print(line)
				raise
	finally:
		process.stdout.close()


context = getDefaultContext()
try:
	db.createSchema()
	files = []
	for arg in sys.argv[1:]:
		if (re.match(r'^.*\.(xml|7z)$', arg)):
			files.append(arg)
		elif (arg.lower() in tables):
			context['tableSet'][arg.lower()] = 1
		elif (re.match(r'^(([a-zA-Z]+ )?([a-zA-Z]+ )?[0-9]+)$', arg)):
			context['resume'] = arg.lower()
		else:
			raise Exception("Unknown argument: " + arg)
	for file in files:
		if (re.match(r'^.*\.xml$', file)):
			if (re.match(r'https?\:\/\/', file)):
				loadXmlUrl(context, file)
			else:
				loadXml(context, file)
		elif (re.match(r'^.*\.7z$', file)):
			loadXml7z(context, file)
finally:
	if (context['count'] > 0):
		logAndCommit(context)
	if (context['skipped'] > 0):
		logSkipped(context)
	db.cnx.close()
