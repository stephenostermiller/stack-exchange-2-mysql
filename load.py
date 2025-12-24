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
	# Extract site name from file path
	basename = re.sub(r'.*/','',file)

	# Handle sites.xml file
	if re.match(r'sites\.', basename, re.IGNORECASE):
		return 'https://stackoverflow.com'  # Default main site

	# Handle dba.stackexchange.com.7z format
	site_match = re.match(r'^([a-zA-Z0-9-]+)\.(stackexchange\.com)(\.(xml|7z))?$', basename)
	if site_match:
		site_name = site_match.group(1)
		return f'https://{site_name}.stackexchange.com'

	# Handle standalone Badges.xml format - check if there's a corresponding site
	# This assumes if we're processing a file without a site name, it should have been loaded with site context
	if '.' in basename and basename.split('.')[0] not in ['sites']:
		# For generic files like Badges.xml, we need to rely on the file context
		# This case should be handled by the setSiteContext function
		pass

	# Fallback - try to extract any alphanumeric prefix
	match = re.match(r'^([a-zA-Z0-9-]+)', basename)
	if match:
		return f'https://{match.group(1)}.stackexchange.com'

	# Ultimate fallback
	return 'https://stackoverflow.com'

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
	return True

def loadLine(line, context):
	line = line.strip()
	if (not line):
		pass
	elif (re.search(r'^.?<\?xml', line, re.IGNORECASE)):
		pass
	elif (re.match(r'^\s*<!--', line)):
		pass
	elif (re.match(r'^\s*-->', line)):
		pass
	elif (re.match(r'^\s*[a-zA-Z]+$', line)):
		pass
	elif (re.search(r'^\s*https?://', line)):
		pass
	elif (re.match(r'^\s*CC BY-SA\s*-\s*(Url:|Version:)?', line)):  # Only match CC BY-SA license lines at start
		pass
	elif (re.search(r'Url:', line)):
		pass
	elif (not re.match(r'^\s*<', line)):
		pass
	elif (re.search(r'We also provide data for non-beta sites', line)):
		pass
	elif (re.search(r'These files are available for download', line)):
		pass
	elif (re.search(r'Data dumps are available', line)):
		pass
	elif (re.search(r'\.beta\.', line, re.IGNORECASE)):
		pass
	else:
		if context['verbose']:
			print(f"Processing line: {line[:50]}...")
		if (re.match(r'^\s*</', line)):
			if "onLoad" in context['table']:
				context['table']["onLoad"]()
			context['table'] = None
		elif (m := re.match(r'^\s*<([a-z]+)>', line)):
			# Skip HTML formatting tags
			if m[1] in ['sub', 'sup', 'code', 'pre', 'em', 'strong', 'p', 'br', 'div', 'span', 'ul', 'ol', 'li']:
				pass
			elif m[1] in tables:
				if context['verbose']:
					print(f"Found table tag: {m[1]}")
				context['table']=tables[m[1]]
				if context['verbose']:
					print(f"Set table to: {context['table']}")
			else:
				raise Exception ("Unknown table: " + m[1])
		elif (re.match(r'^\s*<row ', line)):
			if (not context['table']):
				raise Exception ("Row found with no table")
			data = xmlline.getAttributes(line)
			if context['verbose']:
				print(f"Row data: {len(data)} attributes")
			if ('munge' in context['table']):
				context['table']['munge'](data, context)
			if (len(context['tableSet']) != 0 and context['table']['name'].lower() not in context['tableSet']):
				context['skipped'] = context['skipped'] + 1
			else:
				db.upsert(context['table']['name'], data)
				context['count'] = context['count'] + 1
			if (context['count'] >= 2048):
				logAndCommit(context)
			elif (context['skipped'] >= 10000):
				logSkipped(context)
		else:
			raise Exception ("Unknown line: `" + line + "`")

def setSiteContext(context, fileName):
	# If we already have a site context (e.g., from processing a 7z file), reuse it
	if context.get('site') and context['site'].get('Id'):
		return

	if (re.match(r'^(.*/)?sites\.[a-zA-Z0-9]+$', fileName, re.IGNORECASE)):
		context['site'] = None
	else:
		context['site'] = db.querySite(fileNameToUrl(fileName))
		assert context['site']['Id'], "Could not find site id for " + fileName + "(Load Sites.xml first.)"

def loadXml(context, fileName):
	print("Loading XML: " + fileName)
	setSiteContext(context, fileName)
	fh = open(fileName, mode="r", encoding="utf-8")
	line_count = 0
	try:
		while line := fh.readline():
			line_count += 1
			if line_count <= 10:  # Debug first 10 lines
				print(f"Line {line_count}: {line.strip()}")
			try:
				loadLine(line.strip(), context)
			except Exception as e:
				print(f"Error on line {line_count}: {e}")
				print(f"Line content: {line}")
				raise
	finally:
		fh.close()
	print(f"Total lines processed: {line_count}")

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
	process = subprocess.Popen(["7z", "e", "-so", "-bd", url], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
	try:
		while line := process.stdout.readline():
			try:
				loadLine(line.decode('utf-8').strip(), context)
			except:
				print(line)
				raise
	finally:
		process.stdout.close()


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

context = getDefaultContext()
context['verbose'] = False  # Default to non-verbose mode
try:
	db.createSchema()
	files = []
	for arg in sys.argv[1:]:
		if (re.match(r'^.*\.(xml|7z)$', arg)):
			files.append(arg)
		elif (arg.lower() in tables):
			context['tableSet'][arg.lower()] = 1
		elif (re.match(r'^-v|--verbose$', arg, re.IGNORECASE)):
			context['verbose'] = True
		elif (re.match(r'^-q|--quiet$', arg, re.IGNORECASE)):
			context['verbose'] = False
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