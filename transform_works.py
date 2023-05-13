#!/usr/bin/env python
#
# requires: pip install wget

import os
import sys
import logging
import json
import tarfile
import lib.xml_parse
from concurrent.futures import ProcessPoolExecutor


logging.basicConfig(format		=	"%(asctime)s %(levelname)s: %(message)s", 
					filename	=	__file__.split(".")[0] + ".log",
					level		=	logging.INFO)


SOURCE_FILES = "input/orcid_works_2022.lsv"
OUT_DIR_RAW = "output/raw/"
OUT_DIR = "output"

SEARCH_FOR = {
	"/work:work/work:title/common:title": {"elementName": "title"},
	"/work:work/work:title/common:subtitle": {"elementName": "subTitle"},
	"/work:work/work:journal-title": {"elementName": "journal_title"},
	"/work:work/work:short-description": {"elementName": "short_description"},
	"/work:work/work:citation/work:citation-type": {"elementName": None},									# read value, but not return
	"/work:work/work:citation/work:citation-value": {"elementName": "bibtex"},
	"/work:work/work:type": {"elementName": "type"},
	"publicationDate": {"elementName": "date"},																# aggregated publication year [, month, [,day]]
	"/work:work/common:publication-date/common:year": {"elementName": None},
	"/work:work/common:publication-date/common:month": {"elementName": None},
	"/work:work/common:publication-date/common:day": {"elementName": None},
	"/work:work/common:url": {"elementName": "url"},
	"/work:work/common:external-ids/common:external-id/common:external-id-type": {
		"elementName": "type",
		"groupName": "publicationID",
		"groupPath": "/work:work/common:external-ids/common:external-id"
	},
	"/work:work/common:external-ids/common:external-id/common:external-id-value": {
		"elementName": "id",
		"groupName": "publicationID",
		"groupPath": "/work:work/common:external-ids/common:external-id"
	},
	"/work:work/common:external-ids/common:external-id/common:external-id-normalized": {
		"elementName": None,
		"groupName": "publicationID",
		"groupPath": "/work:work/common:external-ids/common:external-id"
	},
	"/work:work/work:contributors/work:contributor/work:credit-name": {
		"elementName": "fullName",
		"groupName": "authors",
		"groupPath": "/work:work/work:contributors/work:contributor"
	},
	"/work:work/work:contributors/work:contributor/work:contributor-attributes/work:contributor-role": {
		"elementName": None,
		"groupName": "authors",
		"groupPath": "/work:work/work:contributors/work:contributor"
	},
	"/work:work/work:contributors/work:contributor/common:contributor-orcid/common:path": {
		"elementName": "orcid_id",
		"groupName": "authors",
		"groupPath": "/work:work/work:contributors/work:contributor"
	}
}


### classes
class PublicationHandler(lib.xml_parse.XMLHandler):		
	
	AUTHOR_ROLES = ["author", "http://credit.niso.org/contributor-roles/writing-original-draft/"]
	
	def endElement(self, tag):
		# take available parts of publication date
		if "/work:work/common:publication-date" == self.curPath and \
			"/work:work/common:publication-date/common:year" in self.data:
				date = self.data["/work:work/common:publication-date/common:year"]
				del self.data["/work:work/common:publication-date/common:year"]
				
				if "/work:work/common:publication-date/common:month" in self.data:
					date+= "-" + self.data["/work:work/common:publication-date/common:month"]
					del self.data["/work:work/common:publication-date/common:month"]
		
					if "/work:work/common:publication-date/common:day" in self.data:
						date+= "-" + self.data["/work:work/common:publication-date/common:day"]
						del self.data["/work:work/common:publication-date/common:day"]
					
				self.data["publicationDate"] = date
		
	
		# take only citaitons of type 'bibtex' or without type
		if "/work:work/work:citation" == self.curPath and \
			"/work:work/work:citation/work:citation-type" in self.data and \
			"/work:work/work:citation/work:citation-value" in self.data:
			if "bibtex" != self.data["/work:work/work:citation/work:citation-type"]:
				del self.data["/work:work/work:citation/work:citation-value"]
				del self.data["/work:work/work:citation/work:citation-type"]
		
		
		# authors
		if "/work:work/work:contributors/work:contributor" == self.curPath:
			# filter author tags with missing name
			if not "/work:work/work:contributors/work:contributor/work:credit-name" in self.data:
				if "/work:work/work:contributors/work:contributor/common:contributor-orcid/common:path" in self.data:
					del self.data["/work:work/work:contributors/work:contributor/common:contributor-orcid/common:path"]
				if "/work:work/work:contributors/work:contributor/work:contributor-attributes/work:contributor-role" in self.data:
					del self.data["/work:work/work:contributors/work:contributor/work:contributor-attributes/work:contributor-role"]
		
			# take only contributors of role 'author' or without role
			if "/work:work/work:contributors/work:contributor/work:contributor-attributes/work:contributor-role" in self.data:
				if not self.data["/work:work/work:contributors/work:contributor/work:contributor-attributes/work:contributor-role"] in PublicationHandler.AUTHOR_ROLES:
					del self.data["/work:work/work:contributors/work:contributor/work:credit-name"]
					del self.data["/work:work/work:contributors/work:contributor/work:contributor-attributes/work:contributor-role"]
					if "/work:work/work:contributors/work:contributor/common:contributor-orcid/common:path" in self.data:
						del self.data["/work:work/work:contributors/work:contributor/common:contributor-orcid/common:path"]
				
		
		# take normalized id if availabale
		if "/work:work/common:external-ids/common:external-id" == self.curPath and \
			"/work:work/common:external-ids/common:external-id/common:external-id-normalized" in self.data:
			self.data["/work:work/common:external-ids/common:external-id/common:external-id-value"] = self.data["/work:work/common:external-ids/common:external-id/common:external-id-normalized"]	
			del self.data["/work:work/common:external-ids/common:external-id/common:external-id-normalized"]
	
	
		super().endElement(tag)
			
			
### functions
def process_source(url, toDownload):
	fileName_local = url.strip().split("/")[-1]
	fileName = fileName_local + ".gz"
	
	count = 0
	
	logging.info("\t process source: " + fileName)
	
	parser = lib.xml_parse.Parser()
	
	if toDownload:
		os.system("wget -O " + OUT_DIR_RAW + fileName + " " + url)
	
	with open(OUT_DIR + "/works_"+fileName_local+".jsonl", 'w', encoding='utf-8') as outFile:
		with tarfile.open(OUT_DIR_RAW + fileName, 'r:gz') as tar:
			for member in tar:
				if "_works_" in member.name:
					handler = PublicationHandler(SEARCH_FOR)
					publ = parser.parse(handler, tar.extractfile(member))
					
					fileName = member.name.split("/")[-1]
					publ["orcid_publication_id"] = fileName.split(".")[0]
					publ["orcid_id"] = fileName.split("_")[0]
					
					json.dump(publ, outFile)
					outFile.write('\n')
						
					count += 1
					
	logging.info("\t .. source " + fileName + " done: " + str(count) + " records")
					
	return count


### main
def run(toDownload):
	logging.info("start transforming publications ..")
	
	count = 0
	with open(SOURCE_FILES, 'r', newline='') as inFile:
		for url in inFile:
			count += process_source(url, toDownload)

	
	logging.info(str(count) + " publications transformed")
	

### entry
if "__main__" == __name__:
	try:
		run(True)
	except:
		logging.exception(sys.exc_info()[0])