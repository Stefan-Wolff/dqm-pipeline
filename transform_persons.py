#
import os
import sys
import logging
import tarfile
import json
import lib.xml_parse

logging.basicConfig(format		=	"%(asctime)s %(levelname)s: %(message)s", 
					filename	=	__file__.split(".")[0] + ".log",
					level		=	logging.INFO)


SOURCE_URL = "https://orcid.figshare.com/ndownloader/files/37635374"			# 2022
OUT_DIR_RAW = "output/raw/"
OUT_FILE_TRANSFORMED = "output/persons.jsonl"

SEARCH_FOR = {
	"/record:record/person:person/person:name/personal-details:given-names": {"elementName": "firstName"},
	"/record:record/person:person/person:name/personal-details:family-name": {"elementName": "lastName"},
	"/record:record/person:person/person:name/personal-details:credit-name": {"elementName": "publishedName"},
	"/record:record/person:person/other-name:other-names/other-name:other-name/other-name:content": {"elementName": "otherNames"},
	"/record:record/person:person/address:addresses/address:address/address:country": {"elementName": "country"},
	
	"/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary/common:start-date/common:year": {
		"elementName": "startYear",
		"groupName": "affiliations",
		"groupPath": "/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary"
	},
	"/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary/common:end-date/common:year": {
		"elementName": "endYear",
		"groupName": "affiliations",
		"groupPath": "/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary"
	},
	"/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary/common:role-title": {
		"elementName": "role",
		"groupName": "affiliations",
		"groupPath": "/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary"
	},
	"/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary/common:department-name": {
		"elementName": "department",
		"groupName": "affiliations",
		"groupPath": "/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary"
	},
	"/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary/common:organization/common:name": {
		"elementName": "orgName",
		"groupName": "affiliations",
		"groupPath": "/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary"
	},
	"/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary/common:organization/common:address/common:city": {
		"elementName": "orgCity",
		"groupName": "affiliations",
		"groupPath": "/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary"
	},
	"/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary/common:organization/common:address/common:country": {
		"elementName": "orgCountry",
		"groupName": "affiliations",
		"groupPath": "/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary"
	},
	"/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary/common:organization/common:disambiguated-organization/common:disambiguated-organization-identifier": {
		"elementName": "orgID",
		"groupName": "affiliations",
		"groupPath": "/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary"
	},
	"/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary/common:organization/common:disambiguated-organization/common:disambiguation-source": {
		"elementName": "orgIDType",
		"groupName": "affiliations",
		"groupPath": "/record:record/activities:activities-summary/activities:employments/activities:affiliation-group/employment:employment-summary"
	}
}
		

### main
def run(toDownload):
	logging.info("start transforming persons ...")

	fileName = SOURCE_URL.strip().split("/")[-1] + ".gz"

	if toDownload:
		os.system("wget -O " + OUT_DIR_RAW + fileName + " " + SOURCE_URL)

	count = 0
	tar = tarfile.open(OUT_DIR_RAW + fileName)
	parser = lib.xml_parse.Parser()
	
	with open(OUT_FILE_TRANSFORMED, 'w', encoding='utf-8') as outFile:
		for member in tar:
			if member.isfile():				
				handler = lib.xml_parse.XMLHandler(SEARCH_FOR)
				person = parser.parse(handler, tar.extractfile(member))
				
				fileName = member.name.split("/")[-1]
				person["orcid_id"] = fileName.split(".")[0]

				json.dump(person, outFile)
				outFile.write('\n')
				
				count += 1
	
	logging.info(".. " + str(count) + " persons transformed")


### entry
if "__main__" == __name__:
	try:
		run(False)
	except:
		logging.exception(sys.exc_info()[0])