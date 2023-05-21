#
import os
import sys
import logging
import tarfile
import json
import gzip
import lib.xml_parse

logging.basicConfig(format		=	"%(asctime)s %(levelname)s: %(message)s", 
					filename	=	__file__.split(".")[0] + ".log",
					level		=	logging.INFO)

# url from https://orcid.figshare.com/articles/dataset/ORCID_Public_Data_File_2022/21220892
SOURCE_URL = "https://orcid.figshare.com/ndownloader/files/37635374"

TMP_DIR = "data/tmp/"
OUT_DIR = "data/ORCID_persons/"
RESULT_NUM = 32

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
def run():
	logging.info("start transforming persons ...")

	localName = SOURCE_URL.strip().split("/")[-1]
	fileName = localName + ".gz"

	os.system("mkdir -p " + OUT_DIR)
	#os.system("mkdir -p " + TMP_DIR)
	#os.system("wget -O " + TMP_DIR + fileName + " " + SOURCE_URL)

	count = 0
	tar = tarfile.open(TMP_DIR + fileName)
	parser = lib.xml_parse.Parser()
	
	member_num = 0
	for member in tar:
		member_num += 1
		
	max_per_file = int(member_num / RESULT_NUM) + 1
	current_members = 0
	file_index = 0
	outFile = None

	for member in tar:
		if member.isfile():
			if 0 == current_members:
				if outFile:
					outFile.close()
				
				current_outName = OUT_DIR + localName + "_" + str(file_index) + ".jsonl.gz"
				outFile = gzip.open(current_outName, 'wt', encoding='utf-8')
				file_index += 1
		
			handler = lib.xml_parse.XMLHandler(SEARCH_FOR)
			person = parser.parse(handler, tar.extractfile(member))
			
			member_fileName = member.name.split("/")[-1]
			person["orcid_id"] = member_fileName.split(".")[0]

			json.dump(person, outFile)
			outFile.write('\n')
			
			count += 1
			current_members += 1
			if current_members == max_per_file:
				current_members = 0

	outFile.close()
	
	#os.system("rm " + TMP_DIR + fileName)
	
	logging.info(".. " + str(count) + " persons transformed")


### entry
if "__main__" == __name__:
	try:
		run()
	except:
		logging.exception(sys.exc_info()[0])