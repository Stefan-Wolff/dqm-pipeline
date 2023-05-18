import os
import csv
import zipfile
import xml.etree.ElementTree as ET

# from https://www.gleif.org/en/lei-data/gleif-concatenated-file/download-the-concatenated-file
SOURCE_URL = "https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/31275/zip"
OUT_FILE = "data/LEI.csv"
TMP_DIR = "data/tmp/"
TMP_FILE = TMP_DIR + "LEI.zip"


def run():
	os.system("mkdir -p " + TMP_DIR)
	os.system("wget -O " + TMP_FILE + " " + SOURCE_URL)
		
			
	with zipfile.ZipFile(TMP_FILE, 'r') as zip_ref:
		with zip_ref.open(zip_ref.namelist()[0], 'r') as xmlFile:
			with open(OUT_FILE, 'w', encoding='utf-8') as csvOut:
				writer = csv.DictWriter(csvOut, fieldnames=["id", "name"])
				writer.writeheader()
				
				NAMESPACE = {"lei": "http://www.gleif.org/data/schema/leidata/2016"}
				tree = ET.parse(xmlFile)
				records_iter = tree.getroot().iterfind('./lei:LEIRecords/lei:LEIRecord', NAMESPACE)
				
				count = 0
				for record in records_iter:
					id = record.find('./lei:LEI', NAMESPACE).text
					name = record.find('./lei:Entity/lei:LegalName', NAMESPACE).text
				
					writer.writerow({'id': id, 'name': name})
					count += 1
				
				print(count, "lei records written to " + OUT_FILE)

	os.remove(TMP_FILE)

if "__main__" == __name__:
	run()