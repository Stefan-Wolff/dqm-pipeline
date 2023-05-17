import os
import csv
import zipfile
import xml.etree.ElementTree as ET


SOURCE_URL = "https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/31275/zip"
OUT_DIR = "input/"
TMP_DIR = "tmp/"
LEI_FILE = "lei.csv"
TMP_ZIP = "tmp_lei.zip"


def run(toDownload):
	os.system("wget -O " + TMP_DIR + TMP_ZIP + " " + SOURCE_URL)
		
	with zipfile.ZipFile(TMP_DIR + TMP_ZIP, 'r') as zip_ref:
		with zip_ref.open(zip_ref.namelist()[0], 'r') as xmlFile:
			tree = ET.parse(xmlFile)
			root = tree.getroot()
		
			for child in root:
				print(child.tag, child.attrib)
		#	if member.endswith(".csv"):
		#		zip_ref.extract(member, OUT_DIR)
		#		os.rename(OUT_DIR + member, OUT_DIR + ROR_FILE)
		#		print(OUT_DIR + ROR_FILE, " loaded")			
		#		break

	#os.remove(OUT_DIR + fileName)

if "__main__" == __name__:
	run(True)