import os
import gzip
import json
import logging
import sys
import multiprocessing

logging.basicConfig(format		=	"%(asctime)s %(levelname)s: %(message)s", 
					filename	=	__file__.split(".")[0] + ".log",
					level		=	logging.INFO)

CORSSREF_TORRENT = "data/input/April2022Crossref.torrent"
OUT_DIR = "data/input/CrossRef/"
TMP_DIR = "data/tmp/CrossRef/"
RESULT_NUM = 32


def formate_authors(authors, doi):
	result = []
	
	i = 0
	for author in authors:
		result_entry = {}

		if "given" in author:
			result_entry["firstName"] = author["given"]

		if "family" in author:
			result_entry["lastName"] = author["family"]
		
		if "ORCID" in author:
			result_entry["id"] = author["ORCID"].split("/")[-1]
		else:
			result_entry["id"] = doi + "_" + str(i)
				
		if result_entry:
			result.append(result_entry)
			
		i += 1
			
	return result


def fetch_data(record):
	result = {
		"doi": record["DOI"],
		"type": record["type"]
	}
	
	if "title" in record:
		result["title"] = record["title"][0]
	
	if "abstract" in record:
		result["abstract"] = record["abstract"]
	
	if "author" in record:
		formatted_authors = formate_authors(record["author"], result["doi"])
		if formatted_authors:
			result["authors"] = formatted_authors
	
	if "subtitle" in record:
		result["subTitle"] = record["subtitle"][0]
	
	if "ISBN" in record:
		result["isbn"] = record["ISBN"][0]
	
	if "ISSN" in record:
		result["issn"] = record["ISSN"][0]
		
	if "URL" in record:
		result["url"] = record["URL"][0]

	if "published" in record and "date-parts" in record["published"] and record["published"]["date-parts"]:
		result["date"] = "-".join(str(e) for e in record["published"]["date-parts"][0])

	
	return result
	

def extract_data(data):
	outFile = OUT_DIR + str(data["index"]) + ".json.gz"
	logging.info("start " + outFile)
	
	with gzip.open(outFile, 'wt', encoding='utf-8') as outZip:
		for source in data["sources"]:
			with gzip.open(source, 'r') as file:
				records = json.load(file)
				for record in records["items"]:
					json.dump(fetch_data(record), outZip)
					outZip.write('\n')

	logging.info(outFile, " done")
	

def run():
	# harvest data
	os.system("mkdir -p " + OUT_DIR)
	os.system("mkdir -p " + TMP_DIR)
	os.system("aria2c " + CORSSREF_TORRENT + " --seed-time=0 -d " + TMP_DIR)
	
	# organize source files
	for entry in os.listdir(TMP_DIR):
		if os.path.isdir(TMP_DIR + entry):
			source_files = os.listdir(TMP_DIR + entry)
			sources_per_proc = int(len(source_files) / RESULT_NUM) + 1
			
			ordered_sources = []
			curr_data = None
			index = 0
			i = 0
			for source in source_files:
				if 0 == i:
					curr_data = {"index": index, "sources": []}
					ordered_sources.append(curr_data)
					index += 1

				curr_data["sources"].append(TMP_DIR + entry + "/" + source)
				i += 1
				
				if i == sources_per_proc:
					i = 0
					
					
	# extract data in multiple threads
	with multiprocessing.Pool(RESULT_NUM) as pool:
		pool.map(func=extract_data, iterable=ordered_sources)


	# cleanup temp dir
	os.system("rm -f -r " + TMP_DIR)
	
	print(".. finished")


if "__main__" == __name__:
	try:
		run()
	except:
		logging.exception(sys.exc_info()[0])