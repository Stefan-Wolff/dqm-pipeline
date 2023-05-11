#
import os
import logging


### main
def run(sourceURLs, outDir):
	logging.info("start harvest to " + outDir + " ..")

	if not os.path.exists(outDir):
		os.makedirs(outDir)

    
	with open(sourceURLs, 'r', newline='') as inFile:
		for url in inFile:
			fileName = url.strip().split("/")[-1] + ".gz"
			os.system("wget -O " + fileName + " -P tmp " + url)
    
    
	logging.info(".. harvesting done")