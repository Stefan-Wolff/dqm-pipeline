import sys
import logging
import json
from pyspark.sql import SparkSession
from tasks.metric_complete import *

logging.basicConfig(format		=	"%(asctime)s %(levelname)s: %(message)s", 
					filename	=	__file__.split(".")[0] + ".log",
					level		=	logging.INFO)


def run():
	# init spark session
	spark = SparkSession	\
				.builder	\
				.getOrCreate()
	
	# load data
	df_persons = spark.read.json("output/persons.jsonl")
	df_works = spark.read.json("output/works_0.jsonl")
	
	
	# configure metrics
	metrics = [
			Completeness3()
		]
	
	
	# run metrics
	results = {}
	for m in metrics:
		logging.info("start " + m.getName() + " ..")
		results.update(m.calc(df_persons, df_works))

	print(results)
	
	# save results
	with open('repo/quality.jsonl', 'w') as outFile:
		json_string = json.dumps(results, indent=4)
		outFile.write(json_string)
	
	
	logging.info(".. analyzing done")


if "__main__" == __name__:
	try:
		run()
	except:
		logging.exception(sys.exc_info()[0])
