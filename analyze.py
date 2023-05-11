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
				.appName('ORCID')	\
				.getOrCreate()
	
	# load data
	df_persons = spark.read.json("output/persons.jsonl")
	
	
	# configure metrics
	metrics = {
		"Completeness": [
			Completeness1()
		]
	}
	
	# run metrics
	result = {}
	for dimension, metrics in metrics.items():
		indicator = 0
		for m in metrics:
			indicator += m.calc(df_persons, df_persons)
			
		result[dimension] = indicator / len(metrics)
	
	
	# save results
	with open('repo/quality.json', 'w') as outFile:
		json.dump(result, outFile)
	
	
	logging.info(".. analyzing done")


if "__main__" == __name__:
	try:
		run()
	except:
		logging.exception(sys.exc_info()[0])
