import argparse
from pyspark.sql import SparkSession
import json


OUT_PATH = "data/parquets/initial/"
SCHEMES_PATH = "data/schemes/"
SOURCES_CONFIG = "repo/sources.json"


def run(config):
	"""Reads the source data of a specified entity, and saves the data in parquet format."""

	# init spark session
	spark = SparkSession	\
				.builder	\
				.config('spark.driver.memory', '32G')	\
				.getOrCreate()


	# read source config
	with open(SOURCES_CONFIG, 'r', encoding='utf-8') as inFile:
		sources = json.load(inFile)
	
	
	for name, path in sources.items():
		if config.entity:
			if name != config.entity :
				continue
				
		print("processing " + name + " ..")
		
		# load data
		dataFrame = spark.read.csv(path, header=True) if (".csv" in path) else spark.read.json(path)
			
		# save schema
		with open(SCHEMES_PATH + name + ".schema", 'w') as f:
			f.write(dataFrame.schema.json())
		
		# save parquet
		dataFrame.write.parquet(OUT_PATH + name, mode="overwrite")
		
		print(".. " + name + " done")



if "__main__" == __name__:
	# init parameters
	parser = argparse.ArgumentParser(prog='Parquet initializer', description='Initializes the data in parquet format.')
	parser.add_argument('-e', '--entity', help='limit to a single entity')
					
	run(parser.parse_args())