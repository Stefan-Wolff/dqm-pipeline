import argparse
from pyspark.sql import SparkSession
import json


### main
def run(config):
	# init spark session
	spark = SparkSession	\
				.builder	\
				.config('spark.driver.memory', '32G')	\
				.getOrCreate()


	# read source config
	with open('repo/sources.json', 'r', encoding='utf-8') as inFile:
		sources = json.load(inFile)
	
	
	for name, path in sources.items():
		if config.entity:
			if name != config.entity :
				continue
				
		print("processing " + name + " ..")
		
		# load data
		dataFrame = spark.read.csv(path, header=True) if (".csv" in path) else spark.read.json(path)
			
		# save schema
		with open('data/schemes/' + name + ".schema", 'w') as f:
			f.write(dataFrame.schema.json())
		
		# save parquet
		dataFrame.write.parquet("data/parquets/initial/" + name, mode="overwrite")
		
		print(".. " + name + " done")


### entry
if "__main__" == __name__:

	# init parameters
	parser = argparse.ArgumentParser(prog='Parquet initializer', description='Initializes the data in parquet format.')
	parser.add_argument('-e', '--entity', help='limit to a single entity')
					
	run(parser.parse_args())