import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType
from pyspark.storagelevel import StorageLevel


def run(config):
	# init spark session
	spark = SparkSession	\
				.builder	\
				.config('spark.driver.memory', '16G') \
				.getOrCreate()
	
	
	# load data
	if "persons" == config.entity:
		dataPath = "data/ORCID_persons/*"
		schemaPath = "data/orcid_persons.schema.json"
	elif "works" == config.entity:
		dataPath = "data/ORCID_works/*"
		schemaPath = "data/orcid_works.schema.json"
	elif "orgUnits" == config.entity:
		dataPath = "data/orgUnits.jsonl.gz"
		schemaPath = "data/orgUnits.schema.json"
		
	with open(schemaPath, 'r') as f:
		schema = StructType.fromJson(json.load(f))
	dataFrame = spark.read.json(dataPath, schema=schema)


	# nested data
	if "." in config.attribute:
		levels = config.attribute.split(".")
		dataFrame = dataFrame.withColumn("exploded", explode(levels[0]))
		levels[0] = "exploded"
		config.attribute = ".".join(levels)


	# print
	dataFrame.where(dataFrame[config.attribute].isNotNull() & (dataFrame[config.attribute] != ""))	\
			 .select(config.attribute)	\
			 .sort(dataFrame[config.attribute].desc() if (config.desc) else dataFrame[config.attribute].asc())	\
			 .show(config.sample_num, 100)


if "__main__" == __name__:

	# init parameters
	parser = argparse.ArgumentParser(prog='Sample Printer', description='Show samples of all data')
	parser.add_argument('-n', '--sample_num', help='the number of samples to print', default=1000, type=int)
	parser.add_argument('-e', '--entity', help='the entity to print', choices=['persons', 'works', 'orgUnits'], required=True)
	parser.add_argument('-a', '--attribute', help='the attribute to print', required=True)
	parser.add_argument('-d', '--desc', help='desc order', action='store_true')
					

	run(parser.parse_args())
