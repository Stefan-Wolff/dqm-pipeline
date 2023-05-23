import argparse
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel


def run(config):
	# init spark session
	spark = SparkSession	\
				.builder	\
				.config('spark.driver.memory', '16G') \
				.getOrCreate()
	
	# load data
	dataPath = "data/ORCID_persons/*" if ("person" == config.entity) else "data/ORCID_works/*"
	dataFrame = spark.read.json(dataPath)

	# print
	dataFrame.where(dataFrame[config.attribute].isNotNull())	\
			.select(config.attribute)	\
			.sort(dataFrame[config.attribute].desc() if (config.desc) else dataFrame[config.attribute].asc())	\
			.sample()	\
			.show(config.sample_num, 100)


if "__main__" == __name__:

	# init parameters
	parser = argparse.ArgumentParser(prog='Sample Printer', description='Show samples of all data')
	parser.add_argument('-n', '--sample_num', help='the number of samples to print', default=1000, type=int)
	parser.add_argument('-e', '--entity', help='the entity to print', choices=['persons', 'works'], required=True)
	parser.add_argument('-a', '--attribute', help='the attribute to print', required=True)
	parser.add_argument('-d', '--desc', help='desc order', action='store_true')
					

	run(parser.parse_args())
