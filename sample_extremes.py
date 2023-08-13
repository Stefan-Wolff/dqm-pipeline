import argparse
from pyspark.sql.functions import explode
from lib.data_processor import DataProcessor


class Sampler(DataProcessor):
	"""Prints data samples of extrem values"""

	def _run(self, dataFrames, config, spark):
		dataFrame = dataFrames[config.entity]
	
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
	parser.add_argument('-c', '--chain', help='the source data related to the transformation chain', default='initial')
					

	Sampler().run(parser.parse_args())
