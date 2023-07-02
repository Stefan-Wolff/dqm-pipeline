import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class DataProcessor:
	def run(self, config):
		# init spark session
		spark = SparkSession	\
					.builder	\
					.config('spark.driver.memory', '180G') \
					.config('spark.driver.maxResultSize', '2G') \
					.config('spark.sql.parquet.aggregatePushdown', True) \
					.config('spark.sql.codegen.wholeStage', False) \
					.getOrCreate()


		# read source config
		with open('repo/sources.json', 'r', encoding='utf-8') as inFile:
			sources = json.load(inFile)
			
		# load data
		dataFrames = {}
		for entity in sources.keys():
		
			with open("data/schemes/" + entity + ".schema", 'r') as f:
				schema = StructType.fromJson(json.load(f))
			
			# build path stack, to look behind for last written dataframe of current entity
			chain_nodes = config.chain.split(".")
			chain_stack = []
			for i in reversed(range(0, len(chain_nodes))):
				chain_stack.append(".".join(chain_nodes))
				del chain_nodes[i]
			
			chain_stack.append("initial")
			
			for node in chain_stack:
				path = "data/parquets/" + node + "/" + entity
				if os.path.exists(path):
					print("read " + path)
					dataFrames[entity] = spark.read.parquet(path, schema=schema)
					break
			
			
		self._run(dataFrames, config, spark)
