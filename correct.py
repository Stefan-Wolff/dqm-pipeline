import json
from pyspark.sql import SparkSession
from tasks.correct_complete import *
from tasks.metric_complete import *


def run():
	# init spark session
	spark = SparkSession	\
				.builder	\
				.config('spark.driver.memory', '16G') \
				.getOrCreate()
	
	# load data
	df_persons = spark.read.json("data/ORCID_persons_0.jsonl")
	df_works = spark.read.json("data/works_0.jsonl")
	#df_works = spark.read.json("data/ORCID_works/works_37635248.jsonl")
	
	pipeline = [
		ExtractBibtex()
	]
	
	
	# run pipeline
	for task in pipeline:
		df_persons, df_works = task.run(df_persons, df_works)
	
	



if "__main__" == __name__:
	run()
