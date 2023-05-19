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
	df_persons = spark.read.json("data/ORCID_persons.jsonl")
	df_works = spark.read.json("data/works_0.jsonl")
	
	# pipe: analyse - correct - analyse
	#indicator_before = Completeness1().calc(df_persons, df_works, spark)
	df_persons, df_works = Correction1().correct(df_persons, df_works, spark)
	#indicator_after = Completeness1().calc(df_persons, df_works, spark)



if "__main__" == __name__:
	run()
