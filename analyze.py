import json
import argparse
from pyspark.sql import SparkSession
from tasks.metric_complete import *


def run(config):
	# init spark session
	spark = SparkSession	\
				.builder	\
				.config('spark.driver.memory', '16G') \
				.getOrCreate()
	
	# load data
	df_persons = spark.read.json("output/persons.jsonl")
	df_works = spark.read.json("output/works_0.jsonl")
	
	
	# configure metrics
	metrics = [
			Completeness1(),
			Completeness2(),
			Completeness3(),
			Completeness4(),
			Completeness5(),
			Completeness6()
		]
	
	
	# run metrics
	results = {}
	for i in config.metrics:
		metric_results = metrics[i-1].calc(df_persons, df_works, spark, config.sample_num)
		results.update(metric_results)

	print(results)
	
	# save results
	with open('repo/quality.jsonl', 'w') as outFile:
		json_string = json.dumps(results, indent=4)
		outFile.write(json_string)


if "__main__" == __name__:

	# init parameters
	parser = argparse.ArgumentParser(prog='Data Analyzer', description='Run metrics and show samples of invalid data')
	parser.add_argument('-n', '--sample_num', help='the number of samples to print', default=0, type=int)
	parser.add_argument('-m', '--metrics', help='numbers of metrics to run', action="extend", nargs="+", type=int)
					

	run(parser.parse_args())
