import json
import argparse
from pyspark.sql import SparkSession
from tasks.metric_completeness import *
from tasks.metric_correctness import *


def run(config):
	# init spark session
	spark = SparkSession	\
				.builder	\
				.config('spark.driver.memory', '16G') \
				.getOrCreate()
	
	# load data
	#df_persons = spark.read.json("data/ORCID_persons/*")
	df_persons = spark.read.json("data/ORCID_persons_0.jsonl")
	#df_works = spark.read.json("data/ORCID_works/*")
	df_works = spark.read.json("data/ORCID_works/works_37635248.jsonl.gz")
	#df_works = spark.read.json("data/ORCID_works_0.jsonl")
	
	
	# init metrics
	metrics = {
			"MinLength": MinLength(),
			"MinValue": MinValue(),
			"NotNull": NotNull(),
			"MinPopulation": MinPopulation(),
			"MinObject": MinObject(),
			"Completeness" : Completeness(),
			"CorrectSyntax": CorrectSyntax(),
		}
	
	
	
	
	# run metrics
	results = {}
	for name, metric in metrics.items():
		if name in config.metrics:
			metric_results = metric.calc(df_persons, df_works, spark, config.sample_num)
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
	parser.add_argument('-m', '--metrics', help='names of metrics to run', action="extend", nargs="+")
					

	run(parser.parse_args())
