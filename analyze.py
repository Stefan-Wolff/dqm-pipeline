import json
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from tasks.metric_completeness import *
from tasks.metric_correctness import *
from tasks.metric_consistency import *
from datetime import datetime


REPO_FILE = "repo/quality.json"


### functions
def saveResults(results, config):
	repo = {}
	
	if os.path.exists(REPO_FILE):
		with open(REPO_FILE, 'r', encoding='utf-8') as repoIn:
			repo = json.load(repoIn)
		
		
	key = datetime.now().strftime("%d/%m/%Y %H:%M:%S ") + ",".join(config.metrics)
	if config.tag:
		key += "#" + config.tag
		
	repo[key] = results
		
	with open('repo/quality.json', 'w') as outFile:
		json_string = json.dumps(repo, indent=4)
		outFile.write(json_string)


### main
def run(config):
	# init spark session
	spark = SparkSession	\
				.builder	\
				.config('spark.driver.memory', '16G') \
				.config('spark.executor.cores', '16') \
				.getOrCreate()
	
	# schemas
	with open("data/orcid_persons.schema.json", 'r') as f:
		schema_persons = StructType.fromJson(json.load(f))
	with open("data/orcid_works.schema.json", 'r') as f:
		schema_works = StructType.fromJson(json.load(f))
	with open("data/orgUnits.schema.json", 'r') as f:
		schema_orgUnits = StructType.fromJson(json.load(f))
	
	# load data
	#df_persons = spark.read.json("data/ORCID_persons/37635374_31.jsonl.gz", schema=schema_persons)
	df_persons = spark.read.json("data/ORCID_persons/*", schema=schema_persons)
	#df_works = spark.read.json("data/ORCID_works/*", schema=schema_works)
	df_works = spark.read.json("data/orcid_works_0.jsonl", schema=schema_works)						#.repartition(32, "orcid_publication_id")
	df_orgUnits = spark.read.json("data/orgUnits.jsonl.gz", schema=schema_orgUnits)

	
	# init metrics
	metrics = {
			"MinLength": MinLength(),
			"MinValue": MinValue(),
			"NotNull": NotNull(),
			"MinPopulation": MinPopulation(),
			"MinObject": MinObject(),
			"Completeness" : Completeness(),
			"Correctness": Correctness(),
			"UniqueValue": UniqueValue(),
			"NoContradict": NoContradict(),
			"Consistency": Consistency()
		}
	
	
	# run metrics
	results = {}
	for name, metric in metrics.items():
		if name in config.metrics:
			metric_results = metric.calc(df_persons, df_works, df_orgUnits, spark, config.sample_num)
			results.update(metric_results)


	# store
	print(results)
	saveResults(results, config)


### entry
if "__main__" == __name__:

	# init parameters
	parser = argparse.ArgumentParser(prog='Data Analyzer', description='Run metrics and show samples of invalid data')
	parser.add_argument('-n', '--sample_num', help='the number of samples to print', default=0, type=int)
	parser.add_argument('-m', '--metrics', help='names of metrics to run', action="extend", nargs="+", required=True)
	parser.add_argument('-t', '--tag', help='a tag to organize results')
					

	run(parser.parse_args())
