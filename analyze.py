import json
import os
import argparse
from tasks.metric_completeness import *
from tasks.metric_correctness import *
from tasks.metric_consistency import *
from datetime import datetime
from lib.data_processor import DataProcessor


class Analyzer(DataProcessor):

	REPO_FILE = "repo/quality.json"

	METRICS = {
		"MinLength": MinLength(),
		"MinValue": MinValue(),
		"NotNull": NotNull(),
		"MinPopulation": MinPopulation(),
		"MinObject": MinObject(),
		"Completeness" : Completeness(),
		"Correctness": Correctness(),
		"UniqueValue": UniqueValue(),
		"NoContradict": NoContradict(),
		"Consistency": Consistency(),
		"Uniqueness": Uniqueness()
	}

	def _run(self, dataFrames, config, spark):
		# run metrics
		results = {}
		for name, metric in Analyzer.METRICS.items():
			if name in config.metrics:
				metric_results = metric.calc(dataFrames, spark)
				results.update(metric_results)
					

		print(results)

		# save results
		repo = []
		
		if os.path.exists(Analyzer.REPO_FILE):
			with open(Analyzer.REPO_FILE, 'r', encoding='utf-8') as repoIn:
				repo = json.load(repoIn)
			
		time = datetime.now().strftime("%d/%m/%Y %H:%M:%S ") + ",".join(config.metrics)
			
		repo.append({
			"time": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
			"metrics": ",".join(config.metrics),
			"chain": config.chain,
			"indicators": results
		})
			
		with open('repo/quality.json', 'w') as outFile:
			json_string = json.dumps(repo, indent=4)
			outFile.write(json_string)




### entry
if "__main__" == __name__:

	# init parameters
	parser = argparse.ArgumentParser(prog='Data Analyzer', description='Run metrics and show samples of invalid data')
	parser.add_argument('-m', '--metrics', help='names of metrics to run', action="extend", nargs="+", required=True)
	parser.add_argument('-c', '--chain', help='source chain if analyze transformed data', default='initial')
					

	Analyzer().run(parser.parse_args())
