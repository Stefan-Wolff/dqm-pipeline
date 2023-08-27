import json
import os
import argparse
from tasks.metric_completeness import *
from tasks.metric_correctness import *
from tasks.metric_consistency import *
from tasks.entry_count import *
from datetime import datetime
from lib.data_processor import DataProcessor


class Analyzer(DataProcessor):
	"""Runs specified tasks for data analyzing and stores the results"""

	REPO_FILE = "repo/quality.json"

	METRICS = {
		"MinLength": MinLength(),
		"MinValue": MinValue(),
		"NotNull": NotNull(),
		"MinPopulation": MinPopulation(),
		"MinObject": MinObject(),
		"Completeness" : Completeness(),
		"CorrectValue": CorrectValue(),
		"Correctness": Correctness(),
		"UniqueValue": UniqueValue(),
		"NoContradict": NoContradict(),
		"UniqueObject": UniqueObject(),
		"Consistency": Consistency()
	}
	
	METRICS_COMPLETE = [
		"Completeness",
		"Correctness",
		"Consistency"
	]

	def _run(self, dataFrames, config, spark):
		results = {}
		
		# run all metrics
		if "complete" == config.metrics:
			print("running complete metric stack: ", list(Analyzer.METRICS.keys()))
			for name in Analyzer.METRICS_COMPLETE:
				metric = Analyzer.METRICS[name]
				metric_results = metric.calc(dataFrames, spark)
				results.update(metric_results)
		
		# run given metrics
		else:
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
			"counts": CountEntries().calc(dataFrames, spark),
			"indicators": results
		})
			
		with open(Analyzer.REPO_FILE, 'w') as outFile:
			json_string = json.dumps(repo, indent=4)
			outFile.write(json_string)




if "__main__" == __name__:
	# init parameters
	parser = argparse.ArgumentParser(prog='Data Analyzer', description='Run metrics')
	parser.add_argument('-m', '--metrics', help='names of metrics to run', action="extend", nargs="+", default='complete')
	parser.add_argument('-c', '--chain', help='the source data related to the transformation chain', default='initial')
					

	Analyzer().run(parser.parse_args())
