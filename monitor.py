import os
import json
import argparse


class Monitor():
	"""Creates diffs of the quality indicators to support the monitoring process"""

	QUALITY_FILE = "repo/quality.json"
	MONITOR_FILE = "repo/monitor.json"

	def run(self, config):
		# init repo files
		with open(Monitor.QUALITY_FILE, 'r', encoding='utf-8') as qualityIn:
			quality = json.load(qualityIn)
			
		monitor = []
		
		if os.path.exists(Monitor.MONITOR_FILE):
			with open(Monitor.MONITOR_FILE, 'r', encoding='utf-8') as monitorIn:
				monitor = json.load(monitorIn)
			
		# order analyze entries by data name
		ordered = {}
		for entry in quality:
			data_name = entry["chain"]
			ordered.setdefault(data_name, {})
			ordered[data_name].setdefault(entry["metrics"], [])
			ordered[data_name][entry["metrics"]].append(entry)
			
			
		# check if there are enough quality measurements in the quality.json
		for data_state in [config.base, config.result]:
			if not config.metric in ordered[data_state]:
				print("Monitoring failed because of missing quality measurements of " + data_state + " data for quality metric ", config.metric)
				return
			if 2 > len(ordered[data_state][config.metric]):
				print("Monitoring failed because there is only one run of quality measurements of " + data_state + " data for quality metric ", config.metric)
				return
					

		# build diffs
		monitor.append(self._createDiff(ordered[config.base][config.metric][-1], ordered[config.base][config.metric][-2]))
			
		increase_before = self._createDiff(ordered[config.base][config.metric][-2], ordered[config.result][config.metric][-2])
		increase_before["time"] = increase_before["time_2"]
		increase_before["metrics"] = increase_before["metrics"]
		increase_before["chain"] = "diff:" + increase_before["chain_1"] + "-" + increase_before["chain_2"]
		
		increase_last = self._createDiff(ordered[config.base][config.metric][-1], ordered[config.result][config.metric][-1])
		increase_last["time"] = increase_last["time_2"]
		increase_last["metrics"] = increase_last["metrics"]
		increase_last["chain"] = "diff:" + increase_last["chain_1"] + "-" + increase_last["chain_2"]
		
		monitor.append(self._createDiff(increase_before, increase_last))
		
			
		# save
		with open(Monitor.MONITOR_FILE, 'w') as outFile:
			json_string = json.dumps(monitor, indent=4)
			outFile.write(json_string)
		
	def _createDiff(self, first, second):
		return {
			"time_1": first["time"],
			"time_2": second["time"],
			"metrics": first["metrics"],
			"chain_1": first["chain"],
			"chain_2": second["chain"],
			"counts": self._diff(first["counts"], second["counts"]),
			"indicators": self._diff(first["indicators"], second["indicators"])
		}
	
		
	def _diff(self, first, second):
		result = {}
		
		for key, value in first.items():
			result[key] = round(value - second[key], 3)
			
		return result



if "__main__" == __name__:
	# init parameters
	parser = argparse.ArgumentParser(prog='Monitor', description='Monitors the quality improvements over multiple runs.')
	parser.add_argument('-m', '--metric', help='name of metric to monitor', default='complete')
	parser.add_argument('-b', '--base', help='the chain of initial measurements', default='initial')
	parser.add_argument('-r', '--result', help='the chain of resulted measurements', default='complete')

	Monitor().run(parser.parse_args())
