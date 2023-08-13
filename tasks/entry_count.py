from lib.metrics import Metric


class CountEntries(Metric):
	"""Count the number of records of the entities person, publicaiton and organization"""

	def calc(self, dataFrames, spark):
		return {
			"persons": dataFrames["persons"].count(),
			"works": dataFrames["works"].count(),
			"orgUnits": dataFrames["orgUnits"].count()
		}
