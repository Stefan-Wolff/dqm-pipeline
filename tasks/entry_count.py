from lib.metrics import Metric


class CountEntries(Metric):
	def calc(self, dataFrames, spark):
		return {
			"persons": dataFrames["persons"].count(),
			"works": dataFrames["works"].count(),
			"orgUnits": dataFrames["orgUnits"].count()
		}
