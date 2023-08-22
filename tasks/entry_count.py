from lib.metrics import Metric
from pyspark.sql.functions import col


class CountEntries(Metric):
	"""Count the number of records of the entities person, publicaiton and organization"""

	def calc(self, dataFrames, spark):
		return {
			"persons": dataFrames["persons"].count(),
			"affiliations": dataFrames["persons"].where(col("affiliations.orgID").isNotNull()).count(),
			"works": dataFrames["works"].count(),
			"orgUnits": dataFrames["orgUnits"].count()
		}
