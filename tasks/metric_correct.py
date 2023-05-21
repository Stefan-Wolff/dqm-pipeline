from lib.metrics import Metric

class ContentCorrect8(Metric):
	def _weights(self):
		return None
	
	def _calc(self, df_persons, df_works, spark):
		result = {}
	
		df_persons.where(df_persons["affiliations.orgIDType"] == "FUNDREF").select("affiliations.orgID").show(10, 100)
	
		df_fundref = spark.read.json("data/FUNDERREF.csv")
		
	
		return result
		
	