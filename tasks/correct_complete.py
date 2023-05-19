

class Correction1:
	CROSSREF_PATH = "data/CrossRef/25.json.gz"

	def correct(self, df_persons, df_works, spark):
		df_crossref = spark.read.json(Correction1.CROSSREF_PATH, multiLine=True)
		
		df_crossref.show(10)
	