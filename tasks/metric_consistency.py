from lib.metrics import Metric, Aggregation
from pyspark.sql.functions import explode


class UniqueValue(Metric):

	def _weights(self):
												#	works.bibtex
												#		works.orcid_publication_id
												#			works.doi
												#				persons.id
												#					orgUnits.orgID
		return {
			"works.bibtex": 1,					#	1	0	0	0	0
			"works.orcid_publication_id": 8,	#	2	1	2	1	2
			"works.doi": 5,						#	2	1	1	0	1
			"persons.id": 8,					#	2	1	2	1	2
			"orgUnits.id": 4					#	2	0	1	0	1
		}
		
	
	def _calc(self, dataFrames, spark):
		result = {}
		
		for key in self._weights().keys():
			for entity, dataFrame in dataFrames.items():
				if key.startswith(entity):
					field = key.replace(entity + ".", "", 1)

					df_notNull = dataFrame.where(dataFrame[field].isNotNull()).select(field)
					df_valid = df_notNull.distinct()
					indicator = df_valid.count() / df_notNull.count()
					
					result[key] = (indicator, df_notNull.subtract(df_valid))
					break
		
		return result
		
		
		
class NoContradict(Metric):

	def _weights(self):
														#	persons.affiliations.startBeforeEnd
														#		works.isbnXORissn
														#			works.isbnOnlyBook
		return {
			"persons.affiliations.startBeforeEnd": 2,	#	1	0	1
			"works.isbnXORissn": 4,						#	2	1	1
			"works.isbnOnlyBook": 3						#	1	1	1
		}
		
	def _calc(self, dataFrames, spark):
		result = {}
		
		df_persons = dataFrames["persons"]
		df_works = dataFrames["works"]
		
		# startYear <= endYear
		df_affils = df_persons.withColumn("affil_exploded", explode("affiliations"))
		df_notNull = df_affils.where(df_affils["affil_exploded.startYear"].isNotNull() & df_affils["affil_exploded.endYear"].isNotNull())
		df_valid = df_notNull.where(df_notNull["affil_exploded.startYear"] <= df_notNull["affil_exploded.endYear"])
		
		indicator = df_valid.count() / df_notNull.count()
		result["persons.affiliations.startBeforeEnd"] = (indicator, df_notNull.subtract(df_valid))
		
		# ISBN XOR ISSN
		df_notNull = df_works.where(df_works["isbn"].isNotNull() | df_works["issn"].isNotNull())
		df_invalid = df_notNull.where(df_notNull["isbn"].isNotNull() & df_notNull["issn"].isNotNull())
		
		indicator = 1 - df_invalid.count() / df_notNull.count()
		result["works.isbnXORissn"] = (indicator, df_invalid)
		
		# ISBN & ("book" in type)
		df_notNull = df_works.where(df_works["isbn"].isNotNull() & df_works["type"].isNotNull())
		df_invalid = df_notNull.where(df_notNull["isbn"].isNotNull() & ~df_notNull["type"].contains("book"))
		
		indicator = 1 - df_invalid.count() / df_notNull.count()
		result["works.isbnOnlyBook"] = (indicator, df_invalid)
		
		
		return result
	
	
		
		
class Consistency(Aggregation):

	def _weights(self):
									#	UniqueValue
									#		NoContradict
		return {
			"UniqueValue": 3,		#	1	2
			"NoContradict": 1		#	0	1
		}
		
		
	
	def calc(self, dataFrames, spark, sample_num):
		result = {}
		
		result.update(UniqueValue().calc(dataFrames, spark, sample_num))
		result.update(NoContradict().calc(dataFrames, spark, sample_num))
		
		return result