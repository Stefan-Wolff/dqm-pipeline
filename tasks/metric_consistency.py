"""Metric implementations of the quality dimension 'consistency'"""

from lib.metrics import Metric, Aggregation
from pyspark.sql.functions import explode
from pyspark.sql.types import StringType
from lib.duplicates import groupDuplicates


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
					
					notNull_count = df_notNull.count()
					result[key] = df_valid.count() / notNull_count if (0 != notNull_count) else 0
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
		
		notNull_count = df_notNull.count()
		result["persons.affiliations.startBeforeEnd"] = df_valid.count() / notNull_count if (0 != notNull_count) else 0
		
		# ISBN XOR ISSN
		df_notNull = df_works.where(df_works["isbn"].isNotNull() | df_works["issn"].isNotNull())
		df_invalid = df_notNull.where(df_notNull["isbn"].isNotNull() & df_notNull["issn"].isNotNull())
		
		notNull_count = df_notNull.count()
		result["works.isbnXORissn"] = 1 - df_invalid.count() / notNull_count if (0 != notNull_count) else 0
		
		# ISBN & ("book" in type)
		df_notNull = df_works.where(df_works["isbn"].isNotNull() & df_works["type"].isNotNull())
		df_invalid = df_notNull.where(df_notNull["isbn"].isNotNull() & ~df_notNull["type"].contains("book"))
		
		notNull_count = df_notNull.count()
		result["works.isbnOnlyBook"] = 1 - df_invalid.count() / notNull_count if (0 != notNull_count) else 0
		
		
		return result
	
	
class UniqueObject(Metric):

	def _weights(self):
		return {
			"works": 1
		}
	
	def _calc(self, dataFrames, spark):
		df_works = dataFrames["works"]
		
		df_key = groupDuplicates(df_works) \
					.select("key")	\
					.distinct()
						 
		works_count = df_works.count()
					
		return {
			"works": df_key.count() / works_count if (0 != works_count) else 0
		}
	
		
		
		
class Consistency(Aggregation):

	def _weights(self):
									#	UniqueValue
									#		NoContradict
									#			UniqueObject
		return {
			"UniqueValue": 5,		#	1	2	2
			"NoContradict": 1,		#	0	1	0
			"UniqueObject": 3		#	0	2	1
		}
		
		
	
	def calc(self, dataFrames, spark):
		result = {}
		
		result.update(UniqueValue().calc(dataFrames, spark))
		result.update(NoContradict().calc(dataFrames, spark))
		result.update(UniqueObject().calc(dataFrames, spark))
		
		return self._formateResult(result)