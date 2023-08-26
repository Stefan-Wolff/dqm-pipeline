"""Metric implementations of the quality dimension 'completeness'"""

from lib.metrics import Metric, Aggregation
from pyspark.sql.functions import length, array_contains, substring
from datetime import datetime
from pyspark.sql import Row


class MinLength(Metric):

	MIN_NUMS = {								# used in transform_correct
		"works.title": 6,
		"works.journal_title": 6,
		"works.abstract": 12,
		"works.subTitle": 6,
		"persons.firstName": 2,
		"persons.lastName": 2,
		"persons.otherNames": 5,
		"persons.publishedName": 5
	}

	def _weights(self):
												#	works.title
												#		works.journal_title
												#			works.abstract
												#				works.subTitle
												#					persons.firstName
												#						persons.lastName
												#							persons.otherNames
												#								persons.publishedName
		return {
			"works.title": 14,					#	1	2	1	2	2	2	2	2
			"works.journal_title": 6,			#	0	1	0	1	1	1	1	1
			"works.abstract": 14,				#	1	2	1	2	2	2	2	2
			"works.subTitle": 9,				#	0	1	0	1	2	1	2	2	
			"persons.firstName": 5,				#	0	1	0	0	1	1	1	1
			"persons.lastName": 6,				#	0	1	0	1	1	1	1	1
			"persons.otherNames": 5,			#	0	1	0	0	1	1	1	1
			"persons.publishedName": 5			#	0	1	0	0	1	1	1	1
		}

	def _calc(self, dataFrames, spark):
		result = {}
		
		df_persons = dataFrames["persons"]
		df_works = dataFrames["works"]

		for key, minNum in MinLength.MIN_NUMS.items():
			levels = key.split(".")
			field = levels[-1]
			df = df_persons if ("persons" == levels[0]) else df_works
			
			df_notNull = df.where(df[field].isNotNull())
			df_valid = df_notNull.where(minNum <= length(df_notNull[field]))
			
			notNull_count = df_notNull.count()
			result[key] = df_valid.count() / notNull_count if (0 != notNull_count) else 0
		
		return result


class MinValue(Metric):
	def _weights(self):
		return {
			"works.date": 1
		}

	def _calc(self, dataFrames, spark):
		result = {}
		
		df_works = dataFrames["works"]
		
		# [YYYY-MM-DD]
		# Notice: The correctness of the date format will be checked in another metric.
		df_notNull = df_works.where(df_works["date"].isNotNull())
		df_valid = df_notNull.where(10 == length("date"))
		
		notNull_count = df_notNull.count()
		result["works.date"] = df_valid.count() / notNull_count if (0 != notNull_count) else 0
			
			
		return result


class NotNull(Metric):
	def _weights(self):
												#	persons.affiliations			works.authors
												#		persons.country					works.bibtex
												#			persons.firstName				works.date
												#				persons.lastName				works.journal_title
												#					persons.author_id				works.orcid_id
												#						persons.id						works.orcid_publication_id
												#							persons.otherNames				works.doi
												#								persons.publishedName			works.issn
												#																	works.isbn
												#																		works.abstract
												#																			works.subTitle
												#																				works.title
												#																					works.type
												#																						works.url
												#																							orgUnits.id
												#																								orgUnits.type
												#																									orgUnits.name
												#																									
		return {
			"persons.affiliations": 19,			# 	1	2	0	0	0	0	1	2	0	2	0	1	0	0	1	2	0	0	1	0	0	0	2	2	2
			"persons.country": 10,				#	0	1	0	0	0	0	0	1	0	1	0	1	0	0	0	1	0	0	1	0	0	0	1	2	1
			"persons.firstName": 30,			#	2	2	1	1	0	0	2	1	0	2	0	2	0	0	1	2	2	1	2	1	2	0	2	2	2
			"persons.lastName": 31,				#	2	2	2	1	0	0	2	1	0	2	0	2	0	0	1	2	2	1	2	1	2	0	2	2	2
			"persons.author_id": 45,			#	2	2	2	2	1	1	2	2	2	2	2	2	1	1	2	2	2	2	2	2	2	2	2	2	2
			"persons.id": 45,					#	2	2	2	2	1	1	2	2	2	2	2	2	1	1	2	2	2	2	2	2	2	2	2	2	2
			"persons.otherNames": 24,			#	1	2	0	0	0	0	1	1	0	1	0	2	0	0	0	2	1	0	2	1	2	2	2	2	2
			"persons.publishedName": 24,		#	0	1	1	1	0	0	1	1	0	1	0	1	0	0	0	2	1	0	0	0	0	0	1	2	1
			"works.authors": 38,				#	2	2	2	2	0	0	2	2	1	2	1	2	1	1	1	2	2	1	2	1	2	1	2	2	2
			"works.bibtex": 12,					#	0	1	0	0	0	0	1	1	0	1	0	1	0	0	0	1	0	0	1	0	1	1	1	1	1
			"works.date": 36,					#	2	2	2	2	0	0	2	2	1	2	1	2	0	0	0	2	2	1	2	1	2	2	2	2	2
			"works.journal_title": 11,			#	2	1	0	0	0	0	0	1	0	1	0	1	0	0	0	1	0	0	1	0	0	0	1	1	1
			"works.orcid_id": 45,				#	2	2	2	2	1	1	2	2	1	2	2	2	1	2	2	2	2	2	2	2	2	2	2	2	2
			"works.orcid_publication_id": 35,	#	1	0	1	1	0	0	2	2	1	2	2	2	0	1	2	2	2	1	2	1	2	2	2	2	2
			"works.doi": 34,					#	1	2	1	1	0	0	2	2	1	2	2	2	0	0	1	2	1	1	2	1	2	2	2	2	2
			"works.issn": 32,					#	1	2	1	1	0	0	2	2	1	2	2	2	0	0	1	1	0	1	2	1	2	2	2	2	2
			"works.isbn": 33,					#	1	2	1	1	0	0	2	2	1	2	2	2	0	0	1	1	1	1	2	1	2	2	2	2	2
			"works.abstract": 36,				#	2	2	1	1	0	0	2	2	1	2	1	2	0	1	1	2	2	1	2	1	2	2	2	2	2
			"works.subTitle": 12,				#	1	1	0	0	0	0	0	2	0	1	0	1	0	0	0	1	0	0	1	0	0	0	1	2	1
			"works.title": 35,					#	2	2	1	1	0	0	1	2	1	2	1	2	0	1	1	2	2	1	2	1	2	2	2	2	2
			"works.type": 18,					#	2	2	0	0	0	0	0	2	0	1	0	2	0	0	0	1	0	0	2	0	1	1	1	2	1
			"works.url": 28,					#	2	2	2	2	0	0	0	2	1	1	0	2	0	0	0	2	2	0	2	0	1	1	2	2	2
			"orgUnits.id": 12,					#	0	1	0	0	0	0	0	1	0	1	0	1	0	0	0	2	0	0	1	0	1	0	1	2	1
			"orgUnits.type": 3,					#	0	0	0	0	0	0	0	0	0	1	0	1	0	0	0	0	0	0	0	0	0	0	0	1	0
			"orgUnits.name": 12					#	0	1	0	0	0	0	0	1	0	1	0	1	0	0	0	2	0	0	1	0	1	0	1	2	1
		}

	def _calc(self, dataFrames, spark):
		result = {}
		entities = set([e.split(".")[0] for e in self._weights().keys()])
		
		for entity in entities:
			dataFrame = dataFrames[entity]
			row_num = dataFrame.count()
			
			for col in dataFrame.columns:
				df_valid = dataFrame.where(dataFrame[col].isNotNull())
				result[entity + "." + col] = df_valid.count() / row_num if (0 != row_num) else 0

		return result
	

class MinPopulation(Metric):

	TYPES = [
		"book",
		"book-chapter",
		"dissertation-thesis",
		"edited-book",
		"journal-article",
		"working-paper",
		"conference-abstract",
		"conference-paper",
		"conference-poster"
	]
	
	COUNTRIES = ["BE", "BG", "DK", "DE", "EE", "FI", "FR", "GB", "GR", "IE", "IT", "HR", "LV", "LT", "LU", "MT", "NL", "AT", "PL", "PT", "RO", "SE", "SK", "SI", "ES", "CZ", "HU", "UK", "CY", "AL", "AD", "IS", "LI", "MC", "ME", "NO", "SM", "CH", "RS", "UA", "BY"]

	def _weights(self):
											#	works.date
											#		works.type
											#			works.country
		return {
			"works.date": 5,				#	1	2	2
			"works.type": 1,				#	0	1	0
			"works.country": 3				#	0	2	1
		}


	def _calc(self, dataFrames, spark):
		result = {}
		
		df_persons = dataFrames["persons"]
		df_works = dataFrames["works"]
		
		# publication year: 2000 - current
		current_year = datetime.now().year
		df_years = spark.createDataFrame([Row(str(y)) for y in range(2000, current_year + 1)], ["year"])
		df_years_missing = df_years.join(df_works, df_years["year"] == substring(df_works["date"], 0, 4), "leftanti")
		result["works.date"] = 1 - df_years_missing.count() / (current_year + 1 - 2000)
		
		# publication types
		df_types = spark.createDataFrame([Row(t) for t in MinPopulation.TYPES], ["type"])
		df_types_missing = df_types.join(df_works, df_types["type"] == df_works["type"], "leftanti")				
		result["works.type"] = 1 - df_types_missing.count() / len(MinPopulation.TYPES)
	
		# countries
		df_country = spark.createDataFrame([Row(c) for c in MinPopulation.COUNTRIES], ["country"])
		df_country_missing = df_country.join(df_persons, df_country["country"] == df_persons["country"], "leftanti")
		result["works.country"] = 1 - df_country_missing.count() / len(MinPopulation.COUNTRIES)

		return result




class MinObject(Metric):
	def _weights(self):
								#	works
								#		persons
		return {
			"works": 3,			#	1	2
			"persons": 1		#	0	1
		}

	def _calc(self, dataFrames, spark):
		result = {}
		
		df_persons = dataFrames["persons"]
		df_works = dataFrames["works"]
		
		# works
		works_num = df_works.count()
		df_works_valid = df_works.where(df_works["title"].isNotNull() & \
										(df_works["url"].isNotNull() | df_works["doi"].isNotNull()))
		result["works"] = df_works_valid.count() / works_num if (0 != works_num) else 0

		# persons
		persons_num = df_persons.count()
		df_persons_valid = df_persons.where(df_persons["id"].isNotNull() & \
												((df_persons["firstName"].isNotNull() & df_persons["lastName"].isNotNull()) | \
												df_persons["publishedName"].isNotNull() | \
												df_persons["otherNames"].isNotNull()))
		result["persons"] = df_persons_valid.count() / persons_num if (0 != persons_num) else 0
			

		return result

		
class Completeness(Aggregation):

	def _weights(self):
									#	MinLength
									#		MinValue
									#			NotNull
									#				MinPopulation
									#					MinObject
		return {
			"MinLength": 5,			#	1	2	0	2	0
			"MinValue": 2,			#	0	1	0	1	0
			"NotNull": 7,			#	2	2	1	2	0
			"MinPopulation": 2,		#	0	1	0	1	0
			"MinObject": 9			#	2	2	2	2	1
		}


	def calc(self, dataFrames, spark):
		result = {}
		
		result.update(MinLength().calc(dataFrames, spark))
		result.update(MinValue().calc(dataFrames, spark))
		result.update(NotNull().calc(dataFrames, spark))
		result.update(MinPopulation().calc(dataFrames, spark))
		result.update(MinObject().calc(dataFrames, spark))
		
		
		return self._formateResult(result)
		
		
