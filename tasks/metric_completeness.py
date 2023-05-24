from lib.metrics import Metric
from pyspark.sql.functions import length, array_contains, substring
from datetime import datetime
from pyspark.sql import Row


class MinLength(Metric):
	def _weights(self):
												#	works.title
												#		works.journal_title
												#			works.short_description
												#				works.subTitle
												#					persons.firstName
												#						persons.lastName
												#							persons.otherNames
												#								persons.publishedName
		return {
			"works.title": 14,					#	1	2	1	2	2	2	2	2
			"works.journal_title": 6,			#	0	1	0	1	1	1	1	1
			"works.short_description": 14,		#	1	2	1	2	2	2	2	2
			"works.subTitle": 9,				#	0	1	0	1	2	1	2	2	
			"persons.firstName": 5,				#	0	1	0	0	1	1	1	1
			"persons.lastName": 6,				#	0	1	0	1	1	1	1	1
			"persons.otherNames": 5,			#	0	1	0	0	1	1	1	1
			"persons.publishedName": 5			#	0	1	0	0	1	1	1	1
		}

	def _calc(self, df_persons, df_works, spark):
		result = {}
		
		# works.title
		df_works_title = df_works.where(6 <= length(df_works["title"]))
		works_title = df_works_title.count() / df_works.where(df_works["title"].isNotNull()).count()
		result["works.title"] = (works_title, df_works.subtract(df_works_title))
		
		# works.journal_title
		df_works_journal = df_works.where(2 <= length(df_works["journal_title"]))
		works_journal = df_works_journal.count() / df_works.where(df_works["journal_title"].isNotNull()).count()
		result["works.journal_title"] = (works_journal, df_works.subtract(df_works_journal))
		
		# works.short_description
		df_works_description = df_works.where(12 <= length(df_works["short_description"]))
		works_description = df_works_description.count() / df_works.where(df_works["short_description"].isNotNull()).count()
		result["works.short_description"] = (works_description, df_works.subtract(df_works_description))
		
		# works.subTitle
		df_works_subTitle = df_works.where(6 <= length(df_works["subTitle"]))
		works_subTitle = df_works_subTitle.count() / df_works.where(df_works["subTitle"].isNotNull()).count()
		result["works.subTitle"] = (works_subTitle, df_works.subtract(df_works_subTitle))
		
		# persons.firstName
		df_person_firstName = df_persons.where(2 <= length(df_persons["firstName"]))
		person_firstName = df_person_firstName.count() / df_persons.where(df_persons["firstName"].isNotNull()).count()
		result["persons.firstName"] = (person_firstName, df_persons.subtract(df_person_firstName))
		
		# persons.lastName
		df_person_lastName = df_persons.where(2 <= length(df_persons["lastName"]))
		person_lastName = df_person_lastName.count() / df_persons.where(df_persons["lastName"].isNotNull()).count()
		result["persons.lastName"] = (person_lastName, df_persons.subtract(df_person_firstName))
		
		# persons.otherNames
		df_person_otherNames = df_persons.where(5 <= length(df_persons["otherNames"]))
		person_otherNames = df_person_otherNames.count() / df_persons.where(df_persons["otherNames"].isNotNull()).count()
		result["persons.otherNames"] = (person_otherNames, df_persons.subtract(df_person_firstName))
		
		# persons.publishedName
		df_person_publishedName = df_persons.where(5 <= length(df_persons["publishedName"]))
		person_publishedName = df_person_publishedName.count() / df_persons.where(df_persons["publishedName"].isNotNull()).count()
		result["persons.publishedName"] = (person_publishedName, df_persons.subtract(df_person_firstName))

		return result


class MinValue(Metric):
	def _weights(self):
		return {
			"works.date": 1
		}

	def _calc(self, df_persons, df_works, spark):
		result = {}
		date_num = df_works.where(df_works["date"].isNotNull()).count()
		
		# [YYYY-MM-DD]
		# Notice: The correctness of the date format will be checked in another metric.
		df_date = df_works.where(10 == length("date"))
		date_correct = df_date.count() / date_num
		result["works.date"] = (date_correct, df_works.subtract(df_date))
			
			
		return result


class NotNull(Metric):
	def _weights(self):
												#	persons.affiliations		works.authors
												#		persons.country				works.bibtex
												#			persons.firstName			works.date
												#				persons.lastName			works.journal_title
												#					persons.orcid_id			works.orcid_id
												#						persons.otherNames			works.orcid_publication_id
												#							persons.publishedName		works.publicationID
												#															works.short_description
												#																works.subTitle
												#																	works.title
												#																		works.type
												#																			works.url
		return {
			"persons.affiliations": 11,			# 	1	2	0	0	0	1	2	0	2	0	1	0	0	1	0	1	0	0	0
			"persons.country": 5,				#	0	1	0	0	0	0	1	0	1	0	1	0	0	0	0	1	0	0	0
			"persons.firstName": 20,			#	2	2	1	1	0	2	1	0	2	0	2	0	0	1	1	2	1	2	0
			"persons.lastName": 21,				#	2	2	2	1	0	2	1	0	2	0	2	0	0	1	1	2	1	2	0
			"persons.orcid_id": 35,				#	2	2	2	2	1	2	2	2	2	2	2	1	1	2	2	2	2	2	2
			"persons.otherNames": 15,			#	1	2	0	0	0	1	1	0	1	0	2	0	0	0	0	2	1	2	2
			"persons.publishedName": 17,		#	0	1	1	1	0	1	1	0	1	0	1	0	0	0	0	0	0	0	0
			"works.authors": 28,				#	2	2	2	2	0	2	2	1	2	1	2	1	1	1	1	2	1	2	1
			"works.bibtex": 8,					#	0	1	0	0	0	1	1	0	1	0	1	0	0	0	0	1	0	1	1
			"works.date": 26,					#	2	2	2	2	0	2	2	1	2	1	2	0	0	0	1	2	1	2	2
			"works.journal_title": 7,			#	2	1	0	0	0	0	1	0	1	0	1	0	0	0	0	1	0	0	0
			"works.orcid_id": 35,				#	2	2	2	2	1	2	2	1	2	2	2	1	2	2	2	2	2	2	2
			"works.orcid_publication_id": 25,	#	1	0	1	1	0	2	2	1	2	2	2	0	1	2	1	2	1	2	2
			"works.publicationID": 25,			#	1	2	1	1	0	2	2	1	2	2	2	0	0	1	1	2	1	2	2
			"works.short_description": 26,		#	2	2	1	1	0	2	2	1	2	1	2	0	1	1	1	2	1	2	2
			"works.subTitle": 7,				#	1	1	0	0	0	0	2	0	1	0	1	0	0	0	0	1	0	0	0
			"works.title": 25,					#	2	2	1	1	0	1	2	1	2	1	2	0	1	1	1	2	1	2	2
			"works.type": 13,					#	2	2	0	0	0	0	2	0	1	0	2	0	0	0	0	2	0	1	1
			"works.url": 18						#	2	2	2	2	0	0	2	1	1	0	2	0	0	0	0	2	0	1	1
		}

	def _calc(self, df_persons, df_works, spark):
		result = {}
		entities = {"persons": df_persons, "works": df_works}
		
		for entity, dataFrame in entities.items():
			row_num = dataFrame.count()
			
			for col in dataFrame.columns:
				df_valid = dataFrame.where(dataFrame[col].isNotNull())
				indicator = df_valid.count() / row_num
				result[entity + "." + col] = (indicator, dataFrame.subtract(df_valid))

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
											#	works.publicationDate
											#		works.type
											#			works.country
		return {
			"works.publicationDate": 5,		#	1	2	2
			"works.type": 1,				#	0	1	0
			"works.country": 3				#	0	2	1
		}


	def _calc(self, df_persons, df_works, spark):
		result = {}
		
		# publicaiton year: 2000 - current
		current_year = datetime.now().year
		df_years = spark.createDataFrame([Row(str(y)) for y in range(2000, current_year + 1)], ["year"])
		df_years_missing = df_years.join(df_works, df_years["year"] == substring(df_works["date"], 0, 4), "leftanti")
		year_population = 1 - df_years_missing.count() / (current_year + 1 - 2000)
		result["works.publicationDate"] = (year_population, df_years_missing)
		
		# publication types
		df_types = spark.createDataFrame([Row(t) for t in MinPopulation.TYPES], ["type"])
		df_types_missing = df_types.join(df_works, df_types["type"] == df_works["type"], "leftanti")				
		type_population = 1 - df_types_missing.count() / len(MinPopulation.TYPES)
		result["works.type"] = (type_population, df_types_missing)
	
		# countries
		df_country = spark.createDataFrame([Row(c) for c in MinPopulation.COUNTRIES], ["country"])
		df_country_missing = df_country.join(df_persons, df_country["country"] == df_persons["country"], "leftanti")
		country_population = 1 - df_country_missing.count() / len(MinPopulation.COUNTRIES)
		result["works.country"] = (country_population, df_country_missing)

		return result




class MinObject(Metric):
	def _weights(self):
								#	works
								#		persons
		return {
			"works": 3,			#	1	2
			"persons": 1		#	0	1
		}

	def _calc(self, df_persons, df_works, spark):
		result = {}
		
		# works
		works_num = df_works.count()
		df_works_valid = df_works.where(df_works["title"].isNotNull() & \
										(df_works["url"].isNotNull() | \
										df_works["publicationID"].isNotNull() & array_contains("publicationID.type", "doi")))
		works_valid = df_works_valid.count() / works_num
		result["works"] = (works_valid, df_works.subtract(df_works_valid))

		# persons
		persons_num = df_persons.count()
		df_persons_valid = df_persons.where(df_persons["orcid_id"].isNotNull() & \
												((df_persons["firstName"].isNotNull() & df_persons["lastName"].isNotNull()) | \
												df_persons["publishedName"].isNotNull()))
		persons_valid = df_persons_valid.count() / persons_num
		result["persons"] = (persons_valid, df_persons.subtract(df_persons_valid))
			

		return result

		
class Completeness(Metric):
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

	def _calc(self, df_persons, df_works, spark, sample_num):
		result = {}
		
		result.update(MinLength()._calc(df_persons, df_works, spark))
		result.update(MinValue()._calc(df_persons, df_works, spark))
		result.update(NotNull()._calc(df_persons, df_works, spark))
		result.update(MinPopulation()._calc(df_persons, df_works, spark))
		result.update(MinObject()._calc(df_persons, df_works, spark))
		
		
		return result
		