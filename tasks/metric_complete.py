from lib.metrics import Metric
from pyspark.sql.functions import length, array_contains, substring
from datetime import datetime
from pyspark.sql import Row


class Completeness1(Metric):
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

	def calc(self, df_persons, df_works, spark):
		result = {}
		entities = {"persons": df_persons, "works": df_works}
		
		for entity, dataFrame in entities.items():
			row_num = dataFrame.count()
			
			for col in dataFrame.columns:
				df_valid = dataFrame.where(dataFrame[col].isNotNull())
				indicator = df_valid.count() / row_num
				result[entity + "." + col] = (indicator, dataFrame.subtract(df_valid))

		return result
	



class Completeness2(Metric):
	def _weights(self):
										#	works.year
										#		works.year-month
										#			works.year-month-day
		return {
			"works.year": 5,			#	1	2	2
			"works.year-month": 3,		#	0	1	2
			"works.year-month-day": 1	#	0	0	1
		}

	def _calc(self, df_persons, df_works, spark):
		result = {}
		date_num = df_works.where(df_works["date"].isNotNull()).count()
		
		# [YYYY]
		df_year = df_works.where(4 == length("date"))
		year = df_year.count() / date_num
		result["works.year"] = (year, df_works.subtract(df_year))
		
		# [YYYY-MM]
		df_year_month = df_works.where(7 == length("date"))
		year_month = df_year_month.count() / date_num
		result["works.year-month"] = (year_month, df_works.subtract(df_year_month))
		
		# [YYYY-MM-DD]
		df_year_month_day = df_works.where(10 == length("date"))
		year_month_day = df_year_month_day.count() / date_num
		result["works.year-month-day"] = (year_month_day, df_works.subtract(df_year_month_day))
			
			
		return result


class Completeness3(Metric):
	def _weights(self):
									#	works.min-data
									#		persons.min-data
		return {
			"works.min-data": 3,	#	1	2
			"persons.min-data": 1	#	0	1
		}

	def _calc(self, df_persons, df_works, spark):
		result = {}
		
		# works
		works_num = df_works.count()
		df_works_valid = df_works.where(df_works["title"].isNotNull() & \
										(df_works["url"].isNotNull() | \
										df_works["publicationID"].isNotNull() & array_contains("publicationID.type", "doi")))
		works_valid = df_works_valid.count() / works_num
		result["works.min-data"] = (works_valid, df_works.subtract(df_works_valid))

		# persons
		persons_num = df_persons.count()
		df_persons_valid = df_persons.where(df_persons["orcid_id"].isNotNull() & \
												((df_persons["firstName"].isNotNull() & df_persons["lastName"].isNotNull()) | \
												df_persons["publishedName"].isNotNull()))
		persons_valid = df_persons_valid.count() / persons_num
		result["persons.min-data"] = (persons_valid, df_persons.subtract(df_persons_valid))
			

		return result
		
		
class Completeness4(Metric):
	def _calc(self, df_persons, df_works, spark):
		pass									# nothing to check
		
		
class Completeness5(Metric):
	def _weights(self):
														#	works.minLength.title
														#		works.minLength.journal_title
														#			works.minLength.short_description
														#				works.minLength.subTitle
														#					persons.minLength.firstName
														#						persons.minLength.lastName
														#							persons.minLength.otherNames
														#								persons.minLength.publishedName
		return {
			"works.minLength.title": 14,				#	1	2	1	2	2	2	2	2
			"works.minLength.journal_title": 6,			#	0	1	0	1	1	1	1	1
			"works.minLength.short_description": 14,	#	1	2	1	2	2	2	2	2
			"works.minLength.subTitle": 9,				#	0	1	0	1	2	1	2	2	
			"persons.minLength.firstName": 5,			#	0	1	0	0	1	1	1	1
			"persons.minLength.lastName": 6,			#	0	1	0	1	1	1	1	1
			"persons.minLength.otherNames": 5,			#	0	1	0	0	1	1	1	1
			"persons.minLength.publishedName": 5		#	0	1	0	0	1	1	1	1
		}

	def _calc(self, df_persons, df_works, spark):
		result = {}
		
		# works.title
		df_works_title = df_works.where(6 <= length(df_works["title"]))
		works_title = df_works_title.count() / df_works.where(df_works["title"].isNotNull()).count()
		result["works.minLength.title"] = (works_title, df_works.subtract(df_works_title))
		
		# works.journal_title
		df_works_journal = df_works.where(2 <= length(df_works["journal_title"]))
		works_journal = df_works_journal.count() / df_works.where(df_works["journal_title"].isNotNull()).count()
		result["works.minLength.journal_title"] = (works_journal, df_works.subtract(df_works_journal))
		
		# works.short_description
		df_works_description = df_works.where(12 <= length(df_works["short_description"]))
		works_description = df_works_description.count() / df_works.where(df_works["short_description"].isNotNull()).count()
		result["works.minLength.short_description"] = (works_description, df_works.subtract(df_works_description))
		
		# works.subTitle
		df_works_subTitle = df_works.where(6 <= length(df_works["subTitle"]))
		works_subTitle = df_works_subTitle.count() / df_works.where(df_works["subTitle"].isNotNull()).count()
		result["works.minLength.subTitle"] = (works_subTitle, df_works.subtract(df_works_subTitle))
		
		# persons.firstName
		df_person_firstName = df_persons.where(2 <= length(df_persons["firstName"]))
		person_firstName = df_person_firstName.count() / df_persons.where(df_persons["firstName"].isNotNull()).count()
		result["persons.minLength.firstName"] = (person_firstName, df_persons.subtract(df_person_firstName))
		
		# persons.lastName
		df_person_lastName = df_persons.where(2 <= length(df_persons["lastName"]))
		person_lastName = df_person_lastName.count() / df_persons.where(df_persons["lastName"].isNotNull()).count()
		result["persons.minLength.lastName"] = (person_lastName, df_persons.subtract(df_person_firstName))
		
		# persons.otherNames
		df_person_otherNames = df_persons.where(5 <= length(df_persons["otherNames"]))
		person_otherNames = df_person_otherNames.count() / df_persons.where(df_persons["otherNames"].isNotNull()).count()
		result["persons.minLength.otherNames"] = (person_otherNames, df_persons.subtract(df_person_firstName))
		
		# persons.publishedName
		df_person_publishedName = df_persons.where(5 <= length(df_persons["publishedName"]))
		person_publishedName = df_person_publishedName.count() / df_persons.where(df_persons["publishedName"].isNotNull()).count()
		result["persons.minLength.publishedName"] = (person_publishedName, df_persons.subtract(df_person_firstName))


		return result
		
		
class Completeness6(Metric):

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
														#	works.population.publicationDate
														#		works.population.type
														#			works.population.country
		return {
			"works.population.publicationDate": 5,		#	1	2	2
			"works.population.type": 1,					#	0	1	0
			"works.population.country": 3				#	0	2	1
		}


	def _calc(self, df_persons, df_works, spark):
		result = {}
		current_year = datetime.now().year
		
		df_years = spark.createDataFrame([Row(str(y)) for y in range(2000, current_year + 1)], ["year"])
		df_years_missing = df_years.join(df_works, df_years["year"] == substring(df_works["date"], 0, 4), "leftanti")
		year_population = 1 - len(df_years_missing.count()) / (current_year + 1 - 2000)
		result["works.population.publicationDate"] = (year_population, df_years_missing)
		
		# publication types
		df_types = spark.createDataFrame([Row(t) for t in Completeness6.TYPES], ["type"])
		df_types_missing = df_types.join(df_works, df_types["type"] == df_works["type"], "leftanti")				
		type_population = 1 - len(df_types_missing.count()) / len(Completeness6.TYPES)
		result["works.population.type"] = (type_population, df_types_missing)
	
		# countries
		df_country = spark.createDataFrame([Row(c) for c in Completeness6.COUNTRIES], ["country"])
		df_country_missing = df_country.join(df_persons, df_country["country"] == df_persons["country"], "leftanti")
		country_population = 1 - len(df_country_missing.count()) / len(Completeness6.COUNTRIES)
		result["works.population.country"] = (country_population, df_country_missing)

		return result