from lib.metrics import Metric
from pyspark.sql.functions import length, array_contains, substring
from datetime import datetime
from pyspark.sql import Row


class Completeness1(Metric):
	def calc(self, df_persons, df_works, spark, sample_num):
		result = {}
		entities = {"persons": df_persons, "works": df_works}
		
		for entity, dataFrame in entities.items():
			row_num = dataFrame.count()
			
			for col in dataFrame.columns:
				df_valid = dataFrame.where(dataFrame[col].isNotNull())
				indicator = df_valid.count() / row_num
				
				result.update(self.formateResult(entity, col, indicator))
				self.showSample(dataFrame.subtract(df_valid), sample_num, entity + "." + col)

		return result
	



class Completeness2(Metric):
	def calc(self, df_persons, df_works, spark, sample_num):
		result = {}
		
		date_num = df_works.where(df_works["date"].isNotNull()).count()
		
		# [YYYY]
		df_year = df_works.where(4 == length("date"))
		year = df_year.count() / date_num
		
		result.update(self.formateResult("works", "year", year))
		self.showSample(df_works.subtract(df_year), sample_num, "works.year")
		
		
		# [YYYY-MM]
		df_year_month = df_works.where(7 == length("date"))
		year_month = df_year_month.count() / date_num
		
		result.update(self.formateResult("works", "year-month", year_month))
		self.showSample(df_works.subtract(df_year_month), sample_num, "works.year-month")
		
		
		# [YYYY-MM-DD]
		df_year_month_day = df_works.where(10 == length("date"))
		year_month_day = df_year_month_day.count() / date_num
		
		result.update(self.formateResult("works", "year-month-day", year_month_day))
		self.showSample(df_works.subtract(df_year_month_day), sample_num, "works.year-month")
			
			
		return result


class Completeness3(Metric):
	def calc(self, df_persons, df_works, spark, sample_num):
		result = {}
		
		# works
		works_num = df_works.count()
		df_works_valid = df_works.where(df_works["title"].isNotNull() & \
										(df_works["url"].isNotNull() | \
										df_works["publicationID"].isNotNull() & array_contains("publicationID.type", "doi")))
		works_valid = df_works_valid.count() / works_num
		
		result.update(self.formateResult("works", "min-data", works_valid))
		self.showSample(df_works.subtract(df_works_valid), sample_num, "works.min-data")


		# persons
		persons_num = df_persons.count()
		df_persons_valid = df_persons.where(df_persons["orcid_id"].isNotNull() & \
												((df_persons["firstName"].isNotNull() & df_persons["lastName"].isNotNull()) | \
												df_persons["publishedName"].isNotNull()))
		persons_valid = df_persons_valid.count() / persons_num
		
		result.update(self.formateResult("persons", "min-data", persons_valid))
		self.showSample(df_persons.subtract(df_persons_valid), sample_num, "persons.min-data")
			

		return result
		
		
class Completeness4(Metric):
	def calc(self, df_persons, df_works, spark, sample_num):
		pass									# nothing to check
		
		
class Completeness5(Metric):
	def calc(self, df_persons, df_works, spark, sample_num):
		result = {}
		
		# works.title
		df_works_title = df_works.where(6 <= length(df_works["title"]))
		works_title = df_works_title.count() / df_works.where(df_works["title"].isNotNull()).count()
		
		result.update(self.formateResult("works", "minLength.title", works_title))
		self.showSample(df_works.subtract(df_works_title), sample_num, "works.minLength.title")
		
		
		# works.journal_title
		df_works_journal = df_works.where(2 <= length(df_works["journal_title"]))
		works_journal = df_works_journal.count() / df_works.where(df_works["journal_title"].isNotNull()).count()
		
		result.update(self.formateResult("works", "minLength.journal_title", works_journal))
		self.showSample(df_works.subtract(df_works_journal), sample_num, "works.minLength.journal_title")
		
		
		# works.short_description
		df_works_description = df_works.where(12 <= length(df_works["short_description"]))
		works_description = df_works_description.count() / df_works.where(df_works["short_description"].isNotNull()).count()
		
		result.update(self.formateResult("works", "minLength.short_description", works_description))
		self.showSample(df_works.subtract(df_works_description), sample_num, "works.minLength.short_description")
		
		
		# works.subTitle
		df_works_subTitle = df_works.where(6 <= length(df_works["subTitle"]))
		works_subTitle = df_works_subTitle.count() / df_works.where(df_works["subTitle"].isNotNull()).count()
		
		result.update(self.formateResult("works", "minLength.subTitle", works_subTitle))
		self.showSample(df_works.subtract(df_works_subTitle), sample_num, "works.minLength.subTitle")
		
		
		# persons.firstName
		df_person_firstName = df_persons.where(2 <= length(df_persons["firstName"]))
		person_firstName = df_person_firstName.count() / df_persons.where(df_persons["firstName"].isNotNull()).count()
		
		result.update(self.formateResult("persons", "minLength.firstName", person_firstName))
		self.showSample(df_persons.subtract(df_person_firstName), sample_num, "persons.minLength.firstName")
		
		
		# persons.lastName
		df_person_lastName = df_persons.where(2 <= length(df_persons["lastName"]))
		person_lastName = df_person_lastName.count() / df_persons.where(df_persons["lastName"].isNotNull()).count()
		
		result.update(self.formateResult("persons", "minLength.lastName", person_lastName))
		self.showSample(df_persons.subtract(df_person_firstName), sample_num, "persons.minLength.lastName")
		
		
		# TODO
		person_otherNames = df_persons.where(5 <= length(df_persons["otherNames"])).count() / df_persons.where(df_persons["otherNames"].isNotNull()).count()
		person_publishedName = df_persons.where(5 <= length(df_persons["publishedName"])).count() / df_persons.where(df_persons["publishedName"].isNotNull()).count()


		

		


		result.update(self.formateResult("persons", "minLength.lastName", person_lastName))
		result.update(self.formateResult("persons", "minLength.otherNames", person_otherNames))
		result.update(self.formateResult("persons", "minLength.publishedName", person_publishedName))


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


	def calc(self, df_persons, df_works, spark, sample_num):
		result = {}
		
		# years: 2000 - current year
		current_year = datetime.now().year
		
		df_years = spark.createDataFrame([Row(str(y)) for y in range(2000, current_year + 1)], ["year"])
		year_rows = df_years.join(df_works, df_years["year"] == substring(df_works["date"], 0, 4), "leftanti").collect()
		missing_years = [row["year"] for row in year_rows]
				
		year_population = 1 - len(missing_years) / (current_year + 1 - 2000)

		
		# publication types
		df_types = spark.createDataFrame([Row(t) for t in Completeness6.TYPES], ["type"])
		type_rows = df_types.join(df_works, df_types["type"] == df_works["type"], "leftanti").collect()
		missing_types = [row["type"] for row in type_rows]
				
		type_population = 1 - len(missing_types) / len(Completeness6.TYPES)
				
	
		# countries
		df_country = spark.createDataFrame([Row(c) for c in Completeness6.COUNTRIES], ["country"])
		country_rows = df_country.join(df_persons, df_country["country"] == df_persons["country"], "leftanti").collect()
		missing_countries = [row["country"] for row in country_rows]
				
		country_population = 1 - len(missing_countries) / len(Completeness6.COUNTRIES)
		
				
		# results
		result.update(self.formateResult("works", "population.publicationDate", year_population))
		if missing_years:
			result.update(self.addResult("works", "population.publicationDate.missings", str(missing_years)))
		
		
		result.update(self.formateResult("works", "population.type", type_population))
		if missing_types:
			result.update(self.addResult("works", "population.type.missings", str(missing_types)))


		result.update(self.formateResult("persons", "population.country", country_population))
		if missing_types:
			result.update(self.addResult("persons", "population.country.missings", str(missing_countries)))


		return result