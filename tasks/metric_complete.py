from lib.metrics import Metric
from pyspark.sql.functions import length, array_contains


class Completeness1(Metric):
	def calc(self, df_persons, df_works):
		result = {}
		entities = {"persons": df_persons, "works": df_works}
		
		for entity, dataFrame in entities.items():
			row_num = dataFrame.count()
			
			for col in dataFrame.columns:
				count = dataFrame.where(dataFrame[col].isNotNull()).count()
				indicator = count / row_num

				result.update(self.formateResult(entity, col, indicator))

		return result


class Completeness2(Metric):
	def calc(self, df_persons, df_works):
		result = {}
		
		date_num = df_works.where(df_works["date"].isNotNull()).count()
		
		# variants: [YYYY], [YYYY-MM], [YYYY-MM-DD]
		year = df_works.where(4 == length("date")).count() / date_num
		year_month = df_works.where(7 == length("date")).count() / date_num
		year_month_day = df_works.where(10 == length("date")).count() / date_num
		
		result.update(self.formateResult("works", "year", year))
		result.update(self.formateResult("works", "year-month", year_month))
		result.update(self.formateResult("works", "year-month-day", year_month_day))

		return result


class Completeness3(Metric):
	def calc(self, df_persons, df_works):
		result = {}
		
		works_num = df_works.count()
		works_complete = df_works.where(df_works["title"].isNotNull() & \
										(df_works["url"].isNotNull() | \
										df_works["publicationID"].isNotNull() & array_contains("publicationID.type", "doi"))).count() / works_num

		persons_num = df_persons.count()
		persons_complete = df_persons.where(df_persons["orcid_id"].isNotNull() & \
												((df_persons["firstName"].isNotNull() & df_persons["lastName"].isNotNull()) | \
												df_persons["publishedName"].isNotNull())).count() / persons_num

		result.update(self.formateResult("works", "min-data", works_complete))
		result.update(self.formateResult("persons", "min-data", persons_complete))

		return result
		
		
class Completeness4(Metric):
	def calc(self, df_persons, df_works):
		pass									# nothing to check
		
		
class Completeness5(Metric):
	def calc(self, df_persons, df_works):
		result = {}
		
		works_title = df_works.where(6 <= length(df_works["title"])).count() / df_works.where(df_works["title"].isNotNull()).count()
		works_journal = df_works.where(2 <= length(df_works["journal_title"])).count() / df_works.where(df_works["journal_title"].isNotNull()).count()
		works_description = df_works.where(12 <= length(df_works["short_description"])).count() / df_works.where(df_works["short_description"].isNotNull()).count()
		works_subTitle = df_works.where(6 <= length(df_works["subTitle"])).count() / df_works.where(df_works["subTitle"].isNotNull()).count()
		
		person_firstName = df_persons.where(2 <= length(df_persons["firstName"])).count() / df_persons.where(df_persons["firstName"].isNotNull()).count()
		person_lastName = df_persons.where(2 <= length(df_persons["lastName"])).count() / df_persons.where(df_persons["lastName"].isNotNull()).count()
		person_otherNames = df_persons.where(5 <= length(df_persons["otherNames"])).count() / df_persons.where(df_persons["otherNames"].isNotNull()).count()
		person_publishedName = df_persons.where(5 <= length(df_persons["publishedName"])).count() / df_persons.where(df_persons["publishedName"].isNotNull()).count()


		result.update(self.formateResult("works", "minLength.title", works_title))
		result.update(self.formateResult("works", "minLength.journal_title", works_journal))
		result.update(self.formateResult("works", "minLength.short_description", works_description))
		result.update(self.formateResult("works", "minLength.subTitle", works_subTitle))
		result.update(self.formateResult("persons", "minLength.firstName", person_firstName))
		result.update(self.formateResult("persons", "minLength.lastName", person_lastName))
		result.update(self.formateResult("persons", "minLength.otherNames", person_otherNames))
		result.update(self.formateResult("persons", "minLength.publishedName", person_publishedName))


		return result