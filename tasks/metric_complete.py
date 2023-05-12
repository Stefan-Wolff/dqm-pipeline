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