import datetime
from pyspark.sql.functions import explode, year
from pyspark.sql.types import IntegerType
from lib.metrics import Metric


class ContentCorrect8(Metric):
	def _weights(self):
		return {								# same weights because all ids resulting in the same attribute: organization name
			"orgUnit.grid": 1,
			"orgUnit.ror": 1,
			"orgUnit.lei": 1,
			"orgUnit.fundref": 1
		}
	
	def _calc(self, df_persons, df_works, spark):
		result = {}
		
		df_affiliations = df_persons.withColumn("affil_exploded", explode("affiliations"))
		df_ror = spark.read.csv("data/ROR.csv", header=True)
		
		# GRID
		df_grid = df_affiliations.where(df_affiliations["affil_exploded.orgIDType"] == "GRID")
		df_grid_incorrect = df_grid.join(df_ror, (df_grid["affil_exploded.orgID"] == df_ror["`external_ids.GRID.preferred`"]) & \
												 (df_grid["affil_exploded.orgName"] != df_ror["name"]) \
												 , 'inner')
		grid_num = df_grid.count()
		grid_correct = (grid_num - df_grid_incorrect.count()) / grid_num
		result["orgUnit.grid"] = (grid_correct, df_grid_incorrect)
		
		# ROR
		df_ror_intern = df_affiliations.where(df_affiliations["affil_exploded.orgIDType"] == "ROR")
		df_ror_incorrect = df_ror_intern.join(df_ror, (df_ror_intern["affil_exploded.orgID"] == df_ror["id"]) & \
													  (df_ror_intern["affil_exploded.orgName"] != df_ror["name"]) \
													  , 'inner')
		ror_num = df_ror_intern.count()
		ror_correct = (ror_num - df_ror_incorrect.count()) / ror_num
		result["orgUnit.ror"] = (ror_correct, df_ror_incorrect)

		# LEI
		df_lei = spark.read.csv("data/LEI.csv", header=True)
		df_lei_intern = df_affiliations.where(df_affiliations["affil_exploded.orgIDType"] == "LEI")
		df_lei_incorrect = df_lei_intern.join(df_lei, (df_lei_intern["affil_exploded.orgID"] == df_lei["id"]) & \
													  (df_lei_intern["affil_exploded.orgName"] != df_lei["name"]) \
													  , 'inner')
		lei_num = df_lei_intern.count()
		lei_correct = (lei_num - df_lei_incorrect.count()) / lei_num
		result["orgUnit.lei"] = (lei_correct, df_lei_incorrect)

		# FUNDREF
		df_fundref = spark.read.csv("data/FUNDREF.csv", header=True)
		df_fundref_intern = df_affiliations.where(df_affiliations["affil_exploded.orgIDType"] == "FUNDREF")
		df_fundref_incorrect = df_fundref_intern.join(df_fundref, (df_fundref_intern["affil_exploded.orgID"] == df_fundref["uri"]) & \
																  (df_fundref_intern["affil_exploded.orgName"] != df_fundref["primary_name_display"]) \
																  , 'inner')
		fundref_num = df_fundref_intern.count()
		fundref_correct = (fundref_num - df_fundref_incorrect.count()) / fundref_num
		result["orgUnit.fundref"] = (fundref_correct, df_fundref_incorrect)

	
		return result
		
	
class RangeCorrect8(Metric):
	def _weights(self):
														#	works.date
														#		persons.affiliations.startYear
														#			persons.affiliations.endYear
		return {
			"works.date": 5,						#	1	2	2
			"persons.affiliations.startYear": 2,	#	0	1	1
			"persons.affiliations.endYear": 2		#	0	1	1
		}
	
	def _calc(self, df_persons, df_works, spark):
		result = {}
		
		currentYear = datetime.datetime.now().year
		
		# works.date
		df_date = df_works.where(df_works["date"].isNotNull()) \
						  .withColumn("date_value", df_works["date"].cast(IntegerType()))
		df_date_incorrect = df_date.where((df_date["date_value"] > currentYear) | (df_date["date_value"] < 0))

		date_num = df_date.count()
		date_correct = (date_num - df_date_incorrect.count()) / date_num
		result["works.date"] = (date_correct, df_date_incorrect)
		
		print(date_correct)
		
		# persons.affiliations.startYear
		# persons.affiliations.endYear
		df_affiliations = df_persons.withColumn("affil_exploded", explode("affiliations"))
		for field in ["startYear", "endYear"]:
			df_year = df_affiliations.where(df_affiliations["affil_exploded." + field].isNotNull()) \
									 .withColumn("date_value", df_affiliations["affil_exploded." + field].cast(IntegerType()))
			df_year_incorrect = df_year.where((df_year["date_value"] > currentYear) | (df_year["date_value"] < 0))

			year_num = df_year.count()
			year_correct = (year_num - df_year_incorrect.count()) / year_num
			result["persons.affiliations." + field] = (year_correct, df_year_incorrect)

			print(year_correct)

		
		return result
		
