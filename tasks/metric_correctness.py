import datetime
from pyspark.sql.functions import explode, year
from pyspark.sql.types import IntegerType
from lib.metrics import Metric


# persons.affiliations.orgName
class CorrectValue_orgName(Metric):
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


class CorrectSyntax(Metric):

	CONFIG = {
		"works": {
			"bibtex": r'@[a-z]*[A-Z]*[\s]*\{[\S\s]*,[\S\s]*\}',
			"orcid_id": r'[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9|X]',
			"orcid_publication_id": r'[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9|X]_works_[0-9]*',
			"date": r'[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?',
			"url": r'(http://|https://)[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}',
		#	"doi": r'10[.][0-9]{4,}[^\s"\/<>]*\/[^\s"<>]+',
			"issn": r'\d{4}-\d{3}[\dxX]',
			"isbn": r'([0-9X\-]{13}+)|([0-9X\-]{10}+)'
		},
		"persons": {
			"orcid_id": r'[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9|X]',
			"country": r'[A-Z]{2}'
		}
	}
	
	CONFIG_ORG_ID = {
		"GRID": r'grid\.[0-9]+\.[0-9]+',
		"LEI": r'[0-9]{6}[A-Z|0-9]{12}[0-9]{2}',
		"FUNDREF": r'(.*doi.org/)?10[.][0-9]{4,}[^\s"\/<>]*\/[^\s"<>]+',
		"RINGGOLD": r'\d{1,6}',
		"ROR": r'((https://)?ror.org/)?[0-9|a-z]{9}'
	}
	
	
	YEAR = r'\d{4}'
	
	def _weights(self):
		return {
			"works.bibtex": 1,
			"works.orcid_id": 1,
			"works.authors.orcid_id": 1,
			"works.orcid_publication_id": 1,
			"works.date": 1,
			"works.url": 1,
			"works.doi": 1,
			"works.issn": 1,
			"works.isbn": 1,
			"persons.orcid_id": 1,
			"persons.country": 1,
			"persons.affiliations.startYear": 1,
			"persons.affiliations.endYear": 1,
			"persons.affiliations.orgID": 1,
		}


	def _calc(self, df_persons, df_works, spark):
		result = {}
		
		# standard fields
		for entity, patterns in CorrectSyntax.CONFIG.items():
			dataFrame = df_persons if ("persons" == entity) else df_works
			for field, pattern in patterns.items():
				df_notNull = dataFrame.where(dataFrame[field].isNotNull())
				df_invalid = df_notNull.where(~df_notNull[field].rlike(pattern))
				
				indicator = 1 - df_invalid.count() / df_notNull.count()
				
				result[entity + "." + field] = (indicator, df_invalid)
		
		
		# persons.affiliations.orgID
		df_affiliations = df_persons.withColumn("affil_exploded", explode("affiliations"))
		indicator_sum = 0
		df_invalid_all = None
		for id_type, pattern in CorrectSyntax.CONFIG_ORG_ID.items():
			
			df_notNull = df_affiliations.where(id_type == df_affiliations["affil_exploded.orgIDType"])
			df_invalid = df_notNull.where(~df_notNull["affil_exploded.orgID"].rlike(pattern))
				
			indicator = 1 - df_invalid.count() / df_notNull.count()
			
			
			result["persons.affiliations.orgID."+id_type] = (indicator, df_invalid)
		
			indicator_sum += indicator
			df_invalid_all = df_invalid_all.append(df_invalid) if df_invalid_all else df_invalid
				
		result["persons.affiliations.orgID"] = (indicator_sum / len(CorrectSyntax.CONFIG_ORG_ID), df_invalid_all)
		
		
		# persons.affiliations.startYear
		for field in ["startYear", "endYear"]:
			df_year = df_affiliations.where(df_affiliations["affil_exploded." + field].isNotNull())
			df_year_invalid = df_year.where(~df_year["affil_exploded." + field].rlike(CorrectSyntax.YEAR))
			indicator = 1 - df_year_invalid.count() / df_year.count()
			
			result["persons.affiliations." + field] = (indicator, df_year_invalid)
		
		
		
		# works.authors.orcid_id
		df_authors = df_works.where(df_works["authors"].isNotNull()) \
							 .withColumn("authors_exploded", explode("authors"))
							 
		df_authors_invalid = df_authors.where(~df_authors["authors_exploded.orcid_id"].rlike(CorrectSyntax.CONFIG["persons"]["orcid_id"]))
		
		result["works.authors.orcid_id"] = (1 - df_authors_invalid.count() / df_authors.count(), df_authors_invalid)
		
		

		
		
		return result