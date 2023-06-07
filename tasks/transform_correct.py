from pyspark.sql.functions import lit, col, regexp_replace, length, when, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from .metric_correctness import Correctness
from .metric_completeness import MinLength
import re

				
class CorrectOrgs:

	def run(self, dataFrames, spark):
		df_orgs = dataFrames["orgUnits"]
		
		sources = {
			"GRID":		{"df_name": "ROR",		"id_field": "`external_ids.GRID.preferred`",	"name_field": "name"},
			"ROR":		{"df_name": "ROR",		"id_field": "id",								"name_field": "name"},
			"LEI":		{"df_name": "lei",		"id_field": "id",								"name_field": "name"},
			"FUNDREF":	{"df_name": "fundref",	"id_field": "uri",								"name_field": "primary_name_display"}
		}

		df_joined = df_orgs

		for id_type, config in sources.items():
			df_right = dataFrames[config["df_name"]]
			
			# rename columns to prevent ambiguous column names
			for col_name in df_right.columns:
				df_right = df_right.withColumnRenamed(col_name, "r_" + col_name)
		
			# join & take name
			id_field = config["id_field"].replace("`", "`r_", 1) if (config["id_field"].startswith("`")) else "r_" + config["id_field"]
			df_joined = df_orgs.join(df_right, (df_orgs["type"] == lit(id_type)) & \
											   (df_orgs["id"] == df_right[id_field])
									, 'left')	\
								.withColumn("name", col("r_" + config["name_field"]))	\
								.select(df_orgs.columns)
								
				
		return {
			"orgUnits": df_joined
		}
		
	
class CorrectMinLength:

	def run(self, dataFrames, spark):
		result = {}
		
		df_persons = dataFrames["persons"]
		df_works = dataFrames["works"]

		df_persons_cor = df_persons
		df_works_cor = df_works

		for key, minNum in MinLength.MIN_NUMS.items():
			levels = key.split(".")
			field = levels[-1]
			
			if "persons" == levels[0]:
				df_persons_cor = df_persons_cor.withColumn(field, when(minNum > length(col(field)), lit(None)).otherwise(col(field)))
			else:
				df_works_cor = df_works_cor.withColumn(field, when(minNum > length(col(field)), lit(None)).otherwise(col(field)))
			
		
		return {
			"persons": df_persons_cor,
			"works": df_works_cor
		}
		
	
class CorrectValues:
	AFFIL_SCHEMA = ArrayType(StructType([ \
								StructField('role', StringType()), \
								StructField('orgID', StringType()), \
								StructField('startYear', StringType()), \
								StructField('endYear', StringType())]))

	ISSN = Correctness.CONFIG["works"]["issn"]["pattern"]
	ISBN = Correctness.CONFIG["works"]["isbn"]["pattern"]
	URL = Correctness.CONFIG["works"]["url"]["pattern"]
	DOI = Correctness.CONFIG["works"]["doi"]["pattern"]
	ABSTRACT = Correctness.CONFIG["works"]["abstract"]["pattern"]
	
	ABSTRACT_REPLACE = Correctness.INVALID_ABSTRACT + "|" + Correctness.INVALID_TEXT
	TITLE_REPLACE = Correctness.INVALID_TEXT + r'|[\n\r\t]'

	INVALID_ALPHABET = Correctness.INVALID_ALPHABET
	INVALID_NAMES = Correctness.INVALID_NAMES

	YEAR = Correctness.CONFIG["persons"]["affiliations.startYear"]["pattern"]


	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
		df_persons = dataFrames["persons"]
		
		df_works_cor = df_works
		df_persons_cor = df_persons
		
		# issn
		df_works_cor = df_works_cor.withColumn("issn_replaced", regexp_replace(regexp_replace("issn", r'([iI][sS][sS][nN])| ', ''), r'[./_]', '-'))	\
								   .withColumn("issn", when(col("issn_replaced").rlike(CorrectValues.ISSN), col("issn_replaced")).otherwise(lit(None)))				
		# isbn
		df_works_cor = df_works_cor.withColumn("isbn_replaced", regexp_replace(regexp_replace("isbn", r'([iI][sS][bB][nN])| ', ''), r'[./_]', '-'))	\
								   .withColumn("isbn", when(col("isbn_replaced").rlike(CorrectValues.ISBN), col("isbn_replaced")).otherwise(lit(None)))
		# url
		df_works_cor = df_works_cor.withColumn("url_replaced", regexp_replace("url", r'[\\ ]', ''))	\
								   .withColumn("url", when(col("url_replaced").rlike(CorrectValues.URL), col("url_replaced")).otherwise(lit(None)))
		# doi
		df_works_cor = df_works_cor.withColumn("doi_replaced", regexp_replace("doi", r'((http[s]?://)?(((www\.)|(dx\.))?doi\.org/))|[ ]', ''))	\
								   .withColumn("doi", when(col("doi_replaced").rlike(CorrectValues.DOI), col("doi_replaced")).otherwise(lit(None)))
		# abstract
		df_works_cor = df_works_cor.withColumn("abstract_replaced", regexp_replace(regexp_replace(regexp_replace("abstract", CorrectValues.ABSTRACT_REPLACE, ''), '[ ]{2,}', ' '), '^[ ]|[ ]$', ''))	\
								   .withColumn("abstract", when(col("abstract").rlike(CorrectValues.ABSTRACT), col("abstract_replaced")).otherwise(lit(None)))
					
		# alphabet in works
		for field in ["abstract", "title", "subTitle"]:
			df_works_cor = df_works_cor.withColumn(field, when(~col(field).rlike(CorrectValues.INVALID_ALPHABET), col(field)).otherwise(lit(None)))
					
		# names in persons
		for field in ["firstName", "lastName", "otherNames", "publishedName"]:
			df_persons_cor = df_persons_cor.withColumn(field, when(col(field).rlike(CorrectValues.INVALID_ALPHABET), lit(None)).otherwise(col(field)))	\
										   .withColumn(field, regexp_replace(regexp_replace(field, r'\(.*\)', ''), r'^[ ]|[ ]$', ''))	\
										   .withColumn(field, when(col(field).rlike(CorrectValues.INVALID_NAMES), lit(None)).otherwise(col(field)))
			   
		# title
		df_works_cor = df_works_cor.withColumn("title_replaced", regexp_replace(regexp_replace(regexp_replace("title", CorrectValues.TITLE_REPLACE, ''), '[ ]{2,}', ' '), '^[ ]|[ ]$', ''))	\
								   .withColumn("title", when(col("title_replaced") != "", col("title_replaced")).otherwise(lit(None)))
		# subTitle
		df_works_cor = df_works_cor.withColumn("subTitle_replaced", regexp_replace(regexp_replace(regexp_replace("subTitle", CorrectValues.TITLE_REPLACE, ''), '[ ]{2,}', ' '), '^[ ]|[ ]$', ''))	\
								   .withColumn("subTitle", when(col("subTitle_replaced") != "", col("subTitle_replaced")).otherwise(lit(None)))
		
		
		# affiliations.startYear & affiliations.endYear
		cust_affil = udf(lambda affils: self.__correctAffil(affils), CorrectValues.AFFIL_SCHEMA)
		df_persons_cor = df_persons_cor.withColumn("affiliations", cust_affil(col("affiliations")))
		
		
		return {
			"works": df_works_cor.select(df_works.columns),
			"persons": df_persons_cor.select(df_persons.columns)
		}

	def __correctAffil(self, affils):
		if not affils:
			return None
			
		result = []
		for affil in affils:
			entry = {}
			for field in ["startYear", "endYear"]:
				if field in affil and affil[field]:
					if re.search(CorrectValues.YEAR, affil[field]):
						entry[field] = affil[field]
						
			if "orgID" in affil and affil["orgID"]:
				entry["orgID"] = affil["orgID"]
			if "role" in affil and affil["role"]:
				entry["role"] = affil["role"]
				
			if entry:
				result.append(entry)
				

		return result if result else None



class CorrectContradict:

	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
		
		return {
			"works": df_works.withColumn("isbn", when(col("issn").isNotNull(), lit(None)).otherwise(col("isbn")))
		}


class CorrectDuplIDs:

	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
		df_persons = dataFrames["persons"]
		
		df_works_unique = df_works.dropDuplicates(["doi"])	\
								  .dropDuplicates(["bibtex"])	\
								  .dropDuplicates(["orcid_publication_id"])
		
		df_persons_unique = df_persons.dropDuplicates(["id"])
		
		return {
			"works": df_works_unique,
			"persons": df_persons_unique
		}



class Correct:
	def run(self, dataFrames, spark):
		dataFrames.update(CorrectOrgs().run(dataFrames, spark))		
		dataFrames.update(CorrectMinLength().run(dataFrames, spark))		
		dataFrames.update(CorrectValues().run(dataFrames, spark))		
		dataFrames.update(CorrectContradict().run(dataFrames, spark))
		dataFrames.update(CorrectDuplIDs().run(dataFrames, spark))
		
		return dataFrames
	