from pyspark.sql.functions import udf, col
from .transform_correct import CorrectValues


class FilterContradict:
	
	def run(self, dataFrames, spark):
		df_persons = dataFrames["persons"]
		
		# affiliations.startYear & affiliations.endYear
		cust_affil = udf(lambda affils: self.__correctAffil(affils), CorrectValues.AFFIL_SCHEMA)
		df_persons_cor = df_persons.withColumn("affiliations", cust_affil(col("affiliations")))
		
		return {
			"persons": df_persons_cor
		}
		
		
	def __correctAffil(self, affils):
		if not affils:
			return None
			
		result = []
		for affil in affils:
			entry = {}
			for field in ["startYear", "endYear"]:
				if field in affil and affil[field]:
					entry[field] = affil[field]
						
			if "orgID" in affil and affil["orgID"]:
				entry["orgID"] = affil["orgID"]
			if "role" in affil and affil["role"]:
				entry["role"] = affil["role"]
				
			if ("startYear" in entry) and ("endYear" in entry) and (entry["startYear"] > entry["endYear"]):
				del entry["startYear"]
				del entry["endYear"]
				
			if entry:
				result.append(entry)
				

		return result if result else None
		
		
		
class FilterObjects:
	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
		df_persons = dataFrames["persons"]

		# works
		df_works_compl = df_works.where(col("title").isNotNull() & \
										(col("url").isNotNull() | col("doi").isNotNull()))
		
		# persons
		df_persons_compl = df_persons.where(col("id").isNotNull() & \
												((col("firstName").isNotNull() & col("lastName").isNotNull()) | \
												  col("publishedName").isNotNull() | \
												  col("otherNames").isNotNull()))

		return {
			"works": df_works_compl,
			"persons": df_persons_compl
		}
		
		
		
class Filter:
	def run(self, dataFrames, spark):
		dataFrames.update(FilterContradict().run(dataFrames, spark))		
		dataFrames.update(FilterObjects().run(dataFrames, spark))		
		
		return dataFrames