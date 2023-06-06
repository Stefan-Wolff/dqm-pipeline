from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


class RemoveContradict:

	AFFIL_SCHEMA = ArrayType(StructType([ \
								StructField('role', StringType()), \
								StructField('orgID', StringType()), \
								StructField('orgName', StringType()), \
								StructField('role', StringType()), \
								StructField('startYear', StringType()), \
								StructField('endYear', StringType())]))

	YEAR = Correctness.CONFIG["persons"]["affiliations.startYear"]["pattern"]
	
	
	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
		df_persons = dataFrames["persons"]
	
		df_works_cor = df_works
		df_persons_cor = df_persons
		
		# affiliations.startYear & affiliations.endYear
		cust_affil = udf(lambda affils: self.__correctAffil(affils), RemoveContradict.AFFIL_SCHEMA)
		df_persons_cor = df_persons_cor.withColumn("affiliations", cust_affil(col("affiliations")))
		
		
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
					if re.search(RemoveContradict.YEAR, affil[field]):
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