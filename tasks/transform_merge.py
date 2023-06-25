from lib.duplicates import groupDuplicates
from pyspark.sql.functions import col, max, explode

class Merge:

	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
		df_persons = dataFrames["persons"]
					
		df_works_dedupl = groupDuplicates(df_works) \
							.groupBy("key")	\
							.agg(max("authors").alias("authors"),	\
							   max("bibtex").alias("bibtex"),	\
							   max("journal_title").alias("journal_title"),	\
							   max("orcid_id").alias("orcid_id"),	\
							   max("orcid_publication_id").alias("orcid_publication_id"),	\
							   max("date").alias("date"),	\
							   max("doi").alias("doi"),	\
							   max("issn").alias("issn"),	\
							   max("isbn").alias("isbn"),	\
							   max("abstract").alias("abstract"),	\
							   max("subTitle").alias("subTitle"),	\
							   max("title").alias("title"),	\
							   max("type").alias("type"),	\
							   max("url").alias("url"))	\
							.dropDuplicates(["key"])	\
							.drop("key")
							
		# persons by id
		df_works_exploded = df_works_dedupl.withColumn("authors_exploded", explode("authors"))
		df_persons_dedupl = df_persons.join(df_works_exploded, df_persons["id"] == col("authors_exploded"), 'leftsemi')	\
									  .groupBy("id")	\
									  .agg(max("affiliations").alias("affiliations"),	\
										   max("country").alias("country"),	\
										   max("firstName").alias("firstName"),	\
										   max("lastName").alias("lastName"),	\
										   max("otherNames").alias("otherNames"),	\
										   max("publishedName").alias("publishedName"))	\
									  .dropDuplicates(["id"])
							
							
		return {
			"works": df_works_dedupl,
			"persons": df_persons_dedupl
		}