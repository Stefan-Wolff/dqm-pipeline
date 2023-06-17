from lib.duplicates import WorksKey
from pyspark.sql.functions import udf, col, max, when, explode

class Merge:

	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
		df_persons = dataFrames["persons"]
					
		# works by doi OR title#year#authors OR orcid_publication_id
		cust_key = udf(lambda title, date, authors: WorksKey().build(title, date, authors))
		df_works_dedupl = df_works.withColumn("key", when(col("doi").isNotNull(), col("doi"))	\
													.otherwise(
														when(col("title").isNotNull() & col("date").isNotNull() & col("authors").isNotNull(),	\
															cust_key(col("title"), col("date"), col("authors")))	\
														.otherwise(col("orcid_publication_id"))))	\
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