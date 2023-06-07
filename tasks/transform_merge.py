from lib.duplicates import WorksKey
from pyspark.sql.functions import udf, col, max, array_contains

class Merge:

	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
		df_persons = dataFrames["persons"]
					
		cust_key = udf(lambda title, date, authors, publ_id: WorksKey().build(title, date, authors, publ_id))
		df_works_dedupl = df_works.withColumn("key", cust_key(col("title"), col("date"), col("authors"), col("orcid_publication_id")))	\
								  .groupBy("key")	\
								  .agg(max("authors").alias("authors"),	\
									   max("bibtex").alias("bibtex"),	\
									   max("journal_title").alias("journal_title"),	\
									   max("orcid_id").alias("orcid_id"),	\
									   max("orcid_publication_id").alias("orcid_publication_id"),	\
									   max("date").alias("date"),	\
									   max("issn").alias("issn"),	\
									   max("isbn").alias("isbn"),	\
									   max("abstract").alias("abstract"),	\
									   max("subTitle").alias("subTitle"),	\
									   max("title").alias("title"),	\
									   max("type").alias("type"),	\
									   max("url").alias("url"))	\
								  .dropDuplicates(["key"])	\
								  .drop("key")
							
		df_persons_dedupl = df_persons.join(df_works_dedupl, array_contains(df_works_dedupl["authors"], df_persons["id"]), 'leftsemi')
							
							
		return {
			"works": df_works_dedupl,
			"persons": df_persons_dedupl
		}