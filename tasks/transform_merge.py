from lib.duplicates import WorksKey
from pyspark.sql.functions import udf, col, max

class Merge:

	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
					
		cust_key = udf(lambda title, date, authors: WorksKey().build(title, date, authors))
		df_dedupl = df_works.where(col("title").isNotNull() & col("date").isNotNull() & col("authors").isNotNull())	\
							.withColumn("key", cust_key(col("title"), col("date"), col("authors")))	\
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
							
		# TODO: authors
							
		return {
			"works": df_dedupl
		}