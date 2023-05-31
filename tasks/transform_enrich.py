from pyspark.sql.functions import when
		
		
class JoinCrossRef:
	def run(self, df_persons, df_works_joined):
		CROSSREF_PATH = "data/CrossRef/*"

		def correct(self, df_persons, df_works, spark):
			df_crossref = spark.read.json(Correction1.CROSSREF_PATH, multiLine=True)
			
			df_works_joined = df_works.join(df_crossref, df_works["doi"] == df_crossref["doi"], 'left')	\
								.withColumn("date", when(df_works["date"].isNotNull(), df_works["date"]).otherwise(df_crossref["date"])) \
								.withColumn("type", when(df_works["type"].df_works(), df_works["type"]).otherwise(df_crossref["type"])) \
								.withColumn("title", when(df_works["title"].isNotNull(), df_works["title"]).otherwise(df_crossref["title"])) \
								.withColumn("authors", when(df_works["authors"].isNotNull(), df_works["authors"]).otherwise(df_crossref["authors"])) \
								.withColumn("short_description", when(df_works["short_description"].isNotNull(), df_works["short_description"]).otherwise(df_crossref["abstract"])) \
								.withColumn("subTitle", when(df_works["subTitle"].isNotNull(), df_works["subTitle"]).otherwise(df_crossref["subTitle"])) \
								.withColumn("citated", when(df_joined.citated.isNotNull(), df_joined.citated).otherwise(df_joined.crossref_citation_count)) \
								.select(df_works.columns)
			
			
			# join orcid works with crossref data
			return df_persons, df_works_joined
