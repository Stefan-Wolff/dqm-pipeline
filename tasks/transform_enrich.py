"""Data enriching using external data from Crossref"""

from pyspark.sql.functions import when, lit, explode
		
		
class JoinCrossRef:
	def run(self, dataFrames, spark):
		df_persons = dataFrames["persons"]
		df_works = dataFrames["works"]
		df_crossref = dataFrames["crossRef"]

		# rename columns to prevent ambiguous column names
		for col in df_crossref.columns:
			df_crossref = df_crossref.withColumnRenamed(col, "cr_" + col)

		# join
		df_joined = df_works.join(df_crossref, df_works["doi"] == df_crossref["cr_doi"], 'left')
		
		# take data
		for field in ["abstract", "date", "doi", "isbn", "issn", "subTitle", "title", "type", "url"]:
			df_joined = df_joined.withColumn(field, when(df_joined[field].isNotNull(), df_joined[field]).otherwise(df_joined["cr_" + field]))
		
		# take authors
		df_joined = df_joined.withColumn("authors", when(df_joined["authors"].isNotNull(), df_joined["authors"]).otherwise(df_joined["cr_authors.id"]))


		# authors into person dataFrame
		df_exploded = df_joined.withColumn("exploded", explode("cr_authors"))
		df_authors = df_exploded.withColumn("id", df_exploded["exploded.id"])	\
								.withColumn("firstName", df_exploded["exploded.firstName"])	\
								.withColumn("lastName", df_exploded["exploded.lastName"])	\
								.select("id", "firstName", "lastName")
								 
								 
		# add missing columns of persons dataframe
		for col in df_persons.columns:
			if not col in df_authors.columns:
				df_authors = df_authors.withColumn(col, lit(None))
		
		# change column order to persons dataframe
		df_authors = df_authors.select(df_persons.columns)
		

		return {
			"persons": df_persons.union(df_authors),
			"works": df_joined.select(df_works.columns)
		}
