import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, flatten
from pyspark.storagelevel import StorageLevel


def run():
	# init spark session
	spark = SparkSession	\
				.builder	\
				.getOrCreate()
	
	# load data
	df_persons = spark.read.json("data/ORCID_persons/*")
	#df_works = spark.read.json("data/works_0.jsonl")
	#df_works = spark.read.json("data/ORCID_works/works_37635362.jsonl.gz")
	#df_works.where(df_works["issn"].isNotNull()).select("issn").show(100, 100)
	#df_crossref = spark.read.json("data/CrossRef/*", multiLine=True)

	#df_authors = df_works.withColumn("authors_exploded", explode("authors"))
	#df_authors.where(df_authors["authors_exploded.orcid_id"].isNotNull()).select("authors_exploded").asc().show(1000, 100)

	#df_crossref.select("authors.fullName").sort(df_crossref["authors.fullName"].asc()).show(500)
	#df_works.select(explode("publicationID.type")).distinct().show(1000)
	
	# types of org ids
	#df_persons.select(explode("affiliations.orgIDType")).distinct().show(1000)
	

	# extract nested objects
	df_affiliations = df_persons.withColumn("affil_exploded", explode("affiliations"))
	df_affiliations.where(df_affiliations["affil_exploded.orgID"].isNull() & df_affiliations["affil_exploded.orgName"].isNull()).select("affil_exploded").show(1000, 100)
	#df_affiliations.where("ROR" == df_affiliations["affil_exploded.orgIDType"]).select("affil_exploded.orgID").show(1000, 100)

	#df_persons.any({"country": "de"}).show(100)
	
	#df_persons.select("affiliations").where(df_persons["affiliations.orgIDType"].isNotNull()).show(10, 100)
	
	#df_crossref.printSchema()

if "__main__" == __name__:
	run()
