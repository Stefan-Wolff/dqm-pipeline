import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, flatten


def run():
	# init spark session
	spark = SparkSession	\
				.builder	\
				.getOrCreate()
	
	# load data
	df_persons = spark.read.json("output/persons.jsonl")
	#df_works = spark.read.json("output/works_0.jsonl")
	
	# types of org ids
	#df_persons.select(explode("affiliations.orgIDType")).distinct().show(1000)
	

	# extract nested objects
	#df_affiliations = df_persons.withColumn("affil_exploded", explode("affiliations"))
	#df_affiliations.where("ROR" == df_affiliations["affil_exploded.orgIDType"]).select("affil_exploded.orgID").show(1000, 100)

	df_persons.any({"country": "de"}).show(100)
	
	

if "__main__" == __name__:
	run()
