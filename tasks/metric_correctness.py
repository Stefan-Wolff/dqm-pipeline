from datetime import datetime
from pyspark.sql.functions import explode, year
from pyspark.sql.types import IntegerType, ArrayType
from lib.metrics import Metric


class Correctness(Metric):

	# chinese, japanese, korean, cyrillic
	INVALID_ALPHABET = r'.*[\u4e00-\u9FFF\u3040-\u30ff\uac00-\ud7a3а-яА-Я].*'

	COMMON = r'(^[ \t\r\n\*\-/]|[ \t\r\n\*\-/]$)(^[\.,’\(@])([\.\-\*]+)|([\+\?]{1})|(k\.a\.)|(n\.n\.)|(n/a)|("")(.*(<\w+>)|([ ]{2,}).*)'

	ORCID_ID = r'[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9|X]'

	# antipattern for persons.firstName, persons.lastName, persons.otherNames, persons.publishedName
	NAMES = r'.*[\(\)/\t0-9]+.*'

	# syntax pattern
	CONFIG = {
		"works": {
			"bibtex":				{"pattern": r'@[a-z]*[A-Z]*[\s]*\{[\S\s]*,[\S\s]*\}'},
			"orcid_id":				{"pattern": ORCID_ID},
			"orcid_publication_id":	{"pattern": r'[0-9]+'},
			"date":					{"pattern": r'[1800-' + str(datetime.now().year) + '](-[0-9]{2}(-[0-9]{2})?)?'},
			"url":					{"pattern": r'(http://|https://)[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}'},
			"doi":					{"pattern": r'10[.][0-9]{4,}[^\s"\/<>]*\/[^\s"<>]+'},
			"issn":					{"pattern": r'(\d{3}-\d-\d{4}-\d{4}-\d)|(\d{4}-\d{3}[\dxX])'},
			"isbn":					{"pattern": r'([0-9X\-]{13}+)|([0-9X\-]{10}+)'},
			"abstract":				{"pattern": r'([^\s]+ ){4}[^\s]+',									# min 5 words
									 "antipattern": r'^[Aa][Bb][Ss][Tt][Rr][Aa][Cc][Tt]'},
			"title":				{"antipattern": r'.*[\n\r\t].*'},
			"subTitle":				{"antipattern": r'.*[\n\r\t].*'}
		},
		"persons": {
			"id":					{"pattern": r'[0-9X\-_]+'},
			"country":					{"pattern": r'[A-Z]{2}'},
			"affiliations.startYear":	{"pattern": r'(19[0-9][0-9])|(20[0-9][0-9])'},		# 1900-2099
			"affiliations.endYear":		{"pattern": r'(19[0-9][0-9])|(20[0-9][0-9])'},
			"firstName":				{"antipattern": NAMES},
			"lastName":					{"antipattern": NAMES},
			"publishedName":			{"antipattern": NAMES}
		}
	}
	
	
	def _weights(self):
													#	works.bibtex								persons.firstName
													#		works.orcid_id								persons.lastName
													#			works.orcid_publication_id					persons.publishedName
													#				works.date									persons.id
													#					works.url									persons.country
													#						works.doi									persons.affiliations.startYear
													#							works.issn									persons.affiliations.endYear
													#								works.isbn									orgUnits.orgName
													#									works.abstract
													#										works.title
													#											works.subTitle
		return {
			"works.bibtex": 8,						#	1	0	0	1	1	1	2	2	0	0	2	0	0	0	0	2	2	2	2
			"works.orcid_id": 30,					#	2	1	1	2	2	1	2	2	2	1	2	1	1	1	1	2	2	2	2
			"works.orcid_publication_id": 28,		#	2	1	1	1	1	1	2	2	2	1	2	1	1	1	1	2	2	2	2
			"works.date": 24,						#	1	0	1	1	0	1	2	2	1	1	2	1	1	1	1	2	2	2	2
			"works.url": 25,						#	1	0	1	2	1	0	2	2	1	1	2	1	1	1	1	2	2	2	2
			"works.doi": 31,						#	1	1	1	1	1	1	2	2	2	1	2	2	2	2	2	2	2	2	2
			"works.issn": 7,						#	0	0	0	0	0	0	1	1	0	0	1	0	0	0	0	1	1	1	1
			"works.isbn": 6,						#	0	0	0	0	0	0	0	1	0	0	1	0	0	0	0	1	1	1	1
			"works.abstract": 23,					#	2	0	0	1	1	0	2	2	1	0	2	1	1	1	1	2	2	2	2
			"works.title": 32,						#	2	1	1	1	1	1	2	2	2	1	2	2	2	2	2	2	2	2	2
			"works.subTitle": 16,					#	0	0	0	0	0	0	1	1	0	0	1	1	1	1	2	2	2	2	2
			"persons.firstName": 21,				#	2	1	1	1	1	0	2	2	1	0	1	1	0	0	0	2	2	2	2
			"persons.lastName": 25,					#	2	1	1	1	1	0	2	2	1	0	1	2	1	1	1	2	2	2	2
			"persons.publishedName": 25,			#	2	1	1	1	1	0	2	2	1	0	1	2	1	1	1	2	2	2	2
			"persons.id": 24,					#	2	1	1	1	1	0	2	2	1	0	0	2	1	1	1	2	2	2	2
			"persons.country": 6,					#	0	0	0	0	0	0	1	1	0	0	0	0	0	0	0	1	1	1	1
			"persons.affiliations.startYear": 5,	#	0	0	0	0	0	0	1	1	0	0	0	0	0	0	0	1	1	1	0
			"persons.affiliations.endYear": 5,		#	0	0	0	0	0	0	1	1	0	0	0	0	0	0	0	1	1	1	0
			"orgUnits.orgName": 8					#	0	0	0	0	0	0	1	1	0	0	0	0	0	0	0	1	2	2	1
		}


	def _calc(self, dataFrames, spark):
		result = {}
		
		df_persons = dataFrames["persons"]
		df_works = dataFrames["works"]
		df_orgUnits = dataFrames["orgUnits"]
		
		# standard fields
		for entity, patterns in Correctness.CONFIG.items():
			dataFrame = df_persons if ("persons" == entity) else df_works
			for field, config in patterns.items():
				df_base = dataFrame
				attr = field
				
				if "." in attr:
					levels = attr.split(".")
					df_base = df_base.withColumn("exploded_" + levels[0], explode(levels[0]))
					levels[0] = "exploded_" + levels[0]
					attr = ".".join(levels)
				elif isinstance(df_base.schema[attr].dataType, ArrayType):
					df_base = df_base.withColumn("exploded_" + attr, explode(attr))
					attr = "exploded_" + attr
			
			
				df_notNull = df_base.where(df_base[attr].isNotNull())
				df_valid = df_notNull.where(~df_notNull[attr].rlike(Correctness.INVALID_ALPHABET) & \
											~df_notNull[attr].rlike(Correctness.COMMON))
				
				if "pattern" in config:
					df_valid = df_valid.where(df_valid[attr].rlike(config["pattern"]))
					
				if "antipattern" in config:
					df_valid = df_valid.where(~df_valid[attr].rlike(config["antipattern"]))
					
				
				df_invalid = df_notNull.subtract(df_valid)
				indicator = df_valid.count() / df_notNull.count()
				
				result[entity + "." + field] = (indicator, df_invalid)
		

		# orgUnits.name
		df_ror = spark.read.csv("data/ROR.csv", header=True)
		df_lei = spark.read.csv("data/LEI.csv", header=True)
		df_fundref = spark.read.csv("data/FUNDREF.csv", header=True)
		sources = {
			"GRID":		{"df": df_ror,		"id_field": "`external_ids.GRID.preferred`",	"name_field": "name"},
			"ROR":		{"df": df_ror,		"id_field": "id",								"name_field": "name"},
			"LEI":		{"df": df_lei,		"id_field": "id",								"name_field": "name"},
			"FUNDREF":	{"df": df_fundref,	"id_field": "uri",								"name_field": "primary_name_display"}
		}

		indicator = 0
		df_invalids = []
		for id_type, config in sources.items():
			df_notNull = df_orgUnits.where(df_orgUnits["type"] == id_type)
			df_incorrect = df_notNull.join(config["df"], (df_notNull["id"] == config["df"][config["id_field"]]) & \
														 (df_notNull["name"] != config["df"][config["name_field"]]) \
														 , 'leftsemi')
														 
			df_invalids.append(df_incorrect)
			notNull_num = df_notNull.count()
			indicator += (notNull_num - df_incorrect.count()) / notNull_num

		result["orgUnits.orgName"] = (indicator / len(sources), df_invalids)

		return result
		
		
		