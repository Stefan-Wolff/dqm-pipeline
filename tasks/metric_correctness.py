from datetime import datetime
from pyspark.sql.functions import explode, year
from pyspark.sql.types import IntegerType, ArrayType
from lib.metrics import Metric


class Correctness(Metric):

	# chinese, japanese, korean, cyrillic
	INVALID_ALPHABET = r'[\u4e00-\u9FFF\u3040-\u30ff\uac00-\ud7a3а-яА-Я]'
	INVALID_TEXT = r'(<[a-zA-Z/]+>)|(^[^a-zA-Z0-9]+)|([^a-zA-Z0-9.!?"\'\)]+$)|(^[nk][\./]?[na][\./]?$)|(  )'						# used in transform_correct
	INVALID_NAMES = r'[\(\)/\t0-9]+'
	INVALID_ABSTRACT = r'^[Aa][Bb][Ss][Tt][Rr][Aa][Cc][Tt]'
	ORCID_ID = r'^([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9|X])'

	# 1800-2023
	YEAR_1800 = r'(1[8-9][0-9]{2})|(20)(([0-1][0-9])|(2[0-' + str(datetime.now().year)[-1] + ']))'

	# syntax pattern
	CONFIG = {
		"works": {
			"bibtex":				{"pattern": r'^(@[a-zA-Z]+[\s]*\{[\S\s]*\})$'},
			"orcid_id":				{"pattern": ORCID_ID},
			"orcid_publication_id":	{"pattern": r'^([0-9]+)$'},
			"date":					{"pattern": r'^((' + YEAR_1800 + ')(-[0-9]{2}(-[0-9]{2})?)?)$'},
			"url":					{"pattern": r'^(((http[s]?://)?[A-Za-z0-9.-]+)((?:\/[\+~%\/.\w-_\(\)#]*)?\??(?:[-\+~=&;%@.\w_:,/\(\)]*)#?(?:[-\+=&;%@.\w_:,/\(\)]*))?)$'},		# used in transform_correct
			"doi":					{"pattern": r'^(10[.][0-9]{4,}[^\s"\/<>]*\/[^\s"<>]+)$'},																						# used in transform_correct
			"issn":					{"pattern": r'^((977[\-]?)?(\d[\-]?){6}\d([-]?[0-9xX]))$'},
			"isbn":					{"pattern": r'^(97[89][\-]?)?([0-9][-]?){8}[0-9]([-]?[0-9xX])$'},																					# ISBN 10 or ISBN 13
			"abstract":				{"pattern": r'([^\s]+ ){4}[^\s]+',																												# min 5 words
									 "antipattern": [INVALID_ABSTRACT, INVALID_ALPHABET, INVALID_TEXT]},
			"title":				{"antipattern": [r'[\n\r\t]', INVALID_ALPHABET, INVALID_TEXT]},
			"subTitle":				{"antipattern": [r'[\n\r\t]', INVALID_ALPHABET, INVALID_TEXT]}
		},
		"persons": {
			"id":						{"pattern": r'^[0-9X\-_]+$'},
			"country":					{"pattern": r'^[A-Z]{2}$'},
			"affiliations.startYear":	{"pattern": r'^((19[0-9][0-9])|(20[0-9][0-9]))$'},		# 1900-2099
			"affiliations.endYear":		{"pattern": r'^((19[0-9][0-9])|(20[0-9][0-9]))$'},
			"firstName":				{"antipattern": [INVALID_NAMES, INVALID_ALPHABET]},
			"lastName":					{"antipattern": [INVALID_NAMES, INVALID_ALPHABET]},
			"publishedName":			{"antipattern": [INVALID_NAMES, INVALID_ALPHABET]},
			"otherNames":				{"antipattern": [INVALID_NAMES, INVALID_ALPHABET]}
		}
	}
	
	
	def _weights(self):
													#	works.bibtex								persons.firstName
													#		works.orcid_id								persons.lastName
													#			works.orcid_publication_id					persons.publishedName
													#				works.date									persons.otherNames
													#					works.url									persons.id
													#						works.doi									persons.country
													#							works.issn									persons.affiliations.startYear
													#								works.isbn									persons.affiliations.endYear
													#									works.abstract								orgUnits.orgName
													#										works.title
													#											works.subTitle
		return {
			"works.bibtex": 8,						#	1	0	0	1	1	1	2	2	0	0	2	0	0	0	0	0	2	2	2	2
			"works.orcid_id": 30,					#	2	1	1	2	2	1	2	2	2	1	2	1	1	1	1	1	2	2	2	2
			"works.orcid_publication_id": 28,		#	2	1	1	1	1	1	2	2	2	1	2	1	1	1	1	1	2	2	2	2
			"works.date": 24,						#	1	0	1	1	0	1	2	2	1	1	2	1	1	1	1	1	2	2	2	2
			"works.url": 25,						#	1	0	1	2	1	0	2	2	1	1	2	1	1	1	1	1	2	2	2	2
			"works.doi": 31,						#	1	1	1	1	1	1	2	2	2	1	2	2	2	2	2	2	2	2	2	2
			"works.issn": 7,						#	0	0	0	0	0	0	1	1	0	0	1	0	0	0	0	0	1	1	1	1
			"works.isbn": 6,						#	0	0	0	0	0	0	0	1	0	0	1	0	0	0	0	0	1	1	1	1
			"works.abstract": 23,					#	2	0	0	1	1	0	2	2	1	0	2	1	1	1	1	1	2	2	2	2
			"works.title": 32,						#	2	1	1	1	1	1	2	2	2	1	2	2	2	2	2	2	2	2	2	2
			"works.subTitle": 16,					#	0	0	0	0	0	0	1	1	0	0	1	1	1	1	1	2	2	2	2	2
			"persons.firstName": 21,				#	2	1	1	1	1	0	2	2	1	0	1	1	0	0	0	0	2	2	2	2
			"persons.lastName": 25,					#	2	1	1	1	1	0	2	2	1	0	1	2	1	1	1	1	2	2	2	2
			"persons.publishedName": 25,			#	2	1	1	1	1	0	2	2	1	0	1	2	1	1	0	1	2	2	2	2
			"persons.otherNames": 26,				#	2	1	1	1	1	0	2	2	1	0	1	2	1	2	1	1	2	2	2	2
			"persons.id": 24,						#	2	1	1	1	1	0	2	2	1	0	0	2	1	1	1	1	2	2	2	2
			"persons.country": 6,					#	0	0	0	0	0	0	1	1	0	0	0	0	0	0	0	0	1	1	1	1
			"persons.affiliations.startYear": 5,	#	0	0	0	0	0	0	1	1	0	0	0	0	0	0	0	0	1	1	1	0
			"persons.affiliations.endYear": 5,		#	0	0	0	0	0	0	1	1	0	0	0	0	0	0	0	0	1	1	1	0
			"orgUnits.name": 8						#	0	0	0	0	0	0	1	1	0	0	0	0	0	0	0	0	1	2	2	1
		}


	def _calc(self, dataFrames, spark):
		result = {}
		
		# standard fields
		for entity, patterns in Correctness.CONFIG.items():
			for field, config in patterns.items():
				print(entity, field)
				df_base = dataFrames[entity]
				attr = field
				
				if "." in attr:
					levels = attr.split(".")
					df_base = df_base.withColumn("exploded_" + levels[0], explode(levels[0]))
					levels[0] = "exploded_" + levels[0]
					attr = ".".join(levels)
				elif isinstance(df_base.schema[attr].dataType, ArrayType):
					df_base = df_base.withColumn("exploded_" + attr, explode(attr))
					attr = "exploded_" + attr
			
				df_notNull = df_base.where(df_base[field].isNotNull())
				df_valid = df_notNull
				
				if "pattern" in config:
					df_valid = df_valid.where(df_valid[attr].rlike(config["pattern"]))
					
				if "antipattern" in config:
					for antipattern in config["antipattern"]:
						df_valid = df_valid.where(~df_valid[attr].rlike(antipattern))
					
				
				result[entity + "." + field] = df_valid.count() / df_notNull.count()
		

		# orgUnits.name
		sources = {
			"GRID":		{"df_name": "ROR",		"id_field": "`external_ids.GRID.preferred`",	"name_field": "name"},
			"ROR":		{"df_name": "ROR",		"id_field": "id",								"name_field": "name"},
			"LEI":		{"df_name": "lei",		"id_field": "id",								"name_field": "name"},
			"FUNDREF":	{"df_name": "fundref",	"id_field": "uri",								"name_field": "primary_name_display"}
		}

		indicator = 0
		df_orgUnits = dataFrames["orgUnits"]
		for id_type, config in sources.items():
			df_right = dataFrames[config["df_name"]]
			

													 
													 
			# rename columns to prevent ambiguous column names
			for col_name in df_right.columns:
				df_right = df_right.withColumnRenamed(col_name, "r_" + col_name)
		
			# join & take name
			id_field = config["id_field"].replace("`", "`r_", 1) if (config["id_field"].startswith("`")) else "r_" + config["id_field"]
			df_notNull = df_orgUnits.where(df_orgUnits["type"] == id_type)
			df_incorrect = df_notNull.join(df_right, (df_notNull["id"] == df_right[id_field]) & \
													 (df_notNull["name"] != df_right["r_" + config["name_field"]]) \
											, 'inner')
													 
														 
			indicator += 1 - df_incorrect.count() / df_notNull.count()

		result["orgUnits.name"] = indicator / len(sources)

		return result
		
		
		