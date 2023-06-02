from pyspark.sql.functions import lit, col
				
class StandardizeOrgs:

	def run(self, dataFrames, spark):
		df_orgs = dataFrames["orgUnits"]
		
		sources = {
			"GRID":		{"df_name": "ROR",		"id_field": "`external_ids.GRID.preferred`",	"name_field": "name"},
			"ROR":		{"df_name": "ROR",		"id_field": "id",								"name_field": "name"},
			"LEI":		{"df_name": "lei",		"id_field": "id",								"name_field": "name"},
			"FUNDREF":	{"df_name": "fundref",	"id_field": "uri",								"name_field": "primary_name_display"}
		}

		df_joined = df_orgs

		for id_type, config in sources.items():
			df_right = dataFrames[config["df_name"]]
			
			# rename columns to prevent ambiguous column names
			for col_name in df_right.columns:
				df_right = df_right.withColumnRenamed(col_name, "r_" + col_name)
		
			# join & take name
			id_field = config["id_field"].replace("`", "`r_", 1) if (config["id_field"].startswith("`")) else "r_" + config["id_field"]
			df_joined = df_orgs.join(df_right, (df_orgs["type"] == lit(id_type)) & \
											   (df_orgs["id"] == df_right[id_field])
									, 'left')	\
								.withColumn("name", col("r_" + config["name_field"]))	\
								.select(df_orgs.columns)
								
				
		return {
			"orgUnits": df_joined
		}