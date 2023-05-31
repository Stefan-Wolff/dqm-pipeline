import argparse
from tasks.transform_parse import *
from lib.data_processor import DataProcessor


class Transformer(DataProcessor):
	def _run(self, dataFrames, config, spark):
		transforms = {
			"ExtractBibtex": ExtractBibtex(),
			"ParseValues": ParseValues()
		}
			
		class_name = type(self).__name__
		
		#dataFrames["works"] = spark.read.json("data/input/orcid_works_00.jsonl")
		
		df_results = transforms[config.transformation].run(dataFrames, spark)
		for entity, df in df_results.items():
			outPath = "data/parquets/" + config.chain + "." + class_name + "/" + entity
			df.write.parquet(outPath, mode="overwrite")
			print(config.chain + "." + class_name + "/" + entity + " written")

### entry
if "__main__" == __name__:

	# init parameters
	parser = argparse.ArgumentParser(prog='Data Transformer', description='Runs given transformation')
	parser.add_argument('-t', '--transformation', help='the transformation to run', required=True)
	parser.add_argument('-c', '--chain', help='source chain if analyze transformed data', default='initial')
					
	Transformer().run(parser.parse_args())