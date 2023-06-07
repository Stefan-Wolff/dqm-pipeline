import argparse
from tasks.transform_parse import *
from tasks.transform_enrich import *
from tasks.transform_correct import *
from tasks.transform_merge import *
from tasks.transform_filter import *
from lib.data_processor import DataProcessor


class Transformer(DataProcessor):
	def _run(self, dataFrames, config, spark):
		transforms = {
			"ParseBibtex": ParseBibtex(),
			"ParseValues": ParseValues(),
			"Parse": Parse(),
			"JoinCrossRef": JoinCrossRef(),
			"CorrectOrgs": CorrectOrgs(),
			"CorrectMinLength": CorrectMinLength(),
			"CorrectValues": CorrectValues(),
			"CorrectContradict": CorrectContradict(),
			"UniqueIDs": UniqueIDs(),
			"Merge": Merge(),
			"RemoveContradict": RemoveContradict(),
			"IncompleteObjects": IncompleteObjects()
		}
		
		transform_impl = transforms[config.transformation]
		df_results = transform_impl.run(dataFrames, spark)
		for entity, df in df_results.items():
			class_name = type(transform_impl).__name__
			new_chain = config.chain + "." + class_name if ("initial" != config.chain) else class_name

			outPath = "data/parquets/" + new_chain + "/" + entity
			df.write.parquet(outPath, mode="overwrite")
			print(outPath + " written")

### entry
if "__main__" == __name__:

	# init parameters
	parser = argparse.ArgumentParser(prog='Data Transformer', description='Runs given transformation')
	parser.add_argument('-t', '--transformation', help='the transformation to run', required=True)
	parser.add_argument('-c', '--chain', help='source chain if analyze transformed data', default='initial')
					
	Transformer().run(parser.parse_args())