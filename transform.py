import argparse
from tasks.transform_parse import *
from tasks.transform_enrich import *
from tasks.transform_correct import *
from tasks.transform_merge import *
from tasks.transform_filter import *
from lib.data_processor import DataProcessor


class Transformer(DataProcessor):

	FULL_CHAIN = [
		"ParseBibtex",
		"ParseValues",
		"CorrectOrgs",
		"CorrectMinLength",
		"CorrectValues",
		"CorrectContradict",
		"JoinCrossRef",
		"ParseValues",
		"CorrectMinLength",
		"CorrectValues",
		"CorrectContradict",
		"Merge",
		"FilterContradict",
		"FilterObjects"
	]

	def _run(self, dataFrames, config, spark):
		transforms = {
			"ParseBibtex": ParseBibtex(),
			"ParseValues": ParseValues(),
			"Parse": Parse(),
			"CorrectOrgs": CorrectOrgs(),
			"CorrectMinLength": CorrectMinLength(),
			"CorrectValues": CorrectValues(),
			"CorrectContradict": CorrectContradict(),
			"Correct": Correct(),
			"JoinCrossRef": JoinCrossRef(),
			"Merge": Merge(),
			"FilterContradict": FilterContradict(),
			"FilterObjects": FilterObjects(),
			"Filter": Filter()
		}
		
		# complete chain
		if "complete" == config.transformation:
			print("running complete pipeline: " + str(Transformer.FULL_CHAIN))
			
			entities = set()
			for transform_task in Transformer.FULL_CHAIN:
				cur_results = transforms[transform_task].run(dataFrames, spark)
				dataFrames.update(cur_results)
				entities.update(cur_results.keys())
			
			df_results = {e: dataFrames[e] for e in entities}						# save returned dataframes only
			new_chain = config.transformation
			
			
		# single transformation
		else:
			transform_impl = transforms[config.transformation]
			df_results = transform_impl.run(dataFrames, spark)
			
			class_name = type(transform_impl).__name__
			new_chain = config.chain + "." + class_name if ("initial" != config.chain) else class_name
		
		
		# write results
		for entity, df in df_results.items():
			outPath = "data/parquets/" + new_chain + "/" + entity
			df.write.parquet(outPath, mode="overwrite")
			print(outPath + " written")
		
	
	

### entry
if "__main__" == __name__:

	# init parameters
	parser = argparse.ArgumentParser(prog='Data Transformer', description='Runs given transformation')
	parser.add_argument('-t', '--transformation', help='the transformation to run', default='complete')
	parser.add_argument('-c', '--chain', help='source chain if analyze transformed data', default='initial')
					
	Transformer().run(parser.parse_args())