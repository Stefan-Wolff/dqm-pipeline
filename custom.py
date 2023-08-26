from lib.data_processor import DataProcessor
import argparse


class Custom(DataProcessor):

	def _run(self, dataFrames, config, spark):
		df_works = dataFrames["works"]
		
		# add specific analyses here
		df_works.show(1000)

		

if "__main__" == __name__:
	parser = argparse.ArgumentParser(prog='Custom', description='Run custom analyse tasks')
	parser.add_argument('-c', '--chain', help='source chain if analyze transformed data', default='initial')
					

	Custom().run(parser.parse_args())
