#

class Metric:
	
	def getName(self):
		return type(self).__name__
		
		
	def __formateResult(self, calc_result):
		result = {}
		weights = self._weights()
		
		for local_key, metric_result in calc_result.items():
			indicator = metric_result[0] * weights[local_key]
			result[self.getName() + "." + local_key] = round(indicator, 3)
		
		return result
		
		
	def calc(self, df_persons, df_works, spark, sample_num):
		result = self._calc(df_persons, df_works, spark)
		
		self.__showSamples(result, sample_num)
		
		return self.__formateResult(result)
		
	
	def __showSamples(self, calc_result, num):
		if not num:
			return

		for local_key, metric_result in calc_result.items():
			dataFrame = metric_result[1]
			count = dataFrame.count()
			
			print("###\t Sample:  ", self.getName() + "." + local_key)
			print("#\t count: " + str(count))
			if count:
				sample_size = num if (num < count) else count
				fraction = sample_size / count
				dataFrame.sample(fraction).show(num)
			print("############################################")