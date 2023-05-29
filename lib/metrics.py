#

class Metric:
	
	def getName(self):
		return type(self).__name__
		
		
	def _formateResult(self, calc_result):
		result = {}
		weights = self._weights()
		sum_indicators = 0
		sum_weights = 0
		
		for local_key, metric_result in calc_result.items():
			indicator = metric_result[0]
			sum_indicators += indicator * weights[local_key]
			sum_weights += weights[local_key]
			result[self.getName() + "." + local_key] = round(indicator, 3)
		
		result[self.getName()] = round(sum_indicators / sum_weights, 3)
		
		return result
		
		
	def calc(self, df_persons, df_works, df_orgUnits, spark, sample_num):
		result = self._calc(df_persons, df_works, df_orgUnits, spark)
		
		self.__showSamples(result, sample_num)
		
		return self._formateResult(result)
		
	
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
			
			
			

class Aggregation(Metric):
	def _formateResult(self, calc_result):
		result = dict(calc_result)
		weights = self._weights()
		sum_indicators = 0
		sum_weights = 0
		
		for local_key in weights.keys():
			indicator = calc_result[local_key]
			sum_indicators += indicator * weights[local_key]
			sum_weights += weights[local_key]
			
			del calc_result[local_key]
			result[self.getName() + "." + local_key] = indicator
		
		result[self.getName()] = round(sum_indicators / sum_weights, 3)
		
		return result