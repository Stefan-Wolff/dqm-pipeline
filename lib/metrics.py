"""Supports the calculation of specific metrics"""

class Metric:
	"""This class supports the aggregation of indicators."""
	
	def getName(self):
		return type(self).__name__
		
		
	def _formateResult(self, calc_result):
		result = {}
		weights = self._weights()
		sum_indicators = 0
		sum_weights = 0
		
		for local_key, indicator in calc_result.items():
			sum_indicators += indicator * weights[local_key]
			sum_weights += weights[local_key]
			result[self.getName() + "." + local_key] = round(indicator, 3)
		
		result[self.getName()] = round(sum_indicators / sum_weights, 3)
		
		return result
		
		
	def calc(self, dataFrames, spark):
		result = self._calc(dataFrames, spark)
		
		return self._formateResult(result)
			
			
			

class Aggregation(Metric):
	"""This class aggregates indiciators, which are already aggregated (on the next level, i. e. the level of the data quality dimension)"""

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