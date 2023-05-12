#

class Metric:
	
	def getName(self):
		return type(self).__name__
		
	def formateResult(self, entity, local_key, indicator):
		metric_key = self.getName() + "." + entity + "." + local_key
		rounded = round(indicator, 3)
		
		return {metric_key: rounded}
		