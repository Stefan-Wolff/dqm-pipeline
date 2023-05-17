#

class Metric:
	
	def getName(self):
		return type(self).__name__
		
	def addResult(self, entity, local_key, value):
		metric_key = self.getName() + "." + entity + "." + local_key
		
		return {metric_key: value}
		
		
	def formateResult(self, entity, local_key, indicator):
		rounded = round(indicator, 3)
		
		return self.addResult(entity, local_key, rounded)
		
	
	def showSample(self, dataFrame, num, name):
		if not num:
			return

		count = dataFrame.count()
		
		print("###\t Sample:  ", self.getName() + "." + name)
		print("#\t count: " + str(count))
		if count:
			sample_size = num if (num < count) else count
			fraction = sample_size / count
			dataFrame.sample(fraction).show(num)
		print("############################################")