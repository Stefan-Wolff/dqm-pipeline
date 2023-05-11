#
import logging

class Completeness1:
	WEIGHTS = {
		"affiliations": 0.8,
		"country": 0.8,
		"firstName": 0.14,
		"lastName": 0.14,
		"orcid_id": 0.12,
		"otherNames": 0.8,
		"publishedName": 0.6
	}
	
	def calc(self, df_persons, df_works):
		logging.info("start metric completeness 1 ..")

		result = 0
		row_num = df_persons.count()

		for col in df_persons.columns:
			count = df_persons.where(df_persons[col].isNotNull()).count()
			weight = Completeness1.WEIGHTS[col]
			indicator = weight * count / row_num
			result += indicator
			logging.info("\t attribute: " + col + "\t\t\t weight: " + str(weight) + "\t count: " + str(count) + "\t indicator: " + str(indicator))
		
		logging.info(".. metric completeness 1 done")

		return result
