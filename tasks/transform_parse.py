import bibtexparser
from bibtexparser.bparser import BibTexParser
from pylatexenc.latex2text import LatexNodes2Text
from pyspark.sql.functions import udf, when, explode, lit, regexp_extract, regexp_replace, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

				
class ParseBibtex:
	BIBTEX_SCHEMA = StructType([
		StructField('doi', StringType()),
		StructField('date', StringType()),
		StructField('journal_title', StringType()),
		StructField('abstract', StringType()),
		StructField('orcid-numbers', StringType()),
		StructField('authors', ArrayType(StructType([ \
											StructField('id', StringType()), \
											StructField('otherNames', StringType())]))),
		StructField('issn', StringType()),
		StructField('isbn', StringType()),
		StructField('url', StringType()),
		StructField('subTitle', StringType()),
		StructField('title', StringType()),
	])

	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]
		df_persons = dataFrames["persons"]
		
		# parse bibtex
		cust_parse = udf(lambda bibtex, publ_id: self.__parseBibtex(bibtex, publ_id), ParseBibtex.BIBTEX_SCHEMA)
		df_bibtex = df_works.withColumn('bibtex_data', cust_parse(df_works["bibtex"], df_works["orcid_publication_id"]))
		
		
		# take bibtex values
		for col in ["doi", "date", "journal_title", "abstract", "issn", "isbn", "url", "subTitle", "title"]:
			df_bibtex = df_bibtex.withColumn(col, when(df_bibtex[col].isNotNull(), df_bibtex[col]).otherwise(df_bibtex["bibtex_data." + col]))


		# author ids
		df_bibtex = df_bibtex.withColumn("authors", when(df_bibtex["authors"].isNotNull(), df_bibtex["authors"]).otherwise(df_bibtex["bibtex_data.authors.id"]))
		
		
		# authors into person dataFrame
		df_exploded = df_bibtex.withColumn("exploded", explode("bibtex_data.authors"))
		df_authors = df_exploded.withColumn("id", df_exploded["exploded.id"])	\
								 .withColumn("otherNames", df_exploded["exploded.otherNames"])	\
								 .select("id", "otherNames")
								 
								 
		# add missing columns of persons dataframe
		for col in df_persons.columns:
			if not col in df_authors.columns:
				df_authors = df_authors.withColumn(col, lit(None))
		
		# change column order to persons dataframe
		df_authors = df_authors.select(df_persons.columns)

								 
		return {
			"persons": df_persons.union(df_authors),
			"works": df_bibtex
		}


	def __parseBibtex(self, bibtex, publ_id):
		if not bibtex:
			return None
			
		try:
			bib_db = bibtexparser.loads(bibtex, parser=BibTexParser(common_strings = True))
		except:
			return None

		if bib_db.entries and (1 == len(bib_db.entries)):					# should contains exactly one entry, otherwise it's ambiguous
			entry = bib_db.entries[0]

			# decode special chars
			latexDecoder = LatexNodes2Text()
			for name, value in entry.items():
				try:
					entry[name] = latexDecoder.latex_to_text(value)
				except:
					pass

			result = {"type": entry["ENTRYTYPE"]}
			for field in ParseBibtex.BIBTEX_SCHEMA:
				if isinstance(field.dataType, StringType) and (field.name in entry) and entry[field.name]:
					result[field.name] = entry[field.name]
			
			# rename differing attributes to match internal names
			for old, new in {"year": "date", "journal": "journal_title", "subtitle": "subTitle"}.items():
				if old in result:
					result[new] = result.pop(old)
			
			author_value = ""
			if ("orcid-numbers" in entry) and entry["orcid-numbers"]:
				author_value = entry["orcid-numbers"]
			elif ("author" in entry) and entry["author"]:
				author_value = entry["author"]
			
			authors = []
			if author_value:
				i = 0
				for author in author_value.split(" and "):
					author_parts = author.split("/")
					author_entry = {
						"otherNames": author_parts[0],
						"id": author_parts[1] if (2 == len(author_parts)) else publ_id + "_" + str(i)
					}
					authors.append(author_entry)
						
					i += 1
					
				result["authors"] = authors
				
			
			return result
	

class ParseValues:

	FOREIGN = {
		"issn": r'(?<=([iI][sS][sS][nN][^a-z^A-Z]))(((977)-\d{3}-\d{4}-\d{2}-\d)|(\d{4}-\d{3}[0-9xX]))',
		"isbn": r'(?<=([iI][sS][bB][nN][^a-z^A-Z]))(([0-9]{1,5}[-\ ]?[0-9]+[-\ ]?[0-9]+[-\ ]?[0-9X])|(97[89][-\ ]?[0-9]{1,5}[-\ ]?[0-9]+[-\ ]?[0-9]+[-\ ]?[0-9]))',
		"doi": r'(?<=([dD][oO][iI][^a-z^A-Z]))(10[.][0-9]{4,}[^\s"\/<>]*\/[^\s"<>]+)'
	}

	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]

		df_parsed = df_works

		# lookup in foreign fields
		for field, pattern in ParseValues.FOREIGN.items():
			df_parsed = df_parsed.withColumn("foreign", lit(""))
			for foreign in ["doi", "url", "isbn", "subTitle", "journal_title", "title"]:
				df_parsed = df_parsed.withColumn("foreign", when(col(foreign).isNotNull() & (col("foreign") == ""),	\
																regexp_extract(foreign, pattern, 0))	\
															.otherwise(col("foreign")))
																
			df_parsed = df_parsed.withColumn(field, when((col(field).isNull() | (col(field) == "")) & (col("foreign") != ""), col("foreign")).otherwise(col(field)))
		

		return {
			"works": df_parsed.select(df_works.columns)
		}
	

class Parse:
	def run(self, dataFrames, spark):
		dataFrames.update(ParseValues().run(dataFrames, spark))		
		return ParseBibtex().run(dataFrames, spark)
	