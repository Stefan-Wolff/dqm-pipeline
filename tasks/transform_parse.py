import bibtexparser
from bibtexparser.bparser import BibTexParser
from pylatexenc.latex2text import LatexNodes2Text
from pyspark.sql.functions import udf, when, explode, lit, regexp_extract, regexp_replace, lower
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
				
class ExtractBibtex:
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
		cust_parse = udf(lambda bibtex, publ_id: self.__parseBibtex(bibtex, publ_id), ExtractBibtex.BIBTEX_SCHEMA)
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
			for field in ExtractBibtex.BIBTEX_SCHEMA:
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
	ISSN = r'^(((977)-\d{3}-\d{4}-\d{2}-\d)|(\d{4}-\d{3}[0-9xX]))$'
	ISBN = r'^(((978)-\d-\d{3}-\d{5}-\d)|(\d-\d{3}-\d{5}-\d))$'

	def run(self, dataFrames, spark):
		df_works = dataFrames["works"]

		# ISSN & ISBN
		df_parsed = df_works.withColumn("issn_replaced", regexp_replace(regexp_replace(lower("issn"), r'(issn)| ', ''), r'[./_]', '-'))	\
							.withColumn("issn_parsed", regexp_extract("issn_replaced", ParseValues.ISSN, 0))	\
							.withColumn("isbn_replaced", regexp_replace(regexp_replace(lower("isbn"), r'(isbn)| ', ''), r'[./_]', '-'))	\
							.withColumn("isbn_parsed", regexp_extract("isbn_replaced", ParseValues.ISBN, 0))	\
							.withColumn("isbn_parsed2", regexp_extract("issn_replaced", ParseValues.ISBN, 0))
		
		result = {
			"works": df_parsed.withColumn("issn", when(df_parsed["issn_parsed"].isNotNull() & (df_parsed["issn_parsed"] != ""), df_parsed["issn_parsed"]).otherwise(lit(None)))	\
							  .withColumn("isbn", when(df_parsed["isbn_parsed"].isNotNull() & (df_parsed["isbn_parsed"] != ""), df_parsed["isbn_parsed"])	\
												.otherwise(when(	\
													df_parsed["isbn_parsed2"].isNotNull() & (df_parsed["isbn_parsed2"] != ""), df_parsed["isbn_parsed2"]).otherwise(lit(None))	\
												))	\
							   .select(df_works.columns)
		}
		
		result = {
			"works": df_works
		}
		
		return result