import bibtexparser
from bibtexparser.bparser import BibTexParser
from pylatexenc.latex2text import LatexNodes2Text
from pyspark.sql.functions import udf, when
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

				
class ExtractBibtex:
	BIBTEX_SCHEMA = StructType([
		StructField('doi', StringType()),
		StructField('year', StringType()),
		StructField('journal', StringType()),
		StructField('abstract', StringType()),
		StructField('orcid-numbers', StringType()),
		StructField('author', ArrayType(StructType([ \
											StructField('fullName', StringType()), \
											StructField('orcid_id', StringType())]))),
		StructField('issn', StringType()),
		StructField('isbn', StringType()),
		StructField('url', StringType()),
		StructField('subtitle', StringType()),
		StructField('title', StringType()),
	])

	def run(self, df_persons, df_works):
		# parse bibtex
		cust_parse = udf(lambda bibtex: self.__parseBibtex(bibtex), ExtractBibtex.BIBTEX_SCHEMA)
		df_works_parsed = df_works.withColumn('bibtex_data', cust_parse(df_works["bibtex"]))
		
		# take bibtex values
		df_works_bibtex = df_works_parsed.withColumn()
		
		df_works_parsed.where(df_works_parsed["bibtex_data"].isNotNull()).show(100, 100)

		return df_persons, df_works_parsed


	def __parseBibtex(self, bibtex):
		if not bibtex:
			return None
			
		bib_db = bibtexparser.loads(bibtex, parser=BibTexParser(common_strings = True))

		if bib_db.entries and (1 == len(bib_db.entries)):					# should contains exactly one entry, otherwise it's ambiguous
			entry = bib_db.entries[0]

			# decode special chars
			latexDecoder = LatexNodes2Text()
			for name, value in entry.items():
				entry[name] = latexDecoder.latex_to_text(value)

			result = {"type": entry["ENTRYTYPE"]}
			for field in ExtractBibtex.BIBTEX_SCHEMA:
				if isinstance(field.dataType, StringType) and (field.name in entry) and entry[field.name]:
					result[field.name] = entry[field.name]
			
			author_value = ""
			if ("orcid-numbers" in entry) and entry["orcid-numbers"]:
				author_value = entry["orcid-numbers"]
			elif ("author" in entry) and entry["author"]:
				author_value = entry["author"]
			
			authors = []
			for author in author_value.split(" and "):
				author_parts = author.split("/")
				author_entry = {"fullName": author_parts[0]}
				authors.append(author_entry)
				
				if 2 == len(author_parts):
					author_entry["orcid_id"] = author_parts[1]
					
			if authors:
				result["author"] = authors
				
			
			return result
			
			
	def __selectValues(record, bibtex):
		result = dict(record)
		
		if "doi" in bibtex and bibtex["doi"] and not result["doi"]:
			result["doi"] = bibtex["doi"]
		
		if "year" in bibtex and bibtex["year"] and not result["date"]:
			result["date"] = bibtex["year"]
		
		if "journal" in bibtex and bibtex["journal"] and not result["journal"]:
			result["journal"] = bibtex["journal"]

		if "abstract" in bibtex and bibtex["abstract"] and not result["abstract"]:
			result["abstract"] = bibtex["abstract"]


		if "orcid-numbers" in bibtex and bibtex["orcid-numbers"]:
			result["authors"] = bibtex["orcid-numbers"].replace(" and ", "#").replace("/", "|")
			
		elif "author" in bibtex and not record["authors"]:
			result["authors"] = bibtex["author"].replace(" and ", "#")


		return result
		
		
		
class JoinCrossRef:
	def run(self, df_persons, df_works_joined):
		#CROSSREF_PATH = "data/CrossRef/25.json.gz"
		CROSSREF_PATH = "data/CrossRef/*"

		def correct(self, df_persons, df_works, spark):
			df_crossref = spark.read.json(Correction1.CROSSREF_PATH, multiLine=True)
			
			df_works_joined = df_works.join(df_crossref, df_works["doi"] == df_crossref["doi"], 'left')	\
								.withColumn("date", when(df_works["date"].isNotNull(), df_works["date"]).otherwise(df_crossref["date"])) \
								.withColumn("type", when(df_works["type"].df_works(), df_works["type"]).otherwise(df_crossref["type"])) \
								.withColumn("title", when(df_works["title"].isNotNull(), df_works["title"]).otherwise(df_crossref["title"])) \
								.withColumn("authors", when(df_works["authors"].isNotNull(), df_works["authors"]).otherwise(df_crossref["authors"])) \
								.withColumn("short_description", when(df_works["short_description"].isNotNull(), df_works["short_description"]).otherwise(df_crossref["abstract"])) \
								.withColumn("subTitle", when(df_works["subTitle"].isNotNull(), df_works["subTitle"]).otherwise(df_crossref["subTitle"])) \
								.withColumn("citated", when(df_joined.citated.isNotNull(), df_joined.citated).otherwise(df_joined.crossref_citation_count)) \
								.select(df_works.columns)
			
			
			# join orcid works with crossref data
			return df_persons, df_works_joined