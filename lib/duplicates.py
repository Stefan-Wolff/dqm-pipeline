"""This library supports the deduplication process by grouping and normalizing duplicate publications"""
import re
from pyspark.sql.functions import udf, when, col


class WorksKey:
	# 161=¡, 162=¢, 163=£, 164=¤, 165=¥, 166=¦, 167=§, 168=¨, 169=©, 170=ª, 171=«, 172=¬, 173=­, 174=®, 175=¯, 176=°, 177=±, 178=², 179=³, 180=´, 181=µ, 182=¶, 183=·, 184=¸, 185=¹, 186=º, 187=», 188=¼, 189=½, 190=¾, 191=¿, 224=à, 225=á, 226=â, 227=ã, 229=å, 230=æ, 231=ç, 232=è, 233=é, 234=ê, 235=ë, 236=ì, 237=í, 238=î, 239=ï, 240=ð, 241=ñ, 242=ò, 243=ó, 244=ô, 245=õ, 215=×, 248=ø, 249=ù, 250=ú, 251=û, 253=ý, 254=þ, 223=ß, 247=÷, 255=ÿ, 257=ā, 259=ă, 261=ą, 263=ć, 265=ĉ, 267=ċ, 269=č, 271=ď, 273=đ, 275=ē, 277=ĕ, 279=ė, 281=ę, 283=ě, 285=ĝ, 287=ğ, 289=ġ, 291=ģ, 293=ĥ, 295=ħ, 297=ĩ, 299=ī, 301=ĭ, 303=į, 305=ı, 307=ĳ, 309=ĵ, 311=ķ, 312=ĸ, 314=ĺ, 316=ļ, 318=ľ, 320=ŀ, 322=ł, 324=ń, 326=ņ, 328=ň, 329=ŉ, 331=ŋ, 333=ō, 335=ŏ, 337=ő, 339=œ, 341=ŕ, 343=ŗ, 345=ř, 347=ś, 349=ŝ, 351=ş, 353=š, 355=ţ, 357=ť, 359=ŧ, 361=ũ, 363=ū, 365=ŭ, 367=ů, 369=ű, 371=ų, 373=ŵ, 375=ŷ, 378=ź, 380=ż, 382=ž, 383=ſ, 513=ȁ, 515=ȃ, 517=ȅ, 519=ȇ, 521=ȉ, 523=ȋ, 525=ȍ, 527=ȏ, 529=ȑ, 531=ȓ, 533=ȕ, 535=ȗ, 537=ș, 539=ț, 8208=‐, 8209=‑, 8210=‒, 8211=–, 8212=—, 8213=―, 8216=‘, 8217=’, 8218=‚, 8219=‛, 8220=“, 8221=”, 8222=„, 8223=‟, 8224=†, 8225=‡, 8226=•, 8227=‣, 8230=…, 8249=‹, 8250=›, 228=ä, 246=ö, 252=ü
	NORM_CHARS = {161: " ", 162: "cent", 163: "pound", 164: " ", 165: "yen", 166: "|", 167: "paragraph", 168: " ", 169: "(c)", 170: "a", 171: "\"", 172: "not", 173: "-", 174: "(r)", 175: " ", 176: "degree", 177: "plus-minus", 178: "2", 179: "3", 180: " ", 181: "micro", 182: " ", 183: " ", 184: " ", 185: "1", 186: "o", 187: "\"", 188: "1/4", 189: "1/2", 190: "3/4", 191: " ", 224: "a", 225: "a", 226: "a", 227: "a", 229: "a", 230: "ae", 231: "c", 232: "e", 233: "e", 234: "e", 235: "e", 236: "e", 237: "i", 238: "i", 239: "i", 240: "d", 241: "n", 242: "o", 243: "o", 244: "o", 245: "o", 215: "x", 248: "o", 249: "u", 250: "u", 251: "u", 253: "y", 254: "p", 223: "ss", 247: ":", 255: "y", 257: "a", 259: "a", 261: "a", 263: "c", 265: "c", 267: "c", 269: "c", 271: "d", 273: "d", 275: "e", 277: "e", 279: "e", 281: "e", 283: "e", 285: "g", 287: "g", 289: "g", 291: "g", 293: "h", 295: "h", 297: "i", 299: "i", 301: "i", 303: "i", 305: "i", 307: "ij", 309: "j", 311: "k", 312: "k", 314: "l", 316: "l", 318: "l", 320: "l", 322: "l", 324: "n", 326: "n", 328: "n", 329: "'n", 331: "n", 333: "o", 335: "o", 337: "o", 339: "oe", 341: "r", 343: "r", 345: "r", 347: "s", 349: "s", 351: "s", 353: "s", 355: "t", 357: "t", 359: "t", 361: "u", 363: "u", 365: "u", 367: "u", 369: "u", 371: "u", 373: "w", 375: "y", 378: "z", 380: "z", 382: "z", 383: "s", 513: "a", 515: "a", 517: "e", 519: "e", 521: "i", 523: "i", 525: "o", 527: "o", 529: "r", 531: "r", 533: "u", 535: "u", 537: "s", 539: "t", 8208: "-", 8209: "-", 8210: "-", 8211: "-", 8212: "-", 8213: "-", 8216: "'", 8217: "'", 8218: "'", 8219: "'", 8220: "\"", 8221: "\"", 8222: "\"", 8223: "\"", 8224: "+", 8225: "+", 8226: "-", 8227: "-", 8230: "...", 8249: "'", 8250: "'", 228: "ae", 246: "oe", 252: "ue"}

	
	def build(self, title, date, authors):
		if not title or not date or not authors:
			return None
	
		result = title + "#" + date
		
		for author in authors:
			result += "#" + self.__formateAuthor(author)
	
		result = self.__unify(result)
	
		return result
		
		
	def __formateAuthor(self, author):
		result = ""
		
		if "firstName" in author and "lastName" in author:
			result = self.__formateName(author["lastName"] + ", " + author["firstName"])
			
		elif "otherNames" in author:
			result = self.__formateName(author["otherNames"])
			
		elif "publishedName" in author:
			result = self.__formateName(author["publishedName"])
			
		return result
		
		
	def __formateName(self, name):
		name_parts = name.split(",")
		if 1 == len(name_parts):
			name_parts.split(" ")
			name_parts.reverse()
			
		return name_parts[0].strip() + ", " + name_parts[-1].strip()
	
	
	def __unify(self, value):
		result = re.sub(r'[,;:]', '.', value.lower())
		result = re.sub(r'[ ]+', ' ', result)
		result = re.sub(r'^ | $|[\'"´`]', '', result)

		return result.translate(WorksKey.NORM_CHARS)
		
		
		
def groupDuplicates(df_works):
		# group by doi OR title#year#authors OR orcid_publication_id
		cust_key = udf(lambda title, date, authors: WorksKey().build(title, date, authors))
		return df_works.withColumn("key", when(col("title").isNotNull() & col("date").isNotNull() & col("authors").isNotNull(),	\
												cust_key(col("title"), col("date"), col("authors")))	\
											.otherwise(col("orcid_publication_id")))