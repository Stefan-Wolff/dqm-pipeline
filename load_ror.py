import os
import zipfile


SOURCE_URL = "https://zenodo.org/record/7926988/files/v1.25-2023-05-11-ror-data.zip"
OUT_FILE = "data/input/ROR.csv"
TMP_DIR = "data/tmp/"


def run():
	# download data
	os.system("mkdir -p " + TMP_DIR)
	fileName = SOURCE_URL.split("/")[-1]
	os.system("wget -O " + TMP_DIR + fileName + " " + SOURCE_URL)
		
	# extract and save data
	with zipfile.ZipFile(TMP_DIR + fileName, 'r') as zip_ref:
		for member in zip_ref.namelist():
			if member.endswith(".csv"):
				zip_ref.extract(member, TMP_DIR)
				os.rename(TMP_DIR + member, OUT_FILE)
				print(OUT_FILE, " loaded")			
				break

	# clean up temp file
	os.remove(TMP_DIR + fileName)

if "__main__" == __name__:
	run()