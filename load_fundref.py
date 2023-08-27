import os
import csv

# from https://www.crossref.org/documentation/funder-registry/accessing-the-funder-registry/
SOURCE_URL = "https://doi.crossref.org/funderNames?mode=list"
OUT_FILE = "data/input/FUNDREF.csv"


def run():
	# download data
	os.system("wget -O " + OUT_FILE + " " + SOURCE_URL)			# Simple, but keeps the architecture uniform
	print("done")

if "__main__" == __name__:
	run()