mkdir -p data/tmp
aria2c data/April2022Crossref.torrent -d data/tmp
rm -f data/Crossref
mv "tmp/April 2022 Public Data File from Crossref" data/CrossRef