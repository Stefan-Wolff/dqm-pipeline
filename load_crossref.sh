mkdir -p data/tmp
aria2c data/April2022Crossref.torrent -d data/tmp --seed-time=0
rm -f -r data/CrossRef
mv "data/tmp/April 2022 Public Data File from Crossref" data/CrossRef