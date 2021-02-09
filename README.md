# IGUANA Result 2 CSV

small script to convert IGUANA result files from RDF to CSV (and JSON).

Installing with 
```shell
pip install .
``` 

makes the command `iguanaresult2csv` available. Check the help page with 
```shell
iguanaresult2csv --help
```

Using pypy3 is considerably faster than default CPython.

Additionally, the bash script [`preprocess.sh`](preprocess.sh) can convert IGUANA result files from NTriple to Turtle. 
This makes them smaller and more human-readable. (For details on how to run it, please read the script. It is very short. It is not installed by pip.) 