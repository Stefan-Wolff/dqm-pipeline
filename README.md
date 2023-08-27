# Data quality management for the dataset of ORCID

This project provides an implementation of the WQP (Workflow for improving the quality of publication data) specified for the public data of ORCID.

It contains esp. implementation for:
* loading of the source data,
* analyzing the data quality,
* transformation data to improve data quality,
* utilities for sampling of the data and monitoring of the quality improvement.


# Installation
The project requires [Python](https://www.python.org/) 3.8 or above and proposes to use [pip](https://pypi.org/project/pip/) for dependency management.
In addition it uses [Apache Spark](https://spark.apache.org/), which is implemented on Java. The API of Apache Spark is accessed by the Python library [pyspark](https://spark.apache.org/docs/latest/api/python/index.html).


## 1. Download the source code:
* `git clone https://github.com/Stefan-Wolff/dqm-pipeline`

## 2. Install requirements
The requirements can be set up [manually](manual-installation) or using [Docker](installation-using-docker).

### Manual installation
Install Python requirements:
* `cd dqm-pipeline`
* `pip install -r requirements.txt`

Install required system tools according to `requirements.system`, i. e. by:
* `apt install wget`
* `apt install aria2`


### Installation using Docker
Install, start and join Docker container `wqp`:
* `cd dqm-pipeline`
* `docker/setup_docker.sh`

After that, the application is ready for execution. For a quick start see [Example run](#example-run).


# Directory structure

## data
The directories contains all input data, output data and intermediate data.

### data/input
This directory contains all input data. Source information are included in the files with the extension ".src".

### data/parquets
*Parquet* is a data format that can be processed performantly by Apache Spark. It is used as input and cache format.

Intermediate data are stored in this directory. They are stored in directories whose names encode the preceding processing chain, e.g. `ParseBibtex.ParseValues.CorrectOrgs`. When loading intermediate states, the chain is stepped back to the last saved intermediate state.

The unchanged basic data are stored in a directory "initial" and the final output data are stored in the directory "complete".

### data/schemes
This directory contains all schemes of the base data. Entering schemes are not mandatory in the reading process, but they speed up the process. The current implementation uses these schemes and would throw errors if they were missing.

### data/tmp
This directory contains temporary data of the data downloading process. The data is only required while the downloading processes are running. They will be deleted after successful execution.


## lib
This directory contains some python code that has been swapped out because it is used in several places.

### lib/data_processor.py
This class loades the source data.

### lib/duplicates.py
This library encapsulates the strategy of grouping publications to identify duplicates.

### lib/metrics.py
This class supports the aggregation of indicators.

### lib/xml_parse.py
This is a specific SAX parser implementation to parse the ORCID source files in a performent way. It is used in `load_orcid_persons.py` and `load_orcid_works.py`.


## repo
This directory contains configuration and static information stored in json files.

### repo/monitor.json
This file contains measurements of the quality improvement.

### repo/quality.json
This file contains the measurements of the data quality. Every atomic indicator is listed, as well as all aggregated indicators.

### repo/quality_ORCID_2023.json
This file contains measurements of a specific run of the quality process. It is referenced by an external document.

### repo/sources.json
This is the configuration of the basic data, the quality of which is to be improved by the process. Its is used by `init_parquets.py` (transforms source format to parquet format) and `lib/data_processor.py` (loads data into tasks for data correcting and analyzing).


## tasks
This is the core of the processing pipeline. It contains the tasks to process the data. These are tasks to analyze the data (`metric_*.py`) and to correct the data (`transform_*.py`). Each of the scripts contains multiple processing tasks. They are executed in `analyze.py` and `transform.py`.
The script `entry_count.py` simply counts the number of records of the entities person, publication and organization.


# Usage
The main components of this project are stored in the root of the project and provided as separate scripts. The use of these components is explained in the following.

## Analyze data quality
    python analyze.py [-h] -m METRICS [METRICS ...] [-c CHAIN]
    
    options:
      -h                               show this help message and exit
      -m METRICS [METRICS ...]         names of metrics to run, default: run all
      -c CHAIN                         the source data related to the transformation chain, default: initial


Run a data quality measurement of specific metrics, i. e. `python3 analyze.py -m NotNull`. The following metrics are implemented:
* MinLength (Completeness)
* MinValue (Completeness)
* NotNull (Completeness)
* MinPopulation (Completeness)
* MinObject (Completeness)
* CorrectValue (Correctness)
* UniqueValue (Consistency)
* NoContradict (Consistency)
* UniqueObject (Consistency)

Or simply run all metrics with `python3 analyze.py`.
In addition, run all metrics of a specified data quality dimension (use *Completeness*, *Correctness* or *Consistency*), i. e. `python3 analyze.py -m Completeness`.

To run the measurements on a specific intermediate processing state of the data, simply specify the intermediate state, i. e. `python3 analyze.py -m Completeness -c ParseBibtex.ParseValues.CorrectOrgs`.



The results are printed to the console and stored in `repo/quality.json`.

## Correct data
    transform.py [-h] [-t TRANSFORMATION] [-c CHAIN]

    options:
      -h                        show this help message and exit
      -t TRANSFORMATION         the transformation to run, default: complete
      -c CHAIN                  the source data related to the transformation chain, default: initial
	  
Run a specific data transformation, i. e. `python3 transform.py -t ParseBibtex`. The following transformations are implemented:
* ParseBibtex
* ParseValues
* CorrectOrgs
* CorrectMinLength
* CorrectValues
* CorrectContradict
* JoinCrossRef
* Merge
* FilterContradict
* FilterObjects

Or simply run the complete transformation stack with `python3 transform.py`.
To run the transformation on a specific intermediate processing state of the data, simply specify the intermediate state, i. e. `python3 transform.py -c ParseBibtex.ParseValues.CorrectOrgs`.

## Download base data
The scripts `load_orcid_persons.py`, `load_orcid_works.py`, `load_crossref.py`, `load_lei.py` and `load_ror.py` downloads the related data. They don't need console parameters. The sources are stored in specific files in the data directory. The loaded data will be stored in `data/input`.

### Transform to parquet
The loaded data should be transformed to parquet format using `init_parquets.py` to improve processing performance. The resulting parquet files will be stored in `data/parquets/initial/[ENTITY]`.

    usage: init_parquets.py [-h] [-e ENTITY]

    options:
      -h                show this help message and exit
      -e ENTITY         limit to a single entity



## Sample data
Use `sample_extremes.py` to print a sample list of extreme values.

    sample_extremes.py [-h] [-n SAMPLE_NUM] -e {persons,works,orgUnits} -a ATTRIBUTE [-d] [-c CHAIN]

    options:
      -h                             show this help message and exit
      -n SAMPLE_NUM                  the number of samples to print
      -e {persons,works,orgUnits}    the entity to print
      -a ATTRIBUTE                   the attribute to print
      -d                             desc order
      -c CHAIN                       the source data related to the transformation chain


## Monitor quality improvement
The script `monitor.py` calculates the quality changes of the last process run related to the run before. The results will be stored in `repo/monitor.json`.  It calculates the difference between the quality indicators before running the transformation tasks and after there execution. In addition, they build the difference of the result indicators between the last run of the process and the process before.

    monitor.py [-h] [-m METRIC] [-b BASE] [-r RESULT]
    options:
      -h                show this help message and exit
      -m METRIC         name of metric to monitor
      -b BASE           the chain of initial measurements, default: initial
      -r RESULT         the chain of resulted measurements, default: complete
	  
Notice: There must be at least two measurements of base data (before transformation) and two measurements of resulted data (after measurements).


## Custom analyse
The script `custom.py` provides a container for custom code, i. e. specific analyse tasks.

    custom.py [-h] [-c CHAIN]

    options:
      -h               show this help message and exit
      -c CHAIN         source chain if analyze transformed data


## First steps

### Example data
Example data is provided for test purposes. It's a subset of the data from ORCID containing 10.000 records of the entity `works`. Additionally, all records of related entities are included.
This data can be used to perform quality analyses, i. e. `python3 analyze.py -m Correctness -c examples`.
The resulting measurements can be found in `repo/quality.json`.
Data transformations can also be performed, i. e. `python3 transform.py -t CorrectValues -c examples`.
In this case, the result data are stored in `data/parquets/examples.CorrectValues`.
The quality of this transformed data can be analyzed by `python3 analyze.py -m Correctness -c examples.CorrectValues`.


#### Example run

1. Analyze example data: `python3 analyze.py -c examples`
2. Run data transformations: `python3 transform.py -c examples`
3. Analyze resulting data: `python3 analyze.py -c examples.complete`


#### Full run

1. Load data
>`python3 load_orcid_persons.py`
>`python3 init_parquets.py -e persons`
>`python3 init_parquets.py -e orgUnits`
>`python3 load_orcid_works.py`
>`python3 init_parquets.py -e works`
>`python3 load_crossref.py`
>`python3 init_parquets.py -e crossRef`
>`python3 load_lei.py`
>`python3 init_parquets.py -e lei`
>`python3 load_ror.py`
>`python3 init_parquets.py -e ROR`
>`python3 load_fundref.py`
>`python3 init_parquets.py -e fundref`

2. Analyze data quality before transformation: `python3 analyze.py`

3. Run the complete transformation stack: `python3 transform.py`

4. Analyze data quality after transformation: `python3 analyze.py -c complete`

5. Monitor quality changes: `python3 monitor.py`
(only useful from the second run of the process)

# Author
**Stefan Wolff**

# Notice
A reference to a publication will be added in the future that describes the underlying workflow (the WQP) in detail.