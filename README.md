# Data Engineering
This repo is to store the code and learning material that I have studied about data engineering. The content as follows:
* Data Ingestion
* Workflow Scheduling


## Data Ingestion
The purpose of this project is to store the code and documentation for what I have learned about storing the dataset to a temporary storage, preprocessing the dataset by using combined SQL syntax and Python syntax, and querying to retrieve the data from the database through Python.

**Tools**: Python, JupyterLab, Postgres, Git, Linux Terminal, Docker

**Libraries**: SQLAlchemy, Pandas, Pyarrow, Argparse

**Dataset**: TLC Trip Record Data, NYC Open Data [(Source)](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
* [Yellow Taxi Trip Records (Jan 2021)](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet)
* [Taxi Zone Lookup Table ](https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.parquet)

**Dataset description**
* The yellow taxi trip records dataset has 1.3 million rows and 19 variables that provide information about dropoff and pickup date & location, trip distances, fare, rate types, payment types, and passenger count.

**Summary**
* This raw dataset needs to be stored in a temporary storage or database to have more flexibility to preprocess and manipulate.
* Ingested 1.3 million rows of Parquet data file to Postgres database that has been hosted in the Docker container by applying batch-based data ingestion approach through Python.
* Added a new feature, duration, which is the amount of trip time in minutes by using SQL syntax in Jupyter Lab.

**What I have learned**
* Build a connection to postgres database by using Python in Jupyter Lab.
* Read a Parquet file by using Pyarrow libraries.
* Use argparse to create a reusable python script.
* Batch-based data ingestion.
* Add a new column to the existing database table with SQL data manipulation language such as ALTER and UPDATE and subquery.


**File Dictionaries**

There are two folders for this project, [data-ingestion](https://github.com/irfan-fadhlurrahman/practice-data-engineering/tree/main/data-ingestion) and [sql](https://github.com/irfan-fadhlurrahman/practice-data-engineering/tree/main/sql).
1. The data ingestion folder is used to build a container that hosts the database. This folder contains the following files.
* Dockerfile: for dockerized the script of data ingestion.
* docker-compose.yaml: scripts of multiple containers for hosting the temporary database.
* ingest_data.py: a script to ingest data.
* output.parquet: yellow taxi trip dataset (Jan 2021).
* psql-connection-testing.ipynb: to test the connection to the database and for creating the ingest_data.py script.

2. The sql folder is used for experimenting the data ingestion approach and querying and manipulating the data.
* add-duration-column.ipynb: notebook for adding the duration column to the database.
* ingest-data-to-db.ipynb: an experiment notebook for ingesting data to the database.
* ingest_raw_data.py: a script to ingest data.
* query-from-python.ipynb: notebook for querying.
* run_command.txt: command to upload the dataset to the temporary database.


## [Data Pipeline](https://github.com/irfan-fadhlurrahman/practice-data-engineering/tree/main/airflow-dag)
1. Create a simple data pipeline to extract the datetime from terminal, preprocess the returned datetime string, and save the preprocessed datetime to a CSV file.
2. Use Airflow to connect and run each task automatically.
