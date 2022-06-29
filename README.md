# Data Engineering
This repo is to store the code and learning material that I have studied about data engineering. 

**OS**: Ubuntu 18.04 | **Tools**: Python 3.9, Airflow, PostgreSQL, Docker

## [Data Pipeline](https://github.com/irfan-fadhlurrahman/practice-data-engineering/tree/main/airflow-dag)
1. Create a simple data pipeline to extract the datetime from terminal, preprocess the returned datetime string, and save the preprocessed datetime to a CSV file.
2. Use Airflow to connect and run each task automatically.

## [Data Ingestion](https://github.com/irfan-fadhlurrahman/practice-data-engineering/tree/main/data-ingestion)
1. Host a postgres database in the Docker container
2. Preprocess the parquet file ([NYC Taxi Tripdata, January 2021](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page))
3. Upload the preprocessed data to postgres database 

