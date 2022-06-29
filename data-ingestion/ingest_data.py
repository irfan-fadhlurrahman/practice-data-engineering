import os
import argparse
import pandas as pd
import pyarrow.parquet as pq

from time import time
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    
    db = params.db
    table_name = params.table_name
    url = params.url
    file_name = 'output.parquet'
    
    # download the CSV file
    os.system(f"wget {url} -O {file_name}")
    
    # sql engine
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}'
    )
    # initialize the header of table
    parquet_file = pq.ParquetFile(file_name)
    df = parquet_file.read().to_pandas()
    df.head(n=0).to_sql(
        name='yellow_taxi_data', 
        con=engine, 
        if_exists='replace', 
        index=False
    )
    # create generator for parquet file
    parquet_data_generator = parquet_file.iter_batches()

    # upload to database
    while True: 
        try:
            t_start = time()
            batch_df = next(parquet_data_generator).to_pandas()

            batch_df["tpep_pickup_datetime"] = pd.to_datetime(batch_df["tpep_pickup_datetime"])
            batch_df["tpep_dropoff_datetime"] = pd.to_datetime(batch_df["tpep_dropoff_datetime"])

            batch_df.to_sql(
                name='yellow_taxi_data', 
                con=engine, 
                if_exists='append', 
                index=False
            )
            t_end = time()
            print('Inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print('Completed')
            break

if __name__ == '__main__':
    # parse the command line arguments and calls the main program
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the dataset file')

    args = parser.parse_args()

    main(args)
