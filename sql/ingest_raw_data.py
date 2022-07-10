import os
import argparse
import pandas as pd
import pyarrow.parquet as pq

from time import time
from sqlalchemy import create_engine

def main(params):
    # add parameters
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    file_path = params.file_path
    table_name = params.table_name
    
    # database connection
    db_con = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    
    # initialize the header of table
    parquet_file = pq.ParquetFile(file_path)
    df = parquet_file.read().to_pandas()
    df.head(n=0).to_sql(
        name=table_name, 
        con=db_con, 
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
                name=table_name, 
                con=db_con, 
                if_exists='append', 
                index=False
            )
            t_end = time()
            print('Inserted another chunk, took %.3f second' % (t_end - t_start))
        
        except StopIteration:
            print('completed')
            break

    # add trip_id as primary key
    query = f"""
    ALTER TABLE {table_name}
        ADD COLUMN trip_id SERIAL PRIMARY KEY;
    """
    db_con.execute(query)
    
if __name__ == '__main__':
    # parser description
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres database')
    
    # parse the command line arguments and calls the main program
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--file_path', help='directory path of the dataset file')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    
    # run the main function
    args = parser.parse_args()
    main(args)