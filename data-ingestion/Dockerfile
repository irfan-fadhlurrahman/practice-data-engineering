FROM python:3.9.1

# install wget to download the parquet file
RUN apt-get install wget

# install neccessary python packages
RUN pip install sqlalchemy pandas pyarrow psycopg2-binary

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]