
# coding: utf-8

from time import time   
import os

import pandas as pd
from sqlalchemy import create_engine

import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    
    os.system(f"wget {url} -O {csv_name}") # Tải file xuống bằng wget và lưu với tên mong muốn
    
    # download the csv 
    # print(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace') 
    # Nếu table đã tồn tại thì replace
    # Head với n=0 với mục đích chỉ lấy ra tên cột rồi tạo bảng trống trong database,
    # sau đó mơi thêm từng chunk dòng vào table

    df.to_sql(name=table_name, con=engine, if_exists='append') 

    while True:
        try:
            t_start = time()
            
            df = next(df_iter)
            
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time()
            
            print('inserted another chunk, took %.3f second' % (t_end - t_start))
            
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user
    # passwword
    # host
    # port
    # database name
    # table name,
    # url of the csv

    parser.add_argument('--user', help='user for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)

    
