#!/usr/bin/env python
# coding: utf-8
#mary.orihuela@gmail.com

import argparse
import os
import pandas as pd
from time import time
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url, csv_name):

    #trips table
    os.system(f'wget -O {csv_name} {url}')  #download csv.gz
    
    df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, df):
    
    #zones table
    #os.system("wget -O taxi+_zone_lookup.csv https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv") 
    #df_zones = pd.read_csv('taxi+_zone_lookup.csv')
    #df_zones.to_sql(name='zones', con=engine, if_exists='replace')

    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(postgres_url)
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace' )

    df.to_sql(name=table_name, con=engine, if_exists='append')
    """    while True:
        try: 
            t_start = time()
        
            df = next(df_iter)
        
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
            df.to_sql(name=table_name, con=engine, if_exists='append')
        
            t_end = time()
        
            print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))
        
        except StopIteration:
            print("Finished ingesting data into the posgres database")
            break
    """
@flow(name="Subflow", log_prints=True)
def log_subflow(table_name:str):
    print("Logging Subflow for: {table_name}")

@flow(name="Ingest Flow")
def main_flow(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv' 

    log_subflow(table_name)
    raw_data = extract_data(url, csv_name)
    data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, data)

if __name__ == '__main__': 

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='pass for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name ok the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv.gz file')

    args = parser.parse_args()

    main_flow(args)
    
