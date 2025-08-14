import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
import re


logging.basicConfig(
    filename = "logs/ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

engine = create_engine("sqlite:///inventory.db")


def ingest_db(df,table_name,engine):
    '''This function will ingest dataframe into database'''
    df.to_sql(table_name,con=engine,if_exists="replace",index=False,method="multi",chunksize=2000)

def load_raw_data():
    '''This function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir(r"E:/Vendor Performance Data"):
        table_name = re.sub(r'\W+', '_', file[:-11])
        if file.endswith(".csv"):
            try:
                df = pd.read_csv(r"E:/Vendor Performance Data/"+ file)
                logging.info(f"Ingesting {file} in db")
            except Exception as E:
                logging.error(f"Error reading {file}: {E}")
            try:
                ingest_db(df,table_name,engine)
            except:
                logging.error(f"Error writting {file}: {E}")
    end = time.time()
    total_time = (end - start)/60
    logging.info("---------------Ingestion Complete---------------")
    logging.info(f"\nTotal Time Taken: {total_time} minutes")

if __name__=="__main__":
    load_raw_data()