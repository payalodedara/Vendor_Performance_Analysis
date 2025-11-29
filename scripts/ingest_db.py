import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
from dotenv import load_dotenv

# Set base directory
BASE_DIR = os.path.dirname(os.getcwd())
ENV_PATH = os.path.join(BASE_DIR, ".env")
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Ensure logs folder exists
os.makedirs(LOG_DIR, exist_ok=True)

# Load .env
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Logging configuration
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ingestion_db.log"),
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# MySQL engine
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

def ingest_db(file_path, table_name, engine, chunksize=100000):
    """Ingest large CSVs in chunks to MySQL"""
    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        chunk.to_sql(table_name, con=engine, if_exists='append', index=False)

def load_raw_data():
    start = time.time()
    
    for file in os.listdir(DATA_DIR):
        if file.endswith('.csv'):
            file_path = os.path.join(DATA_DIR, file)
            table_name = file[:-4]  # remove .csv
            logging.info(f'Ingesting {file} into database')
            
            ingest_db(file_path, table_name, engine)
    
    end = time.time()
    total_time = (end - start)/60
    logging.info('--------------Ingestion Complete------------')
    logging.info(f'Total Time Taken: {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()
