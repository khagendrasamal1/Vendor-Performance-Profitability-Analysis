import pandas as pd 
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine, if_exists = 'replace', index=False)
    
def load_raw_data():
    '''This function loads CSVs in chunks to save memory'''
    start = time.time()
    
    # Ensure the data directory exists
    if not os.path.exists('data'):
        logging.error("Data directory not found")
        return

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            table_name = file[:-4]
            logging.info(f'Starting ingestion for {file}')

            # Use chunksize to read the file in manageable pieces (e.g., 100,000 rows)
            # Adjust chunksize based on your RAM (smaller = less RAM usage)
            chunksize = 100000 
            
            first_chunk = True
            for chunk in pd.read_csv(file_path, chunksize=chunksize):
                # For the first chunk, replace the table. 
                # For subsequent chunks, append to the same table.
                if_exists_param = 'replace' if first_chunk else 'append'
                
                chunk.to_sql(table_name, con=engine, if_exists=if_exists_param, index=False)
                first_chunk = False
                
            logging.info(f'Successfully ingested {file} into {table_name}')

    end = time.time()
    total_time = (end - start) / 60
    logging.info(f'Ingestion Complete. Total Time: {total_time:.2f} minutes')
    
if __name__ == '__main__':
    load_raw_data()