import os
import pandas as pd
from sqlalchemy import create_engine

# Set up MariaDB connection
username = 'root'
password = ''
host = 'localhost'  # or your MariaDB server address
port = '3306'  # default MariaDB port
database = 'pract' # >>>>>>>>>>>>>>เปลี่ยนชื่อ DATABASE ตรงนี้ตามชื่อ DATABASE ใน SQL <<<<<<<<<<<<<<<<<<<<<<


db_url = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(db_url)
# Define the folder containing CSV files
folder_path = '../data/archive' #>>>>>>>>ย้ายตามจุดที่ folder archieve อยู่<<<<<<<<<<

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        # Get the table name by removing the .csv extension
        table_name = filename[:-4]  # Remove the last 4 characters (.csv)
        file_path = os.path.join(folder_path, filename)

        # Read the CSV file in chunks
        chunk_size = 100000  # Adjust based on memory availability
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            # Write each chunk to the database
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)

        print(f'{filename} has been imported into the table {table_name}.')

print('All files have been imported into the database.')
