import boto3
import pandas as pd
import psycopg2
import os
import configparser
import csv

config = configparser.ConfigParser()
config.read('.env')
access_key = config['AWS']['access_key_id']
secret_access_key = config['AWS']['secret_access_key']
bucket_name = config['AWS']['bucket_name']


def download_from_s3(bucket_name, s3_file_key, local_file_path):
    '''
     Downloads a file from an S3 bucket to a local file path.

    Args:
        bucket_name (str): The name of the S3 bucket.
        s3_file_key (str): The key of the file in the S3 bucket.
        local_file_path (str): The local path where the file will be saved.
    '''
    s3_client = boto3.client('s3',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_access_key
                             )
    try:
        print(f'Downloading {s3_file_key} from s3 bucket {bucket_name}')
        s3_client.download_file(bucket_name, s3_file_key, local_file_path)
        print(f'File downloaded successfully to {local_file_path}')
    except Exception as e:
        print(f'Error while downloading file {e}')


s3_file_key = 'metal.csv'
local_file_path = 'data/metal.csv'
download_from_s3(bucket_name, s3_file_key=s3_file_key,
                 local_file_path=local_file_path)


data = pd.read_csv(f'{local_file_path}')
# # # #
# # print(data)
old_col = data.columns

cleaned_col = [x.replace('time (edt)', 'time_edt') for x in old_col]
data.columns = cleaned_col

# Change data type and drop contract column
# print(data)
data = (data.assign(price=lambda x: x['price'].str.replace(',', '').astype(float),
                    change=lambda x: x['change'].astype(float),
                    perc_change=lambda x: x['perc_change'].str.strip(
                        '12+%').astype(float)
                    )
        )


cleaned_data = data.drop(columns='contract')
cleaned_data.to_csv('data/metal_data.csv', index=False)


with open('secret.txt') as f:
    file = f.readlines()

secret = [x.split('=')[1].strip() for x in file]


def db_connection():
    try:
        conn = psycopg2.connect(
            database=secret[0],
            user=secret[1],
            password=secret[2],
            host=secret[3],
            port=secret[4]
        )
        print(f'Successfully connected to database')
    except Exception as e:
        print(f'connection failed ==>{e}')

    return conn


connection = db_connection()
# Create a cursor obj
cursor = connection.cursor()

# Creating table in postgres
drop_table = 'DROP TABLE IF EXISTS metal;'

metal = ''' 
    CREATE TABLE metal (
    metal_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    units VARCHAR(10),
    price DECIMAL(10, 2),
    change DECIMAL(10, 4),
    perc_change DECIMAL(10,2),
    time_edt VARCHAR(50)
    );

'''
cursor.execute(f'{drop_table}')  # drop table if exist

cursor.execute(f'{metal}')  # create table statement
connection.commit()

print(f'Sql table {metal} created successfully')

# Copy csv file to postresql database
try:
    with open('data/metal_data.csv', 'r') as f:
        next(f)  # This skips the header row if the csv has a header
        # Copy data from file into the table in database
        cursor.copy_expert("""                   
                           COPY metal(name, units, price, change, perc_change, time_edt) 
                           FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"')
                           """, f)
        print(f'Data copied successfully to postresql')
except Exception as e:
    print(f'Copying data to postresql failed because of the error: {e}')
    raise e


cursor.close()
connection.close()
