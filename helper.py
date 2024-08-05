
import boto3
import pandas as pd
import os
import configparser
from learn import bucket_name, access_key,secret_access_key


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
local_file_path = 'items/new.csv'
download_from_s3(bucket_name, s3_file_key=s3_file_key,
                 local_file_path=local_file_path)
