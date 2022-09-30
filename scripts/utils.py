""" Supporting classes for saving to S3."""

import os

import boto3
from loguru import logger

class S3Backup():

    def __init__(self, bucket_name, s3_subfolder):
        self.s3 = boto3.resource('s3')
        self.bucket_name = bucket_name
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.s3_subfolder = s3_subfolder

    def get_s3_path(self, local_path, folder):
            return os.path.join(self.s3_subfolder, folder, os.path.basename(local_path))

    # uploads file from local path with specified name
    def upload_file(self, local_path, folder):
        extra_args = {}
        if local_path.endswith('.png'):
            extra_args = {'ContentType': 'image/png'}
        elif local_path.endswith('.pdf'):
            extra_args = {'ContentType': 'application/pdf', 'ContentDisposition': 'inline'}
        elif local_path.endswith('.xlsx') or local_path.endswith('.xls'):
            extra_args = {'ContentType': 'application/vnd.ms-excel', 'ContentDisposition': 'inline'}
        elif local_path.endswith('.zip'):
            extra_args = {'ContentType': 'application/zip'}
        elif local_path.endswith('.json'):
            extra_args = {'ContentType': 'application/json'}

        s3_path = self.get_s3_path(local_path, folder)
        logger.info(f'Uploading file at {local_path} to {s3_path}')
        self.s3.meta.client.upload_file(local_path, self.bucket_name, s3_path, ExtraArgs=extra_args)


