#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This script performs 3 operations:
    - To load data from a S3 bucket
    - Convert it to gz parquet
    - Upload it to another S3 bucket.

This script uses Python3.
"""

import gzip
import io
import boto3
import pandas as pd

# TO-DO use logger


def list_of_files(bucket, delimiter, prefix):
    """Collect list of certain files from S3 bucket.

    Args:
        bucket (string): S3 bucket
        delimiter (string): path inside the prefix
        prefix (string): object path

    Returns:
        list: list of files
    """
    list_keys = []
    for obj in bucket.objects.filter(Delimiter=delimiter, Prefix=prefix):
        list_keys.append(obj.key)

    return list_keys


def s3_to_pandas(client, bucket, key):
    """Read pandas dataframe from json file on S3.

    Args:
        client (string): S3 client
        key (string): S3 key path

    Returns:
        pandas dataframe: json read from S3 file
    """
    obj = client.get_object(Bucket=bucket, Key=key)
    gz = gzip.GzipFile(fileobj=obj["Body"])

    return pd.read_json(gz, lines=True)


def pandas_to_s3(df, client, bucket, key):
    """Convert pandas dataframe to parquet and upload on S3.

    Args:
        df (pandas dataframe): dataframe to be saved onto S3
        client (string): S3 client
        bucket (string): new S3 bucket
        key (string): S3 key path
    """
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    parquet_buffer.seek(0)

    gz_buffer = io.BytesIO()

    with gzip.GzipFile(mode="w", fileobj=gz_buffer) as gz_file:
        gz_file.write(parquet_buffer.getvalue())

    client.put_object(Bucket=bucket, Key=key, Body=gz_buffer.getvalue())
