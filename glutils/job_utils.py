# -*- coding: utf-8 -*-
"""glutils.job_utils

This module contains commonly used functions for the glue etl jobs.
"""
import base64
import zlib
import json
from pyspark.sql import DataFrame
from functools import reduce


def zipped_b64_to_string(val):
    if val:
        zipped_string = base64.b64decode(val)
        return zlib.decompress(zipped_string, 16 + zlib.MAX_WBITS).decode('utf-8')


def code_format(val):
    if val:
        return val.strip().replace(" ", "_").upper()


# UDF to read DynamoDB Json values directly
def get_dynamodb_value(dynamodb_string):
    if dynamodb_string:
        return list(json.loads(dynamodb_string).values())[0]


# This function does an UNIONALL of all the dataframes passed in
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)
