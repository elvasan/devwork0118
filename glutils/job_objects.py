# -*- coding: utf-8 -*-
"""glutils.job_objects

This module contains commonly used objects for the glue etl jobs.
"""
from pyspark.sql.types import StructField, StructType, StringType


# DynamoDB Objects
n_schema = StructType([StructField("n", StringType())])
s_schema = StructType([StructField("s", StringType())])
sS_schema = StructType([StructField("sS", StringType())])
bOOL_schema = StructType([StructField("bOOL", StringType())])
m_schema = StructType([StructField('m', StringType())])
b_schema = StructType([StructField('b', StringType())])
