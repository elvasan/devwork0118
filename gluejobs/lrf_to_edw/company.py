"""
Table: Company
From: LRF
To: EDW
"""

import sys

from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions # pylint: disable=import-error
from awsglue.context import GlueContext # pylint: disable=import-error
from awsglue.dynamicframe import DynamicFrame # pylint: disable=import-error
from awsglue.job import Job # pylint: disable=import-error

args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Creating DynamicFrame from company's LRF table
company_lrf_df = glueContext.create_dynamic_frame.from_catalog(database="lrf",
                                                               table_name="company",
                                                               transformation_ctx="company_lrf_df").toDF()

# Selecting required columns to be loaded into EDW company table
company_edw_df = company_lrf_df.select(
    company_lrf_df.account_key,
    company_lrf_df.account_id,
    company_lrf_df.company_nm,
    company_lrf_df.entity_id,
    company_lrf_df.is_active_ind,
    company_lrf_df.role_nm,
    company_lrf_df.insert_ts,
    company_lrf_df.insert_batch_run_id,
    company_lrf_df.insert_job_run_id,
    company_lrf_df.source_ts
)

# Converting selected DataFrame back to DynamicFrame to be used for writing data to Redshift using Glue Library
company_edw_glue_df = DynamicFrame.fromDF(company_edw_df, glueContext, "company_df")


datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=company_edw_glue_df,
                                                           catalog_connection="redshift_connector",
                                                           connection_options={"dbtable": "company", "database": "edw"},
                                                           redshift_tmp_dir=args["TempDir"],
                                                           transformation_ctx="datasink4")

job.commit()
