"""
Table: campaign
From: LRF
To: EDW
"""

import sys

from pyspark.context import SparkContext
from awsglue.transforms import ApplyMapping # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

ARGS = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
SC = SparkContext()
GLUE_CONTEXT = GlueContext(SC)
SPARK = GLUE_CONTEXT.spark_session
JOB = Job(GLUE_CONTEXT)
JOB.init(ARGS['JOB_NAME'], ARGS)

# define catalog source
SOURCE_DB_NAME = 'lrf'
SOURCE_TABLE = 'campaign'

# define target
TARGET_DB_NAME = 'edw'
TARGET_TABLE = 'campaign'
GLUE_CONNECTOR = 'redshift_connector'

# output directories
SOURCE_DIR = "s3://jornaya-dev-us-east-1-{}/{}".format(SOURCE_DB_NAME, SOURCE_TABLE)
STAGING_DIR = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(ARGS['JOB_NAME'])
TEMP_DIR = "s3://jornaya-dev-us-east-1-tmp/{}".format(ARGS['JOB_NAME'])


# Create dynamic frames from the source tables
SRCDATA = GLUE_CONTEXT.create_dynamic_frame.from_catalog(database=SOURCE_DB_NAME,
                                                         table_name=SOURCE_TABLE,
                                                         transformation_ctx='srcdata')

## @type: ApplyMapping
APPLY_MAPPING = ApplyMapping.apply(frame=SRCDATA,
                                   mappings=[("campaign_key", "string", "campaign_key", "string"),
                                             ("campaign_nm", "string", "campaign_nm", "string"),
                                             ("campaign_desc", "string", "campaign_desc", "string"),
                                             ("account_id", "string", "account_id", "string"),

                                             ("insert_ts", "timestamp", "insert_ts", "timestamp"),
                                             ("insert_job_run_id", "int", "insert_job_run_id", "int"),
                                             ("insert_batch_run_id", "int", "insert_batch_run_id", "int"),
                                             ("source_ts_date", "timestamp", "source_ts", "timestamp")],
                                   transformation_ctx="applymapping1")

SELECT_FIELDS = APPLY_MAPPING.select_fields(["campaign_key",
                                             "campaign_nm",
                                             "campaign_desc",
                                             "account_id",

                                             "insert_ts",
                                             "insert_job_run_id",
                                             "insert_batch_run_id",
                                             "source_ts"])

# write to redshift
GLUE_CONTEXT.write_dynamic_frame.from_jdbc_conf(frame=SELECT_FIELDS,
                                                catalog_connection=GLUE_CONNECTOR,
                                                connection_options={"dbtable": TARGET_TABLE,
                                                                    "database": TARGET_DB_NAME},
                                                redshift_tmp_dir=TEMP_DIR)
JOB.commit()
