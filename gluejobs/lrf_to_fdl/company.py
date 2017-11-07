# Company table transformation from LRF to FDL

import sys
from pyspark.context import SparkContext


from awsglue.utils import getResolvedOptions # pylint: disable=import-error
from awsglue.context import GlueContext # pylint: disable=import-error
from awsglue.job import Job # pylint: disable=import-error

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# define catalog variables
tbl_name = 'company'

# Pass these file paths in as args instead of hard coding them
output_dir = "s3://jornaya-dev-us-east-1-fdl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])


# Reading in the source files from LFR
db_name = 'lrf'
source_tbl = "company"

# Read in the accounts table into an Dataframe
company_lrf_df = glueContext.create_dynamic_frame.from_catalog(database="{}".format(db_name),
                                                               table_name="{}".format(source_tbl),
                                                               transformation_ctx="{}".format(source_tbl)).toDF()

# Select fdl specific columns
company_fdl_df = company_lrf_df.select(
    company_lrf_df.account_key,
    company_lrf_df.account_id,
    company_lrf_df.source_mod_ts,
    company_lrf_df.company_nm,
    company_lrf_df.entity_id,
    company_lrf_df.is_active_ind,
    company_lrf_df.role_nm,
    company_lrf_df.insert_ts,
    company_lrf_df.insert_batch_run_id,
    company_lrf_df.insert_job_run_id,
    company_lrf_df.source_ts
)

# write company table values to fdl
company_fdl_df.write.parquet(output_dir, mode='overwrite')

job.commit()
