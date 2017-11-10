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
tbl_name = 'campaign'

# Pass these file paths in as args instead of hard coding them
output_dir = "s3://jornaya-dev-us-east-1-fdl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])


# Reading in the source files from LFR
db_name = 'lrf'
source_tbl = "campaign"

# Read in the accounts table into an Dataframe
campaign_lrf_df = glueContext.create_dynamic_frame.from_catalog(database="{}".format(db_name),
                                                               table_name="{}".format(source_tbl),
                                                               transformation_ctx="{}".format(source_tbl)).toDF()

# Select fdl specific columns
campaign_fdl_df = campaign_lrf_df.select(
    campaign_lrf_df.campaign_key,
    campaign_lrf_df.campaign_nm,
    campaign_lrf_df.campaign_desc,
    campaign_lrf_df.industry_nm,
    campaign_lrf_df.account_id,
    campaign_lrf_df.insert_ts,
    campaign_lrf_df.insert_batch_run_id,
    campaign_lrf_df.insert_job_run_id,
    campaign_lrf_df.source_ts,
    campaign_lrf_df.attrib_hash_value
)

# write company table values to fdl
campaign_fdl_df.write.parquet(output_dir, mode='overwrite')

job.commit()
