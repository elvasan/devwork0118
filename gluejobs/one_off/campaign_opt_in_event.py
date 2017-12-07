import sys

from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp, when
from pyspark.sql.types import ShortType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TABLE_NAME = 'campaign_opt_in_event'

# Define the output directory
output_dir = "s3://jornaya-dev-us-east-1-prj/publisher_permissions/setup/{}".format(TABLE_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

campaign_opt_in_event_df = glueContext.create_dynamic_frame \
    .from_catalog(database='rdl', table_name=TABLE_NAME) \
    .toDF()

# Grab application table so we can join application to the campaigns DataFrame and get app key
application_df = glueContext.create_dynamic_frame \
    .from_catalog(database='prj', table_name='application') \
    .toDF()

campaign_opt_in_event = campaign_opt_in_event_df \
    .join(application_df, campaign_opt_in_event_df.application == application_df.application_nm) \
    .select('campaign_key', 'state', 'application_key', 'date') \
    .withColumn('opt_in_ts', to_timestamp(campaign_opt_in_event_df.date, 'yyyy-MM-dd')) \
    .withColumn('opt_in_ind', (when(col('state') == 'In', 1).otherwise(0)).cast(ShortType())) \
    .drop('state', 'date')

campaign_opt_in_event.write.parquet(output_dir, mode='overwrite')

job.commit()
