import sys

from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, lower, length, trim
from pyspark.sql.types import ShortType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

CAMPAIGN_TABLE_NAME = 'campaign_opt_in_event'
APPLICATION = 'application'
COL_CAMPAIGN_KEY = 'campaign_key'
COL_STATE = 'state'
COL_APPLICATION_KEY = 'application_key'
COL_OPT_IN_IND = 'opt_in_ind'

# Define the output directory
output_dir = "s3://jornaya-dev-us-east-1-prj/publisher_permissions/setup/{}".format(CAMPAIGN_TABLE_NAME)

campaign_opt_in_event_df = glueContext.create_dynamic_frame \
    .from_catalog(database='rdl', table_name=CAMPAIGN_TABLE_NAME) \
    .toDF() \
    .select(COL_CAMPAIGN_KEY, COL_STATE, APPLICATION) \
    .dropna(subset=[COL_CAMPAIGN_KEY]) \
    .filter(length(trim(col(COL_CAMPAIGN_KEY))) > 0)

# Grab application table so we can join application to the campaigns DataFrame and get app key
application_df = glueContext.create_dynamic_frame \
    .from_catalog(database='prj', table_name=APPLICATION) \
    .toDF()

campaign_opt_in_event = campaign_opt_in_event_df \
    .join(application_df, lower(campaign_opt_in_event_df.application) == lower(application_df.application_nm)) \
    .select(COL_CAMPAIGN_KEY, COL_STATE, COL_APPLICATION_KEY) \
    .withColumn(COL_OPT_IN_IND, (when(lower(col(COL_STATE)) == 'in', 1)
                                 .otherwise(when(lower(col(COL_STATE)) == 'out', 0)
                                            .otherwise(None))).cast(ShortType())) \
    .dropna(subset=[COL_OPT_IN_IND]) \
    .drop(COL_STATE) \
    .dropDuplicates([COL_CAMPAIGN_KEY, COL_APPLICATION_KEY])

campaign_opt_in_event.write.parquet(output_dir, mode='overwrite')

job.commit()
