import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, current_timestamp, lit, from_unixtime

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
db_name = 'udl'
tbl_name = 'campaigns'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-lrf/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# This needs to change so we directly read it from Glue's Catalog and not use Glue Libraries
campaigns_udl_df = glueContext.create_dynamic_frame.from_catalog(database="{}".format(db_name),
                                                         table_name="{}".format(tbl_name),
                                                         transformation_ctx="{}".format(tbl_name)).toDF()

campaigns_udl_lrf = campaigns_udl_df.select(
    col('account_code').cast(StringType()).alias('account_id'),
    col('description').cast(StringType()).alias('campaign_desc'),
    col('key').cast(StringType()).alias('campaign_key'),
    col('name').cast(StringType()).alias('campaign_nm'),
    col('modified')
)

campaigns_udl_lrf_formatted = campaigns_udl_lrf \
  .withColumn("source_ts", from_unixtime(campaigns_udl_lrf.modified, 'yyyy-MM-dd HH:mm:ss')
              .cast(TimestampType())) \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType())) \
  .withColumn("load_action_ind", lit('i').cast(StringType()))

campaigns_udl_lrf_selected_fields = campaigns_udl_lrf_formatted \
  .select('account_id', 'campaign_desc', 'campaign_key', 'campaign_nm', 'source_ts', 'insert_ts', 'insert_job_run_id',
          'insert_batch_run_id', 'load_action_ind')

campaigns_udl_lrf_selected_fields.write.parquet(output_dir,
                                                mode='overwrite',
                                                compression='snappy')

job.commit()
