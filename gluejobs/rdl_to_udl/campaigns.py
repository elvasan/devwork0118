import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json, current_timestamp, lit

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

from glutils.job_objects import n_schema, s_schema, sS_schema

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
TBL_NAME = 'campaigns'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
campaigns_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl",
                                                              table_name=TBL_NAME,
                                                              transformation_ctx="campaigns_rdl").toDF()

keys = [
    'account_code',
    'audience_id_tags',
    'campaign_javascript_version',
    'created',
    'created_by',
    'description',
    'forensiq_default',
    'hash_urls',
    'key',
    'log_level',
    'log_limit',
    'log_targets',
    'modified',
    'modified_by',
    'name',
    'threatmetrix_default'
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
campaigns = campaigns_rdl.select(*exprs)

campaigns_extract = campaigns.select(
    from_json(campaigns['account_code'], s_schema).getItem('s').alias('account_code').cast(StringType()),
    from_json(campaigns['audience_id_tags'], sS_schema).getItem('sS').alias('audience_id_tags').cast(StringType()),
    from_json(campaigns['campaign_javascript_version'],
              s_schema).getItem('s').alias('campaign_javascript_version').cast(StringType()),
    from_json(campaigns['created'], n_schema).getItem('n').alias('created').cast(DoubleType()),
    from_json(campaigns['created_by'], n_schema).getItem('n').alias('created_by').cast(IntegerType()),
    from_json(campaigns['description'], s_schema).getItem('s').alias('description').cast(StringType()),
    from_json(campaigns['forensiq_default'], n_schema).getItem('n').alias('forensiq_default').cast(IntegerType()),
    from_json(campaigns['hash_urls'], n_schema).getItem('n').alias('hash_urls').cast(IntegerType()),
    from_json(campaigns['key'], s_schema).getItem('s').alias('key').cast(StringType()),
    from_json(campaigns['log_level'], n_schema).getItem('n').alias('log_level').cast(IntegerType()),
    from_json(campaigns['log_limit'], n_schema).getItem('n').alias('log_limit').cast(IntegerType()),
    from_json(campaigns['log_targets'], n_schema).getItem('n').alias('log_targets').cast(IntegerType()),
    from_json(campaigns['modified'], n_schema).getItem('n').alias('modified').cast(DoubleType()),
    from_json(campaigns['modified_by'], n_schema).getItem('n').alias('modified_by').cast(IntegerType()),
    from_json(campaigns['name'], s_schema).getItem('s').alias('name').cast(StringType()),
    from_json(campaigns['threatmetrix_default'],
              n_schema).getItem('n').alias('threatmetrix_default').cast(IntegerType()))

campaigns_df = campaigns_extract \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

# TODO: pass the write mode in as an arg
campaigns_df.write.parquet(output_dir,
                           mode='overwrite',
                           compression='snappy')
job.commit()
