import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, ShortType
from pyspark.sql.functions import col, from_json, from_unixtime, to_date, current_timestamp, lit

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from glutils.job_objects import n_schema, s_schema

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
TBL_NAME = 'urls'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
source_dir = "s3://jornaya-dev-us-east-1-rdl/{}".format(TBL_NAME)
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
df = spark.read.parquet(source_dir)

keys = [
  'campaign_key',
  'client_time',
  'created',
  'hash',
  'http_Content-Length',
  'http_User-Agent',
  'http_X-Forwarded-For',
  'iframe',
  'page_id',
  'ref_hash',
  'ref_url',
  'sequence_number',
  'token',
  'url',
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

# TODO: generate the types from the DDL
df = df.select(
    from_json(df['campaign_key'], s_schema).getItem('s').alias('campaign_key').cast(StringType()),
    from_json(df['client_time'], n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(df['created'], n_schema).getItem('n').alias('created').cast(DoubleType()),
    from_json(df['hash'], s_schema).getItem('s').alias('hash').cast(StringType()),
    from_json(df['http_Content-Length'], n_schema).getItem('n').alias('http_content_length').cast(IntegerType()),
    from_json(df['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(df['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(StringType()),
    from_json(df['iframe'], n_schema).getItem('n').alias('iframe').cast(ShortType()),
    from_json(df['page_id'], s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(df['ref_hash'], s_schema).getItem('s').alias('ref_hash').cast(StringType()),
    from_json(df['ref_url'], s_schema).getItem('s').alias('ref_url').cast(StringType()),
    from_json(df['sequence_number'], n_schema).getItem('n').alias('sequence_number').cast(ShortType()),
    from_json(df['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(df['url'], s_schema).getItem('s').alias('url').cast(StringType()),
)

# add the job run columns
df = df \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

df = df.withColumn('create_day', to_date(from_unixtime(df.created, 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
df.write.parquet(output_dir,
                 mode='overwrite',
                 partitionBy=['create_day'])

job.commit()
