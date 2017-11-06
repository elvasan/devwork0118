import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, ShortType
from pyspark.sql.functions import col, from_json, from_unixtime, to_date, current_timestamp, lit

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

from glutils.job_objects import n_schema, s_schema, nS_schema

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
TBL_NAME = 'snapshots'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
source_dir = "s3://jornaya-dev-us-east-1-rdl/{}".format(TBL_NAME)
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
df = spark.read.parquet(source_dir)

keys = [
  'capture_time',
  'client_time',
  'content_hash',
  'content_url',
  'element_ids',
  'http_Content-Length',
  'http_User-Agent',
  'http_X-Forwarded-For',
  'page_id',
  'sequence_number',
  'server_time',
  'token',
  'type',
  'url',
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

# TODO: generate the types from the DDL
df = df.select(
    from_json(df['capture_time'], n_schema).getItem('n').alias('capture_time').cast(LongType()),
    from_json(df['client_time'], n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(df['content_hash'], s_schema).getItem('s').alias('content_hash').cast(StringType()),
    from_json(df['content_url'], s_schema).getItem('s').alias('content_url').cast(StringType()),
    from_json(df['element_ids'], nS_schema).getItem('nS').alias('element_ids').cast(ShortType()),
    from_json(df['http_Content-Length'], n_schema).getItem('n').alias('http_content_length').cast(IntegerType()),
    from_json(df['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(df['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(StringType()),
    from_json(df['page_id'], s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(df['sequence_number'], n_schema).getItem('n').alias('sequence_number').cast(ShortType()),
    from_json(df['server_time'], n_schema).getItem('n').alias('server_time').cast(DoubleType()),
    from_json(df['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(df['type'], s_schema).getItem('s').alias('type').cast(StringType()),
    from_json(df['url'], s_schema).getItem('s').alias('url').cast(StringType()),
)

df = df \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

df = df.withColumn('create_day', to_date(from_unixtime(df.server_time, 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
df.write.parquet(output_dir,
                 mode='overwrite',
                 partitionBy=['create_day'])

job.commit()
