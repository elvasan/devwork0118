import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, from_json, from_unixtime, to_date, current_timestamp, lit

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

from glutils.job_objects import n_schema, s_schema

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
TBL_NAME = 'leads'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
source_dir = "s3://jornaya-dev-us-east-1-rdl/{}".format(TBL_NAME)
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
df = spark.read.parquet(source_dir)

keys = [
  'ip',
  'account_code',
  'Browser',
  'Browser_Maker',
  'browser_name',
  'browser_name_pattern',
  'browser_name_regex',
  'campaign_key',
  'client_time',
  'Comment',
  'created',
  'Device_Pointing_Method',
  'Device_Type',
  'geoip_city',
  'geoip_continent_code',
  'geoip_country_code',
  'geoip_isp',
  'geoip_postal_code',
  'geoip_region',
  'http_Content-Length',
  'http_User-Agent',
  'http_X-Forwarded-For',
  'IsMobileDevice',
  'MajorVer',
  'MinorVer',
  'page_id',
  'Parent',
  'Platform',
  'sequence_number',
  'token',
  'Version',
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

# TODO: generate the types from the DDL
df = df.select(
    from_json(df['ip'], n_schema).getItem('n').alias('ip').cast(LongType()),
    from_json(df['account_code'], s_schema).getItem('s').alias('account_code').cast(StringType()),
    from_json(df['Browser'], s_schema).getItem('s').alias('browser').cast(StringType()),
    from_json(df['Browser_Maker'], s_schema).getItem('s').alias('browser_maker').cast(StringType()),
    from_json(df['browser_name'], s_schema).getItem('s').alias('browser_name').cast(StringType()),
    from_json(df['browser_name_pattern'], s_schema).getItem('s').alias('browser_name_pattern').cast(StringType()),
    from_json(df['browser_name_regex'], s_schema).getItem('s').alias('browser_name_regex').cast(StringType()),
    from_json(df['campaign_key'], s_schema).getItem('s').alias('campaign_key').cast(StringType()),
    from_json(df['client_time'], n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(df['Comment'], s_schema).getItem('s').alias('comment').cast(StringType()),
    from_json(df['created'], n_schema).getItem('n').alias('created').cast(DoubleType()),
    from_json(df['Device_Pointing_Method'], s_schema).getItem('s').alias('device_pointing_method').cast(StringType()),
    from_json(df['Device_Type'], s_schema).getItem('s').alias('device_type').cast(StringType()),
    from_json(df['geoip_city'], s_schema).getItem('s').alias('geoip_city').cast(StringType()),
    from_json(df['geoip_continent_code'], s_schema).getItem('s').alias('geoip_continent_code').cast(StringType()),
    from_json(df['geoip_country_code'], s_schema).getItem('s').alias('geoip_country_code').cast(StringType()),
    from_json(df['geoip_isp'], s_schema).getItem('s').alias('geoip_isp').cast(StringType()),
    from_json(df['geoip_postal_code'], s_schema).getItem('s').alias('geoip_postal_code').cast(StringType()),
    from_json(df['geoip_region'], s_schema).getItem('s').alias('geoip_region').cast(StringType()),
    from_json(df['http_Content-Length'], n_schema).getItem('n').alias('http_content_length').cast(IntegerType()),
    from_json(df['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(df['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(StringType()),
    from_json(df['IsMobileDevice'], s_schema).getItem('s').alias('is_mobile_device').cast(StringType()),
    from_json(df['MajorVer'], s_schema).getItem('s').alias('major_version').cast(StringType()),
    from_json(df['MinorVer'], s_schema).getItem('s').alias('minor_version').cast(StringType()),
    from_json(df['page_id'], s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(df['Parent'], s_schema).getItem('s').alias('parent').cast(StringType()),
    from_json(df['Platform'], s_schema).getItem('s').alias('platform').cast(StringType()),
    from_json(df['sequence_number'], n_schema).getItem('n').alias('sequence_number').cast(IntegerType()),
    from_json(df['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(df['Version'], s_schema).getItem('s').alias('version').cast(StringType()),)

# add the job run columns
df = df \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

# add the partition column
df = df.withColumn('create_day', to_date(from_unixtime(df.created, 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
df.write.parquet(output_dir,
                 mode='overwrite',
                 partitionBy=['create_day'])

job.commit()
