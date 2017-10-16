import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DecimalType, LongType
from pyspark.sql.functions import col, from_json, from_unixtime, to_date

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
db_name = 'rdl'
tbl_name = 'leads'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
leads = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                      table_name=tbl_name,
                                                      transformation_ctx='leads')

df = leads.toDF()

keys = [
  'ip',
  'account_code',
  'browser',
  'browser_maker',
  'browser_name',
  'browser_name_pattern',
  'browser_name_regex',
  'campaign_key',
  'client_time',
  'comment',
  'created',
  'device_pointing_method',
  'device_type',
  'geoip_city',
  'geoip_continent_code',
  'geoip_country_code',
  'geoip_isp',
  'geoip_postal_code',
  'geoip_region',
  'http_Content-Length',
  'http_User-Agent',
  'http_X-Forwarded-For',
  'is_mobile_device',
  'major_version',
  'minor_version',
  'page_id',
  'parent',
  'platform',
  'sequence_number',
  'token',
  'version',
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

n_schema = StructType([StructField("n", StringType())])
s_schema = StructType([StructField("s", StringType())])

df = (df
      .withColumnRenamed('http_Content-Length', 'http_content_length')
      .withColumnRenamed('http_User-Agent', 'http_user_agent')
      .withColumnRenamed('http_X-Forwarded-For', 'http_x_forwarded_for'))

df = df.select(
    from_json(df.ip, n_schema).getItem('n').alias('ip').cast(LongType()),
    from_json(df.account_code, s_schema).getItem('s').alias('account_code').cast(StringType()),
    from_json(df.browser, s_schema).getItem('s').alias('browser').cast(StringType()),
    from_json(df.browser_maker, s_schema).getItem('s').alias('browser_maker').cast(StringType()),
    from_json(df.browser_name, s_schema).getItem('s').alias('browser_name').cast(StringType()),
    from_json(df.browser_name_pattern, s_schema).getItem('s').alias('browser_name_pattern').cast(StringType()),
    from_json(df.browser_name_regex, s_schema).getItem('s').alias('browser_name_regex').cast(StringType()),
    from_json(df.campaign_key, s_schema).getItem('s').alias('campaign_key').cast(StringType()),
    from_json(df.client_time, n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(df.comment, s_schema).getItem('s').alias('comment').cast(StringType()),
    from_json(df.created, n_schema).getItem('n').alias('created').cast(DecimalType(14, 4)),
    from_json(df.device_pointing_method, s_schema).getItem('s').alias('device_pointing_method').cast(StringType()),
    from_json(df.device_type, s_schema).getItem('s').alias('device_type').cast(StringType()),
    from_json(df.geoip_city, s_schema).getItem('s').alias('geoip_city').cast(StringType()),
    from_json(df.geoip_continent_code, s_schema).getItem('s').alias('geoip_continent_code').cast(StringType()),
    from_json(df.geoip_country_code, s_schema).getItem('s').alias('geoip_country_code').cast(StringType()),
    from_json(df.geoip_isp, s_schema).getItem('s').alias('geoip_isp').cast(StringType()),
    from_json(df.geoip_postal_code, s_schema).getItem('s').alias('geoip_postal_code').cast(StringType()),
    from_json(df.geoip_region, s_schema).getItem('s').alias('geoip_region').cast(StringType()),
    from_json(df.http_content_length, n_schema).getItem('n').alias('http_Content-Length').cast(IntegerType()),
    from_json(df.http_user_agent, s_schema).getItem('s').alias('http_User-Agent').cast(StringType()),
    from_json(df.http_x_forwarded_for, s_schema).getItem('s').alias('http_X-Forwarded-For').cast(StringType()),
    from_json(df.is_mobile_device, s_schema).getItem('s').alias('is_mobile_device').cast(StringType()),
    from_json(df.major_version, s_schema).getItem('s').alias('major_version').cast(StringType()),
    from_json(df.minor_version, s_schema).getItem('s').alias('minor_version').cast(StringType()),
    from_json(df.page_id, s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(df.parent, s_schema).getItem('s').alias('parent').cast(StringType()),
    from_json(df.platform, s_schema).getItem('s').alias('platform').cast(StringType()),
    from_json(df.sequence_number, n_schema).getItem('n').alias('sequence_number').cast(IntegerType()),
    from_json(df.token, s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(df.version, s_schema).getItem('s').alias('version').cast(StringType()),
)

df = df.withColumn('create_day', to_date(from_unixtime(df.created, 'yyyy-MM-dd')))

df.write.parquet(output_dir,
                 mode='overwrite',
                 partitionBy=['create_day'])

job.commit()
