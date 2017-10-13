import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DecimalType, LongType, ShortType
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
tbl_name = 'urls'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
urls = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                     table_name=tbl_name,
                                                     transformation_ctx='urls')

df = urls.toDF()

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

n_schema = StructType([StructField("n", StringType())])
s_schema = StructType([StructField("s", StringType())])

df = (df
      .withColumnRenamed('http_Content-Length', 'http_content_length')
      .withColumnRenamed('http_User-Agent', 'http_user_agent')
      .withColumnRenamed('http_X-Forwarded-For', 'http_x_forwarded_for'))

df = df.select(
    from_json(df.capture_time, n_schema).getItem('n').alias('capture_time').cast(LongType()),
    from_json(df.client_time, n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(df.content_hash, s_schema).getItem('s').alias('content_hash').cast(StringType()),
    from_json(df.content_url, s_schema).getItem('s').alias('content_url').cast(StringType()),
    from_json(df.element_ids, s_schema).getItem('s').alias('element_ids').cast(ShortType()),
    from_json(df.http_content_length, n_schema).getItem('n').alias('http_Content-Length').cast(IntegerType()),
    from_json(df.http_user_agent, s_schema).getItem('s').alias('http_User-Agent').cast(StringType()),
    from_json(df.http_x_forwarded_for, s_schema).getItem('s').alias('http_X-Forwarded-For').cast(StringType()),
    from_json(df.page_id, s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(df.sequence_number, n_schema).getItem('n').alias('sequence_number').cast(ShortType()),
    from_json(df.server_time, s_schema).getItem('n').alias('server_time').cast(DecimalType(14, 4)),
    from_json(df.token, s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(df.type, s_schema).getItem('s').alias('type').cast(StringType()),
    from_json(df.url, s_schema).getItem('s').alias('url').cast(StringType()),
)

df = df.withColumn('create_day', to_date(from_unixtime(df.created, 'yyyy-MM-dd')))

df.write.parquet(output_dir,
                 mode='overwrite',
                 partitionBy=['create_day'])

job.commit()
