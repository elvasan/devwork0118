import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DecimalType, LongType, ShortType,\
  BooleanType
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
tbl_name = 'lead_dom'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
lead_dom = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                         table_name=tbl_name,
                                                         transformation_ctx='lead_dom')

df = lead_dom.toDF().repartition(200)

keys = [
  'client_time',
  'created',
  'dst',
  'execution_time',
  'flash_version',
  'http_Content-Length',
  'http_User-Agent',
  'http_X-Forwarded-For',
  'localStorage',
  'navigator\\appCodeName',
  'navigator\\language',
  'navigator\\platform',
  'navigator\\productSub',
  'navigator\\userAgent',
  'page_id',
  'screen\\height',
  'screen\\width',
  'sequence_number',
  'sessionStorage',
  'token',
  'tz',
  'WebSocket',

]

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

n_schema = StructType([StructField("n", StringType())])
s_schema = StructType([StructField("s", StringType())])

df = (df
      .withColumnRenamed('http_Content-Length', 'http_content_length')
      .withColumnRenamed('http_User-Agent', 'http_user_agent')
      .withColumnRenamed('http_X-Forwarded-For', 'http_x_forwarded_for')
      .withColumnRenamed('navigator\\appCodeName', 'navigator_appCodeName')
      .withColumnRenamed('navigator\\language', 'navigator_language')
      .withColumnRenamed('navigator\\platform', 'navigator_platform')
      .withColumnRenamed('navigator\\productSub', 'navigator_productSub')
      .withColumnRenamed('navigator\\userAgent', 'navigator_userAgent')
      .withColumnRenamed('screen\\height', 'screen_height')
      .withColumnRenamed('screen\\width', 'screen_width')
      )

df = df.select(
    from_json(df.client_time, s_schema).getItem('s').alias('client_time').cast(LongType()),
    from_json(df.created, n_schema).getItem('n').alias('created').cast(DecimalType(14, 4)),
    from_json(df.dst, s_schema).getItem('s').alias('dst').cast(BooleanType()),
    from_json(df.execution_time, s_schema).getItem('s').alias('execution_time').cast(IntegerType()),
    from_json(df.flash_version, s_schema).getItem('s').alias('flash_version').cast(StringType()),
    from_json(df.http_content_length, n_schema).getItem('n').alias('http_Content_Length').cast(IntegerType()),
    from_json(df.http_user_agent, s_schema).getItem('s').alias('http_User_Agent').cast(StringType()),
    from_json(df.http_x_forwarded_for, s_schema).getItem('s').alias('http_X_Forwarded_For').cast(StringType()),
    from_json(df.localStorage, s_schema).getItem('s').alias('localStorage').cast(BooleanType()),
    from_json(df.navigator_appCodeName, s_schema).getItem('s').alias('navigator\\appCodeName').cast(StringType()),
    from_json(df.navigator_language, s_schema).getItem('s').alias('navigator\\language').cast(StringType()),
    from_json(df.navigator_platform, s_schema).getItem('s').alias('navigator\\platform').cast(StringType()),
    from_json(df.navigator_productSub, s_schema).getItem('s').alias('navigator\\productSub').cast(StringType()),
    from_json(df.navigator_userAgent, s_schema).getItem('s').alias('navigator\\userAgent').cast(StringType()),
    from_json(df.page_id, s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(df.screen_height, s_schema).getItem('s').alias('screen\\height').cast(IntegerType()),
    from_json(df.screen_width, s_schema).getItem('s').alias('screen\\width').cast(IntegerType()),
    from_json(df.sequence_number, n_schema).getItem('n').alias('sequence_number').cast(IntegerType()),
    from_json(df.sessionStorage, s_schema).getItem('s').alias('sessionStorage').cast(BooleanType()),
    from_json(df.token, s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(df.tz, s_schema).getItem('s').alias('tz').cast(ShortType()),
    from_json(df.WebSocket, s_schema).getItem('s').alias('WebSocket').cast(BooleanType()),
)

df = df.withColumn('create_day', to_date(from_unixtime(df.created, 'yyyy-MM-dd')))

df.write.parquet(output_dir,
                 mode='overwrite',
                 partitionBy=['create_day'])

job.commit()
