import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType
from pyspark.sql.functions import col, from_json, from_unixtime, to_date, udf, coalesce

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from glutils.job_objects import n_schema, s_schema, bOOL_schema, m_schema, b_schema
from glutils.job_utils import zipped_b64_to_string

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
# db_name = 'rdl'
tbl_name = 'formdata'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
# formdata = glueContext.create_dynamic_frame.from_catalog(database=db_name,
#                                                          table_name=tbl_name,
#                                                          transformation_ctx='formdata')

df = spark.read.parquet('s3://jornaya-dev-us-east-1-rdl/{}'.format(tbl_name))
df = df.repartition(1000)
# df = formdata.toDF().repartition(200)

keys = [
    'checked',
    'client_time',
    'created',
    'email',
    'execution_time',
    'fieldvisibility',
    'http_Content-Length',
    'http_User-Agent',
    'http_X-Forwarded-For',
    'id',
    'init',
    'label',
    'labelvisibility',
    'name',
    'optionCount',
    'optionLabel',
    'page_id',
    'phone',
    'sequence_number',
    'token',
    'type',
    'value'
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

df = (df
      .withColumnRenamed('http_Content-Length', 'http_Content_Length')
      .withColumnRenamed('http_User-Agent', 'http_User_Agent')
      .withColumnRenamed('http_X-Forwarded-For', 'http_X_Forwarded_For'))

df = df.select(
    from_json(df.checked, bOOL_schema).getItem('bOOL').alias('checked').cast(IntegerType()),
    from_json(df.client_time, n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(df.created, n_schema).getItem('n').alias('created').cast(DecimalType(14, 4)),
    from_json(df.email, n_schema).getItem('n').alias('email').cast(IntegerType()),
    from_json(df.execution_time, n_schema).getItem('n').alias('execution_time').cast(IntegerType()),
    from_json(df.fieldvisibility, m_schema).getItem('m').alias('fieldvisibility').cast(StringType()),
    from_json(df.http_Content_Length, n_schema).getItem('n').alias('http_Content_Length').cast(IntegerType()),
    from_json(df.http_User_Agent, s_schema).getItem('s').alias('http_User_Agent').cast(StringType()),
    from_json(df.http_X_Forwarded_For, s_schema).getItem('s').alias('http_X_Forwarded_For').cast(StringType()),
    from_json(df.id, s_schema).getItem('s').alias('id').cast(StringType()),
    # need further processing on these next two lines
    # base64 decode then unzip then coalesce the two tmp rows into a single row
    from_json(df.init, s_schema).getItem('s').alias('tmp_string_init').cast(StringType()),
    from_json(df.init, b_schema).getItem('b').alias('tmp_binary_init').cast(StringType()),
    # end further processing
    from_json(df.label, s_schema).getItem('s').alias('label').cast(StringType()),
    from_json(df.labelvisibility, m_schema).getItem('m').alias('labelvisibility').cast(StringType()),
    from_json(df.name, s_schema).getItem('s').alias('name').cast(StringType()),
    from_json(df.optionCount, n_schema).getItem('n').alias('optionCount').cast(IntegerType()),
    from_json(df.optionLabel, s_schema).getItem('s').alias('optionLabel').cast(StringType()),
    from_json(df.page_id, s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(df.phone, s_schema).getItem('s').alias('phone').cast(StringType()),
    from_json(df.sequence_number, n_schema).getItem('n').alias('sequence_number').cast(IntegerType()),
    from_json(df.token, s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(df.type, n_schema).getItem('n').alias('type').cast(IntegerType()),
    from_json(df.value, s_schema).getItem('s').alias('value').cast(StringType())
)

# decode any binary init values
b64_udf = udf(zipped_b64_to_string, StringType())
df = df.withColumn('tmp_decoded_init', b64_udf('tmp_binary_init'))

# coalesce the string and decoded_binary init fields
df = df.withColumn('init', coalesce(df.tmp_string_init, df.tmp_decoded_init).cast(StringType()))

# drop the tmp fields
df = df.select([c for c in df.columns if c in keys])

# add the partition column
df = df.withColumn('create_day', to_date(from_unixtime(df.created, 'yyyy-MM-dd')))

# df.write.parquet(output_dir,
#                  mode='overwrite',
#                  partitionBy=['create_day, create_hour'])

# TODO: Chris Snyder's Method, consider these?
df.repartition('create_day').write.mode('overwrite').partitionBy('create_day').parquet(output_dir)
# df.write.partitionBy('create_day').parquet(output_dir, mode='overwrite')

job.commit()
