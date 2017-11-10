import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, from_json, from_unixtime, to_date

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

from glutils.job_objects import n_schema, s_schema, bOOL_schema, m_schema

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
DB_NAME = 'rdl'
TBL_NAME = 'formdata'

# output directories
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
formdata = glueContext.create_dynamic_frame.from_catalog(database=DB_NAME,
                                                         table_name=TBL_NAME,
                                                         transformation_ctx='formdata')

df = formdata.toDF()

keys = [
    'created',
    'checked',
    'client_time',
    'email',
    'execution_time',
    'fieldvisibility',
    'http_Content-Length',
    'http_User-Agent',
    'http_X-Forwarded-For',
    'id',
    'label',
    'labelvisibility',
    'name',
    'options',
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

# TODO: generate the types from the DDL
df = df.select(
    from_json(df['created'], n_schema).getItem('n').alias('created').cast(DoubleType()),
    from_json(df['checked'], bOOL_schema).getItem('bOOL').alias('checked').cast(IntegerType()),
    from_json(df['client_time'], n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(df['email'], n_schema).getItem('n').alias('email').cast(IntegerType()),
    from_json(df['execution_time'], n_schema).getItem('n').alias('execution_time').cast(IntegerType()),
    from_json(df['fieldvisibility'], m_schema).getItem('m').alias('field_visibility').cast(StringType()),
    from_json(df['http_Content-Length'], n_schema).getItem('n').alias('http_content_length').cast(IntegerType()),
    from_json(df['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(df['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(StringType()),
    from_json(df['id'], s_schema).getItem('s').alias('id').cast(StringType()),
    from_json(df['label'], s_schema).getItem('s').alias('label').cast(StringType()),
    from_json(df['labelvisibility'], m_schema).getItem('m').alias('label_visibility').cast(StringType()),
    from_json(df['name'], s_schema).getItem('s').alias('name').cast(StringType()),
    from_json(df['options'], s_schema).getItem('s').alias('option_count').cast(IntegerType()),
    from_json(df['optionLabel'], s_schema).getItem('s').alias('option_label').cast(StringType()),
    from_json(df['page_id'], s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(df['phone'], s_schema).getItem('s').alias('phone').cast(StringType()),
    from_json(df['sequence_number'], n_schema).getItem('n').alias('sequence_number').cast(IntegerType()),
    from_json(df['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(df['type'], n_schema).getItem('n').alias('type').cast(IntegerType()),
    from_json(df['value'], s_schema).getItem('s').alias('value').cast(StringType())
)

# add the partition column
df = df.withColumn('create_day', to_date(from_unixtime(df['created'], 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
df.write.parquet(output_dir,
                 mode='overwrite',
                 partitionBy=['create_day'])

job.commit()
