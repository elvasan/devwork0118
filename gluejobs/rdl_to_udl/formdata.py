import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, from_json, from_unixtime, to_date, udf

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

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
TBL_NAME = 'formdata'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
formdata_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl",
                                                             table_name=TBL_NAME,
                                                             transformation_ctx="formdata_rdl").toDF()

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
formdata = formdata_rdl.select(*exprs)

# TODO: generate the types from the DDL
formdata_extract = formdata.select(
    from_json(formdata['checked'], bOOL_schema).getItem('bOOL').alias('checked').cast(IntegerType()),
    from_json(formdata['client_time'], n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(formdata['created'], n_schema).getItem('n').alias('created').cast(DoubleType()),
    from_json(formdata['email'], n_schema).getItem('n').alias('email').cast(IntegerType()),
    from_json(formdata['execution_time'], n_schema).getItem('n').alias('execution_time').cast(IntegerType()),
    from_json(formdata['fieldvisibility'], m_schema).getItem('m').alias('fieldvisibility').cast(StringType()),
    from_json(formdata['http_Content-Length'], n_schema).getItem('n').alias('http_content_length').cast(IntegerType()),
    from_json(formdata['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(formdata['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(StringType()),
    from_json(formdata['id'], s_schema).getItem('s').alias('id').cast(StringType()),
    # note: need further processing on these next two lines
    # base64 decode then unzip then coalesce the two tmp rows into a single row
    from_json(formdata['init'], s_schema).getItem('s').alias('tmp_string_init').cast(StringType()),
    from_json(formdata['init'], b_schema).getItem('b').alias('tmp_binary_init').cast(StringType()),
    # end note
    from_json(formdata['label'], s_schema).getItem('s').alias('label').cast(StringType()),
    from_json(formdata['labelvisibility'], m_schema).getItem('m').alias('labelvisibility').cast(StringType()),
    from_json(formdata['name'], s_schema).getItem('s').alias('name').cast(StringType()),
    from_json(formdata['optionCount'], n_schema).getItem('n').alias('optionCount').cast(IntegerType()),
    from_json(formdata['optionLabel'], s_schema).getItem('s').alias('optionLabel').cast(StringType()),
    from_json(formdata['page_id'], s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(formdata['phone'], s_schema).getItem('s').alias('phone').cast(StringType()),
    from_json(formdata['sequence_number'], n_schema).getItem('n').alias('sequence_number').cast(IntegerType()),
    from_json(formdata['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(formdata['type'], n_schema).getItem('n').alias('type').cast(IntegerType()),
    from_json(formdata['value'], s_schema).getItem('s').alias('value').cast(StringType())
)

# decode any binary init values
b64_udf = udf(zipped_b64_to_string, StringType())
formdata_decoded = formdata_extract.withColumn('tmp_decoded_init', b64_udf('tmp_binary_init'))

# coalesce the string and decoded_binary init fields
# df = df.withColumn('init', coalesce(df.tmp_string_init, df.tmp_decoded_init).cast(StringType()))

# drop the tmp fields
# df = df.select([c for c in df.columns if c in keys])

# add the partition column
formdata_df = formdata_decoded.withColumn('create_day', to_date(from_unixtime(formdata_decoded.created, 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
formdata_df.write.parquet(output_dir,
                          mode='overwrite',
                          partitionBy=['create_day'],
                          compression='snappy')

job.commit()
