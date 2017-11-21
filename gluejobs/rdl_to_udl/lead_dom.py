import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType, ShortType, BooleanType
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
TBL_NAME = 'lead_dom'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
lead_dom_rdl = glueContext.create_dynamic_frame.from_catalog(database='rdl',
                                                             table_name=TBL_NAME,
                                                             transformation_ctx='lead_dom').toDF()

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
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
lead_dom = lead_dom_rdl.select(*exprs)

# TODO: generate the types from the DDL
lead_dom_extract = lead_dom.select(
    from_json(lead_dom['client_time'], s_schema).getItem('s').alias('client_time').cast(DoubleType()),
    from_json(lead_dom['created'], n_schema).getItem('n').alias('created').cast(DoubleType()),
    from_json(lead_dom['dst'], s_schema).getItem('s').alias('dst').cast(BooleanType()),
    from_json(lead_dom['execution_time'], s_schema).getItem('s').alias('execution_time').cast(IntegerType()),
    from_json(lead_dom['flash_version'], s_schema).getItem('s').alias('flash_version').cast(StringType()),
    from_json(lead_dom['http_Content-Length'], n_schema).getItem('n').alias('http_content_length').cast(IntegerType()),
    from_json(lead_dom['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(lead_dom['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(StringType()),
    from_json(lead_dom['localStorage'], s_schema).getItem('s').alias('local_Storage').cast(BooleanType()),
    from_json(lead_dom['navigator\\appCodeName'], s_schema).getItem('s').alias('navigator_app_code_name').cast(
        StringType()),
    from_json(lead_dom['navigator\\language'], s_schema).getItem('s').alias('navigator_language').cast(StringType()),
    from_json(lead_dom['navigator\\platform'], s_schema).getItem('s').alias('navigator_platform').cast(StringType()),
    from_json(lead_dom['navigator\\productSub'], s_schema).getItem('s').alias('navigator_product_sub').cast(
        StringType()),
    from_json(lead_dom['navigator\\userAgent'], s_schema).getItem('s').alias('navigator_user_agent').cast(StringType()),
    from_json(lead_dom['page_id'], s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(lead_dom['screen\\height'], s_schema).getItem('s').alias('screen_height').cast(IntegerType()),
    from_json(lead_dom['screen\\width'], s_schema).getItem('s').alias('screen_width').cast(IntegerType()),
    from_json(lead_dom['sequence_number'], n_schema).getItem('n').alias('sequence_number').cast(IntegerType()),
    from_json(lead_dom['sessionStorage'], s_schema).getItem('s').alias('session_storage').cast(BooleanType()),
    from_json(lead_dom['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(lead_dom['tz'], s_schema).getItem('s').alias('tz').cast(ShortType()),
)

lead_dom_df = lead_dom_extract \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

lead_dom_partitioned = lead_dom_df.withColumn('create_day', to_date(from_unixtime(lead_dom_df.created, 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
lead_dom_partitioned.write.parquet(output_dir,
                                   mode='overwrite',
                                   partitionBy=['create_day'],
                                   compression='snappy')

job.commit()
