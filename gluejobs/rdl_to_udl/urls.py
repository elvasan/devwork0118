import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, ShortType
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
TBL_NAME = 'urls'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
urls_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl",
                                                         table_name=TBL_NAME,
                                                         transformation_ctx="urls_rdl").toDF()

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
urls = urls_rdl.select(*exprs)

# TODO: generate the types from the DDL
urls_extract = urls.select(
    from_json(urls['campaign_key'], s_schema).getItem('s').alias('campaign_key').cast(StringType()),
    from_json(urls['client_time'], n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(urls['created'], n_schema).getItem('n').alias('created').cast(DoubleType()),
    from_json(urls['hash'], s_schema).getItem('s').alias('hash').cast(StringType()),
    from_json(urls['http_Content-Length'], n_schema).getItem('n').alias('http_content_length').cast(IntegerType()),
    from_json(urls['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(urls['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(StringType()),
    from_json(urls['iframe'], n_schema).getItem('n').alias('iframe').cast(ShortType()),
    from_json(urls['page_id'], s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(urls['ref_hash'], s_schema).getItem('s').alias('ref_hash').cast(StringType()),
    from_json(urls['ref_url'], s_schema).getItem('s').alias('ref_url').cast(StringType()),
    from_json(urls['sequence_number'], n_schema).getItem('n').alias('sequence_number').cast(ShortType()),
    from_json(urls['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(urls['url'], s_schema).getItem('s').alias('url').cast(StringType()),
)

# add the job run columns
urls_df = urls_extract \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

urls_partitioned = urls_df.withColumn('create_day', to_date(from_unixtime(urls_df.created, 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
urls_partitioned.write.parquet(output_dir,
                               mode='overwrite',
                               partitionBy=['create_day'],
                               compression='snappy')

job.commit()
