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
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
snapshots_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl",
                                                              table_name=TBL_NAME,
                                                              transformation_ctx="snapshots_rdl").toDF()

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
snapshots = snapshots_rdl.select(*exprs)

# TODO: generate the types from the DDL
snapshots_extract = snapshots.select(
    from_json(snapshots['capture_time'], n_schema).getItem('n').alias('capture_time').cast(LongType()),
    from_json(snapshots['client_time'], n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(snapshots['content_hash'], s_schema).getItem('s').alias('content_hash').cast(StringType()),
    from_json(snapshots['content_url'], s_schema).getItem('s').alias('content_url').cast(StringType()),
    from_json(snapshots['element_ids'], nS_schema).getItem('nS').alias('element_ids').cast(ShortType()),
    from_json(snapshots['http_Content-Length'], n_schema).getItem('n').alias('http_content_length').cast(IntegerType()),
    from_json(snapshots['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(snapshots['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(
        StringType()),
    from_json(snapshots['page_id'], s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(snapshots['sequence_number'], n_schema).getItem('n').alias('sequence_number').cast(ShortType()),
    from_json(snapshots['server_time'], n_schema).getItem('n').alias('server_time').cast(DoubleType()),
    from_json(snapshots['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(snapshots['type'], s_schema).getItem('s').alias('type').cast(StringType()),
    from_json(snapshots['url'], s_schema).getItem('s').alias('url').cast(StringType()),
)

snapshots_df = snapshots_extract \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

snapshots_partitioned = snapshots_df.withColumn('create_day',
                                                to_date(from_unixtime(snapshots_df.server_time, 'yyyy-MM-dd')))

snapshots_repartitioned = snapshots_partitioned.repartition(1)
# TODO: pass the write mode in as an arg
snapshots_repartitioned.write.parquet(output_dir,
                                      mode='overwrite',
                                      partitionBy=['create_day'],
                                      compression='snappy')

job.commit()
