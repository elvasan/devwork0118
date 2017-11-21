import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType
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
TBL_NAME = 'deviceid'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
deviceid_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl",
                                                             table_name=TBL_NAME,
                                                             transformation_ctx="deviceid_rdl").toDF()

keys = [
    'created',
    'http_User-Agent',
    'http_X-Forwarded-For',
    'methods',
    'token',
    'uuid'
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
deviceid = deviceid_rdl.select(*exprs)

# TODO: generate the types from the DDL
deviceid_extract = deviceid.select(
    from_json(deviceid['created'], n_schema).getItem('n').alias('created').cast(DoubleType()),
    from_json(deviceid['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(deviceid['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(StringType()),
    from_json(deviceid['methods'], n_schema).getItem('n').alias('methods').cast(IntegerType()),
    from_json(deviceid['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(deviceid['uuid'], s_schema).getItem('s').alias('uuid').cast(StringType()),
)

deviceid_df = deviceid_extract \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

# add partition column
deviceid_partitioned = deviceid_df.withColumn('create_day', to_date(from_unixtime(deviceid_df.created, 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
deviceid_partitioned.write.parquet(output_dir,
                                   mode='overwrite',
                                   partitionBy=['create_day'],
                                   compression='snappy')

job.commit()
