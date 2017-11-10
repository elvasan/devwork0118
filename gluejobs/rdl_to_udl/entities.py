# Entities Transformation from RDL to UDL

import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql import functions as fun

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

from glutils.job_utils import get_dynamodb_value

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
TBL_NAME = 'entities'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
source_dir = "s3://jornaya-dev-us-east-1-rdl/{}".format(TBL_NAME)
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])


# UDF To get the DynamoDB json value
get_dynamodb_value_udf = udf(get_dynamodb_value, StringType())

# Create data frame from the source tables
# This needs to change so we directly read it from Glue's Catalog and not use Glue Libraries
entities_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl", table_name="entities",
                                                              transformation_ctx="entities").toDF()
df = spark.read.parquet(source_dir)

keys = ['active',
        'code',
        'created',
        'industry',
        'modified',
        'name']

exprs = [col("item").getItem(k).alias(k) for k in keys]
entities = entities_rdl.select(*exprs)

# TODO: generate the types from the DDL
entities_extract = entities.select(
    get_dynamodb_value_udf(entities['active']).alias('active').cast(IntegerType()),
    get_dynamodb_value_udf(entities['code']).alias('code').cast(StringType()),
    get_dynamodb_value_udf(entities['created']).alias('created').cast(DoubleType()),
    get_dynamodb_value_udf(entities['industry']).alias('industry').cast(IntegerType()),
    get_dynamodb_value_udf(entities['modified']).alias('modified').cast(DoubleType()),
    get_dynamodb_value_udf(entities['name']).alias('name').cast(StringType()),
    fun.from_unixtime(get_dynamodb_value_udf(entities['modified'])).alias('source_ts').cast(TimestampType())
)


# add the job run columns
entities_df = entities_extract \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

# TODO: pass the write mode in as an arg
entities_df.write.parquet(output_dir,
                 mode='overwrite')

job.commit()
