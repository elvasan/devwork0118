import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, from_json, current_timestamp, lit

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

from glutils.job_objects import s_schema

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
TBL_NAME = 'pii_hashing'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
pii_hashing_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl",
                                                                table_name=TBL_NAME,
                                                                transformation_ctx="pii_hashing_rdl").toDF()

keys = ['canonical_hash',
        'hash',
        'hash_type',
        ]

exprs = [col("item").getItem(k).alias(k) for k in keys]
pii_hashing = pii_hashing_rdl.select(*exprs)

pii_hashing_extract = pii_hashing.select(
    from_json(pii_hashing['canonical_hash'], s_schema).getItem('s').alias('canonical_hash').cast(StringType()),
    from_json(pii_hashing['hash'], s_schema).getItem('s').alias('hash').cast(StringType()),
    from_json(pii_hashing['hash_type'], s_schema).getItem('s').alias('hash_type').cast(StringType()),
)

# add the job run columns
pii_hashing_df = pii_hashing_extract \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

# TODO: pass the write mode in as an arg
pii_hashing_df.write.parquet(output_dir,
                             mode='overwrite',
                             compression='snappy')
job.commit()
