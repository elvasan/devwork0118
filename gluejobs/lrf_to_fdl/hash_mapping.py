import sys

from pyspark.context import SparkContext

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
db_name = 'fdl'
tbl_name = 'hash_mapping'
source_tbl = "hash_mapping"
source_db_name = "lrf"

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
source_dir = "s3://jornaya-dev-us-east-1-{}/{}".format(source_db_name, source_tbl)
output_dir = "s3://jornaya-dev-us-east-1-{}/{}".format(db_name, tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# pii_hashing udl
hash_mapping_tbl_lrf_df = spark.read.parquet(source_dir)

# TODO: HERE will be select based on date_time range
hash_mapping_tbl_fdl_df = hash_mapping_tbl_lrf_df.select(
    "canonical_hash_value",
    "hash_type_cd",
    "hash_value",
    "source_ts",
    "insert_ts",
    "insert_job_run_id",
    "insert_batch_run_id"
)

# write hash_mapping to fdl
hash_mapping_tbl_fdl_df.write.parquet(output_dir, mode='overwrite')

job.commit()
