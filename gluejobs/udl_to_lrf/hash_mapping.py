import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp, upper, lit, udf

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from glutils.job_utils import code_format

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
db_name = 'udl'
tbl_name = 'hash_mapping'
domain_name = 'hash_type_cd'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-lrf/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# pii_hashing udl
pii_hashing_tbl = "pii_hashing"
pii_hashing_udl_df = spark.read.parquet("s3://jornaya-dev-us-east-1-udl/{}".format(pii_hashing_tbl))

# code_ref udl
code_ref_tbl = "code_ref"
code_ref_udl_df = spark.read.parquet('s3://jornaya-dev-us-east-1-udl/{}'.format(code_ref_tbl))
code_ref_udl_df = code_ref_udl_df.where(code_ref_udl_df.domain_nm == 'hash_type_cd')
code_format_udf = udf(code_format, StringType())
code_ref_udl_df = code_ref_udl_df.withColumn("code_nm_formatted", code_format_udf("code_nm"))

# Join  PII_HASHING and CODE_REF table based on hash_type
join_df = pii_hashing_udl_df.alias('pii').\
  join(code_ref_udl_df.alias('code_ref'), col('code_ref.code_nm_formatted') == upper(col('pii.hash_type'))).\
  select(col('pii.canonical_hash').alias('canonical_hash_value'),
         col('code_ref.value_cd').alias('hash_type_cd'),
         col('pii.hash').alias('hash_value'),
         col('pii.insert_ts').alias('source_ts'))  # store the UDL timestamp because the FDL record doesn't have one

join_with_jobs_df = join_df \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType())) \
  .withColumn("load_action_ind", lit('i').cast(StringType()))

join_with_jobs_df.write.parquet(output_dir, mode='overwrite')

job.commit()
