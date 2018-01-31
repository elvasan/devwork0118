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
tbl_name = 'ip_geolocation'
source_tbl = "ip_geolocation"
source_db_name = "lrf"

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-{}/{}".format(db_name, tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# pii_hashing udl
ip_geolocation_tbl_lrf_df = glueContext.create_dynamic_frame.from_catalog(database="{}".format(source_db_name),
                                                                          table_name="{}".format(source_tbl),
                                                                          transformation_ctx="{}".format(
                                                                              source_tbl)).toDF()

# TODO: HERE will be select based on date_time range
ip_geolocation_tbl_fdl_df = ip_geolocation_tbl_lrf_df.select(
    "ip_geolocation_key",
    "country_cd",
    "region_cd",
    "city_nm",
    "postal_cd",
    "isp_nm",
    "insert_ts",
    "insert_batch_run_id",
    "insert_job_run_id",
    "source_ts",
)

# write hash_mapping to fdl
ip_geolocation_tbl_fdl_df.write.parquet(output_dir,
                                        mode='overwrite',
                                        compression='snappy')

job.commit()
