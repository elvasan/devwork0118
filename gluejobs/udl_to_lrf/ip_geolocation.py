import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp, upper, lit, udf, coalesce, concat_ws, md5

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
db_name = 'udl'
tbl_name = 'ip_geolocation'
domain_name = 'hash_type_cd'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-lrf/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create DataFrame of LEADS table
leads_tbl = "leads"
leads_udl_df = spark.read.parquet("s3://jornaya-dev-us-east-1-udl/{}".format(leads_tbl))
ip_geo_leads = leads_udl_df.select(
    "created",
    "token",
    "geoip_country_code",
    "geoip_region",
    "geoip_city",
    "geoip_postal_code",
    "geoip_isp")

# Create DataFrame of CODE_REF table
code_ref_tbl = "code_ref"
code_ref_udl_df = spark.read.parquet("s3://jornaya-dev-us-east-1-udl/{}".format(code_ref_tbl))

# Create DataFrame of CODE_REF - country table
countries_codes = code_ref_udl_df.select(
    col('value_cd').alias('c_value_cd'),
    col('code_desc').alias('c_code_desc'),
    col('code_ref_key').alias('c_code_ref_key')
).where(col('domain_nm') == "country_cd")

# Create DataFrame of CODE_REF - region table
region_codes = code_ref_udl_df.select(
    col('value_cd').alias('r_value_cd'),
    col('code_desc').alias('r_code_desc'),
    col('code_ref_key').alias('r_code_ref_key'),
).where(col('domain_nm') == "region_cd")

# JOIN country_code and region_code table
result_table = ip_geo_leads.alias('l') \
  .join(
    countries_codes.alias('c'),
    (upper(col('c_value_cd')) == upper(col('l.geoip_country_code'))),
    'left_outer') \
  .join(
    region_codes.alias('r'),
    ((upper(col('r_code_desc')) == upper(col('geoip_region'))) & (col('r_value_cd') == col('c_value_cd'))),
    'left_outer') \
  .select(
    coalesce(col('r_code_ref_key'), lit('-1')).alias('region_cd'),
    coalesce(col('c_code_ref_key'), lit('-1')).alias('country_cd'),
    col('l.geoip_city').alias('city_nm'),
    col("geoip_postal_code").alias('postal_cd'),
    col("geoip_isp").alias('isp_nm'),
    col("created").alias("source_ts")
)

# Add IP_Geolocation KEY which is (an MD5 hash of the following fields
# Country_cd, Region_cd, City_nm, Postal_cd, isp_nm)
ip_geolocation_with_key_df = result_table \
  .withColumn("ip_geolocation_key",
              md5(concat_ws('|',
                            col('country_cd'),
                            col('region_cd'),
                            col('city_nm'),
                            col('postal_cd'),
                            col('isp_nm')))) \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType())) \
  .withColumn("load_action_ind", lit('i').cast(StringType()))

ip_geolocation_with_deduped_key_df = ip_geolocation_with_key_df.dropDuplicates(['ip_geolocation_key'])

ip_geolocation_with_deduped_key_df.write.parquet(output_dir, mode='overwrite')

job.commit()
