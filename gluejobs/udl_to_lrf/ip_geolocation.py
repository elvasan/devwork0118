import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, current_timestamp, upper, lit, coalesce, concat_ws, md5, \
    from_unixtime, length, when

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
db_name = 'udl'
tbl_name = 'ip_geolocation'
domain_name = 'hash_type_cd'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-lrf/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

leads_tbl = "leads"
code_ref_tbl = "code_ref"
# Create DataFrame of LEADS table
leads_udl_df = glueContext.create_dynamic_frame.from_catalog(database="{}".format(db_name),
                                                             table_name="{}".format(leads_tbl),
                                                             transformation_ctx="{}".format(leads_tbl)).toDF()
# Create DataFrame of CODE_REF table
code_ref_udl_df = glueContext.create_dynamic_frame.from_catalog(database="{}".format(db_name),
                                                                table_name="{}".format(code_ref_tbl),
                                                                transformation_ctx="{}".format(code_ref_tbl)).toDF()

ip_geo_leads = leads_udl_df.select(
    "created",
    "token",
    "geoip_country_code",
    "geoip_region",
    "geoip_city",
    "geoip_postal_code",
    "geoip_isp")

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
    when(((length(col('l.geoip_country_code')) >= 1) & (col('c_value_cd').isNotNull())), col('c_value_cd'))
        .when(((length(col('l.geoip_country_code')) >= 1) & (col('c_value_cd').isNull())), lit('-1'))
        .when(((length(col('l.geoip_country_code')) == 0) | (col('l.geoip_country_code').isNull())), lit('-2'))
        .otherwise(lit('-3')).cast(StringType()).alias('country_cd'),

    when(((length(col('l.geoip_region')) >= 1) & (col('r_code_desc').isNotNull())), col('r_code_desc'))
        .when(((length(col('l.geoip_region')) >= 1) & (col('r_code_desc').isNull())), lit('-1'))
        .when(((length(col('l.geoip_region')) == 0) | (col('l.geoip_region').isNull())), lit('-2'))
        .otherwise(lit('-3')).cast(StringType()).alias('region_cd'),

    col('l.geoip_city').alias('city_nm'),
    col("geoip_postal_code").alias('postal_cd'),
    col("geoip_isp").alias('isp_nm'),
    col("created")
)

result_table_ts = result_table.withColumn('source_ts',
                                          from_unixtime(
                                              result_table.created,
                                              'yyyy-MM-dd HH:mm:ss').cast(TimestampType())
                                          )

# Add IP_Geolocation KEY which is (an MD5 hash of the following fields
# Country_cd, Region_cd, City_nm, Postal_cd, isp_nm)
ip_geolocation_with_key_df = result_table_ts \
    .withColumn("ip_geolocation_key",
                md5(concat_ws('|',
                              col('country_cd'),
                              col('region_cd'),
                              coalesce(col('city_nm'), lit('').cast(StringType())),
                              coalesce(col('postal_cd'), lit('').cast(StringType())),
                              coalesce(col('isp_nm'), lit('').cast(StringType()))
                              ))) \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType())) \
    .withColumn("load_action_ind", lit('i').cast(StringType()))

ip_geolocation_with_deduped_key_df = ip_geolocation_with_key_df \
    .select('country_cd', 'region_cd', 'city_nm', 'postal_cd', 'isp_nm', 'source_ts',
            'ip_geolocation_key', 'insert_ts', 'insert_job_run_id', 'insert_batch_run_id', 'load_action_ind') \
    .dropDuplicates(['ip_geolocation_key'])

ip_geolocation_with_deduped_key_df.write.parquet(output_dir,
                                                 mode='overwrite',
                                                 compression='snappy')

job.commit()
