import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType
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
TBL_NAME = 'leads'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
leads_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl",
                                                          table_name=TBL_NAME,
                                                          transformation_ctx="leads_rdl").toDF()

keys = [
    'ip',
    'account_code',
    'Browser',
    'Browser_Maker',
    'browser_name',
    'browser_name_pattern',
    'browser_name_regex',
    'campaign_key',
    'client_time',
    'Comment',
    'created',
    'Device_Pointing_Method',
    'Device_Type',
    'geoip_city',
    'geoip_continent_code',
    'geoip_country_code',
    'geoip_isp',
    'geoip_postal_code',
    'geoip_region',
    'http_Content-Length',
    'http_User-Agent',
    'http_X-Forwarded-For',
    'isMobileDevice',
    'MajorVer',
    'MinorVer',
    'page_id',
    'Parent',
    'Platform',
    'sequence_number',
    'token',
    'Version',
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
leads = leads_rdl.select(*exprs)

# TODO: generate the types from the DDL
leads_extract = leads.select(
    from_json(leads['ip'], n_schema).getItem('n').alias('ip').cast(LongType()),
    from_json(leads['account_code'], s_schema).getItem('s').alias('account_code').cast(StringType()),
    from_json(leads['Browser'], s_schema).getItem('s').alias('browser').cast(StringType()),
    from_json(leads['Browser_Maker'], s_schema).getItem('s').alias('browser_maker').cast(StringType()),
    from_json(leads['browser_name'], s_schema).getItem('s').alias('browser_name').cast(StringType()),
    from_json(leads['browser_name_pattern'], s_schema).getItem('s').alias('browser_name_pattern').cast(StringType()),
    from_json(leads['browser_name_regex'], s_schema).getItem('s').alias('browser_name_regex').cast(StringType()),
    from_json(leads['campaign_key'], s_schema).getItem('s').alias('campaign_key').cast(StringType()),
    from_json(leads['client_time'], n_schema).getItem('n').alias('client_time').cast(LongType()),
    from_json(leads['Comment'], s_schema).getItem('s').alias('comment').cast(StringType()),
    from_json(leads['created'], n_schema).getItem('n').alias('created').cast(DoubleType()),
    from_json(leads['Device_Pointing_Method'], s_schema).getItem('s').alias('device_pointing_method').cast(
        StringType()),
    from_json(leads['Device_Type'], s_schema).getItem('s').alias('device_type').cast(StringType()),
    from_json(leads['geoip_city'], s_schema).getItem('s').alias('geoip_city').cast(StringType()),
    from_json(leads['geoip_continent_code'], s_schema).getItem('s').alias('geoip_continent_code').cast(StringType()),
    from_json(leads['geoip_country_code'], s_schema).getItem('s').alias('geoip_country_code').cast(StringType()),
    from_json(leads['geoip_isp'], s_schema).getItem('s').alias('geoip_isp').cast(StringType()),
    from_json(leads['geoip_postal_code'], s_schema).getItem('s').alias('geoip_postal_code').cast(StringType()),
    from_json(leads['geoip_region'], s_schema).getItem('s').alias('geoip_region').cast(StringType()),
    from_json(leads['http_Content-Length'], n_schema).getItem('n').alias('http_content_length').cast(IntegerType()),
    from_json(leads['http_User-Agent'], s_schema).getItem('s').alias('http_user_agent').cast(StringType()),
    from_json(leads['http_X-Forwarded-For'], s_schema).getItem('s').alias('http_x_forwarded_for').cast(StringType()),
    from_json(leads['isMobileDevice'], n_schema).getItem('n').alias('is_mobile_device').cast(IntegerType()),
    from_json(leads['MajorVer'], s_schema).getItem('s').alias('major_ver').cast(StringType()),
    from_json(leads['MinorVer'], s_schema).getItem('s').alias('minor_ver').cast(StringType()),
    from_json(leads['page_id'], s_schema).getItem('s').alias('page_id').cast(StringType()),
    from_json(leads['Parent'], s_schema).getItem('s').alias('parent').cast(StringType()),
    from_json(leads['Platform'], s_schema).getItem('s').alias('platform').cast(StringType()),
    from_json(leads['sequence_number'], n_schema).getItem('n').alias('sequence_number').cast(IntegerType()),
    from_json(leads['token'], s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(leads['Version'], s_schema).getItem('s').alias('version').cast(StringType()), )

# add the job run columns
leads_df = leads_extract \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

# add the partition column
leads_partitioned = leads_df.withColumn('create_day', to_date(from_unixtime(leads_df.created, 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
leads_partitioned.write.parquet(output_dir,
                                mode='overwrite',
                                partitionBy=['create_day', 'insert_job_run_id'],
                                compression='snappy')

job.commit()
