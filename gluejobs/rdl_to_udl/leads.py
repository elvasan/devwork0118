import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import explode, col, from_json

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
db_name = 'rdl'
tbl_name = 'leads'

# DO NOT SET THESE MANUALLY
output_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/output/{}".format(args['JOB_NAME'])
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create a dynamic frame from the source catalog
leads = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                      table_name=tbl_name,
                                                      transformation_ctx='leads')

df = leads.toDF().repartition(200)

keys = (df
        .select(explode("item"))
        .select('key')
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect())

exprs = [col("item").getItem(k).alias(k) for k in keys]

df = df.select(*exprs)

n_schema = StructType([StructField("n", NType())])
s_schema = StructType([StructField("s", StringType())])

df = (df
      .withColumnRenamed('http_Content-Length', 'http_Content_Length')
      .withColumnRenamed('http_User-Agent', 'http_User_Agent')
      .withColumnRenamed('http_X-Forwarded-For', 'http_X_Forwarded_For'))

df = df.select(
    from_json(df.account_code, s_schema).getItem('s').alias('account_code'),
    from_json(df.Browser, s_schema).getItem('s').alias('browser'),
    from_json(df.Browser_Maker, s_schema).getItem('s').alias('browser_maker'),
    from_json(df.browser_name, s_schema).getItem('s').alias('browser_name'),
    from_json(df.browser_name_pattern, s_schema).getItem('s').alias('browser_name_pattern'),
    from_json(df.browser_name_regex, s_schema).getItem('s').alias('browser_name_regex'),
    from_json(df.campaign_key, s_schema).getItem('s').alias('campaign_key'),
    from_json(df.client_time, n_schema).getItem('n').alias('client_time'),
    from_json(df.Comment, s_schema).getItem('s').alias('comment'),
    from_json(df.created, n_schema).getItem('n').alias('created'),
    from_json(df.Device_Pointing_Method, s_schema).getItem('s').alias('device_pointing_method'),
    from_json(df.Device_Type, s_schema).getItem('s').alias('device_type'),
    from_json(df.geoip_city, s_schema).getItem('s').alias('geoip_city'),
    from_json(df.geoip_continent_code, s_schema).getItem('s').alias('geoip_continent_code'),
    from_json(df.geoip_country_code, s_schema).getItem('s').alias('geoip_country_code'),
    from_json(df.geoip_isp, s_schema).getItem('s').alias('geoip_isp'),
    from_json(df.geoip_postal_code, s_schema).getItem('s').alias('geoip_postal_code'),
    from_json(df.geoip_region, s_schema).getItem('s').alias('geoip_region'),
    from_json(df.http_Content_Length, n_schema).getItem('n').alias('http_content_length'),
    from_json(df.http_User_Agent, s_schema).getItem('s').alias('http_user_agent'),
    from_json(df.http_X_Forwarded_For, s_schema).getItem('s').alias('http_x_forwarded_for'),
    from_json(df.ip, n_schema).getItem('n').alias('ip'),
    from_json(df.isMobileDevice, n_schema).getItem('n').alias('is_mobile_device'),
    from_json(df.MajorVer, s_schema).getItem('s').alias('major_version'),
    from_json(df.MinorVer, s_schema).getItem('s').alias('minor_version'),
    from_json(df.page_id, s_schema).getItem('s').alias('page_id'),
    from_json(df.Parent, s_schema).getItem('s').alias('parent'),
    from_json(df.Platform, s_schema).getItem('s').alias('platform'),
    from_json(df.sequence_number, n_schema).getItem('n').alias('sequence_number'),
    from_json(df.token, s_schema).getItem('s').alias('token'),
    from_json(df.Version, s_schema).getItem('s').alias('version'))

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

# write to output dir
glueContext.write_dynamic_frame.from_options(frame=dyf,
                                             connection_type='s3',
                                             connection_options={'path': output_dir},
                                             format='parquet')

job.commit()
