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
tbl_name = 'campaigns'

# output directories
output_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/output/{}".format(args['JOB_NAME'])
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
campaigns = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                          table_name=tbl_name,
                                                          transformation_ctx='campaigns')
df = campaigns.toDF()

keys = (df
        .select(explode("item"))
        .select('key')
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect())

exprs = [col("item").getItem(k).alias(k) for k in keys]

df = df.select(*exprs)

n_schema = StructType([StructField("n", StringType())])
s_schema = StructType([StructField("s", StringType())])
sS_schema = StructType([StructField("sS", StringType())])

df = df.select(from_json(df.account_code, s_schema).getItem('s').alias('account_code'),
               from_json(df.audience_id_tags, sS_schema).getItem('sS').alias('audience_id_tags'),
               from_json(df.campaign_javascript_version, s_schema).getItem('s').alias('campaign_javascript_version'),
               from_json(df.created, n_schema).getItem('n').alias('created'),
               from_json(df.created_by, n_schema).getItem('n').alias('created_by'),
               from_json(df.description, s_schema).getItem('s').alias('description'),
               from_json(df.forensiq_default, n_schema).getItem('n').alias('forensiq_default'),
               from_json(df.hash_urls, n_schema).getItem('n').alias('hash_urls'),
               from_json(df.key, s_schema).getItem('s').alias('key'),
               from_json(df.log_level, n_schema).getItem('n').alias('log_level'),
               from_json(df.log_limit, n_schema).getItem('n').alias('log_limit'),
               from_json(df.log_targets, n_schema).getItem('n').alias('log_targets'),
               from_json(df.modified, n_schema).getItem('n').alias('modified'),
               from_json(df.modified_by, n_schema).getItem('n').alias('modified_by'),
               from_json(df.name, s_schema).getItem('s').alias('name'),
               from_json(df.threatmetrix_default, n_schema).getItem('n').alias('threatmetrix_default'))

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

# write to output dir
glueContext.write_dynamic_frame.from_options(frame=dyf,
                                             connection_type='s3',
                                             connection_options={'path': output_dir},
                                             format='parquet')

job.commit()
