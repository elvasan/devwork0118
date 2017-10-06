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
db_name = 'dynamodb_reference'
tbl_accounts = 'accounts'

# output directories
output_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/output/{}".format(args['JOB_NAME'])
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
accounts = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                         table_name=tbl_accounts,
                                                         transformation_ctx='accounts')

df = accounts.toDF()

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

df = df.select(from_json(df.active, n_schema).getItem('n').alias('active'),
               from_json(df.affiliate_click_network, n_schema).getItem('n').alias('affiliate_click_network'),
               from_json(df.api_key, s_schema).getItem('s').alias('api_key'),
               from_json(df.audit_auth, n_schema).getItem('n').alias('audit_auth'),
               from_json(df.audit_full, n_schema).getItem('n').alias('audit_full'),
               from_json(df.audit_pre, n_schema).getItem('n').alias('audit_pre'),
               from_json(df.audit_self, n_schema).getItem('n').alias('audit_self'),
               from_json(df.call_center, n_schema).getItem('n').alias('call_center'),
               from_json(df.code, s_schema).getItem('s').alias('code'),
               from_json(df.contribute, n_schema).getItem('n').alias('contribute'),
               from_json(df.create, n_schema).getItem('n').alias('create'),
               from_json(df.created, n_schema).getItem('n').alias('created'),
               from_json(df.email, s_schema).getItem('s').alias('email'),
               from_json(df.entity_code, s_schema).getItem('s').alias('entity_code'),
               from_json(df.industry, n_schema).getItem('n').alias('industry'),
               from_json(df.lead_aggregator, n_schema).getItem('n').alias('lead_aggregator'),
               from_json(df.lead_originator, n_schema).getItem('n').alias('lead_originator'),
               from_json(df.legal_agreed, n_schema).getItem('n').alias('legal_agreed'),
               from_json(df.legal_agreed_date, n_schema).getItem('n').alias('legal_agreed_date'),
               from_json(df.logging, n_schema).getItem('n').alias('logging'),
               from_json(df.marketplace, n_schema).getItem('n').alias('marketplace'),
               from_json(df.mobile_network, n_schema).getItem('n').alias('mobile_network'),
               from_json(df.modified, n_schema).getItem('n').alias('modified'),
               from_json(df.name, s_schema).getItem('s').alias('name'),
               from_json(df.referral_source, s_schema).getItem('s').alias('referral_source'),
               from_json(df.role, s_schema).getItem('s').alias('role'),
               from_json(df.status, n_schema).getItem('n').alias('status'),
               from_json(df.testing, n_schema).getItem('n').alias('testing'),
               from_json(df.website, s_schema).getItem('s').alias('website'))

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

# write to output dir
glueContext.write_dynamic_frame.from_options(frame=dyf,
                                             connection_type='s3',
                                             connection_options={'path': output_dir},
                                             format='orc')

job.commit()
