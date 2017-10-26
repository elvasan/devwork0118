import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, from_json, current_timestamp, lit

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from glutils.job_objects import n_schema, s_schema

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# define catalog source
TBL_NAME = 'accounts'

# output directories
# TODO: pass the buckets in as args instead of hardcoding them
source_dir = "s3://jornaya-dev-us-east-1-rdl/{}".format(TBL_NAME)
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
df = spark.read.parquet(source_dir)

# TODO: generate this list from the DDL
keys = ['active',
        'affiliate_click_network',
        'api_key',
        'audit_auth',
        'audit_full',
        'audit_pre',
        'audit_self',
        'call_center',
        'code',
        'contribute',
        'create',
        'created',
        'email',
        'entity_code',
        'industry',
        'lead_aggregator',
        'lead_originator',
        'legal_agreed',
        'legal_agreed_date',
        'logging',
        'marketplace',
        'mobile_network',
        'modified',
        'name',
        'referral_source',
        'role',
        'status',
        'testing',
        'website'
        ]

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

# TODO: generate the types from the DDL
df = df.select(
    from_json(df['active'], n_schema).getItem('n').alias('active').cast(IntegerType()),
    from_json(df['affiliate_click_network'], n_schema).getItem('n').alias('affiliate_click_network').cast(IntegerType()),
    from_json(df['api_key'], s_schema).getItem('s').alias('api_key').cast(StringType()),
    from_json(df['audit_auth'], n_schema).getItem('n').alias('audit_auth').cast(IntegerType()),
    from_json(df['audit_full'], n_schema).getItem('n').alias('audit_full').cast(IntegerType()),
    from_json(df['audit_pre'], n_schema).getItem('n').alias('audit_pre').cast(IntegerType()),
    from_json(df['audit_self'], n_schema).getItem('n').alias('audit_self').cast(IntegerType()),
    from_json(df['call_center'], n_schema).getItem('n').alias('call_center').cast(IntegerType()),
    from_json(df['code'], s_schema).getItem('s').alias('code').cast(StringType()),
    from_json(df['contribute'], n_schema).getItem('n').alias('contribute').cast(IntegerType()),
    from_json(df['create'], n_schema).getItem('n').alias('create').cast(IntegerType()),
    from_json(df['created'], n_schema).getItem('n').alias('created').cast(IntegerType()),
    from_json(df['email'], s_schema).getItem('s').alias('email').cast(StringType()),
    from_json(df['entity_code'], s_schema).getItem('s').alias('entity_code').cast(StringType()),
    from_json(df['industry'], n_schema).getItem('n').alias('industry').cast(IntegerType()),
    from_json(df['lead_aggregator'], n_schema).getItem('n').alias('lead_aggregator').cast(IntegerType()),
    from_json(df['lead_originator'], n_schema).getItem('n').alias('lead_originator').cast(IntegerType()),
    from_json(df['legal_agreed'], n_schema).getItem('n').alias('legal_agreed').cast(IntegerType()),
    from_json(df['legal_agreed_date'], n_schema).getItem('n').alias('legal_agreed_date').cast(IntegerType()),
    from_json(df['logging'], n_schema).getItem('n').alias('logging').cast(IntegerType()),
    from_json(df['marketplace'], n_schema).getItem('n').alias('marketplace').cast(IntegerType()),
    from_json(df['mobile_network'], n_schema).getItem('n').alias('mobile_network').cast(IntegerType()),
    from_json(df['modified'], n_schema).getItem('n').alias('modified').cast(IntegerType()),
    from_json(df['name'], s_schema).getItem('s').alias('name').cast(StringType()),
    from_json(df['referral_source'], s_schema).getItem('s').alias('referral_source').cast(StringType()),
    from_json(df['role'], s_schema).getItem('s').alias('role').cast(IntegerType()),
    from_json(df['status'], n_schema).getItem('n').alias('status').cast(IntegerType()),
    from_json(df['testing'], n_schema).getItem('n').alias('testing').cast(IntegerType()),
    from_json(df['website'], s_schema).getItem('s').alias('website').cast(StringType()))

df = df \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

# TODO: pass the write mode in as an arg
df.write.parquet(output_dir,
                 mode='overwrite')

job.commit()
