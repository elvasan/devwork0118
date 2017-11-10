# Accounts Transformation from RDL to UDL

import sys


from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql import functions as fun

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

from glutils.job_utils import get_dynamodb_value

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


# UDF to get the DynamoDB json value
get_dynamodb_value_udf = udf(get_dynamodb_value, StringType())

# Read in the accounts table into an Dataframe
# This needs to change so we directly read it from Glue's Catalog and not use Glue Libraries
accounts_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl", table_name="accounts",
                                                              transformation_ctx="accounts").toDF()
#accounts_rdl = spark.read.parquet(source_dir)

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
accounts = accounts_rdl.select(*exprs)

# TODO: generate the types from the DDL
accounts_extract = accounts.select(get_dynamodb_value_udf(accounts['active']).alias('active').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['affiliate_click_network']).alias('affiliate_click_network').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['api_key']).alias('api_key').cast(StringType()),
    get_dynamodb_value_udf(accounts['audit_auth']).alias('audit_auth').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['audit_full']).alias('audit_full').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['audit_pre']).alias('audit_pre').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['audit_self']).alias('audit_self').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['call_center']).alias('call_center').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['code']).alias('code').cast(StringType()),
    get_dynamodb_value_udf(accounts['contribute']).alias('contribute').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['create']).alias('create').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['created']).alias('created').cast(DoubleType()),
    get_dynamodb_value_udf(accounts['email']).alias('email').cast(StringType()),
    get_dynamodb_value_udf(accounts['entity_code']).alias('entity_code').cast(StringType()),
    get_dynamodb_value_udf(accounts['industry']).alias('industry').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['lead_aggregator']).alias('lead_aggregator').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['lead_originator']).alias('lead_originator').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['legal_agreed']).alias('legal_agreed').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['legal_agreed_date']).alias('legal_agreed_date').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['logging']).alias('logging').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['marketplace']).alias('marketplace').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['mobile_network']).alias('mobile_network').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['modified']).alias('modified').cast(DoubleType()),
    get_dynamodb_value_udf(accounts['name']).alias('name').cast(StringType()),
    get_dynamodb_value_udf(accounts['referral_source']).alias('referral_source').cast(StringType()),
    get_dynamodb_value_udf(accounts['role']).alias('role').cast(StringType()),
    get_dynamodb_value_udf(accounts['status']).alias('status').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['testing']).alias('testing').cast(IntegerType()),
    get_dynamodb_value_udf(accounts['website']).alias('website').cast(StringType()),
    fun.from_unixtime(get_dynamodb_value_udf(accounts['modified'])).alias('source_ts').cast(TimestampType())
    )

# add the job run columns
accounts_df = accounts_extract \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

# TODO: pass the write mode in as an arg
accounts_df.write.parquet(output_dir,
                 mode='overwrite')

job.commit()
