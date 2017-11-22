# Company transformation from UDL to LRF
import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit, coalesce, concat_ws, md5, when, from_unixtime
from pyspark.sql.types import StringType, TimestampType, IntegerType

from awsglue.utils import getResolvedOptions # pylint: disable=import-error
from awsglue.context import GlueContext # pylint: disable=import-error
from awsglue.job import Job # pylint: disable=import-error

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# define catalog variables
tbl_name = 'company'

# Pass these file paths in as args instead of hard coding them
output_dir = "s3://jornaya-dev-us-east-1-lrf/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])


# Reading in the source files from UDL
db_name = 'udl'
source1_tbl = "accounts"
source2_tbl = "entities"
#entities = spark.read.parquet("s3://jornaya-dev-us-east-1-udl/{}".format(source1_tbl))
#accounts = spark.read.parquet("s3://jornaya-dev-us-east-1-udl/{}".format(source2_tbl))

# Read in the accounts/Entities table into an Dataframe
# This needs to change so we directly read it from Glue's Catalog and not use Glue Libraries
accounts = glueContext.create_dynamic_frame.from_catalog(database="{}".format(db_name),
                                                         table_name="{}".format(source1_tbl),
                                                         transformation_ctx="{}".format(source1_tbl)).toDF()

entities = glueContext.create_dynamic_frame.from_catalog(database="{}".format(db_name),
                                                         table_name="{}".format(source2_tbl),
                                                         transformation_ctx="{}".format(source2_tbl)).toDF()

# Variable declaration
curr_tmstp = current_timestamp()


# Transformations for company table to lrf
company_extract = entities \
    .join(
    accounts, entities.code == accounts.entity_code, 'right_outer') \
    .select(
    md5(concat_ws('|', accounts.code, from_unixtime(accounts.modified))).alias('account_key').cast(StringType()),
    accounts.code.alias('account_id'),
    when((accounts.modified).isNull() & (accounts.created).isNull(), '9999-12-31 23:59:59.999999')
        .otherwise(coalesce(from_unixtime(accounts.modified), from_unixtime(accounts.created)))
        .cast(TimestampType()).alias('source_mod_ts'),
    when(entities.name != ' ', entities.name).otherwise(accounts.name).alias('company_nm'),
    accounts.entity_code.alias('entity_id'),
    when(accounts.active == 1, accounts.active).otherwise(0).alias('is_active_ind').cast(IntegerType()),
    accounts.role.alias('role_nm'),
    curr_tmstp.alias('insert_ts').cast(TimestampType()),
    accounts.source_ts
)

# The below transformation should be updated once we have ABC fully ready
company_fnl = company_extract \
    .withColumn("insert_job_run_id", lit(-1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(-1).cast(IntegerType())) \
    .withColumn("load_action_cd", lit('i').cast(StringType()))

company_fnl.write.parquet(output_dir, mode='overwrite')

job.commit()
