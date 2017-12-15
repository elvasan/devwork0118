import sys

from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from pyspark.context import SparkContext  # pylint: disable=wrong-import-order
from pyspark.sql.functions import col, when, lower, length, trim  # pylint: disable=wrong-import-order
from pyspark.sql.types import ShortType  # pylint: disable=wrong-import-order

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ACCOUNT_TABLE_NAME = 'account_opt_in'
APPLICATION_TABLE_NAME = 'application'
COL_APPLICATION_NM = 'application_nm'
COL_ACCOUNT_ID = 'account_id'
COL_APPLICATION_KEY = 'application_key'
COL_OPT_IN_IND = 'opt_in_ind'

# Define the output directory
output_dir = "s3://jornaya-dev-us-east-1-prj/publisher_permissions/setup/{}".format(ACCOUNT_TABLE_NAME)

# From the account_opt_in table select account_id, opt_in_ind and application_nm
# Then drop any account_ids that are null and filter out any that are blank strings.
account_opt_in_df = glueContext.create_dynamic_frame \
    .from_catalog(database='rdl', table_name=ACCOUNT_TABLE_NAME) \
    .toDF() \
    .select(COL_ACCOUNT_ID, COL_OPT_IN_IND, COL_APPLICATION_NM) \
    .dropna(subset=[COL_ACCOUNT_ID]) \
    .filter(length(trim(col(COL_ACCOUNT_ID))) > 0)

# Grab application table so we can join to account_opt_in to provide the application key
application_df = glueContext.create_dynamic_frame \
    .from_catalog(database='rdl', table_name=APPLICATION_TABLE_NAME) \
    .toDF()

# Join account_opt_in to application to get app key, replace "In/Out" values with 1/0/None and drop any rows
# that come back with None in opt_in_ind
account_opt_in = account_opt_in_df \
    .join(application_df, lower(account_opt_in_df.application_nm) == lower(application_df.application_nm)) \
    .select(COL_ACCOUNT_ID, COL_OPT_IN_IND, COL_APPLICATION_KEY) \
    .withColumn(COL_APPLICATION_KEY, col(COL_APPLICATION_KEY).cast(ShortType())) \
    .withColumn(COL_OPT_IN_IND, (when(lower(col(COL_OPT_IN_IND)) == 'in', 1)
                                 .otherwise(when(lower(col(COL_OPT_IN_IND)) == 'out', 0)
                                            .otherwise(None))).cast(ShortType())) \
    .dropna(subset=[COL_OPT_IN_IND]) \
    .dropDuplicates([COL_ACCOUNT_ID, COL_APPLICATION_KEY])

account_opt_in.write.parquet(output_dir, mode='overwrite')

job.commit()
