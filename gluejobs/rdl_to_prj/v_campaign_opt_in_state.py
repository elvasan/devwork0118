import sys

from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from pyspark.context import SparkContext  # pylint: disable=wrong-import-order
from pyspark.sql.functions import coalesce, current_timestamp, lit, col  # pylint: disable=wrong-import-order
from pyspark.sql.types import ShortType, StringType  # pylint: disable=wrong-import-order

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATABASE_FDL = 'fdl'
DATABASE_PRJ = 'prj'
COL_CAMPAIGN_OPT_IN = 'campaign_opt_in'
COL_ACCOUNT_OPT_IN = 'account_opt_in'
COL_OPT_IN_IND = 'opt_in_ind'
COL_DEFAULT_OPT_IN_IND = 'default_opt_in_ind'
COL_CAMPAIGN_KEY = 'campaign_key'
COL_ACCOUNT_ID = 'account_id'
COL_APPLICATION_KEY = 'application_key'
JOIN_TYPE_LEFT = 'left'

# Define the output directory
output_dir = 's3://jornaya-dev-us-east-1-prj/publisher_permissions/v_campaign_opt_in_state/'

# Get the campaign opt in information and select out the max timestamp for each campaign key
campaign_opt_in = glueContext.create_dynamic_frame \
    .from_catalog(database=DATABASE_PRJ, table_name='campaign_opt_in') \
    .toDF()

# Get the account opt in information
account_opt_in = glueContext.create_dynamic_frame \
    .from_catalog(database=DATABASE_PRJ, table_name='account_opt_in') \
    .toDF()

# Get all campaigns
campaigns = glueContext.create_dynamic_frame \
    .from_catalog(database=DATABASE_FDL, table_name='campaign') \
    .toDF()

# Grab application table so we can join application to the campaigns DataFrame and get app key
application = glueContext.create_dynamic_frame \
    .from_catalog(database=DATABASE_PRJ, table_name='application') \
    .toDF()

# Filter out only the information we need from campaigns and drop any duplicates:
campaigns = campaigns.select(COL_CAMPAIGN_KEY, COL_ACCOUNT_ID).dropDuplicates()

# Take the campaigns and cross join to applications to get a default opt in value for each campaign and application
# +--------------------+--------------------+---------------+--------------------+
# |        campaign_key|          account_id|application_key|default_opt_in_ind|
# +--------------------+--------------------+---------------+--------------------+
# |480E8B12-D269-333...|C540C367-B2A6-98B...|              1|                   1|
# |480E8B12-D269-333...|C540C367-B2A6-98B...|              2|                   0|
# |480E8B12-D269-333...|C540C367-B2A6-98B...|              3|                   0|
# +--------------------+--------------------+---------------+--------------------+
campaigns_applications_joined = campaigns.crossJoin(application) \
    .drop('application_nm') \
    .alias('campaigns_applications_joined')

# Join the campaigns with the account opt in table
campaigns_joined_accounts = campaigns_applications_joined.join(account_opt_in, [
    campaigns_applications_joined.account_id == account_opt_in.account_id,
    campaigns_applications_joined.application_key == account_opt_in.application_key], JOIN_TYPE_LEFT) \
    .withColumnRenamed(COL_OPT_IN_IND, COL_ACCOUNT_OPT_IN) \
    .select(campaigns_applications_joined.campaign_key,
            campaigns_applications_joined.application_key,
            campaigns_applications_joined.default_opt_in_ind,
            COL_ACCOUNT_OPT_IN) \
    .alias('campaigns_joined_accounts')

# Join the campaigns with the campaign opt in table
camp_acct_campaign_state_join = campaigns_joined_accounts.join(campaign_opt_in, [
    campaigns_joined_accounts.campaign_key == campaign_opt_in.campaign_key,
    campaigns_joined_accounts.application_key == campaign_opt_in.application_key], JOIN_TYPE_LEFT) \
    .withColumnRenamed(COL_OPT_IN_IND, COL_CAMPAIGN_OPT_IN) \
    .select(campaigns_joined_accounts.campaign_key,
            campaigns_joined_accounts.application_key,
            campaigns_joined_accounts.default_opt_in_ind,
            campaigns_joined_accounts.account_opt_in,
            COL_CAMPAIGN_OPT_IN)

# Finally, coalesce the three columns with the precedence: campaign > account > default
v_campaign_opt_in_state = camp_acct_campaign_state_join.select('*', coalesce(
    camp_acct_campaign_state_join[COL_CAMPAIGN_OPT_IN],
    camp_acct_campaign_state_join[COL_ACCOUNT_OPT_IN],
    camp_acct_campaign_state_join[COL_DEFAULT_OPT_IN_IND])) \
    .withColumnRenamed('coalesce(campaign_opt_in, account_opt_in, default_opt_in_ind)', COL_OPT_IN_IND) \
    .drop(COL_CAMPAIGN_OPT_IN, COL_ACCOUNT_OPT_IN, COL_DEFAULT_OPT_IN_IND) \
    .withColumn(COL_APPLICATION_KEY, col(COL_APPLICATION_KEY).cast(ShortType())) \
    .withColumn(COL_OPT_IN_IND, col(COL_OPT_IN_IND).cast(ShortType())) \
    .withColumn('insert_ts', current_timestamp()) \
    .withColumn('insert_job_run_id', lit(1).cast(StringType()))

v_campaign_opt_in_state.write.parquet(output_dir, mode='overwrite', compression='snappy')

job.commit()
