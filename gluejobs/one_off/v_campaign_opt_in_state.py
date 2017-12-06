import sys

from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from pyspark.context import SparkContext
from pyspark.sql.functions import coalesce, current_timestamp, lit, max as max_
from pyspark.sql.types import IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the output directory
output_dir = 's3://jornaya-dev-us-east-1-prj/publisher_permissions/v_campaign_opt_in_state/v_campaign_opt_in_state/'
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Get the campaign opt in information and select out the max timestamp for each campaign key
campaign_opt_in_event = glueContext.create_dynamic_frame \
    .from_catalog(database='rdl', table_name='campaign_opt_in_event') \
    .toDF()

grouped_campaign_events = (campaign_opt_in_event.groupBy(['campaign_key', 'application_key'])) \
    .agg(max_('opt_in_ts')) \
    .withColumnRenamed('max(opt_in_ts)', 'opt_in_ts')

campaign_join_expression = [grouped_campaign_events.campaign_key == campaign_opt_in_event.campaign_key,
                            grouped_campaign_events.application_key == campaign_opt_in_event.application_key,
                            grouped_campaign_events.opt_in_ts == campaign_opt_in_event.opt_in_ts]

campaign_opt_in_event = grouped_campaign_events.join(campaign_opt_in_event, campaign_join_expression, 'left') \
    .select(grouped_campaign_events.campaign_key,
            grouped_campaign_events.application_key,
            campaign_opt_in_event.opt_in_ind)

# Get the account opt in information and select out the max timestamp for each account id
# Group by account_id and app key and filter out timestamps which are less then the max for that grouping
account_opt_in_event = glueContext.create_dynamic_frame \
    .from_catalog(database='rdl', table_name='account_opt_in_event') \
    .toDF()

grouped_account_events = (account_opt_in_event.groupBy(['account_id', 'application_key'])) \
    .agg(max_('opt_in_ts')) \
    .withColumnRenamed('max(opt_in_ts)', 'opt_in_ts')

account_join_expression = [grouped_account_events.account_id == account_opt_in_event.account_id,
                           grouped_account_events.application_key == account_opt_in_event.application_key,
                           grouped_account_events.opt_in_ts == account_opt_in_event.opt_in_ts]

account_opt_in_event = grouped_account_events.join(account_opt_in_event, account_join_expression, 'left') \
    .select(grouped_account_events.campaign_key,
            grouped_account_events.application_key,
            account_opt_in_event.opt_in_ind)

# Get all campaigns
campaigns = glueContext.create_dynamic_frame \
    .from_catalog(database='fdl', table_name='campaign') \
    .toDF()

# Grab application table so we can join application to the campaigns DataFrame and get app key
application = glueContext.create_dynamic_frame \
    .from_catalog(database='prj', table_name='application') \
    .toDF()

# Filter out only the information we need from campaigns:
campaigns = campaigns.select('campaign_key', 'account_id')

# Take the campaigns and cross join to applications to get a default opt in value for each campaign and application
# +--------------------+--------------------+---------------+--------------------+
# |        campaign_key|          account_id|application_key|default_opt_in_state|
# +--------------------+--------------------+---------------+--------------------+
# |480E8B12-D269-333...|C540C367-B2A6-98B...|              1|                   1|
# |480E8B12-D269-333...|C540C367-B2A6-98B...|              2|                   0|
# |480E8B12-D269-333...|C540C367-B2A6-98B...|              3|                   0|
# +--------------------+--------------------+---------------+--------------------+
campaigns_applications_joined = campaigns.crossJoin(application) \
    .drop('application_nm') \
    .alias('campaigns_applications_joined')

# Join the campaigns with the account opt in event table
campaigns_account_event = campaigns_applications_joined.join(account_opt_in_event, [
    campaigns_applications_joined.account_id == account_opt_in_event.account_id,
    campaigns_applications_joined.application_key == account_opt_in_event.application_key], 'left') \
    .withColumnRenamed('opt_in_ind', 'account_opt_in') \
    .select(campaigns_applications_joined.campaign_key,
            campaigns_applications_joined.application_key,
            campaigns_applications_joined.default_opt_in_state,
            'account_opt_in') \
    .alias('campaigns_account_event')

# Join the campaigns with the campaign opt in event table
camp_acct_campaign_event_join = campaigns_account_event.join(campaign_opt_in_event, [
    campaigns_account_event.campaign_key == campaign_opt_in_event.campaign_key,
    campaigns_account_event.application_key == campaign_opt_in_event.application_key], 'left') \
    .withColumnRenamed('opt_in_ind', 'campaign_opt_in') \
    .select(campaigns_account_event.campaign_key,
            campaigns_account_event.application_key,
            campaigns_account_event.default_opt_in_state,
            campaigns_account_event.account_opt_in,
            'campaign_opt_in')

# Finally, coalesce the three columns with the precedence: campaign > account > default
v_campaign_opt_in_state = camp_acct_campaign_event_join.select('*', coalesce(
    camp_acct_campaign_event_join['campaign_opt_in'],
    camp_acct_campaign_event_join['account_opt_in'],
    camp_acct_campaign_event_join['default_opt_in_state'])) \
    .withColumnRenamed('coalesce(campaign_opt_in, account_opt_in, default_opt_in_state)', 'opt_in_ind') \
    .drop('campaign_opt_in', 'account_opt_in', 'default_opt_in_state') \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType()))

v_campaign_opt_in_state.write.parquet(output_dir, mode='overwrite')

job.commit()
