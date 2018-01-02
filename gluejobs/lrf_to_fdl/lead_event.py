import sys
from pyspark.context import SparkContext
from awsglue.transforms import ApplyMapping, SelectFields, ResolveChoice  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog variables
tbl_name = 'lead_event'

# Pass these file paths in as args instead of hard coding them
output_dir = "s3://jornaya-dev-us-east-1-fdl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])


ld_lrf_df = glueContext.create_dynamic_frame.from_catalog(database="lrf",
                                                          table_name="lead_event",
                                                          transformation_ctx="ld_lrf_df")

ld_app_map_df = ApplyMapping.apply(frame=ld_lrf_df,
                                   mappings=[("lead_id", "string", "lead_id", "string"),
                                             ("server_gmt_ts", "timestamp", "server_gmt_ts", "timestamp"),
                                             ("server_gmt_date_key", "long", "server_gmt_date_key", "long"),
                                             ("server_gmt_time_key", "long", "server_gmt_time_key", "long"),
                                             ("client_local_ts", "timestamp", "client_local_ts", "timestamp"),
                                             ("client_local_date_key", "long", "client_local_date_key", "long"),
                                             ("client_local_time_key", "long", "client_local_time_key", "long"),
                                             ("generator_account_key", "string", "generator_account_key", "string"),
                                             ("account_id", "string", "account_id", "string"),
                                             ("ip_geolocation_key", "string", "ip_geolocation_key", "string"),
                                             ("user_agent_key", "string", "user_agent_key", "string"),
                                             ("campaign_snapshot_key", "long", "campaign_snapshot_key", "long"),
                                             ("campaign_key", "string", "campaign_key", "string"),
                                             ("ip_address", "string", "ip_address", "string"),
                                             ("message_size_bytes", "int", "message_size_bytes", "int"),
                                             ("x_forwarded_for_txt", "string", "x_forwarded_for_txt", "string"),
                                             ("user_agent_txt", "string", "user_agent_txt", "string"),
                                             ("insert_ts", "timestamp", "insert_ts", "timestamp"),
                                             ("insert_job_run_id", "timestamp", "insert_job_run_id", "timestamp"),
                                             ("source_ts", "timestamp", "source_ts", "timestamp"),
                                             ("load_action_cd", "string", "load_action_cd", "string"),
                                             ("server_gmt_dt", "string", "server_gmt_dt", "string")],
                                   transformation_ctx="ld_app_map_df")

ld_sel_fld_df = SelectFields.apply(frame=ld_app_map_df,
                                   paths=["lead_id", "server_gmt_ts", "server_gmt_date_key", "server_gmt_time_key",
                                          "client_local_ts", "client_local_date_key", "client_local_time_key",
                                          "generator_account_key", "account_id", "ip_geolocation_key", "user_agent_key",
                                          "campaign_snapshot_key", "campaign_key", "ip_address", "message_size_bytes",
                                          "x_forwarded_for_txt", "user_agent_txt", "insert_ts", "source_ts",
                                          "server_gmt_dt", "insert_job_run_id"],
                                   transformation_ctx="ld_sel_fld_df")

ld_rescho_df = ResolveChoice.apply(frame=ld_sel_fld_df,
                                   choice="MATCH_CATALOG",
                                   database="fdl",
                                   table_name="lead_event",
                                   transformation_ctx="ld_rescho_df")


ld_rescho_df.toDF().write.parquet(output_dir,
                                  mode='overwrite',
                                  partitionBy=['server_gmt_dt', 'insert_job_run_id'],
                                  compression='snappy')

job.commit()
