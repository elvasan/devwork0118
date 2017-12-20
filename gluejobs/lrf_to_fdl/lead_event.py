import sys

from pyspark.context import SparkContext

#from awsglue.transforms import *  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ip_table_name = 'lead_event'
op_tbl_name = 'lead_event'

output_dir = "s3://jornaya-dev-us-east-1-fdl/{}".format(op_tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

ld_evnt_lrf_df = glueContext.create_dynamic_frame.from_catalog(database="lrf",
                                                               table_name=ip_table_name,
                                                               transformation_ctx="ld_evnt_lrf_df")

ld_evnt_fdl_df = ld_evnt_lrf_df.apply_mapping([('lead_id', 'string', 'lead_id', 'string'),
                                                    ('server_gmt_ts', 'long', 'server_gmt_ts', 'long'),
                                                    ('provider name', 'string', 'provider.name', 'string'),
                                                    ('server_gmt_date_key', 'string', 'server_gmt_date_key', 'string'),
                                                    ('server_gmt_time_key', 'string', 'server_gmt_time_key', 'string'),
                                                    ('client_local_ts', 'long', 'client_local_ts', 'long'),
                                                    ('client_local_date_key', 'string',
                                                     'client_local_date_key', 'string'),
                                                    ('client_local_time_key', 'string',
                                                     'client_local_time_key', 'double'),
                                                    ('generator_account_key', 'string',
                                                     'charges.generator_account_key', 'double'),
                                                    ('account_id', 'string', 'account_id', 'double'),
                                                    ('ip_geolocation_key', 'string', 'ip_geolocation_key', 'string'),
                                                    ('user_agent_key', 'string', 'user_agent_key', 'string'),
                                                    ('campaign_snapshot_key', 'int', 'campaign_snapshot_key', 'int'),
                                                    ('campaign_key', 'string', 'campaign_key', 'string'),
                                                    ('ip_address', 'string', 'ip_address', 'string'),
                                                    ('message_size_bytes', 'int', 'message_size_bytes', 'int'),
                                                    ('x_forwarded_for_txt', 'string', 'x_forwarded_for_txt', 'string'),
                                                    ('user_agent_txt', 'string', 'user_agent_txt', 'string'),
                                                    ('insert_ts', 'timestamp', 'ip_geolocation_key', 'timestamp'),
                                                    ('source_ts', 'timestamp', 'source_ts', 'timestamp')])

ld_evnt_fdl_df.write.parquet(output_dir,
                             mode='overwrite',
                             partitionBy=['server_gmt_dt', 'insert_job_run_id'],
                             compression='snappy')

job.commit()
