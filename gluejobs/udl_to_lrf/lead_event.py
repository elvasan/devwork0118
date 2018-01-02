import sys

from pyspark.context import SparkContext
from pyspark.sql.functions import from_unixtime, date_format, hour, minute, second, when, md5, concat_ws, lit, \
    to_date, udf
from pyspark.sql.functions import max as SparkMax
from pyspark.sql.types import IntegerType, TimestampType, StringType

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from glutils.job_utils import ip_address  # pylint: disable=import-error

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ip_conv = udf(ip_address, StringType())

tbl_name = 'lead_event'

output_dir = "s3://jornaya-dev-us-east-1-lrf/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

leads_df = glueContext.create_dynamic_frame.from_catalog(database="udl",
                                                         table_name="leads",
                                                         transformation_ctx="leads_df").toDF()

company_df = glueContext.create_dynamic_frame.from_catalog(database='lrf',
                                                           table_name='company',
                                                           transformation_ctx='company_df').toDF()

cmp_grpby_df = company_df.groupBy(company_df.account_id).agg(SparkMax(company_df.source_ts).alias('source_max_ts'))


ld_udf_df = leads_df.select(leads_df.token,
                            leads_df.account_code,
                            leads_df.created,
                            leads_df.campaign_key,
                            ip_conv(leads_df.ip).alias('ip_address'),
                            leads_df.http_content_length,
                            leads_df.http_user_agent,
                            leads_df.http_x_forwarded_for,
                            leads_df.insert_ts,
                            leads_df.insert_job_run_id,
                            from_unixtime(leads_df.created).alias('server_gmt_ts').cast(TimestampType()),
                            date_format(from_unixtime(leads_df.created), 'YYYYMMdd').alias('server_gmt_date_key')
                            .cast(IntegerType()),
                            (
                                (hour(from_unixtime(leads_df.created))*3600) +
                                (minute(from_unixtime(leads_df.created))*60) +
                                second(from_unixtime(leads_df.created))
                            ).alias('server_gmt_time_key').cast(IntegerType()),
                            from_unixtime(leads_df.client_time/1000).alias('client_local_ts').cast(TimestampType()),
                            date_format(from_unixtime(leads_df.client_time/1000), 'YYYYMMdd')
                            .alias('client_local_date_key').cast(IntegerType()),
                            (
                                (hour(from_unixtime(leads_df.client_time/1000))*3600) +
                                (minute(from_unixtime(leads_df.client_time/1000))*60) +
                                second(from_unixtime(leads_df.client_time/1000))
                            ).alias('client_local_time_key').cast(IntegerType()),
                            md5(concat_ws('|',
                                          leads_df.geoip_country_code,
                                          leads_df.geoip_region,
                                          leads_df.geoip_city,
                                          leads_df.geoip_postal_code,
                                          leads_df.geoip_isp)
                                ).alias('ip_geolocation_key').cast(StringType()),
                            md5(concat_ws('|',
                                          leads_df.browser_name,
                                          leads_df.major_ver,
                                          leads_df.minor_ver,
                                          leads_df.is_mobile_device,
                                          leads_df.browser_maker)
                                ).alias('user_agent_key').cast(StringType())
                            )

ld_cp_jn_df = ld_udf_df \
                .join(company_df, ld_udf_df.account_code == company_df.account_id) \
                .join(cmp_grpby_df, "account_id") \
                .select(
                    ld_udf_df.token.alias('lead_id'),
                    ld_udf_df.server_gmt_ts,
                    ld_udf_df.server_gmt_date_key,
                    ld_udf_df.server_gmt_time_key,
                    ld_udf_df.client_local_ts,
                    ld_udf_df.client_local_date_key,
                    ld_udf_df.client_local_time_key,
                    when((ld_udf_df.account_code == company_df.account_id) &
                         (cmp_grpby_df.source_max_ts <= from_unixtime(ld_udf_df.created)), company_df.account_key)
                    .when((company_df.source_ts == '9999/12/31:23:59:59'), company_df.account_key)
                    .otherwise(0).alias('generator_account_key'),
                    ld_udf_df.account_code.alias('account_id'),
                    ld_udf_df.ip_geolocation_key,
                    ld_udf_df.user_agent_key,
                    ld_udf_df.campaign_key,
                    ld_udf_df.ip_address,
                    ld_udf_df.http_content_length.alias('message_size_bytes'),
                    ld_udf_df.http_user_agent.alias('user_agent_txt'),
                    ld_udf_df.http_x_forwarded_for.alias('x_forwarded_for_txt'),
                    ld_udf_df.insert_ts,
                    ld_udf_df.insert_job_run_id,
                    cmp_grpby_df.source_max_ts.alias('source_ts'),
                    to_date(from_unixtime(ld_udf_df.created, 'yyyy-MM-dd')).alias('server_gmt_dt')
                ).withColumn("load_action_cd", lit('I').cast(StringType())) \
                 .withColumn("campaign_snapshot_key", lit(1).cast(IntegerType()))


ld_cp_jn_df.write.parquet(output_dir,
                          mode='overwrite',
                          partitionBy=['server_gmt_dt', 'insert_job_run_id'],
                          compression='snappy')

job.commit()
