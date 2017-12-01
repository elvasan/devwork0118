import sys
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, from_unixtime, col, explode, get_json_object, concat, lit, udf, \
     current_timestamp, to_date

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

from glutils.job_objects import b_schema
from glutils.job_utils import zipped_b64_to_string

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Schema definition for Init fields to be extracted

init_schema = StructType([
    StructField("fields", ArrayType(
        StructType([
            StructField("checked", IntegerType(), True),
            StructField("email", IntegerType(), True),
            StructField("fieldvisibility", StringType(), True),
            StructField("id", StringType(), True),
            StructField("label", StringType(), True),
            StructField("labelvisibility", StringType(), True),
            StructField("name", StringType(), True),
            StructField("options", IntegerType(), True),
            StructField("option_label", StringType(), True),
            StructField("phone", IntegerType(), True),
            StructField("type", IntegerType(), True),
            StructField("value", StringType(), True),
        ])
    ), True)
])

# Define the UDF for decompressing binary init values
b64_udf = udf(zipped_b64_to_string, StringType())

# define catalog source
# db_name = 'rdl'
TBL_NAME = 'formdata'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create data frame from the source tables
formdata_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl",
                                                             table_name=TBL_NAME,
                                                             transformation_ctx="formdata_rdl").toDF()

# Only process rows where init column with string and binary datatypes are NULL

form_wthout_init_df = formdata_rdl \
    .where(get_json_object(formdata_rdl['item.init'], '$.b').isNull() &
           get_json_object(formdata_rdl['item.init'], '$.s').isNull()) \
    .select(
        get_json_object('item.checked', '$.n').alias('checked').cast(IntegerType()),
        get_json_object('item.client_time', '$.n').alias('client_time').cast(IntegerType()),
        get_json_object('item.created', '$.n').alias('created').cast(DoubleType()),
        get_json_object('item.email', '$.n').alias('email').cast(IntegerType()),
        get_json_object('item.execution_time', '$.n').alias('execution_time').cast(IntegerType()),
        get_json_object('item.fieldvisibility', '$.m').alias('fieldvisibility').cast(StringType()),
        get_json_object('item.http_Content-Length', '$.n').alias('http_content_length').cast(IntegerType()),
        get_json_object('item.http_User-Agent', '$.s').alias('http_user_agent').cast(StringType()),
        get_json_object('item.http_X-Forwarded-For', '$.s').alias('http_x_forwarded_for').cast(StringType()),
        get_json_object('item.id', '$.s').alias('id').cast(StringType()),
        get_json_object('item.label', '$.s').alias('label').cast(StringType()),
        get_json_object('item.labelvisibility', '$.s').alias('labelvisibility').cast(StringType()),
        get_json_object('item.name', '$.s').alias('name').cast(StringType()),
        get_json_object('item.options', '$.s').alias('optioncount').cast(IntegerType()),
        get_json_object('item.optionLabel', '$.s').alias('optionlabel').cast(StringType()),
        get_json_object('item.page_id', '$.s').alias('page_id').cast(StringType()),
        get_json_object('item.phone', '$.n').alias('phone').cast(IntegerType()),
        get_json_object('item.sequence_number', '$.n').alias('sequence_number').cast(IntegerType()),
        get_json_object('item.token', '$.s').alias('token').cast(StringType()),
        get_json_object('item.type', '$.n').alias('type').cast(IntegerType()),
        get_json_object('item.value', '$.s').alias('value').cast(StringType()),
        from_unixtime(get_json_object('item.created', '$.n')).alias('source_ts').cast(TimestampType())
    )

# Only process on init values with string datatype and extracts values as needed for UDL

form_init_str_df = formdata_rdl \
    .where(formdata_rdl['item.init'].isNotNull()) \
    .select(concat(lit('{"fields":'), get_json_object(formdata_rdl['item.init'], '$.s'), lit('}')).alias('fields'),
            formdata_rdl['item']) \
    .select(explode(from_json('fields', init_schema)['fields']).alias('fields'), formdata_rdl['item']) \
    .select(
        col('fields.checked').alias('checked').cast(IntegerType()),
        get_json_object('item.client_time', '$.n').alias('client_time').cast(IntegerType()),
        get_json_object('item.created', '$.n').alias('created').cast(DoubleType()),
        col('fields.email').alias('email').cast(IntegerType()),
        get_json_object('item.execution_time', '$.n').alias('execution_time').cast(IntegerType()),
        col('fields.labelvisibility').alias('fieldvisibility').cast(StringType()),
        get_json_object('item.http_Content-Length', '$.n').alias('http_content_length').cast(IntegerType()),
        get_json_object('item.http_User-Agent', '$.s').alias('http_user_agent').cast(StringType()),
        get_json_object('item.http_X-Forwarded-For', '$.s').alias('http_x_forwarded_for').cast(StringType()),
        col('fields.id').alias('id').cast(StringType()),
        col('fields.label').alias('label').cast(StringType()),
        col('fields.labelvisibility').alias('labelvisibility').cast(StringType()),
        col('fields.name').alias('name').cast(StringType()),
        col('fields.options').alias('optioncount').cast(IntegerType()),
        col('fields.option_label').alias('optionlabel').cast(StringType()),
        get_json_object('item.page_id', '$.s').alias('page_id').cast(StringType()),
        col('fields.phone').alias('phone').cast(IntegerType()),
        get_json_object('item.sequence_number', '$.n').alias('sequence_number').cast(IntegerType()),
        get_json_object('item.token', '$.s').alias('token').cast(StringType()),
        col('fields.type').alias('type').cast(IntegerType()),
        col('fields.value').alias('value').cast(StringType()),
        from_unixtime(get_json_object('item.created', '$.n')).alias('source_ts').cast(TimestampType())
    )

# The below extracts only init fields with binary value and applies the UDF to decompress and extract the required
# Fields
form_init_bin_df = formdata_rdl \
    .where(get_json_object(formdata_rdl['item.init'], '$.b').isNotNull()) \
    .select(from_json(formdata_rdl['item.init'], b_schema).alias('init_binary').cast(StringType()),
            formdata_rdl['item']) \
    .select(concat(lit('{"fields":'), b64_udf('init_binary'), lit('}')).alias('fields'), formdata_rdl['item']) \
    .select(explode(from_json('fields', init_schema)['fields']).alias('fields'), formdata_rdl['item']) \
    .select(
        col('fields.checked').alias('checked').cast(IntegerType()),
        get_json_object('item.client_time', '$.n').alias('client_time').cast(IntegerType()),
        get_json_object('item.created', '$.n').alias('created').cast(DoubleType()),
        col('fields.email').alias('email').cast(IntegerType()),
        get_json_object('item.execution_time', '$.n').alias('execution_time').cast(IntegerType()),
        col('fields.labelvisibility').alias('fieldvisibility').cast(StringType()),
        get_json_object('item.http_Content-Length', '$.n').alias('http_content_length').cast(IntegerType()),
        get_json_object('item.http_User-Agent', '$.s').alias('http_user_agent').cast(StringType()),
        get_json_object('item.http_X-Forwarded-For', '$.s').alias('http_x_forwarded_for').cast(StringType()),
        col('fields.id').alias('id').cast(StringType()),
        col('fields.label').alias('label').cast(StringType()),
        col('fields.labelvisibility').alias('labelvisibility').cast(StringType()),
        col('fields.name').alias('name').cast(StringType()),
        col('fields.options').alias('optioncount').cast(IntegerType()),
        col('fields.option_label').alias('optionlabel').cast(StringType()),
        get_json_object('item.page_id', '$.s').alias('page_id').cast(StringType()),
        col('fields.phone').alias('phone').cast(IntegerType()),
        get_json_object('item.sequence_number', '$.n').alias('sequence_number').cast(IntegerType()),
        get_json_object('item.token', '$.s').alias('token').cast(StringType()),
        col('fields.type').alias('type').cast(IntegerType()),
        col('fields.value').alias('value').cast(StringType()),
        from_unixtime(get_json_object('item.created', '$.n')).alias('source_ts').cast(TimestampType())
    )


# Use the Union All function to merge all the dataframes
form_un_df = reduce(DataFrame.unionAll, [form_wthout_init_df, form_init_str_df, form_init_bin_df]).persist()

# Add in the additional columns
forms_df = form_un_df \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType())) \
    .withColumn('create_day', to_date(from_unixtime(form_un_df.created, 'yyyy-MM-dd')))


# TODO: pass the write mode in as an arg
forms_df.write.parquet(output_dir,
                       mode='overwrite',
                       partitionBy=['create_day'],
                       compression='snappy')

job.commit()
