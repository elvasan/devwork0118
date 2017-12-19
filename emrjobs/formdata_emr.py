from functools import reduce
import base64
import zlib
from pyspark.sql.functions import from_json, from_unixtime, udf, col, explode, get_json_object, concat, lit, \
    current_timestamp, to_date
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, IntegerType, DoubleType, TimestampType, \
    MapType, LongType, BooleanType
from pyspark.sql import SparkSession, DataFrame

# Instruction on running this job on EMR cluster
# Spin up cluster with 4.2xlarge as Master and 8 4.8xlarge core instances (running command is optimized for this setup)
# Need to copy the code from Gitlab into EMR's home directory calling it as formdata.py
# Use the below command to run the job on EMR
# spark-submit formdata.py --num-executors 39 --executor-memory 43gb --driver-memory 43gb --executor-cores 6
# --driver-cores 6
# Once complete you will need to run emrhdfs_to_s3_copy.sh scripts command on EMR to copy files over to S3

# This schema definition is used to extract the binary init values, using other schema like '$.b' fails on zip udf
b_schema = StructType([StructField('b', StringType())])

input_schema = StructType([StructField("item", MapType(StringType(), StringType()))])


# This function reads in the binary compressed dynamodb column and returns a string value
def zipped_b64_to_string(val):
    if not val:
        return None

    zipped_string = base64.b64decode(val)
    return zlib.decompress(zipped_string, 16 + zlib.MAX_WBITS).decode('utf-8')


# This function does an UNIONALL of all the dataframes passed in
def unionall(*dfs):
    return reduce(DataFrame.unionAll, dfs)


# UDF to read DynamoDB Json values directly
b64_udf = udf(zipped_b64_to_string, StringType())

init_schema = StructType([
    StructField("fields", ArrayType(
        StructType([
            StructField("checked", BooleanType(), True),
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

#TBL_NAME = 'formdata'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
#output_dir = "s3://jornaya-dev-us-east-1-udl/krish/{}".format(TBL_NAME)

# This will be stored on the EMR local HDFS
output_dir = "/home/hadoop/formdata/"

spark = SparkSession.builder \
    .appName("formdata_trans") \
    .config("spark.yarn.executor.memoryOverhead", "5120") \
    .config("spark.yarn.driver.memoryOverhead", "5120") \
    .config("spark.sql.parquet.mergeSchema false", "false") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.hadoop.parquet.enable.summary-metadata", "false") \
    .config("spark.sql.hive.metastorePartitionPruning", "true") \
    .getOrCreate()

df = spark.read.schema(input_schema).parquet("s3://jornaya-dev-us-east-1-rdl/formdata/")

fm_noinit_df = df \
    .where(get_json_object(df['item.init'], '$.b').isNull() & get_json_object(df['item.init'], '$.s').isNull())

form_wthout_init_df = fm_noinit_df \
    .select(
    get_json_object('item.checked', '$.bOOL').alias('checked').cast(IntegerType()),
    get_json_object('item.client_time', '$.n').alias('client_time').cast(LongType()),
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

fm_initstr_df = df \
    .where(df['item.init'].isNotNull())

form_init_str_df = fm_initstr_df \
    .select(concat(lit('{"fields":'), get_json_object(df['item.init'], '$.s'), lit('}')).alias('fields'), df['item']) \
    .select(explode(from_json('fields', init_schema)['fields']).alias('fields'), df['item']) \
    .select(
    col('fields.checked').alias('checked').cast(IntegerType()),
    get_json_object('item.client_time', '$.n').alias('client_time').cast(LongType()),
    get_json_object('item.created', '$.n').alias('created').cast(DoubleType()),
    col('fields.email').alias('email').cast(IntegerType()),
    get_json_object('item.execution_time', '$.n').alias('execution_time').cast(IntegerType()),
    col('fields.fieldvisibility').alias('fieldvisibility').cast(StringType()),
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

fm_initbin_df = df \
    .where(get_json_object(df['item.init'], '$.b').isNotNull())

form_init_bin_df = fm_initbin_df \
    .select(from_json(df['item.init'], b_schema).alias('init_binary').cast(StringType()), df['item']) \
    .select(concat(lit('{"fields":'), b64_udf('init_binary'), lit('}')).alias('fields'), df['item']) \
    .select(explode(from_json('fields', init_schema)['fields']).alias('fields'), df['item']) \
    .select(
    col('fields.checked').alias('checked').cast(IntegerType()),
    get_json_object('item.client_time', '$.n').alias('client_time').cast(LongType()),
    get_json_object('item.created', '$.n').alias('created').cast(DoubleType()),
    col('fields.email').alias('email').cast(IntegerType()),
    get_json_object('item.execution_time', '$.n').alias('execution_time').cast(IntegerType()),
    col('fields.fieldvisibility').alias('fieldvisibility').cast(StringType()),
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

form_un_df = unionall(form_wthout_init_df, form_init_str_df, form_init_bin_df).persist()

forms_df = form_un_df \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

formdata_df = forms_df.withColumn('create_day', to_date(from_unixtime(forms_df.created, 'yyyy-MM-dd')))

# TODO: pass the write mode in as an arg
formdata_df.write.parquet(output_dir,
                          mode='overwrite',
                          partitionBy=['create_day'],
                          compression='snappy')
