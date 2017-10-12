import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DecimalType
from pyspark.sql.functions import col, from_json

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
db_name = 'rdl'
tbl_name = 'deviceid'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
deviceid = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                         table_name=tbl_name,
                                                         transformation_ctx='deviceid')

df = deviceid.toDF().repartition(200)

keys = [
    'created',
    'http_User-Agent',
    'http_X-Forwarded-For',
    'methods',
    'token',
    'uuid'
]

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

n_schema = StructType([StructField("n", StringType())])
s_schema = StructType([StructField("s", StringType())])

df = (df
      .withColumnRenamed('http_User-Agent', 'http_User_Agent')
      .withColumnRenamed('http_X-Forwarded-For', 'http_X_Forwarded_For'))

df = df.select(
    from_json(df.created, n_schema).getItem('n').alias('created').cast(DecimalType(14, 4)),
    from_json(df.http_User_Agent, s_schema).getItem('s').alias('http_User-Agent').cast(StringType()),
    from_json(df.http_X_Forwarded_For, s_schema).getItem('s').alias('http_X-Forwarded-For').cast(StringType()),
    from_json(df.methods, n_schema).getItem('n').alias('methods').cast(IntegerType()),
    from_json(df.token, s_schema).getItem('s').alias('token').cast(StringType()),
    from_json(df.uuid, s_schema).getItem('s').alias('uuid').cast(StringType()),
)

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

# write to output dir
glueContext.write_dynamic_frame.from_options(frame=dyf,
                                             connection_type='s3',
                                             connection_options={'path': output_dir},
                                             format='parquet')

job.commit()
