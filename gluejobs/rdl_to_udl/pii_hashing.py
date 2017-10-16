import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType
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
tbl_name = 'pii_hashing'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
pii_hashing = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                         table_name=tbl_name,
                                                         transformation_ctx='pii_hashing')

df = pii_hashing.toDF()

keys = ['canonical_hash',
        'hash',
        'hash_type',
        ]

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

n_schema = StructType([StructField("n", StringType())])
s_schema = StructType([StructField("s", StringType())])

df = df.select(
    from_json(df.canonical_hash, s_schema).getItem('s').alias('canonical_hash').cast(StringType()),
    from_json(df.hash, s_schema).getItem('s').alias('hash').cast(StringType()),
    from_json(df.hash_type, s_schema).getItem('s').alias('hash_type').cast(StringType()),
)

df.write.parquet(output_dir,
                 mode='overwrite')
job.commit()
