import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DecimalType
from pyspark.sql.functions import col, from_json

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from glutils.job_objects import n_schema, s_schema

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
db_name = 'rdl'
tbl_name = 'entities'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
entities = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                         table_name=tbl_name,
                                                         transformation_ctx='entities')

df = entities.toDF()

keys = ['active',
        'code',
        'created',
        'industry',
        'modified',
        'name']

exprs = [col("item").getItem(k).alias(k) for k in keys]
df = df.select(*exprs)

df = df.select(
    from_json(df['active'], n_schema).getItem('n').alias('active').cast(IntegerType()),
    from_json(df['code'], s_schema).getItem('s').alias('code').cast(StringType()),
    from_json(df['created'], n_schema).getItem('n').alias('created').cast(IntegerType()),
    from_json(df['industry'], n_schema).getItem('n').alias('industry').cast(IntegerType()),
    from_json(df['modified'], n_schema).getItem('n').alias('modified').cast(IntegerType()),
    from_json(df['name'], s_schema).getItem('s').alias('name').cast(StringType()),
)

df.write.parquet(output_dir,
                 mode='overwrite')

job.commit()
