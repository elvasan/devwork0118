import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from glutils.job_utils import code_format

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
db_name = 'rdl'
tbl_name = 'code_ref'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
code_ref = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                         table_name=tbl_name,
                                                         transformation_ctx='code_ref')

df = code_ref.toDF()

code_format_udf = udf(code_format, StringType())
df = df.withColumn('value_cd', code_format_udf('value_cd'))

# cast all vals to stringtype
df = df.select([col(x).alias(x).cast(StringType()) for x in df.schema.names])

df.write.parquet(output_dir,
                 mode='overwrite')

job.commit()
