import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf, col, current_timestamp, lit

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

from glutils.job_utils import code_format

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define catalog source
TBL_NAME = 'code_ref'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(TBL_NAME)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# define udfs
code_format_udf = udf(code_format, StringType())

# Create data frame from the source tables
code_ref_rdl = glueContext.create_dynamic_frame.from_catalog(database="rdl",
                                                             table_name=TBL_NAME,
                                                             transformation_ctx="code_ref_rdl").toDF()

code_ref_formatted = code_ref_rdl.withColumn('value_cd', code_format_udf('value_cd'))

# cast all vals to stringtype
code_ref_strings = code_ref_formatted. \
    select([col(x).alias(x).cast(StringType()) for x in code_ref_formatted.schema.names])

code_ref_df = code_ref_strings \
    .withColumn("insert_ts", current_timestamp()) \
    .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
    .withColumn("insert_batch_run_id", lit(1).cast(IntegerType()))

# TODO: pass the write mode in as an arg
code_ref_strings.write.parquet(output_dir,
                               mode='overwrite',
                               compression='snappy')

job.commit()
