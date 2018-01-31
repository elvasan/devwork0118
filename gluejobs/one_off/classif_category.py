import sys

from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit

from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# context and job setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# TODO: Data source should be passed in as an arg
# define catalog source
db_name = 'rdl'
tbl_name = 'classif_category'

# output directories
# TODO: pass these file paths in as args instead of hardcoding them
output_dir = "s3://jornaya-dev-us-east-1-udl/{}".format(tbl_name)
staging_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/staging/{}".format(args['JOB_NAME'])
temp_dir = "s3://jornaya-dev-us-east-1-etl-code/glue/jobs/tmp/{}".format(args['JOB_NAME'])

# Create dynamic frames from the source tables
category = glueContext.create_dynamic_frame.from_catalog(database=db_name,
                                                                table_name=tbl_name,
                                                                transformation_ctx='category')

df = category.toDF()

# TODO: insert_job_run_id should be passed in as an arg
# TODO: insert_batch_run_id should be passed in as an arg
df = df \
  .withColumn("insert_ts", current_timestamp()) \
  .withColumn("insert_job_run_id", lit(1).cast(IntegerType())) \
  .withColumn("insert_batch_run_id", lit(1).cast(IntegerType())) \
  .withColumn("load_action_ind", lit('i').cast(StringType()))

df.write.parquet(output_dir,
                 mode='overwrite')

job.commit()
