import sys

from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.job import Job  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from pyspark.context import SparkContext  # pylint: disable=wrong-import-order

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'BUCKET_NAME'])

# context and job setup
glueContext = GlueContext(SparkContext())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TABLE_NAME = 'application'
DATABASE_NAME = args['DATABASE_NAME']
BUCKET_NAME = args['BUCKET_NAME']

# Define the output directory
output_dir = "s3://{}/publisher_permissions/setup/{}".format(BUCKET_NAME, TABLE_NAME)

application_df = glueContext.create_dynamic_frame \
    .from_catalog(database=DATABASE_NAME, table_name=TABLE_NAME) \
    .toDF()

application_df.write.csv(output_dir, mode='overwrite')

job.commit()
