from invoke import task

from glutils.job_utils import update_or_create_job, run_glue_job, get_glue_job


def get_bucket(s3path):
    """
    Expects an S3 path string: {bucket name}/{key}/{filename}.{ext}
    returns the bucket_name

    :param s3path: String
    :return: String
    """
    parts = s3path.split('/')
    return parts[0]


def get_keypath(s3path):
    """
    Expects an S3 path string: {bucket name}/{key}/{filename}.{ext}
    returns the keypath and filename

    :param s3path: String
    :return: String
    """
    parts = s3path.split('/')
    return '/'.join(parts[1:])


@task
def updateglue(ctx):
    ctx.run("curl https://codeload.github.com/awslabs/aws-glue-libs/tar.gz/master | \
             tar -xvz --strip=1 aws-glue-libs-master/awsglue")
    ctx.run("cp .awsglue_setup.py awsglue/setup.py")
    ctx.run("pip install -e ./awsglue")


@task
def uploadjob(ctx, jobfile, dpu=10):
    update_or_create_job(jobfile, dpu)


@task
def runjob(ctx, jobfile, dpu=None):
    run_glue_job(jobfile, dpu)


@task
def getjob(ctx, jobfile, jobid):
    get_glue_job(jobfile, jobid)
