import logging
import os
from zipfile import ZipFile

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger('glue_deployer.deployer_utils')

ERROR_LOG_BASE_URI = 'https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:group=\
/aws-glue/jobs/error;stream={}'
OUTPUT_LOG_BASE_URI = 'https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:group=\
/aws-glue/jobs/output;stream={}'

JOB_BUCKET = 'jornaya-{}-us-east-1-etl-code'
JOB_PATH = '{}/glue/jobs/{}'
TMP_PATH = '{}/glue/jobs/tmp/{}/'
STAGING_PATH = '{}/glue/jobs/staging/{}/'

JOB_DICT = {
    'Name': '',
    'Role': 'Glue_DefaultRole',
    'ExecutionProperty': {
        'MaxConcurrentRuns': 1
    },
    'Command': {
        'Name': 'glueetl',
        'ScriptLocation': ''
    },
    'DefaultArguments': {
        '--TempDir': '',
        '--job-bookmark-option': 'job-bookmark-disable',
        '--extra-py-files': ''
    },
    'MaxRetries': 0,
    'AllocatedCapacity': 2
}


def _init_boto_session():
    return boto3.Session()


def _file_from_path(filepath):
    return os.path.split(filepath)[1]


def _filename_from_filepath(filepath):
    return os.path.splitext(_file_from_path(filepath))[0]


def _jobname_from_file(user, filename):
    return user + '_' + os.path.splitext(filename)[0]


def _package_path_from_jobfile(jobfile, pkgname, user):
    return os.path.join(os.path.dirname(JOB_PATH.format(user, _jobname_from_file(user, jobfile))), pkgname)


def _get_s3path(s3object):
    return 's3://{}/{}'.format(s3object.bucket_name, s3object.key)


def _get_job_bucket(env):
    return JOB_BUCKET.format(env)


def _build_job_dict(jobfile, s3key, env, user):
    tmp = TMP_PATH.format(user, _jobname_from_file(user, jobfile))
    zipname = f'{_filename_from_filepath(jobfile)}_libs.zip'
    JOB_DICT['Name'] = _jobname_from_file(user, jobfile)
    JOB_DICT['Command']['ScriptLocation'] = s3key
    JOB_DICT['DefaultArguments']['--TempDir'] = 's3://{}/{}'.format(_get_job_bucket(env), tmp)
    JOB_DICT['DefaultArguments']['--extra-py-files'] = 's3://{}/{}'.format(_get_job_bucket(env),
                                                                           _package_path_from_jobfile(jobfile, zipname,
                                                                                                      user))
    return JOB_DICT


def _job_path_from_jobfile(user, jobfile):
    return JOB_PATH.format(user, _jobname_from_file(user, jobfile))


def _upload_jobfile(jobfile, env, user, s3):
    filelocation = './gluejobs/{}'.format(jobfile)
    data = open(filelocation, 'rb')
    keypath = _job_path_from_jobfile(user, jobfile)
    logger.debug(f'Uploading jobfile to: {_get_job_bucket(env)}/{keypath}')
    return s3.Bucket(_get_job_bucket(env)).put_object(Key=keypath, Body=data)


def _create_job_directories(jobfile, env, user, s3):
    staging = STAGING_PATH.format(user, _jobname_from_file(user, jobfile))
    tmp = TMP_PATH.format(user, _jobname_from_file(user, jobfile))
    for k in [staging, tmp]:
        logger.debug(f'Creating job directory: {_get_job_bucket(env)}/{k}')
        s3.Bucket(_get_job_bucket(env)).put_object(Key=k, Body='')


def _recursive_s3_delete(path_const, jobfile, env, user, s3_client):
    top_dir = path_const.format(user, _jobname_from_file(user, jobfile))
    logger.info("Destroying glue job artifacts found in: {}".format(top_dir))
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=_get_job_bucket(env),
                               Prefix=top_dir)
    argdict = {'Objects': []}
    for item in pages.search('Contents'):
        if item:
            argdict['Objects'].append({'Key': item['Key']})

        # flush once aws limit reached
        if len(argdict['Objects']) >= 1000:
            logger.debug("Destroying Artifact: {}".format(argdict['Objects'][0]['Key']))
            s3_client.delete_objects(Bucket=_get_job_bucket(env),
                                     Delete=argdict)
            argdict = dict(Objects=[])

    if argdict['Objects']:
        logger.debug("Destroying Artifact: {}".format(argdict['Objects'][0]['Key']))
        s3_client.delete_objects(Bucket=_get_job_bucket(env),
                                 Delete=argdict)


def _zipdir(path, zfile):
    for root, _, files in os.walk(path):
        for file in files:
            target = os.path.join(root, file)
            logger.debug(f'Creating zipfile: {target}')
            zfile.write(target)


def _list_jobfiles_for_stage(stage):
    rootdir = './gluejobs'
    jobfiles = set()
    for d, _, f in os.walk(rootdir):
        for filename in f:
            rel_dir = os.path.relpath(d, rootdir)
            rel_file = os.path.join(rel_dir, filename)
            if stage in rel_file:
                jobfiles.add(rel_file)
                logger.debug(f'Staging {rel_file} job artifacts for destruction.')

    return jobfiles


def _generate_zip_name_from_jobfile(jobfile):
    return f'{_filename_from_filepath(jobfile)}_libs.zip'


def _upload_packages(jobfile, env, user, s3):
    # create the default zip with glutils package
    zipname = _generate_zip_name_from_jobfile(jobfile)
    zippath = f'./libs/{zipname}'
    with ZipFile(zippath, 'w') as zf:
        _zipdir('./glutils', zf)
    keypath = _package_path_from_jobfile(jobfile, zipname, user)
    with open(zippath, 'rb') as data:
        s3.Bucket(_get_job_bucket(env)).put_object(Key=keypath, Body=data)


def update_or_create_job(jobfile, env, user):  # pylint:disable=inconsistent-return-statements
    session = _init_boto_session()
    glue_client = session.client('glue')
    s3_client = session.resource('s3')
    _upload_packages(jobfile, env, user, s3_client)
    try:
        if glue_client.get_job(JobName=_jobname_from_file(user, jobfile)):
            print("Updating Existing Job: {}".format(_jobname_from_file(user, jobfile)))
            _upload_jobfile(jobfile, env, user, s3_client)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print("Creating New Job: {}".format(_jobname_from_file(user, jobfile)))
            s3file = _upload_jobfile(jobfile, env, user, s3_client)
            _create_job_directories(jobfile, env, user, s3_client)
            argdict = _build_job_dict(jobfile, _get_s3path(s3file), env, user)
            return glue_client.create_job(**argdict)


def update_or_create_all_jobs_for_stage(stage, env, user):
    joblist = _list_jobfiles_for_stage(stage)
    for job in joblist:
        update_or_create_job(job, env, user)


def destroy_job(jobfile, env, user):
    session = _init_boto_session()
    glue_client = session.client('glue')
    s3_client = session.client('s3')
    glue_client.delete_job(JobName=_jobname_from_file(user, jobfile))
    job_key = _job_path_from_jobfile(user, jobfile)
    job_lib_key = _package_path_from_jobfile(jobfile,
                                             _generate_zip_name_from_jobfile(jobfile),
                                             user)
    for k in [STAGING_PATH, TMP_PATH]:
        _recursive_s3_delete(path_const=k, jobfile=jobfile, env=env, user=user, s3_client=s3_client)
    print("Destroying Jobfile: {}".format(job_key))
    s3_client.delete_object(Bucket=_get_job_bucket(env), Key=job_key)
    print("Destroying Job Libs: {}".format(job_lib_key))
    s3_client.delete_object(Bucket=_get_job_bucket(env), Key=job_lib_key)


def destroy_all_jobs_for_stage(stage, env, user):
    joblist = _list_jobfiles_for_stage(stage)
    for job in joblist:
        destroy_job(job, env, user)
