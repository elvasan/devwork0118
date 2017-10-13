import os
import time
import pprint

import boto3

from botocore.exceptions import ClientError

session = boto3.Session(profile_name='jornaya-dev')
client = session.client('glue')
s3 = session.resource('s3')
pp = pprint.PrettyPrinter(indent=2)
error_log_base_uri = 'https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:group=/aws-glue/jobs/error;stream={}'
output_log_base_uri = 'https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:group=/aws-glue/jobs/output;stream={}'

JOB_BUCKET = 'jornaya-dev-us-east-1-etl-code'
JOB_PATH = 'glue/jobs/{}'
STAGING_PATH = 'glue/jobs/staging/{}/'
TMP_PATH = 'glue/jobs/tmp/{}/'

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
        '--job-bookmark-option': 'job-bookmark-disable'
    },
    'MaxRetries': 0,
    'AllocatedCapacity': 1
}


def _humanize_seconds(seconds):
    return '{}m:{}s'.format(int(seconds/60), seconds % 60)


def _jobname_from_file(filename):
    return os.path.splitext(filename)[0]


def _get_s3path(s3object):
    return 's3://{}/{}'.format(s3object.bucket_name, s3object.key)


def _build_job_dict(jobfile, s3key, dpu):
    JOB_DICT['Name'] = _jobname_from_file(jobfile)
    JOB_DICT['Command']['ScriptLocation'] = s3key
    JOB_DICT['DefaultArguments']['--TempDir'] = 's3://{}/{}'.format(JOB_BUCKET, TMP_PATH)
    JOB_DICT['AllocatedCapacity'] = int(dpu)
    return JOB_DICT


def _upload_jobfile(jobfile):
    filelocation = './gluejobs/{}'.format(jobfile)
    data = open(filelocation, 'rb')
    keypath = JOB_PATH.format(_jobname_from_file(jobfile))
    return s3.Bucket(JOB_BUCKET).put_object(Key=keypath, Body=data)


def _create_job_directories(jobfile):
    staging = STAGING_PATH.format(_jobname_from_file(jobfile))
    tmp = TMP_PATH.format(_jobname_from_file(jobfile))
    for k in [staging, tmp]:
        s3.Bucket(JOB_BUCKET).put_object(Key=k, Body='')


def update_or_create_job(jobfile, dpu):
    try:
        if client.get_job(JobName=_jobname_from_file(jobfile)):
            print("Updating Existing Job: {}".format(_jobname_from_file(jobfile)))
            _upload_jobfile(jobfile)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print("Creating New Job: {}".format(_jobname_from_file(jobfile)))
            s3file = _upload_jobfile(jobfile)
            _create_job_directories(jobfile)
            argdict = _build_job_dict(jobfile, _get_s3path(s3file), dpu)
            return client.create_job(**argdict)


def run_glue_job(jobfile, dpu):
    try:

        # TODO: check if we need a conditional here or if AllocatedCapacity of None falls back to the job default
        if dpu:
            response = client.start_job_run(JobName=_jobname_from_file(jobfile),
                                            AllocatedCapacity=int(dpu))
        else:
            response = client.start_job_run(JobName=_jobname_from_file(jobfile))

        jobid = response['JobRunId']
        print("Job Name: {}".format(_jobname_from_file(jobfile)))
        print("Job Run Id: {}".format(jobid))
        start_time = time.time()
        response = client.get_job_run(JobName=_jobname_from_file(jobfile), RunId=jobid)
        print("DPU Allocation: {}".format(response['JobRun']['AllocatedCapacity']))
        while 'CompletedOn' not in response['JobRun'].keys():
            response = client.get_job_run(JobName=_jobname_from_file(jobfile), RunId=jobid)
            print("Job Status ({}): {}".format(_humanize_seconds(int(time.time() - start_time)),
                                               response['JobRun']['JobRunState']))
            time.sleep(10)
        if 'ErrorMessage' in response['JobRun'].keys():
            print("Failure Message: {}".format(response['JobRun']['ErrorMessage']))
        print("Output Log: '{}'".format(output_log_base_uri.format(jobid)))
        print("Error Log: '{}'".format(error_log_base_uri.format(jobid)))
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print("Couldn't find a job named '{}', are you sure it exists?".format(_jobname_from_file(jobfile)))
        else:
            print(pp.pprint(e.response))


def get_glue_job(jobfile, jobid):
    try:
        response = client.get_job_run(JobName=_jobname_from_file(jobfile), RunId=jobid)
        pp.pprint(response)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print("Couldn't find a job run with job name: {} and run id: {}, are you sure it exists?".format(
                _jobname_from_file(jobfile), jobid))
