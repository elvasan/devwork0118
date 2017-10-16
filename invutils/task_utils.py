import os
# import shutil
from zipfile import ZipFile
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
        '--job-bookmark-option': 'job-bookmark-disable',
        '--extra-py-files': ''
    },
    'MaxRetries': 0,
    'AllocatedCapacity': 1
}


def _humanize_seconds(seconds: int) -> str:
    """returns `seconds` as minutes and seconds

    :param seconds: number of seconds
    :return: minutes and seconds formatted as '%-Mm:$-Ss'

    >>> _humanize_seconds(356)
    '5m:56s'
    >>> _humanize_seconds(600)
    '10m:0s'
    """
    return '{}m:{}s'.format(int(seconds/60), seconds % 60)


def _file_from_path(filepath: str) -> str:
    """ returns just a filename from a filepath

    :param filepath: path to the file
    :return: filename

    >>> _file_from_path('/foo/bar/baz.txt')
    'baz.txt'
    >>> _file_from_path('baz.txt')
    'baz.txt'
    """
    return os.path.split(filepath)[1]


def _filename_from_filepath(filepath: str) -> str:
    """ extracts a filename without an extension from a path

    :param filepath: path to the file
    :return: name of the file

    >>> _filename_from_filepath('/foo/bar/baz.txt')
    'baz'
    >>> _filename_from_filepath('baz.txt')
    'baz'
    """
    return os.path.splitext(_file_from_path(filepath))[0]


def _jobname_from_file(filepath: str) -> str:
    """ generates a glue job name from a filepath

    :param filepath: path to the file
    :return: job name

    >>> _jobname_from_file('/foo/bar/baz/qux_job.txt')
    'baz/qux_job'
    >>> _jobname_from_file('baz/qux_job.txt')
    'baz/qux_job'
    >>> _jobname_from_file('qux_job.txt')
    'qux_job'
    """
    jobdir = os.path.basename(os.path.dirname(filepath))
    return os.path.join(jobdir, _filename_from_filepath(filepath))


def _package_path_from_jobfile(jobfile: str, pkgname: str) -> str:
    """ generates an S3 key to a glue job's extra python packages zipfile

    :param jobfile: local path to the parent job file
    :param pkgname: name of the zipfile package being uploaded
    :return: S3 key for the corresponding job's zipfile

    >>> _package_path_from_jobfile('/foo/bar/baz/qux_job.txt', 'qux_job_libs.zip')
    'glue/jobs/baz/qux_job_libs.zip'
    >>> _package_path_from_jobfile('qux_job.txt', 'qux_job_libs.zip')
    'glue/jobs/qux_job_libs.zip'
    """
    return os.path.join(os.path.dirname(JOB_PATH.format(_jobname_from_file(jobfile))), pkgname)


def _get_s3path(s3object):
    """ generates an S3 URI from an s3object

    :param s3object: an s3 Object
    :type s3object: boto3.resources.factory.s3.Object
    :return: s3 URI for the `s3object`
    """
    print(type(s3object))
    return 's3://{}/{}'.format(s3object.bucket_name, s3object.key)


def _build_job_dict(jobfile, s3key, dpu):
    """ builds a glue job dictionary request body

    :param jobfile: local file to derive the jobname from
    :param s3key: s3 key for the python file
    :param dpu: value to use for the default allocated DPU capacity
    :return: dictionary request body for the boto3 glue `create_job` command
    """
    tmp = TMP_PATH.format(_jobname_from_file(jobfile))
    zipname = f'{_filename_from_filepath(jobfile)}_libs.zip'
    JOB_DICT['Name'] = _jobname_from_file(jobfile)
    JOB_DICT['Command']['ScriptLocation'] = s3key
    JOB_DICT['AllocatedCapacity'] = int(dpu)
    JOB_DICT['DefaultArguments']['--TempDir'] = 's3://{}/{}'.format(JOB_BUCKET, tmp)
    JOB_DICT['DefaultArguments']['--extra-py-files'] = 's3://{}/{}'.format(JOB_BUCKET,
                                                                           _package_path_from_jobfile(jobfile, zipname))
    return JOB_DICT


def _upload_jobfile(jobfile):
    """ Uploads a jobfile to s3 """
    filelocation = './gluejobs/{}'.format(jobfile)
    data = open(filelocation, 'rb')
    keypath = JOB_PATH.format(_jobname_from_file(jobfile))
    return s3.Bucket(JOB_BUCKET).put_object(Key=keypath, Body=data)


def _create_job_directories(jobfile):
    """ create the scratch directories each job requires """
    staging = STAGING_PATH.format(_jobname_from_file(jobfile))
    tmp = TMP_PATH.format(_jobname_from_file(jobfile))
    for k in [staging, tmp]:
        s3.Bucket(JOB_BUCKET).put_object(Key=k, Body='')


def _zipdir(path, zfile):
    """ adds directories to an existing zipfile """
    for root, dirs, files in os.walk(path):
        for file in files:
            zfile.write(os.path.join(root, file))


def _upload_packages(jobfile):
    """ uploads the extra python libraries required by a glue job """
    # create the default zip with glutils package
    zipname = f'{_filename_from_filepath(jobfile)}_libs.zip'
    zippath = f'./libs/{zipname}'
    with ZipFile(zippath, 'w') as zf:
        _zipdir('./glutils', zf)

    keypath = _package_path_from_jobfile(jobfile, zipname)
    with open(zippath, 'rb') as data:
        s3.Bucket(JOB_BUCKET).put_object(Key=keypath, Body=data)


def update_or_create_job(jobfile, dpu):
    """ creates a new gluejob or updates the python script if it already exists """
    _upload_packages(jobfile)
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
    """ runs a glue job, polls the job every 10 seconds to see what the status is """
    try:

        # launch the job
        if dpu:
            response = client.start_job_run(JobName=_jobname_from_file(jobfile),
                                            AllocatedCapacity=int(dpu))
        else:
            response = client.start_job_run(JobName=_jobname_from_file(jobfile))

        # poll the job and print the response until the job completes or our MFA token expires...
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
    """ queries the status of a gluejob """
    try:
        response = client.get_job_run(JobName=_jobname_from_file(jobfile), RunId=jobid)
        pp.pprint(response)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print("Couldn't find a job run with job name: {} and run id: {}, are you sure it exists?".format(
                _jobname_from_file(jobfile), jobid))


if __name__ == '__main__':
    import doctest
    doctest.testmod()
