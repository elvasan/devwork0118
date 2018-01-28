import unittest

from glue_deployer import deployer_utils as u


class TestDeployerUtilFunctions(unittest.TestCase):
    def test_file_from_path(self):
        self.assertEqual('foo.txt', u._file_from_path('/baz/bar/foo.txt'))

    def test_filename_from_filepath(self):
        self.assertEqual('foo', u._filename_from_filepath('/baz/bar/foo.txt'))

    def test_jobname_from_file(self):
        self.assertEqual('dkelly_bar/foo', u._jobname_from_file('dkelly', 'bar/foo.txt'))
        self.assertEqual('dkelly_baz/bar/foo', u._jobname_from_file('dkelly', 'baz/bar/foo.txt'))

    def test_package_path_from_jobfile(self):
        self.assertEqual('dkelly/glue/jobs/dkelly_bar/baz_libs.zip', u._package_path_from_jobfile('bar/baz.txt', 'baz_libs.zip', 'dkelly'))

    def test_get_job_bucket(self):
        self.assertEqual('jornaya-dev-us-east-1-etl-code', u._get_job_bucket('dev'))
        self.assertEqual('jornaya-qa-us-east-1-etl-code', u._get_job_bucket('qa'))
        self.assertEqual('jornaya-prod-us-east-1-etl-code', u._get_job_bucket('prod'))

    def test_build_job_dict(self):
        expected = {
            'Name': 'dkelly_bar/foo',
            'Role': 'Glue_DefaultRole',
            'ExecutionProperty': {
                'MaxConcurrentRuns': 1
            },
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': '/foo/bar/foo.txt'
            },
            'DefaultArguments': {
                '--TempDir': 's3://jornaya-dev-us-east-1-etl-code/dkelly/glue/jobs/tmp/dkelly_bar/foo/',
                '--job-bookmark-option': 'job-bookmark-disable',
                '--extra-py-files': 's3://jornaya-dev-us-east-1-etl-code/dkelly/glue/jobs/dkelly_bar/foo_libs.zip'
            },
            'MaxRetries': 0,
            'AllocatedCapacity': 2
        }
        self.assertEqual(expected, u._build_job_dict('bar/foo.txt', '/foo/bar/foo.txt', 'dev', 'dkelly'))


if __name__ == '__main__':
    unittest.main()
