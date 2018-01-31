from distutils.core import setup  # pylint: disable=import-error

setup(
    name='glue_deployer',
    description='AWS Glue Deployer',
    author='Jornaya',
    author_email='engineering@jornaya.com',
    url='https://gitlab.com/leadid-dev/datalake/leads-etl',
    install_requires=['boto3>=1.4.7'],
    packages=['glue_deployer'],
    entry_points={
        'console_scripts': [
            'glue-deployer=glue_deployer.app:main'
        ],
    }
)
