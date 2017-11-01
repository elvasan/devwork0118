# Leads ETL 
This is the home repository for all ETL scripts related to the Leads Datamart and Data Lake.

## System PreRequisites
* Python 3.6
* cURL

## Developer Setup
Activate your virtual environment and run the following commands
```
$ pip install -e .
$ pip install -r requirements.txt
$ inv updateglue
```

## Development Workflow
1. run the `updateglue` invoke task to make sure you have the most up-to-date `awsglue` library available
2. author a new script in the `gluejobs` directory under the correct stage subdirectoryo


## Deployment Workflow
# TODO: this, once gitlab is working


## Running Jobs
# TODO: this, once gitlab is working


## Invoke Task Reference

### Task: `updateglue`

#### Description: 
downloads and installs the latest `awsglue` package from github.

*Note: Glue jobs can't execute locally.  The only reason we include the `awsglue` package is for code 
hinting in an editor.*

#### Usage:

```
$ inv updateglue
```
- - -

### Task: `combinecsvs`

#### Description
Combines multiple csv files into a single csv file using a common header

#### Usage Examples:
_runs a job using job defaults_
```
$ inv combinecsvs --headersrc ./myfiles/myfile1.csv --csvdir ./myfiles --csvdest ./myfiles/mycombinedfile.csv 
```
- - -


## Useful AWS CLI Commands
Copy all files recursively to a local directory
```
aws --profile=jornaya-dev s3 cp s3://jornaya-dev-us-east-1-udl/accounts ./src_data/UDL/accounts --recursive 
```

## Amazon resources
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-python-intro.html
https://forums.aws.amazon.com/forum.jspa?forumID=262&start=0
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-etl-scripts-pyspark-transforms.html
https://docs.aws.amazon.com/glue/latest/dg/built-in-transforms.html

## Examples and Libs
https://github.com/search?q=org%3Aawslabs+glue

## ToDo
- [x] If we want the awsglue package to be installable, we should create a setup.py (done)
- [x] check if connections are required when using the aws cli (AWS Fixed this)
