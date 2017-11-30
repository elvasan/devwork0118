#!/bin/bash

# Copies data from local EMR HDFS to S3 location

s3-dist-cp --src /home/hadoop/krish/formdata/ --dest s3://jornaya-dev-us-east-1-udl/formdata/ --srcPattern='.' --multipartUploadChunkSize=5000
