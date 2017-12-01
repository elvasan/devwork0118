#!/bin/bash

# Copies data from local EMR HDFS to S3 location
# Copying a months data of 250 GB takes 2 minutes using this method, just to keep in mind

s3-dist-cp --src /home/hadoop/formdata/ --dest s3://jornaya-dev-us-east-1-udl/formdata/ --srcPattern '.*.'

