#!/bin/sh

if aws s3 ls $2 2>&1 | grep -q 'NoSuchBucket'
then
    echo "Creating S3 bucket: $2"
    aws s3 mb $2
fi
echo "Uploading $1 to $2/$3"
aws s3 cp $1 $2/$3