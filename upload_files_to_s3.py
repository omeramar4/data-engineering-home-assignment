#!/usr/bin/env python3
import boto3
import os

# Load environment variables from .env
with open('.env', 'r') as f:
    for line in f:
        if '=' in line and not line.startswith('#'):
            key, value = line.strip().split('=', 1)
            os.environ[key] = value

# AWS credentials and bucket name
aws_access_key = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
stack_name = os.environ['STACK_NAME']

# Create S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# Get bucket name from CloudFormation
cf = boto3.client(
    'cloudformation',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

response = cf.describe_stacks(StackName=stack_name)
bucket_name = None
for output in response['Stacks'][0]['Outputs']:
    if output['OutputKey'] == 'S3BucketName':
        bucket_name = output['OutputValue']
        break

print(f"Bucket: {bucket_name}")

# Upload files
print("Uploading assignment/objectives/stock_stats.py...")
s3.upload_file('assignment/objectives/stock_stats.py', bucket_name, 'scripts/stock_stats.py')
print("✅ Stock stats script uploaded")

print("Uploading stocks_data.csv...")
s3.upload_file('stocks_data.csv', bucket_name, 'input/stocks_data.csv')
print("✅ CSV file uploaded")

print("🎉 All files uploaded successfully!")
