import json
import boto3

# import requests
s3 = boto3.client('s3')


def lambda_handler(event, context):

    obj = s3.get_object(Bucket='testperf-bucket', Key='test.csv')
    body = obj['Body'].read().decode('utf-8')

    obj1000 = s3.get_object(Bucket='testperf-bucket', Key='test-1000.csv')
    body1000 = obj1000['Body'].read().decode('utf-8')

    obj2000 = s3.get_object(Bucket='testperf-bucket', Key='test-2000.csv')
    body2000 = obj2000['Body'].read().decode('utf-8')

    obj5000 = s3.get_object(Bucket='testperf-bucket', Key='test-5000.csv')
    body5000 = obj5000['Body'].read().decode('utf-8')

    obj10000 = s3.get_object(Bucket='testperf-bucket', Key='test-10000.csv')
    body10000 = obj10000['Body'].read().decode('utf-8')

    return {
        "statusCode": 200,
        "body": json.dumps({
            'data': body.count('\n'),
            'data1000': body1000.count('\n'),
            'data2000': body2000.count('\n'),
            'data5000': body5000.count('\n'),
            'data10000': body10000.count('\n')
        }),
    }
