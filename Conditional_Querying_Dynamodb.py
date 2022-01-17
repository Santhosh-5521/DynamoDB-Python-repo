import boto3
import decimal
import json
import pandas as pd

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key,Attr

def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb",region_name = "eu-west-1")
    return dynamodb
    
def conditional_querying_dynamodb(prm_key,srt_key):
    response = get_dynamodb_resource().Table('dev-ofr-datahub-cms-clinical-study-consolidation').query(
        KeyConditionExpression=Key('studycode').eq(prm_key) & Key('viewID#vendorshortname').eq(srt_key)
    )
    
    data = response['Items']
    df = pd.DataFrame(data)
    return df

def lambda_handler(event, context):
    output_df = conditional_querying_dynamodb('D999XX00001','ECOA2#CMERT')
    print(output_df)
