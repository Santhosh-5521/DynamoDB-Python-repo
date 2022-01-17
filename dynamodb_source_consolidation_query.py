import boto3
import decimal
import numpy as np
import json
import pandas as pd
import re
import io
from ast import literal_eval
from boto3.dynamodb.conditions import Key,Attr

session = boto3.Session()
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb",region_name = "eu-west-1")
    return dynamodb

def reading_source_data():
    response = get_dynamodb_resource().Table('dev-ofr-datahub-cms-clinical-study-consolidation').scan()
        
    data = response['Items']
    
    df = pd.DataFrame(data)
    
    return df
    
if __name__ == "__main__":
    output = reading_source_data()
    print(output)
    