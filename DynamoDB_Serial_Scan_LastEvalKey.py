import boto3
import pandas as pd


def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb",region_name = "eu-west-1")
    return dynamodb
    
def reading_source_data():
    response = get_dynamodb_resource().Table('dev-ofr-datahub-cms-clinical-study-consolidation').scan()
        
    data = response['Items']    

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
        
    df = pd.DataFrame(data)
    
    return df
        
if __name__ == "__main__":
    output = reading_source_data()
    print(output)