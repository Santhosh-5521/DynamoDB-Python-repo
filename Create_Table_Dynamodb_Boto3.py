import boto3

def get_dynamodb_client():
    dynamodb = boto3.client("dynamodb",region_name="us-east-1")
    """ :type : pyboto3.dynamodb """
    return dynamodb

def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb",region_name = "us-east-1")
    """ :type : pyboto3.dynamodb """
    return dynamodb

def create_table():
    table_name = "Movies_1"

    attribute_definitions = [
        {
            'AttributeName': 'year',
            'AttributeType':'N'
        },
        {
            'AttributeName':'title',
            'AttributeType':'S'
        }
    ]

    key_schema = [
        {
            'AttributeName' : 'year',
            'KeyType': 'HASH'
        },
        {
            'AttributeName' : 'title',
            'KeyType' : 'RANGE'
        }
    ]

    initial_iops = {
        'ReadCapacityUnits':10,
        'WriteCapacityUnits':10
    }

    dynamodb_table_response = get_dynamodb_client().create_table(
        AttributeDefinitions=attribute_definitions,
        TableName=table_name,
        KeySchema=key_schema,
        ProvisionedThroughput=initial_iops
    )

    print("Created DynamoDB table:" +str(dynamodb_table_response))

if __name__ == '__main__':
    create_table()
