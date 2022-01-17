import boto3
import decimal

from botocore.exceptions import ClientError

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

def put_item_on_table():
    try:
        response = get_dynamodb_resource().Table("Movies_1").put_item(
            Item={
                'year' : 2015,
                'title' : "The Big New Movie",
                'info':{
                    'plot':"Nothing happens at all",
                    'rating' : decimal.Decimal(0)
                }
            }
        )
        print("A new movie added to the collection successfully!")
        print(str(response))

    except Exception as error:
        print(error)

def update_item_on_table():
    response = get_dynamodb_resource().Table("Movies_1").update_item(
        Key={
            'year':2015,
            'title':'The Big New Movie'
        },
        UpdateExpression="set info.rating = :r, info.plot = :p, info.actors = :a",
        ExpressionAttributeValues={
            ':r' : decimal.Decimal(3.5),
            ':p' : 'Everything happens all at once',
            ':a' : ["Larry","Moe","David"]
        },
        ReturnValues="UPDATED_NEW"
    )

    print("Updating existing movie was success!")
    print(str(response))

def conditionally_update_an_item():
    try:
        response = get_dynamodb_resource().Table("Movies_1").update_item(
            Key={
                'year': 2015,
                'title': 'The Big New Movie'
            },
            UpdateExpression="remove info.actors[0]",
            ConditionExpression="size(info.actors) >= :num",
            ExpressionAttributeValues={
                ':num' : 3
            },
            ReturnValues="UPDATED_NEW"
        )

    except ClientError as error:
        if error.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(error.response['Error']['Message'])
        else:
            raise
    else:
        print("Updated item on table conditionally!")
        print(str(response))

def get_item_on_table():
    try:
        response = get_dynamodb_resource().Table("Movies_1").get_item(
            Key={
                'year':2015,
                'title':"The Big New Movie"
            }
        )

    except ClientError as error:
        print(error.response['Error']['Message'])
    else:
        item = response['Item']
        print("Got the item successfully")
        print(str(response))

if __name__ == '__main__':
    #create_table()
    #put_item_on_table()
    #update_item_on_table()
    #conditionally_update_an_item()
    get_item_on_table()


