import boto3
dynamo = boto3.resource('dynamodb',region_name = "us-east-1")

def truncateTable(tableName):
    table = dynamo.Table(tableName)
    
    tableKeyNames = [key.get("AttributeName") for key in table.key_schema]
    #print(tableKeyNames)
    
    projectionExpression = ", ".join('#' + key for key in tableKeyNames)
    #print(projectionExpression)
    
    expressionAttrNames = {'#'+key: key for key in tableKeyNames}
    #print(expressionAttrNames)
    
    counter = 0
    
    page = table.scan(ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames)
    #print(page)
    
    with table.batch_writer() as batch:
         while page["Count"] > 0:
            counter += page["Count"]
            #print(counter)
             
            for itemKeys in page["Items"]:
                 batch.delete_item(Key=itemKeys)
                 
            if 'LastEvaluatedKey' in page:
                page = table.scan(
                    ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames,
                    ExclusiveStartKey=page['LastEvaluatedKey'])
            else:
                break
            
    print(f"Deleted {counter}")
             
             
def lambda_handler(event, context):
    truncateTable("standardized_table_universal")