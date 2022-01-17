import json
import pandas as pd
import boto3
import s3fs
import re
from decimal import Decimal
from datetime import datetime

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
s3 = s3fs.S3FileSystem(anon=False)

config_bucket = 'dev-data-lake-ds-raw-eu-west-1-239126490696'
config_key = 'Config_files/config.json'

def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb",region_name = "eu-west-1")
    return dynamodb


if __name__ == "__main__":
    bucket = "dev-data-lake-ds-raw-eu-west-1-239126490696"
    key = "rave_clinical_view/V_D3253C00001_prod.V_D3253C00001_AE_20210714_145624.899.csv"
    
    #process = re.findall("Source\_(.*?)\.csv",key)
    
    strings = ["AE", "DS", "ecoa","site","ENROL","IxRS","Death"]
    
    result = [x for x in strings if x in key]
    
    str1 = " "
    proc = str1.join(result)
    
    if proc == "AE":
        proc = "rave_clinical_view"
    
    with s3.open(f's3://{config_bucket}/{config_key}', 'r') as json_file:
      config_dic = json.load(json_file,parse_float=Decimal)
      
    key_columns = config_dic[proc]['key columns']
    table_columns = config_dic[proc]['Target_table_columns']
    output_bucket = config_dic[proc]['output_bucket']
    output_key = config_dic[proc]['output_key']
    
    data = pd.read_csv(f's3://{bucket}/{key}')
    
    if proc in ['ecoa']:
        data['Folder'] = proc
        data["Study"].fillna(method ='ffill', inplace = True)
    elif proc == "rave_clinical_view":
        data["Folder"].replace({"AE": "rave_clinical_view"}, inplace=True)
        
    data.sort_values([key_columns[0],key_columns[1]],inplace=True)
    
    data.fillna(value="Null",inplace=True)
    
    data1 = pd.DataFrame(columns=[table_columns[0],table_columns[1],table_columns[2]])
    
    item_list = [e for e in data.columns if e not in (key_columns[0],key_columns[1])]
    
    for i,j in data.iterrows():
      data1 = pd.concat([pd.DataFrame([[j[key_columns[0]],j[key_columns[1]],dict(j[item_list])]], columns=data1.columns), data1], ignore_index=True)
      
    final_df = data1.groupby([table_columns[0],table_columns[1]])[table_columns[2]].apply(list).reset_index()
    
    records = []
    
    for i,j in final_df.iterrows():
      records.append(j.to_dict())
      
    print(records)
    
    ddb_data = json.loads(json.dumps(records), parse_float=Decimal)
    
    print(ddb_data)
    
    table = get_dynamodb_resource().Table("dev-ofr-datahub-cms-clinical-study-consolidation")
    
    for x in ddb_data:
      study_code = x['study_code']
      view = x['view']
      data = x['data']
      
      table.put_item(
        Item={
          'studycode' : study_code,
          'viewID#vendorshortname' : view,
          'data': data
        }
      )