import json
import boto3
import os
import sys
import pandas as pd
from boto3.dynamodb.conditions import Key



def lambda_handler(event, context):

    year = event["year"]
    start_date = event["start_date"]
    end_date = event["end_date"]
    
    table_name = 'weather_data'
    client = boto3.resource('dynamodb')
    table = client.Table(table_name)

    response = table.query(
        KeyConditionExpression=Key('year').eq(year)&Key('date').between(start_date, end_date)
        )
    items = response['Items']
    item_list = []
    for item in items:
        item_list.append(item)
    df = pd.DataFrame(item_list)
    local_file = '/tmp/climate_data_selected_result.xlsx'
    df.to_excel(local_file)
    s3 = boto3.client('s3')
    bucket_name = 'my-bucket-name'
    s3_file = 'climate_data_selected_result.xlsx'
    s3.upload_file(local_file, bucket_name, s3_file)