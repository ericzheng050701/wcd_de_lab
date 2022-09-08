import json
import csv
import boto3



def create_table():
    table_name = 'climate_data'
    client = boto3.client('dynamodb')
    response = client.list_tables()
    if table_name in response['TableNames']:
        print('table exists')
    else:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
                {
                    'AttributeName': 'year',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'
                }

            ],
            AttributeDefinitions=[
                 {
                    'AttributeName': 'year',
                    'AttributeType': 'S'
                },
                 {
                    'AttributeName': 'date',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5,
                }
            )
        
        # print("Table status:", table.table_status)
        
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print('table created')

def get_year():
    s3 = boto3.client('s3')
    bucket = 'my-bucket_name'
    key = 'config.json'
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body']
    config = json.loads(content.read())
    year = config['year']
    print(year)
    return year
    

def lambda_handler(event, context):
    create_table()
    year = get_year() 
    
    try:
        s3 = boto3.client('s3')
        dynamodb = boto3.client('dynamodb')
        table_name = 'climate_data'
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        if str(year) in key:
            print(bucket, key)
            csv_file = s3.get_object(Bucket=bucket,Key=key)
            record_list = csv_file['Body'].read().decode('utf-8').split('\n')
            csv_reader = csv.reader(record_list, delimiter=',',quotechar='"')
            next(csv_reader)
            for row in csv_reader:
                lon = row[3]
                lat = row[4]
                station_name = row[5]
                climateid = row[6]
                date = row[7]
                year = row[8]
                month = row[9]
                day = row[10]
                max_temp = row[12]
                min_temp = row[14]
                mean_temp = row[16]
                # print(lon,lat,station_name,climateid,date)
                add_to_db = dynamodb.put_item(TableName=table_name,Item = {
                    'lon':{'S': str(lon)},
                    'lat':{'S': str(lat)},
                    'station_name':{'S': str(station_name)},
                    'climateid':{'S': str(climateid)},
                    'date':{'S': str(date)},
                    'year':{'S': str(year)},
                    'month':{'S': str(month)},
                    'day':{'S': str(day)},
                    'max_temp':{'S': str(max_temp)},
                    'min_temp':{'S': str(min_temp)},
                    'mean_temp':{'S': str(mean_temp)}
                }) 

   
        else:
            print('The uploaded file is not the right year')
    
        

        
            
    except Exception as e:
        print(str(e))
    
    return {'statusCode':200, 'body':json.dumps('File uploaded to DynamoDB')}
