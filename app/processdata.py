import os
import sys
import boto3
import requests
import pandas as pd
from datetime import date, datetime
from transformdata import transform_data
from io import StringIO


# Lambda function which is invoked by a scheduled (once a day) CloudWatch event
def lambda_handler_process_data(event, context):

    # Get the urls
    urls = get_urls()

    # Verify the url exists
    verify_urls_exist(urls)

    # Get the raw data
    raw_data = get_raw_data(urls)

    # Verify the raw data format
    verify_raw_data_schema(raw_data)

    # Transform the data
    transformed_data = transform_raw_data(raw_data)

    # Upload transformed data to the database
    upload_data_to_database(transformed_data)

    upload_data_to_s3(transformed_data)



# Get the urls
def get_urls():

    nyt_url = os.environ['nytimes']
    jh_url = os.environ['jhopkins']
    urls = [
        {'source': 'nyt', 'url': nyt_url, 'data': None}, 
        {'source': 'jh', 'url': jh_url, 'data': None}
    ]
    return urls


# Verify the urls exist
def verify_urls_exist(url_list: list):

    # Loop through each item in the url_list
    for item in url_list:
        url = item['url']
        #source = item['source']

        # Make a GET request for each url
        try:
            response = requests.get(url)
            if not response.status_code == 200:
                message = f'Unable to get resource from url. Response Code = {response.status_code}.'
                send_sns_notification(message)
        except Exception as error:
            message = f'{error}'
            send_sns_notification(message)


# Get the actual raw data
def get_raw_data(url_list: list):

    iteration = 0
    for item in url_list:

        url = item['url']

        try:
            # Download the CSV content using Pandas
            raw_data = pd.read_csv(url, error_bad_lines=False, low_memory=False)

            # Add the dataframe to the data list
            item['data'] = raw_data
        except Exception as error:
            message = f'{error}'
            send_sns_notification(message)

    return url_list


# Verify the schema of the raw data
def verify_raw_data_schema(data_list: list):
    
    for item in data_list:
        item['data'].columns = [column.lower() for column in item['data'].columns]
        actual_columns = item['data'].columns
        
        # Check the format of the data
        if set(['date', 'cases', 'deaths']).issubset(actual_columns):
            try:

                # Verify the 'date' column
                pd.notnull(item['data']['date'])
                pd.to_datetime(item['data']['date'], format='%Y-%m-%d', errors='raise').notnull().all()

                # Verify the 'cases' and 'deaths' columns
                pd.notnull(item['data']['cases'])
                pd.notnull(item['data']['deaths'])
                pd.to_numeric(item['data']['cases'], downcast='integer', errors='raise').notnull().all()
                pd.to_numeric(item['data']['deaths'], downcast='integer', errors='raise').notnull().all()

            except Exception as error:
                message = f'{error}'
                send_sns_notification(message)
        elif set(['date', 'country/region', 'recovered']).issubset(actual_columns):
            try:

                # Verify the 'date' column
                pd.notnull(item['data']['date'])
                pd.to_datetime(item['data']['date'], format='%Y-%m-%d', errors='raise').notnull().all()

                # Verify the 'recovered' column
                pd.notnull(item['data']['recovered'])
                pd.to_numeric(item['data']['recovered'], downcast='integer', errors='raise').notnull().all()

                # Verify the 'country/region' column
                pd.notnull(item['data']['country/region'])
                pd.Series(item['data']['country/region']).str.isalpha()

            except Exception as error:
                message = f'{error}'
                send_sns_notification(message)
        else:
            message = 'Unexpected issue with the raw csv data schema.'
            send_sns_notification(message)

     
# Transform the data
def transform_raw_data(data_list: list):

    # Transform the data
    try:
        return transform_data(data_list)
    except Exception as error:
        message = f'{error}'
        send_sns_notification(message)


# Send the data to the database
def upload_data_to_database(data):
    
    dynamodb_client = boto3.client('dynamodb')
    table_name = os.environ['dbtablename']
    
    # Get total items in table
    response = dynamodb_client.scan(TableName=table_name)
    total_items = response['Count']
    
    # This is for first time data load
    if total_items == 0:
        try:
            for d in data.index:
                case_date = data['date'][d]
                total_cases = data['cases'][d]
                total_deaths = data['deaths'][d]
                total_recovered = data['recovered'][d]

                new_db_item = {
                    'reportdate': {'S': f'{case_date}'},
                    'cases': {'N': f'{total_cases}'},
                    'deaths': {'N': f'{total_deaths}'},
                    'recovered': {'N': f'{total_recovered}'},
                    'countryname': {'S': 'US'}
                }

                dynamodb_client.put_item(TableName=table_name, Item=new_db_item)
        except Exception as error:
            message = f'{error}'
            send_sns_notification(message)
        
        num_items = len(data.index)
        message = f'Covid19 Dataset updated. Total new items added to dataset = {num_items}'
        send_sns_notification(message)
    else:
        # Query the database to get the last most recent item date
        try: 
            today = date.today()
            d1 = today.strftime("%Y/%m/%d")
            exp_attributes = {':cn': {'S': 'US'}}
            key_cond_exp = "countryname = :cn"
            query_result = dynamodb_client.query(
                TableName=table_name,
                Limit=1,
                ExpressionAttributeValues=exp_attributes,
                KeyConditionExpression=key_cond_exp,
                ScanIndexForward=False
            )

            # Convert to date object to filter out data from the dataframe
            datetime_str = query_result['Items'][0]['reportdate']['S']
            datetime_object = datetime.strptime(datetime_str, '%Y-%m-%d').date()
            df_update = data[data['date'] > datetime_object ]

            # Update the database with the new data
            if not df_update.size == 0:
                for d in df_update.index:
                    case_date = data['date'][d]
                    total_cases = data['cases'][d]
                    total_deaths = data['deaths'][d]
                    total_recovered = data['recovered'][d]

                    new_db_item = {
                        'reportdate': {'S': f'{case_date}'},
                        'cases': {'N': f'{total_cases}'},
                        'deaths': {'N': f'{total_deaths}'},
                        'recovered': {'N': f'{total_recovered}'},
                        'countryname': {'S': 'US'}
                    }

                    dynamodb_client.put_item(TableName=table_name, Item=new_db_item)
            else: 
                message = 'Attempted to update databse but there were no items to add.'
                send_sns_notification(message)
        except Exception as error:
            message = f'{error}'
            send_sns_notification(message)
        
        num_items = len(df_update.index)
        message = f'Covid19 Dataset updated. Total new items added to dataset = {num_items}'
        send_sns_notification(message)



# Save data to S3
def upload_data_to_s3(data_list: list):
    print('attempting upload_data_to_s3')
    s3_resource = boto3.resource('s3')
    #bucket = s3.Bucket('S3BucketCovid19Data')
    #key = 'uscovid19data.csv'
    #objs = list(bucket.objects.filter(Prefix=key))

    #df = data_list
    #csv_data = df.to_csv('uscovid19data.csv', index = True)
    #print(csv_data)

    try:
        csv_buffer = StringIO()

        # Write dataframe to buffer
        data_list.to_csv(csv_buffer, index=False)

        # Create S3 object
        s3_resource = boto3.resource("s3")

        # Write buffer to S3 object
        s3_resource.Object('s3bucketcovid19data', 'uscovid19data.csv').put(Body=csv_buffer.getvalue())

        #response = s3_client.upload_file(file_name, bucket, object_name)
        #response = s3.meta.client.upload_file('/tmp/uscovid19data.csv', 'S3BucketCovid19Data', 'uscovid19data.csv')
        #s3_client = boto3.client('s3')
        #s3_client.upload_file('tmp/'+csv_data, "s3bucketcovid19data", "uscovid19data.csv")
        #with open(csv_data, "rb") as f:
            #s3_client.upload_fileobj(f, "s3bucketcovid19data", "uscovid19data.csv")
    except Exceptin as error:
        message = "unable to upload file to s3"
        send_sns_notification(message)


def send_sns_notification(message: str):
    sns = boto3.client('sns')
    sns_topic_arn = os.environ['snstopic']
    sns.publish(TopicArn = sns_topic_arn, Message=message)

