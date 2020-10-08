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
    for item in urls:
        url = item['url']
        verify_urls_exist(url)

    # Get the raw data
    raw_data = get_raw_data(urls)

    # Verify the raw data contains the expected columns
    for item in raw_data:
        df = item['data']
        verify_raw_data_columns(df)

    for item in raw_data:
        df = item['data']
        verify_raw_data_values(df)

    # Transform the data
    transformed_data = transform_raw_data(raw_data)

    # Upload transformed data to the database
    upload_data_to_database(transformed_data)

    # Upload transformed data to S3 for AWS QuickSight
    upload_data_to_s3(transformed_data)



# Get the urls
def get_urls():

    print('getting urls')

    nyt_url = os.environ['nytimes']
    jh_url = os.environ['jhopkins']
    urls = [
        {'source': 'nyt', 'url': nyt_url, 'data': None}, 
        {'source': 'jh', 'url': jh_url, 'data': None}
    ]
    return urls


# Verify the urls exist
def verify_urls_exist(data_url):

    print('verifying url exist')

    try:
        response = requests.get(data_url)
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as error:
        print(f'Error checking if URL exists. Error = {error}')
        sys.exit(1)


# Get the actual raw data
def get_raw_data(url_list: list):

    print('getting raw data')

    for item in url_list:

        url = item['url']

        try:
            # Download the CSV content using Pandas
            raw_data = pd.read_csv(url, error_bad_lines=False, low_memory=False)

            # Add the dataframe to the data list
            item['data'] = raw_data
        except Exception as error:
            print(f'Unable to download the raw data. Error = {error}')
            sys.exit(1)

    return url_list


def verify_raw_data_columns(data_frame):
    print('verify_raw_data_columns')
    nyt_expected_columns = ['date', 'cases', 'deaths']
    jh_expected_columns = ['date', 'country/region', 'recovered']
    data_frame.columns = [column.lower() for column in data_frame.columns]

    if set(nyt_expected_columns).issubset(data_frame.columns):
        return True
    elif set(jh_expected_columns).issubset(data_frame.columns):
        return True
    else:
        message = 'Expected columns in raw data not found.'
        print('Expected columns in raw data not found.')
        sys.exit(1)


def verify_raw_data_values(data_frame):
    print('verify_raw_data_values')
    nyt_expected_columns = ['date', 'cases', 'deaths']
    jh_expected_columns = ['date', 'recovered', 'country/region']
    data_frame.columns = [column.lower() for column in data_frame.columns]

    if set(nyt_expected_columns).issubset(data_frame.columns):
        df = data_frame[nyt_expected_columns]
        if df.isnull().values.any():
            message = 'NY Times dataset is missing required data'
            print(message)
            sys.exit(1)
        elif not df['date'].map(type).all() == str:
            message = 'NY Times \'date\' contains unexpected datatype'
            print(message)
            sys.exit(1)
        elif not df['cases'].map(type).all() == int:
            message = 'NY Times \'cases\' contains unexpected datatype'
            print(message)
            sys.exit(1)
        elif not df['deaths'].map(type).all() == int:
            message = 'NY Times \'deaths\' contains unexpected datatype'
            print(message)
            sys.exit(1)

    elif set(jh_expected_columns).issubset(data_frame.columns):
        df = data_frame.loc[data_frame['country/region'] == 'US']
        df = df[jh_expected_columns]
        if len(df) == 0:
            message = 'John Hopkins dataset is missing data for country = US'
            print(message)
            sys.exit(1)
        elif df.isnull().values.any():
            message = 'John Hopkins dataset is missing required data'
            print(message)
            sys.exit(1)
        elif not df['date'].map(type).all() == str:
            message = 'John Hopkins \'date\' contains unexpected datatype'
            print(message)
            sys.exit(1)
        elif not df['recovered'].map(type).all() == float:
            message = 'John Hopkins \'recovered\' contains unexpected datatype'
            print(message)
            sys.exit(1)


# Transform the data
def transform_raw_data(data_list: list):

    print('transform raw data')

    # Transform the data
    try:
        return transform_data(data_list)
    except Exception as error:
        message = f'{error}'
        print(message)
        send_sns_notification(message)


# Send the data to the database
def upload_data_to_database(data):
    
    print('uploaded data to database')

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
        
        # Send sns message with number of updated items
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
        
        # Send sns message with number of updated items
        num_items = len(df_update.index)
        message = f'Covid19 Dataset updated. Total new items added to dataset = {num_items}'
        send_sns_notification(message)


# Save data to S3
def upload_data_to_s3(data_list: list):
    print('uploading data to s3')
    bucket_name = nyt_url = os.environ['s3bucketname']
    try:
        csv_buffer = StringIO()

        # Write dataframe to buffer
        data_list.to_csv(csv_buffer, index=False)

        # Create S3 object
        s3_resource = boto3.resource("s3")

        # Write buffer to S3 object
        s3_resource.Object(bucket_name, 'uscovid19data.csv').put(Body=csv_buffer.getvalue())

    except Exception as error:
        print(f'Error with uploading data to S3. Error: {error}')
        sys.exit(1)


def send_sns_notification(message: str):
    print('sending sns notification')
    sns = boto3.client('sns')
    sns_topic_arn = os.environ['snstopic']
    sns.publish(TopicArn = sns_topic_arn, Message=message)

