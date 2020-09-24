import sys
import uuid
import json
import pandas as pd
import requests
import boto3
from transformdata import transform_data


# Lambda function which is invoked by a scheduled (once a day) CloudWatch event
def lambda_function_process_data():
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    # Get the json data from s3 containing the urls for processing.
    urls = get_data_from_s3()

    # Verify the url exists before attempting to download the raw data.
    verify_url_exists(urls)

    # Get the raw data provided from each url
    raw_data = get_actual_raw_data_from_url(urls)
    
    # Transform the data
    transform_dataset(raw_data)


# Get the JSON file containing the urls from s3
def get_data_from_s3():
    current_function = get_name_of_current_function()
    print(f'Inside Function = {current_function}')

    #key = os.environ['FILE_NAME']
    #bucket_name = os.environ['BUCKET_NAME']

    # Get the data from S3
    #try:
        #s3_resource = boto3.Object('s3')
        #s3_object = s3_resource.Object(bucket_name, key)
        #data = s3_object.get()['Body'].read().decode('utf-8')
        #json_data = json.loads(data)
        #return json_data
    #except botocore.exceptions.ClientError as error:
        #process_error(name_of_function, error)

    data = [
                { 
                    'data': None,
                    'source': 'NY Times', 
                    'url': 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv',
                    'filters': ['date', 'cases', 'deaths'],
                    'regions': None
                },
                { 
                    'data': None,
                    'source': 'John Hopkins', 
                    'url': 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv',
                    'filters': ['Date', 'Recovered'],
                    'regions': ['US']
                }
            ]
    return data


# Get the actual raw data
def get_actual_raw_data_from_url(url_list: list):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    iteration = 0
    for item in url_list:

        url = item['url']
        source = item['source']
        iteration += 1

        try:
            # Download the CSV content using Pandas
            print(f'Attempting to download raw data for Item #{iteration}.')
            data_file = pd.read_csv(url, error_bad_lines=False)

            # Add the dataframe to the data list
            item['data'] = data_file
            print(f'Raw data for Item #{iteration} was downloaded.')
        except pandas.errors as e:
            process_error(function_name, e)

    return url_list


# Verify the urls exist
def verify_url_exists(url_list: list):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    # Loop through each item in the url_list
    for item in url_list:
        url = item['url']
        source = item['source']

        # Make a GET request for each url
        # Application should stop (and send an SNS message) if any url returns a non-200 status.
        try:
            response = requests.get(url)
            if not response.status_code == 200:
                error = f'Unable to get resource from url. Response Code = {response.status_code}.'
                process_error(function_name, error)
        except requests.exceptions.RequestException as e:
            process_error(function_name, e)


# Transform the data
def transform_dataset(data_list: list):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    # Transform the data
    transformed_data = transform_data(data_list)
    print(transformed_data)


# Send the data to the database
def send_data_to_database():
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')


# Function to handle the printing of error messages
def process_error(name_of_function, actual_error):
    error = f'An error has occurred inside function "{name_of_function}".\n{actual_error}'
    print(error)
    # TODO - Send a SNS message with error details
    sys.exit(1)


# Function which returns the name of the current function
def get_name_of_current_function():
    return sys._getframe(1).f_code.co_name


if __name__ == "__main__":
    lambda_function_process_data()