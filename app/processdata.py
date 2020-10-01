import os
import sys
import boto3
import requests
import pandas as pd
from transformdata import transform_data


# Lambda function which is invoked by a scheduled (once a day) CloudWatch event
def lambda_handler_process_data(event, context):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

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
    #upload_data_to_database(transformed_data)


# Get the urls
def get_urls():
    current_function = get_name_of_current_function()
    print(f'Inside Function = {current_function}')

    nyt_url = os.environ['nytimes']
    jh_url = os.environ['jhopkins']
    urls = [
        {'source': 'nyt', 'url': nyt_url, 'data': None}, 
        {'source': 'jh', 'url': jh_url, 'data': None}
    ]
    return urls


# Verify the urls exist
def verify_urls_exist(url_list: list):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    # Loop through each item in the url_list
    for item in url_list:
        url = item['url']
        #source = item['source']

        # Make a GET request for each url
        try:
            response = requests.get(url)
            if not response.status_code == 200:
                error = f'Unable to get resource from url. Response Code = {response.status_code}.'
                process_error(function_name, error)
        except requests.exceptions.RequestException as e:
            process_error(function_name, e)


# Get the actual raw data
def get_raw_data(url_list: list):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    iteration = 0
    for item in url_list:

        url = item['url']

        try:
            # Download the CSV content using Pandas
            raw_data = pd.read_csv(url, error_bad_lines=False, low_memory=False)

            # Add the dataframe to the data list
            item['data'] = raw_data
        except Exception as e:
            process_error(function_name, e)

    return url_list


# Verify the schema of the raw data
def verify_raw_data_schema(data_list: list):
    function_name = get_name_of_current_function()
    
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

            except Exception as e:
                process_error(function_name, e)
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

            except Exception as e:
                process_error(function_name, e)
        else:
            error = f'Unexpected issue with the raw csv data schema.'
            process_error(function_name, error)

     
# Transform the data
def transform_raw_data(data_list: list):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    # Transform the data
    try:
        return transform_data(data_list)
    except Exception as e:
        process_error(function_name, e)


# Send the data to the database
def upload_data_to_database(data):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    #dynamodb_client = boto3.client('dynamodb')
    #print(transformed_data)
    for d in data.index:
        print(data['date'][d], data['cases'][d], data['deaths'][d], data['recovered'][d])


# Function to handle the printing of error messages
def process_error(name_of_function, actual_error):
    if not actual_error:
        error = f'An error has occurred inside function "{name_of_function}".'
        print(error)
    else:
        error = f'An error has occurred inside function "{name_of_function}".\nError = {actual_error}'
        print(error)
    # TODO - Send a SNS message with error details
    sys.exit(1)


def send_sns_notification(message: str):
    print(message)

# Function which returns the name of the current function
def get_name_of_current_function():
    return sys._getframe(1).f_code.co_name


#if __name__ == "__main__":
    #lambda_function_process_data()