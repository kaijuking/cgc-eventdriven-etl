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

    # Check database for last successful data upload to the database
    # TODO - return a string for 'last_upload_date' needed in transform_dataset

    # Get the urls
    urls = get_urls()

    # Verify the url exists before attempting to download the raw data.
    verify_url_exists(urls)

    # Get the raw data provided from each url
    raw_data = get_actual_raw_data_from_url(urls)

    # Verify the raw data format
    verify_raw_data_schema(raw_data)

    # Transform the data
    last_upload_date = 'test'
    transform_validated_raw_data(raw_data, last_upload_date)

    # Upload transformed data to the database
    #upload_transformed_data_to_database(transformed_data)


# Get the urls
def get_urls():
    current_function = get_name_of_current_function()
    print(f'Inside Function = {current_function}')

    data = [
                { 
                    'data': None,
                    'source': 'NY Times', 
                    'url': 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv',
                    'filters': ['date', 'cases', 'deaths'],
                    'columns': ['date', 'cases', 'deaths'],
                    'regions': None
                },
                { 
                    'data': None,
                    'source': 'John Hopkins', 
                    'url': 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv',
                    'filters': ['date', 'recovered'],
                    'columns': ['date', 'country/region', 'recovered'],
                    'regions': ['US']
                }
            ]
    return data


# Verify the urls exist
def verify_url_exists(url_list: list):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    # Loop through each item in the url_list
    for item in url_list:
        url = item['url']
        source = item['source']

        # Make a GET request for each url
        try:
            response = requests.get(url)
            if not response.status_code == 200:
                error = f'Unable to get resource from url. Response Code = {response.status_code}.'
                process_error(function_name, error)
        except requests.exceptions.RequestException as e:
            process_error(function_name, e)


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
            data_file = pd.read_csv(url, error_bad_lines=False, low_memory=False)

            # Add the dataframe to the data list
            item['data'] = data_file
            print(f'Raw data for Item #{iteration} was downloaded.')

        except pandas.errors as e:
            process_error(function_name, e)

    return url_list


# Verify the schema of the raw data
def verify_raw_data_schema(data_list: list):
    function_name = get_name_of_current_function()
    
    for item in data_list:
        item['data'].columns = [column.lower() for column in item['data'].columns]
        actual_columns = item['data'].columns
        
        expected_columns = item['columns']
        data_source = item['source']

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
            error = f'CSV Column mismatch. Expected = {expected_columns}. Actual = {actual_columns}'
            process_error(function_name, error)

     
# Transform the data
def transform_validated_raw_data(data_list: list, last_upload_date):
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')

    # Transform the data
    try:
        transformed_data = transform_data(data_list, last_upload_date)
        #print(transformed_data)
    except Exception as e:
        process_error(function_name, e)


# Send the data to the database
def send_data_to_database():
    function_name = get_name_of_current_function()
    print(f'Inside Function = {function_name}')


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


# Function which returns the name of the current function
def get_name_of_current_function():
    return sys._getframe(1).f_code.co_name


if __name__ == "__main__":
    lambda_function_process_data()