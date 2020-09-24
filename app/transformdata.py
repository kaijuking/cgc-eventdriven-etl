import pandas as pd


def transform_data(data_list):
  
    # JOHN HOPKINS
    # - Remove non-us data from john hopkins
    # - Only take recovery data

    # Date
    # - need to be converted to a date object

    # us.csv
    # date, cases, deaths
    # 2020-01-21, 1, 0

    # john hopkins.csv
    # Date, Country/Region, Province/State, Lat, Long, Confirmed, Recovered, Deaths
    # 2020-01-22, Afghanistan, , 33.93911, 67.709953, 0, 0, 0

    # date, cases, deaths, recovered

    # Data Sanitization
    # Lowercase all filters
    # Add underscore to any column name which has spaces
    # - https://www.geeksforgeeks.org/python-filtering-data-with-pandas-query-method/
    for item in data_list:
        item['filters'] = [f.lower() for f in item['filters']]
        item['data'].columns = [column.replace(" ", "_") for column in item['data'].columns]
        item['data'].columns = [column.lower() for column in item['data'].columns]

    # Filter each dataframe by a set of filters and regions
    for item in data_list:

        filters = ['adfs']#item['filters']

        if not item['regions']:
            try:
                item['data'] = item['data'].filter(items=filters)
                print(item['data'])
            except pd.errors as e:
                print(e)
                break
        else:
            # Only return data for the 'regions' provided
            regions = item['regions']
            item['data'] = item['data'].loc[item['data']['country/region'].isin(regions)]

    return "hello world"


# this means that if this script is executed, then 
# main() will be executed
if __name__ == '__main__':
    main()