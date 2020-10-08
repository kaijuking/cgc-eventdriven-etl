import pandas as pd


def transform_data(data_to_transform):
  
    # Filter each dataframe
    for item in data_to_transform:

        # lower case columns
        item['data'].columns = [column.lower() for column in item['data'].columns]

        # Convert the date column to a date object
        df = item['data']
        
        #item['data']['date'] = pd.to_datetime(df['date']).dt.date
        item['data']['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.date
        
        # Filter data
        if item['source'] == 'jh':
            # Get only the 'US' data
            item['data'] = item['data'].loc[item['data']['country/region'] == 'US']

            # Get only the 'date' and 'recovered' data
            item['data'] = item['data'][['date', 'recovered']]

            # Set 'recovered' to int64 just in case
            item['data']['recovered'].fillna(0.0).astype('int64')
            
            jh_data = item['data']
        elif item['source'] == 'nyt':
            nyt_data = item['data']

    final_dataset = pd.merge(nyt_data, jh_data, how='right', on=['date'])

    return final_dataset