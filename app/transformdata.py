import pandas as pd
import numpy as np


def transform_data(data_to_transform):
  
    # Filter each dataframe
    for item in data_to_transform:

        # Convert the date column to a date object
        df = item['data']
        #item['data']['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        item['data']['date'] = pd.to_datetime(df['date']).dt.date
        
        # Filter data
        if item['source'] == 'jh':
            print(item['data'].head())
            print(item['data'].dtypes)
            # Get only the 'US' data
            item['data'] = item['data'].loc[item['data']['country/region'].isin(['US'])]
            item['data'] = item['data'][['date', 'recovered']]
            item['data']['recovered'].fillna(0.0).astype('int64')
            jh_data = item['data']
        elif item['source'] == 'nyt':
            nyt_data = item['data']

    print(jh_data)
    final_dataset = pd.merge(nyt_data, jh_data, how='right', on=['date'])
    print(jh_data.head())
    print(jh_data.dtypes)
    #print('-----------------')
    #final_dataset['recovered'].astype('int64').dtypes
    #print(final_dataset.head())
    #print(final_dataset.dtypes)
    return final_dataset


# this means that if this script is executed, then 
# main() will be executed
#if __name__ == '__main__':
    #main() 