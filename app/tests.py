import unittest
import pandas as pd
from datetime import date, datetime
from processdata import verify_urls_exist, verify_raw_data_columns, verify_raw_data_values
from transformdata import transform_data
          

class TestVerifyUrls(unittest.TestCase):

    def test_valid_urls(self):
        #print('Test - Verify Valid Urls Exist')
        url_list = [
            {
                'source': 'nyt', 
                'url': 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv',
                'statusCode': 200
            },
            {
                'source': 'jh', 
                'url': 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv',
                'statusCode': 200
            }
        ]
        
        for item in url_list:
            url = item['url']
            response = verify_urls_exist(url)
            actual_status_code = response.status_code
            expected_status_code = item['statusCode']
            self.assertEqual(actual_status_code, expected_status_code)

    def test_invalid_urls(self):
        #print('Test - Verify Invalid Urls Raise System Exit')
        url_list = [
            {
                'source': 'nyt_fake', 
                'url': 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us_fake.csv',
                'statusCode': 404
            },
            {
                'source': 'jh_fake', 
                'url': 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined_fake.csv',
                'statusCode': 404
            }
        ]
        
        for item in url_list:
            with self.assertRaises(SystemExit) as cm:
                url = item['url']
                verify_urls_exist(url)
            self.assertEqual(cm.exception.code, 1)


class TestVerifyRawDataSchema(unittest.TestCase):

    def test_raw_data_valid_column_names(self):

        # Represent NYT data
        ds1 = { 'date': ['2020-10-05'], 'cases': [1], 'deaths': [1], 'other': [0] }

        # Represents John Hopkins data
        ds2 = { 'date': ['2020-10-05'], 'country/region': ['US'], 'recovered': [1], 'other': [0] }

        df1 = pd.DataFrame(data=ds1)
        df2 = pd.DataFrame(data=ds2)

        data_list = [df1, df2]

        for item in data_list:
            actual_result = verify_raw_data_columns(item)
            self.assertEqual(actual_result, True)

    def test_raw_data_unexpected_column_names(self):
        nyt_ds1 = { 'date!': ['2020-10-05'], 'cases': [1], 'deaths': [1] }
        nyt_ds2 = { ' date ': ['2020-10-05'], ' cases ': [1], ' deaths ': [1] }
        nyt_ds3 = { 'date': ['2020-10-05'], 'cases': [1], 'columndoesnotexist': [1] }

        jh_ds1 = { 'date!': ['2020-10-05'], 'country/region': ['US'], 'recovered': [1], 'other': [0] }
        jh_ds2 = { '  date  ': ['2020-10-05'], '  country/region  ': ['US'], '  recovered  ': [1], 'other': [0] }
        jh_ds3 = { 'columndoesnotexist': ['2020-10-05'], 'country/region': ['US'], 'recovered': [1], 'other': [0] }

        other_ds1 = { 'TEST1': ['2020-10-05'], 'TEST2': [1], 'TEST3': [1] }
        other_ds2 = { 'date': ['2020-10-05']}

        df1 = pd.DataFrame(data=nyt_ds1)
        df2 = pd.DataFrame(data=nyt_ds2)
        df3 = pd.DataFrame(data=nyt_ds3)
        df4 = pd.DataFrame(data=jh_ds1)
        df5 = pd.DataFrame(data=jh_ds2)
        df6 = pd.DataFrame(data=jh_ds3)
        df7 = pd.DataFrame(data=other_ds1)
        df8 = pd.DataFrame(data=other_ds2)

        data_list = [df1, df2, df3, df4, df5, df6, df7, df8]

        for item in data_list:
            with self.assertRaises(SystemExit) as cm:
                result = verify_raw_data_columns(item)
                expected = "Expected columns in raw data not found."
            self.assertEqual(cm.exception.code, 1)

    def test_raw_data_unexpected_values(self):
        # Date, Cases, Deaths, Recovered = None
        ds1 = { 'date': [None], 'cases': [1], 'deaths': [1], 'test': ['ds1']}
        ds2 = { 'date': ['2020-10-05'], 'cases': [None], 'deaths': [1], 'test': ['ds2']}
        ds3 = { 'date': ['2020-10-05'], 'cases': [1], 'deaths': [None], 'test': ['ds3']}
        ds4 = { 'date': ['2020-10-05'], 'country/region': ['US'], 'recovered': [None], 'test': ['ds4']}

        # Country/Region = Various non-'US' values
        ds5 = { 'date': ['2020-10-05'], 'country/region': ['United States of America'], 'recovered': [1], 'other': [0], 'test': ['ds5']}
        ds6 = { 'date': ['2020-10-05'], 'country/region': ['UAA'], 'recovered': [1], 'other': [0], 'test': ['ds6']}
        ds7 = { 'date': ['2020-10-05'], 'country/region': ['USAA'], 'recovered': [1], 'other': [0], 'test': ['ds7']}
        ds8 = { 'date': ['2020-10-05'], 'country/region': ['us'], 'recovered': [1], 'other': [0], 'test': ['ds8']}
        ds9 = { 'date': ['2020-10-05'], 'country/region': [None], 'recovered': [1], 'other': [0], 'test': ['ds9']}

        # Date = int
        ds10 = { 'date': [1], 'cases': [1], 'deaths': [1], 'test': ['ds10']}

        # Cases, Deaths, Recovered = str
        ds11 = { 'date': ['2020-10-05'], 'cases': ['1'], 'deaths': [1], 'test': ['ds11']}
        ds12 = { 'date': ['2020-10-05'], 'cases': [1], 'deaths': ['1'], 'test': ['ds12']}
        ds13 = { 'date': ['2020-10-05'], 'country/region': ['US'], 'recovered': ['1'], 'test': ['ds13']}

        df1 = pd.DataFrame(data=ds1)
        df2 = pd.DataFrame(data=ds2)
        df3 = pd.DataFrame(data=ds3)
        df4 = pd.DataFrame(data=ds4)
        df5 = pd.DataFrame(data=ds5)
        df6 = pd.DataFrame(data=ds6)
        df7 = pd.DataFrame(data=ds7)
        df8 = pd.DataFrame(data=ds8)
        df9 = pd.DataFrame(data=ds9)
        df10 = pd.DataFrame(data=ds10)
        df11 = pd.DataFrame(data=ds11)
        df12 = pd.DataFrame(data=ds12)
        df13 = pd.DataFrame(data=ds13)

        data_list = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13]

        for item in data_list:
            with self.assertRaises(SystemExit) as cm:
                print(item)
                verify_raw_data_values(item)
            self.assertEqual(cm.exception.code, 1)
            

class TestTransformData(unittest.TestCase):

    def test_transform_data(self):

        # Represent NYT data
        ds1 = { 'date': ['2020-10-05'], 'cases': [1], 'deaths': [1] }

        # Represents John Hopkins data
        ds2 = { 'date': ['2020-10-05'], 'country/region': ['US'], 'recovered': [1], 'other': [0] }

        # Represents the expected final transformed data
        ds3 = { 'date': ['2020-10-05'], 'cases': [1], 'deaths': [1], 'recovered': [1] }

        # Create and format the dataframes
        df1 = pd.DataFrame(data=ds1)
        df2 = pd.DataFrame(data=ds2)
        expected_dataframe = pd.DataFrame(data=ds3)
        expected_dataframe['date'] = pd.to_datetime(expected_dataframe['date'], format='%Y-%m-%d').dt.date

        data_list = [
            { 'source': 'nyt', 'data': df1 },
            { 'source': 'jh', 'data': df2 }
        ]

        actual_dataframe = transform_data(data_list)
        pd.util.testing.assert_frame_equal(actual_dataframe, expected_dataframe)


if __name__ == '__main__':
    unittest.main()