import os
import requests
import pandas as pd
import sqlalchemy

from dotenv import load_dotenv
load_dotenv()

class AlphaVantageScraper():
    '''
    Methods
        - connect_to_db(self)
        - query_crypto(self)
        - query_stock(self)
        - process_results(self, json_data)
        - get_cutoff_date(self)
        - run(self)
    '''

    def __init__(self, db_table, symbol, endpoint='CRYPTO_INTRADAY', market='USD', interval='1min'):
        # Connect to our SQL database
        self.db_table = db_table
        self.engine = self.connect_to_db()

        # Set the AlphaVantage endpoint to be queried
        self.endpoint = endpoint
        # Set the symbol for the cryptocurrency whose price we want to query
        self.symbol = symbol
        # Set market to query
        self.market = market
        # Set interval of the price breakdown
        self.interval = interval
    
    def connect_to_db(self):
        '''
        Function to connect to the database used to store results.
        This code is run with both MySQL and MariaDB databases, which are
        functionally the same, but require slightly different connection strings.
        '''
        # Getting SQL database credentials
        mysql_user = os.getenv('MYSQL_USER')
        mysql_pwd = os.getenv('MYSQL_PWD')
        mysql_host = os.getenv('MYSQL_HOST')
        mysql_db = os.getenv('MYSQL_DB')

        # Setting up connection to SQL database
        # I have set this up to handle either mariadb or mysql because I run this on two
        # different computers which use these different SQL databases.
        try:
            engine_str = f'mariadb+mariadbconnector://{mysql_user}:{mysql_pwd}@{mysql_host}/{mysql_db}'
            engine = sqlalchemy.create_engine(engine_str)
            print('Using mariadb database')
        except:
            engine_str = f'mysql+pymysql://{mysql_user}:{mysql_pwd}@{mysql_host}/{mysql_db}'
            engine = sqlalchemy.create_engine(engine_str)
            print('Using mysql database')
        
        return engine
    
    def query_crypto(self):
        # Getting my Alpha Vantage API key
        ALPHAVANTAGE_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')

        # Pulling stock data using the API
        url = f'https://www.alphavantage.co/query?function={self.endpoint}&symbol={self.symbol}&market={self.market}&interval={self.interval}&outputsize=full&apikey={ALPHAVANTAGE_API_KEY}'
        request_result = requests.get(url).json() # Return the request as a json object
        # Alphavantage return metadata and the actual data. We only want the actual data.
        
        json_data = request_result[f'Time Series Crypto ({self.interval})']
        
        return json_data
    
    def query_stock(self):
        # Getting my Alpha Vantage API key
        ALPHAVANTAGE_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')

        # Pulling stock data using the API
        url = f'https://www.alphavantage.co/query?function={self.endpoint}&symbol={self.symbol}&interval={self.interval}&outputsize=full&apikey={ALPHAVANTAGE_API_KEY}'
        request_result = requests.get(url).json() # Return the request as a json object
        # Alphavantage return metadata and the actual data. We only want the actual data.
        
        json_data = request_result[f'Time Series Intraday ({self.interval})']
        
        return json_data
    
    def process_results(self, json_data):
        # Convert the json dict to a Pandas dataframe
        data = pd.DataFrame.from_dict(json_data, orient='index')

        # When the data arrives, the datetime is the index column.
        # We want the datetime to be its own non-index column.
        data.reset_index(inplace=True)
        data.rename(columns={"index": "date"}, inplace=True)

        # Sort by date from oldest to newest
        data.sort_values(by='date', ascending=True, inplace=True)

        # Get the latest date of price data in the db
        cutoff_date = self.get_cutoff_date()
        # We want to filter out data we already have.
        data = data[data['date'] > cutoff_date]

        return data

    def get_cutoff_date(self):
        # Get cutoff date
        mysql_query = f'''
        SELECT date
        FROM stock_sentiment_project.{self.db_table}
        ORDER BY `date` desc
        LIMIT 1;
        '''

        cutoff_date_df = pd.read_sql_query(mysql_query, self.engine)

        cutoff_date = cutoff_date_df['date'].iloc[0]

        return cutoff_date
    
    def run(self):
        results_json = self.query_alphavantage()
        if self.endpoint == 'CRYPTO_INTRADAY':
            results_json = self.query_crypto()
        elif self.endpoint == 'TIME_SERIES_INTRADAY':
            results_json = self.query_stock()
        else:
            print('ERROR: unrecognized endpoint')
            return
        
        results_df = self.process_results(results_json)

        return results_df