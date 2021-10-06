import os
from dotenv import load_dotenv
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import sqlalchemy

'''
Alphavantage's API offers 5 calls/minute or 500/day.

How frequently should I be checking stock price?
'''

# Loading my local environment
load_dotenv()

# Getting my Alpha Vantage API key
ALPHAVANTAGE_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')

# Getting SQL database credentials
mysql_user = os.getenv('MYSQL_USER')
mysql_pwd = os.getenv('MYSQL_PWD')
mysql_host = os.getenv('MYSQL_HOST')
mysql_db = os.getenv('MYSQL_DB')

# Setting up connection to SQL database
engine_str = 'mariadb+mariadbconnector://' + mysql_user + ':' + mysql_pwd + '@' + mysql_host  + '/' + mysql_db
engine = sqlalchemy.create_engine(engine_str)

# Pulling stock data using the API
ts = TimeSeries(key=ALPHAVANTAGE_API_KEY, output_format='pandas', indexing_type='date')

data, meta_data = ts.get_intraday(symbol='pltr', interval='1min', outputsize='full')

# When the data arrives, the datetime is the index column.
# We want the datetime to be its own non-index column.
data.reset_index(inplace=True)

data.sort_values(by='date', ascending=True, inplace=True)

# Get cutoff date
mysql_query = '''
SELECT date
FROM stock_sentiment_project.palantir_stock
ORDER BY `date` desc
LIMIT 1;
'''

cutoff_date_df = pd.read_sql_query(mysql_query, engine)

cutoff_date = cutoff_date_df['date'].iloc[0]

# We want to filter out data we already have.
data = data[data['date'] > cutoff_date]

# Append our results to the existing SQL database
data.to_sql(
    name = 'palantir_stock',
    con=engine,
    index=False,
    if_exists='append'
)