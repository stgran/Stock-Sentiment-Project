import os
import requests
from dotenv import load_dotenv
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
url = 'https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol=ETH&market=USD&interval=1min&apikey=' + ALPHAVANTAGE_API_KEY
request_result = requests.get(url).json() # Return the request as a json object
# Alphavantage return metadata and the actual data. We only want the actual data.
json_data = request_result['Time Series Crypto (1min)']
# Convert the json dict to a Pandas dataframe
data = pd.DataFrame.from_dict(json_data, orient='index')

# When the data arrives, the datetime is the index column.
# We want the datetime to be its own non-index column.
data.reset_index(inplace=True)
data.rename(columns={"index": "date"}, inplace=True)

print(data.head())

data.sort_values(by='date', ascending=True, inplace=True)

# Get cutoff date
mysql_query = '''
SELECT date
FROM stock_sentiment_project.ethereum_stock
ORDER BY `date` desc
LIMIT 1;
'''

cutoff_date_df = pd.read_sql_query(mysql_query, engine)

cutoff_date = cutoff_date_df['date'].iloc[0]

# We want to filter out data we already have.
data = data[data['date'] > cutoff_date]

# Append our results to the existing SQL database
data.to_sql(
    name = 'ethereum_stock',
    con=engine,
    index=False,
    if_exists='append'
)