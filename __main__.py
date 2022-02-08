from twitter_scraper_class import TwitterScraper
from alphavantage_scraper_class import AlphaVantageScraper
import json
import sqlalchemy
import os
import time

from dotenv import load_dotenv
load_dotenv()

def connect_to_db():
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

def main():
    # Get our query info for each company
    with open('query_info.json') as f:
        query_info = json.load(f)

    # Connect to our database
    engine = connect_to_db()

    # Iterate over the companies
    for company in query_info:
        print(company) # Print the name of the company
        
        query_terms = query_info[company]['query_terms'] # Search terms
        tweet_table = query_info[company]['tweet_table'] # Destination table
        # Import and run our Twitter scraper
        twitter_scraper = TwitterScraper(query_terms=query_terms, db_table=tweet_table, use_since_id=True)
        twitter_results = twitter_scraper.run()

        symbol = query_info[company]['symbol'] # Stock symbol
        stock_table = query_info[company]['stock_table'] # Destination table
        # Import and run our AlphaVantage scraper
        if company in ['Bitcoin', 'Ethereum', 'Polkadot']:
            stock_scraper = AlphaVantageScraper(db_table=stock_table, symbol=symbol, endpoint='CRYPTO_INTRADAY')
        else:
            stock_scraper = AlphaVantageScraper(db_table=stock_table, symbol=symbol, endpoint='TIME_SERIES_INTRADAY')
        stock_results = stock_scraper.run()

        # Send the Twitter results to the respective table in the db
        twitter_results.to_sql(
            name=tweet_table,
            con=engine,
            index=False,
            if_exists='append'
        )

        # Send the stock results to the respective table in the db
        stock_results.to_sql(
            name=stock_table,
            con=engine,
            index=False,
            if_exists='append'
        )

    # AlphaVantage's API limits us to 5 requests per minute so we sleep for 21
    # seconds between companies to ensure we don't hit this limit
    time.sleep(21)

    return 0

if __name__ == '__main__':
    main()