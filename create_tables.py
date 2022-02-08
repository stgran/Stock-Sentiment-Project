from curses import meta
from ipaddress import collapse_addresses
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Date, String, Float
import os
import json

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
        engine = create_engine(engine_str)
        print('Using mariadb database')
    except:
        engine_str = f'mysql+pymysql://{mysql_user}:{mysql_pwd}@{mysql_host}/{mysql_db}'
        engine = create_engine(engine_str)
        print('Using mysql database')
    
    return engine

def create_tweet_table(table_name):
    engine = connect_to_db()
    if not engine.dialect.has_table(engine, table_name):
        metadata = MetaData(engine)
        Table(
            table_name,
            metadata,
            Column('tweet_id', Integer, primary_key=True, nullable=False),
            Column('datetime', Date),
            Column('tweet_text', String),
            Column('polarity', Float),
            Column('sentiment', String),
            Column('author_id', Integer),
            Column('followers_count', Integer),
            Column('retweet_count', Integer),
            Column('like_count', Integer),
            Column('collection_time', Date),
            Column('original_tweet_id', Integer)
        )
        metadata.create_all()

def create_stock_table(table_name):
    engine = connect_to_db()
    if not engine.dialect.has_table(engine, table_name):
        metadata = MetaData(engine)
        Table(
            table_name,
            metadata,
            Column('date', Date, primary_key=True, nullable=False),
            Column('1. open', Float),
            Column('2. high', Float),
            Column('3. low', Float),
            Column('4. close', Float),
            Column('5. volumne', Integer)
        )

with open('query_info.json') as f:
    query_info = json.load(f)

for company in query_info:
    tweet_table_name = query_info[company]['tweet_table']
    stock_table_name = query_info[company]['stock_table']

    print(f'Creating Tweet table for {company}')
    create_tweet_table(tweet_table_name)

    print(f'Creating stock table for {company}')
    create_stock_table(stock_table_name)