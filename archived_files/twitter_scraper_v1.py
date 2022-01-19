from twitter import *
import pandas as pd
import sqlalchemy
import numpy as np
import time
import requests

import os
from dotenv import load_dotenv
load_dotenv()

from twitter_scraper_methods import *

# Getting API tokens
# API_TOKEN = os.getenv('TWITTER_API_TOKEN')
# API_TOKEN_SECRET = os.getenv('TWITTER_API_TOKEN_SECRET')
# API_CONSUMER_KEY = os.getenv('TWITTER_API_CONSUMER_KEY')
# API_CONSUMER_SECRET = os.getenv('TWITTER_API_CONSUMER_SECRET')
API_BEARER_TOKEN = os.getenv('TWITTER_API_BEARER_TOKEN')

# Getting SQL database credentials
mysql_user = os.getenv('MYSQL_USER')
mysql_pwd = os.getenv('MYSQL_PWD')
mysql_host = os.getenv('MYSQL_HOST')
mysql_db = os.getenv('MYSQL_DB')

# Setting up connection to SQL database
engine_str = 'mysql+pymysql://' + mysql_user + ':' + mysql_pwd + '@' + mysql_host  + '/' + mysql_db
engine = sqlalchemy.create_engine(engine_str)

# Define main Twitter scraping function
def get_twitter_results(query, since_id=None, max_id=None):
    '''
    Queries Twitter.
    Parameters:
        query: The search query. Generally, I will use simple company names such as 'apple'.
        since:
        until:
    '''
    query_results = t.search.tweets(
        q=query,
        lang="en",
        result_type='recent',
        count=100,
        since_id=since_id,
        max_id=max_id
        )
    
    collection_time = restructure_datetime(time.asctime( time.localtime(time.time()) ))

    query_results_df = pd.DataFrame(query_results['statuses'])
    query_results_df = query_results_df[['id_str', 'created_at', 'text', 'user', 'is_quote_status', 'retweet_count', 'favorite_count']]

    query_results_df['datetime_string'] = query_results_df['created_at'].apply(lambda row: restructure_datetime(row))
    query_results_df['datetime'] = pd.to_datetime(query_results_df['datetime_string'], format='%Y%b%d%H%M%S')

    query_results_df['screen_name'] = query_results_df['user'].apply(lambda user: user['screen_name'])
    query_results_df['followers'] = query_results_df['user'].apply(lambda user: user['followers_count'])

    query_results_df['tweet_text'] = query_results_df['text'].apply(lambda tweet: clean_tweet(tweet))
    query_results_df['polarity'] = query_results_df['tweet_text'].apply(lambda tweet: get_tweet_sentiment(tweet)[0])
    query_results_df['sentiment'] = query_results_df['tweet_text'].apply(lambda tweet: get_tweet_sentiment(tweet)[1])

    query_results_df['collection_time'] = pd.to_datetime(collection_time, format='%Y%b%d%H%M%S')

    query_results_df = query_results_df[[
        'id_str', 
        'datetime', 
        'tweet_text', 
        'polarity', 
        'sentiment', 
        'screen_name', 
        'followers', 
        'is_quote_status', 
        'retweet_count', 
        'favorite_count', 
        'collection_time'
        ]]

    query_results_df.sort_values(by='datetime', inplace=True)
    return query_results_df

# Setting up connection to Twitter API
t = Twitter(
    auth=OAuth(
        token = API_TOKEN, 
        token_secret = API_TOKEN_SECRET, 
        consumer_key = API_CONSUMER_KEY, 
        consumer_secret = API_CONSUMER_SECRET
        )
    )

# file_path = 'twitter_data/palantir/palantir_tweets.csv'
search_term = 'palantir'
requests_count = 0
crossover_flag = False
since_id = None
max_id = None

# Import most recent Tweet ID from MySQL database
mysql_query = '''
SELECT id_str
FROM stock_sentiment_project.palantir_tweets
ORDER BY `datetime` desc
LIMIT 1;
'''

since_id_df = pd.read_sql_query(mysql_query, engine)

# Set most recent Tweet ID as 'since_id' parameter so we don't pull Tweets we have already pulled
since_id = since_id_df['id_str'].iloc[0]

# if os.path.isfile(file_path):
#     twitter_data = pd.read_csv(file_path)
#     since_id = twitter_data['id_str'].iloc[-1]

results_df = pd.DataFrame(columns = [
    'id_str', 
    'datetime', 
    'tweet_text', 
    'polarity', 
    'sentiment', 
    'screen_name', 
    'followers', 
    'is_quote_status', 
    'retweet_count', 
    'favorite_count', 
    'collection_time'
    ])

while requests_count < 15 and crossover_flag == False:
    request_results = get_twitter_results(query=search_term, since_id=since_id, max_id=max_id)
    requests_count += 1
    print('Request: ', requests_count)
    print('Requests remaining: ', 15 - requests_count)

    size, _ = request_results.shape

    print('Results size: ', size)

    if size < 100:
        crossover_flag = True

    results_df = pd.concat([request_results, results_df])

    max_id = request_results['id_str'].iloc[0]
    max_id = np.int64(max_id) - np.int64(1)

# Append our results to the existing SQL database
results_df.to_sql(
    name = 'palantir_tweets',
    con=engine,
    index=False,
    if_exists='append'
)

# if os.path.isfile(file_path):
#     twitter_data = pd.concat([twitter_data, results_df])
#     twitter_data.to_csv('twitter_data/palantir/palantir_tweets.csv', index=False)
# else:
#     results_df.to_csv('twitter_data/palantir/palantir_tweets_081321.csv', index=False)