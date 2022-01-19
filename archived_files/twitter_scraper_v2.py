import pandas as pd
import requests
import os
import numpy as np
import sqlalchemy

from dotenv import load_dotenv
load_dotenv()

from twitter_scraper_methods import *

'''
This file queries Twitter's Recent Search API (v2).
The specific endpoint used is GET /2/tweets/search/recent.
This file uses requests to query the endpoint.

For more information on this endpoint, please visit
https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent

'''
# Getting API bearer token for Twitter
API_BEARER_TOKEN = os.getenv('TWITTER_API_BEARER_TOKEN')

# Getting SQL database credentials
mysql_user = os.getenv('MYSQL_USER')
mysql_pwd = os.getenv('MYSQL_PWD')
mysql_host = os.getenv('MYSQL_HOST')
mysql_db = os.getenv('MYSQL_DB')

# Setting up connection to SQL database
# I have set this up to handle either mariadb or mysql because I run this on two
# different computers which use these different SQL databases.
try:
    engine_str = 'mariadb+mariadbconnector://' + mysql_user + ':' + mysql_pwd + '@' + mysql_host  + '/' + mysql_db
    engine = sqlalchemy.create_engine(engine_str)
    print('Using mariadb database')
except:
    engine_str = 'mysql+pymysql://' + mysql_user + ':' + mysql_pwd + '@' + mysql_host  + '/' + mysql_db
    engine = sqlalchemy.create_engine(engine_str)
    print('Using mysql database')

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {API_BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def get_twitter_results(query_terms, since_id=None, until_id=None):
    '''
    Queries Twitter.
    Parameters:
        query_terms: A string of all the search terms separated by spaces.
            Should be different names for the same company or stock.
        since_id: A 
        until_id:
    '''
    search_url = 'https://api.twitter.com/2/tweets/search/recent'
    
    query_params = {'tweet.fields': 'id,text,created_at,public_metrics', 'expansions': 'author_id,referenced_tweets.id', 'user.fields': 'public_metrics', 'max_results': 100}

    query = '(' + query_terms + ') lang:en'
    query_params['query'] = query

    if since_id:
        query_params['since_id'] = since_id
    if until_id:
        query_params['until_id'] = until_id
    
    response = requests.get(search_url, auth=bearer_oauth, params=query_params)
    return response.json()

query_terms = 'palantir OR pltr'
requests_count = 0
since_id = None
until_id = None

# Import most recent Tweet ID from MySQL database
mysql_query = '''
SELECT tweet_id
FROM stock_sentiment_project.palantir_tweets
ORDER BY `datetime` desc
LIMIT 1;
'''

# since_id_df = pd.read_sql_query(mysql_query, engine)

# Set most recent Tweet ID as 'since_id' parameter so we don't pull Tweets we have already pulled
# since_id = since_id_df['tweet_id'].iloc[0]

results_df = pd.DataFrame(columns = [
    'tweet_id',
    'datetime',
    'tweet_text',
    'polarity',
    'sentiment',
    'author_id',
    'followers_count', 
    'retweet_count',
    'like_count',
    'collection_time',
    'original_tweet_id'
    ])
original_tweet_df = results_df.copy(deep=True)

while requests_count < 15:
    json_results = get_twitter_results(query_terms, since_id=since_id, until_id=until_id)
    # print(json_results)
    results_size = json_results['meta']['result_count']
        
    requests_count += 1
    print('Request: ', requests_count)
    print('Requests remaining: ', 15 - requests_count)

    print('Results size: ', results_size)

    # If no Tweets are returned, we have likely reached the point where
    # since_id and until_id meet. We will end our search.
    if results_size == 0:
        print('Returned no Tweets. Ending search.')
        break

    # Provided we have results, we process these Tweets.
    if results_size > 0:
        request_results = process_twitter_results(
            json_results['data'], json_results['includes']['users']
            )

        results_df = pd.concat([request_results, results_df])

        until_id = request_results['tweet_id'].iloc[0]
        until_id = np.int64(until_id) - np.int64(1)
    
    # We need to keep track of the original tweets as we call the API
    if 'tweets' in json_results['includes']:
        referenced_tweets = process_twitter_results(
            json_results['includes']['tweets'], json_results['includes']['users']
            )
        original_tweet_df = pd.concat([referenced_tweets, original_tweet_df])
        original_tweet_df = original_tweet_df.drop_duplicates(subset='tweet_id', keep='first')

# Append our results to the existing SQL database
# results_df.to_sql(
#     name = 'palantir_tweets',
#     con=engine,
#     index=False,
#     if_exists='append'
# )