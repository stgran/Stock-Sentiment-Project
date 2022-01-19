from textblob import TextBlob
import pandas as pd
import re
import time
import sqlalchemy
import os

from dotenv import load_dotenv
load_dotenv()

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

def parse_tweet_list(json_response):
    tweet_dict = {
        'tweet_id': [],
        'created_at': [],
        'like_count': [],
        'retweet_count': [],
        'text': [],
        'author_id': [],
        'original_tweet_id': []
    }
    for tweet in json_response:
        tweet_dict['tweet_id'].append(tweet['id'])
        tweet_dict['created_at'].append(tweet['created_at'])
        tweet_dict['like_count'].append(tweet['public_metrics']['like_count'])
        tweet_dict['retweet_count'].append(tweet['public_metrics']['retweet_count'])
        tweet_dict['text'].append(tweet['text'])
        tweet_dict['author_id'].append(tweet['author_id'])
        
        # We need to handle retweets differently because their public metrics are inaccurate.
        # So we will need to figure out the public metrics of retweets ourselves.
        if 'referenced_tweets' in tweet:
            retweet_status = False
            for referenced_tweet in tweet['referenced_tweets']:
                if referenced_tweet['type'] == 'retweeted':
                    tweet_dict['original_tweet_id'].append(referenced_tweet['id'])
                    retweet_status = True
            if retweet_status == False:
                tweet_dict['original_tweet_id'].append(None)
        else:
            tweet_dict['original_tweet_id'].append(None)
    return pd.DataFrame(tweet_dict)

def get_user_data(users_list):
    followers_count_dict = {}
    for user in users_list:
        followers_count_dict[user['id']] = user['public_metrics']['followers_count']
    return followers_count_dict

def get_ot_metrics(json_response):
    original_tweet_list = json_response['includes']['tweets']
    original_tweet_dict = {}
    for tweet in original_tweet_list:
        original_tweet_dict[tweet['id']] = (tweet['public_metrics']['retweet_count'], tweet['public_metrics']['like_count'])
    return original_tweet_dict

def get_user_metrics(user_id, followers_counts):
    if user_id in followers_counts:
        followers_count = followers_counts[user_id]
        return followers_count
    else:
        return ''

def calculate_rt_metrics(original_tweet, tweet_df):
    # First we need a query to determine if we have interacted with this tweet before
    mysql_query_1 = '''
    SELECT tweet_id, retweet_count, like_count
    FROM stock_sentiment_project.palantir_tweets
    WHERE tweet_id = ''' + original_tweet + ''';'''

    # Second we need a query to get the sums of the public metrics of its retweets
    mysql_query_2 = '''
    SELECT SUM(retweet_count), SUM(like_count)
    FROM stock_sentiment_project.palantir_tweets
    WHERE original_tweet_id = ''' + original_tweet + ''';'''

    # Let's check if the original tweet is already stored in the database
    original_tweet_df = pd.read_sql_query(mysql_query_1, engine)
    tweets, _ = original_tweet_df.shape
    if tweets < 1:
        # In this case, we will add the original Tweet to our database
        # and distribute its public metrics among it and its retweets
        retweets = tweet_df[tweet_df['original_tweet_id'] == original_tweet]
        
    # elif tweets >= 1:
    #     # In this case, we will find the difference between the original Tweet's
    #     # current metrics and its metrics the last time we checked.

def clean_tweet(tweet): 
    ''' 
    Utility function to clean tweet text by removing links, special characters 
    using simple regex statements. 
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

def get_tweet_sentiment(tweet): 
    ''' 
    Utility function to classify sentiment of passed tweet 
    using textblob's sentiment method 
    '''
    # create TextBlob object of passed tweet text 
    analysis = TextBlob(clean_tweet(tweet))
    polarity = analysis.sentiment.polarity
    # set sentiment 
    if analysis.sentiment.polarity > 0: 
        sentiment = 'positive'
    elif analysis.sentiment.polarity == 0: 
        sentiment = 'neutral'
    else: 
        sentiment = 'negative'
    return polarity, sentiment

def process_twitter_results(tweet_json, user_json=None):
    response_df = parse_tweet_list(tweet_json)
    
    # Add follower counts of the Tweet posters to df
    if user_json:
        user_followers_dict = get_user_data(user_json)
        response_df['followers_count'] = response_df['author_id'].apply(lambda author_id: get_user_metrics(author_id, user_followers_dict))

    # Convert created_at to datetime
    # Additionally, created_at is returned in the UTC-0 time zone
    # I want to convert it to UTC-4 because that time zone (US/Easter)
    # is the timezone in which the NYSE operates.
    response_df['datetime_utc+0'] = pd.to_datetime(response_df['created_at'])
    response_df['datetime_utc-4'] = response_df['datetime_utc+0'].dt.tz_convert(tz='US/Eastern')
    response_df['datetime'] = response_df['datetime_utc-4'].dt.tz_localize(tz=None)

    # Clean tweet texts
    response_df['tweet_text'] = response_df['text'].apply(lambda tweet: clean_tweet(tweet))
    
    # Get polarity and sentiment of tweets
    response_df['polarity'] = response_df['tweet_text'].apply(lambda tweet: get_tweet_sentiment(tweet)[0])
    response_df['sentiment'] = response_df['tweet_text'].apply(lambda tweet: get_tweet_sentiment(tweet)[1])

    # Add collection time to df
    collection_time = time.asctime( time.localtime(time.time()) )
    response_df['collection_time'] = pd.to_datetime(collection_time)

    # Keep only the columns we want
    response_df = response_df[[
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
        ]]

    # Sort by datetime so the Tweets go from oldest to newest
    response_df.sort_values(by='datetime', inplace=True)

    return response_df