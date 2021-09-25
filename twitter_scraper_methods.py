from textblob import TextBlob
import pandas as pd
import re
import time

def parse_tweet_list(json_response):
    tweet_list = json_response['data']
    tweet_dict = {
        'tweet_id': [],
        'created_at': [],
        'like_count': [],
        'retweet_count': [],
        'text': [],
        'author_id': []
    }
    for tweet in tweet_list:
        tweet_dict['tweet_id'].append(tweet['id'])
        tweet_dict['created_at'].append(tweet['created_at'])
        tweet_dict['like_count'].append(tweet['public_metrics']['like_count'])
        tweet_dict['retweet_count'].append(tweet['public_metrics']['retweet_count'])
        tweet_dict['text'].append(tweet['text'])
        tweet_dict['author_id'].append(tweet['author_id'])

    return pd.DataFrame(tweet_dict)

def get_user_data(json_response):
    users_list = json_response['includes']['users']
    followers_count_dict = {}
    for user in users_list:
            followers_count_dict[user['id']] = user['public_metrics']['followers_count']
    return followers_count_dict

def get_user_metrics(user_id, followers_counts):
    followers_count = followers_counts[user_id]
    return followers_count

def process_twitter_results(json_response):
    response_df = parse_tweet_list(json_response)
    
    # Add follower counts of the Tweet posters to df
    user_followers_dict = get_user_data(json_response)
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
        'collection_time'
        ]]

    # Sort by datetime so the Tweets go from oldest to newest
    response_df.sort_values(by='datetime', inplace=True)

    return response_df

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

def check_for_duplicates(tweet_id, tweet_ids):
    if tweet_id in tweet_ids:
        original_status = False
    else:
        original_status = True
    return original_status

def restructure_datetime(datetime):
    year = datetime[-4:]
    month = datetime[4:7]
    date = datetime[8:10]
    hour = datetime[11:13]
    minute = datetime[14:16]
    second = datetime[17:19]
    
    restructured_datetime = year + month + date + hour + minute + second
    return restructured_datetime