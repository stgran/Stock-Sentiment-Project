from textblob import TextBlob
import pandas as pd
import re
import time
import sqlalchemy
import os
import requests
import numpy as np

from dotenv import load_dotenv
load_dotenv()

class TwitterScraper():
    '''
    NEEDS DOCUMENTATION

    Methods
        - connect_to_db(self)
        - bearer_oauth(self, r)
        - aggregate_twitter_results(self, query_terms, requests_limit=15)
        - query_twitter(self, query_terms, since_id=None, until_id=None)
        - process_query_results(self, tweet_json, user_json=None)
        - parse_tweet_list(self, json_response)
        - get_user_data(self, users_list)
        - get_ot_metrics(self, json_response)
        - get_user_metrics(self, user_id, followers_counts)
        - clean_tweet(self, tweet)
        - get_tweet_sentiment(self, tweet)
        - get_since_id(self)
        - ot_metrics_in_db(self, tweet_id)
        - rt_metrics_in_db(self, original_tweet_id)
        - dist_metrics(self, id, follower_dict, results_df, total_likes=None, total_retweets=None)
        - calculate_rt_metrics(self, original_tweet_df, results_df)
    
    EXPANSIONS
    When I request Twitter's API, I request two expansions: Users and Original Tweets. The Users expansion
    contains information about each Twitter user whose tweet or retweet ends up in the query results.
    I use this expansion to get the follower count of the user who tweeted each tweet.

    The Original Tweets expansion contains information about the original tweets which appear in
    retweets, quote tweets, or replies in the query results. I use this information to calculate the
    public metrics of retweets.

    CALCULATING THE PUBLIC METRICS OF RETWEETS
    For some reason, the Twitter API does not offer accurate public metrics for
    retweets. Because of this, I have decided to estimate the public metrics of retweets.

    To do so, I use the Original Tweets expansion to calculate how many new retweets and likes
    a retweeted tweet has received since the last time I checked.

    I then then distribute those metrics among its retweets based on follower counts. The assumption
    here, which I do not expect to be 100% correct, is that users with higher follower counts will
    get more likes and retweets on their retweet than a user with a lower follower count.
    
    I complete this calculation by iterating over the Original Tweets expansion. For each Original
    Tweet (OT), I complete the following steps.
    1. I check if the OT also appears in the API request results.
        a. If so, I take the OT's likes and retweets and distribute them to the OT and its RTs
        proportional to follower counts.
    2. I check if the OT exists in my Tweet database.
        a. If so, I calculate how many likes and retweets that OT and its RTs have in the database
        in total.
            i. I then determine how many likes and retweets have accumulated since then by looking
            at the OT in the OT expansion.
            ii. I distribute the difference among the new RTs proportional to follower count.
        b. If the Tweet is not already in the results or in the database:
            i. I add the OT to the results.
            ii. I take its likes and retweets and distribute them to it and its RTs proportional to
            follower counts. However, I do not have the OT's user's follower count. In this case,
            because I have no way of knowing that follower count, I will give half of the likes and
            retweets to the OT and distribute the rest to the RTs (if there are any) proportionally.
    '''

    def __init__(self, query_terms, db_table, use_since_id=True):
        # Connect to our SQL database
        self.db_table = db_table
        self.engine = self.connect_to_db()
        
        # Determine if we will query out DB for a since_id
        self.use_since_id = use_since_id

        # Query terms
        self.query_terms = query_terms

    def connect_to_db(self):
        '''
        NEEDS DOCUMENTATION
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
            engine_str = 'mariadb+mariadbconnector://' + mysql_user + ':' + mysql_pwd + '@' + mysql_host  + '/' + mysql_db
            engine = sqlalchemy.create_engine(engine_str)
            print('Using mariadb database')
        except:
            engine_str = 'mysql+pymysql://' + mysql_user + ':' + mysql_pwd + '@' + mysql_host  + '/' + mysql_db
            engine = sqlalchemy.create_engine(engine_str)
            print('Using mysql database')
        
        return engine
    
    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """
        # Getting API bearer token for Twitter
        API_BEARER_TOKEN = os.getenv('TWITTER_API_BEARER_TOKEN')

        r.headers["Authorization"] = f"Bearer {API_BEARER_TOKEN}"
        r.headers["User-Agent"] = "v2RecentSearchPython"
        return r
    
    def aggregate_query_results(self, query_terms, requests_limit=15):
        '''
        NEEDS DOCUMENTATION
        '''
        if self.use_since_id:
            since_id = self.get_since_id()
        else:
            since_id = None

        requests_count = 0
        until_id = None

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

        while requests_count < requests_limit:
            json_results = self.query_twitter(query_terms, since_id=since_id, until_id=until_id)
            
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
                request_results = self.process_query_results(
                    json_results['data'], json_results['includes']['users']
                    )

                results_df = pd.concat([request_results, results_df])

                until_id = request_results['tweet_id'].iloc[0]
                until_id = np.int64(until_id) - np.int64(1)
            
            if 'tweets' in json_results['includes']:
                referenced_tweets = self.process_query_results(
                    json_results['includes']['tweets'], json_results['includes']['users']
                    )
                original_tweet_df = pd.concat([referenced_tweets, original_tweet_df])
                original_tweet_df = original_tweet_df.drop_duplicates(subset='tweet_id', keep='first')
        
        return results_df, original_tweet_df

    def query_twitter(self, query_terms, since_id=None, until_id=None):
        '''
        FIX DOCUMENTATION
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
        
        response = requests.get(search_url, auth=self.bearer_oauth, params=query_params)
        return response.json()
    
    def process_query_results(self, tweet_json, user_json=None):
        '''
        NEEDS DOCUMENTATION
        '''
        response_df = self.parse_tweet_list(tweet_json)
        
        # Add follower counts of the Tweet posters to df
        if user_json:
            user_followers_dict = self.get_user_data(user_json)
            response_df['followers_count'] = response_df['author_id'].apply(lambda author_id: self.get_user_metrics(author_id, user_followers_dict))

        # Convert created_at to datetime
        # Additionally, created_at is returned in the UTC-0 time zone
        # I want to convert it to UTC-4 because that time zone (US/Easter)
        # is the timezone in which the NYSE operates.
        response_df['datetime_utc+0'] = pd.to_datetime(response_df['created_at'])
        response_df['datetime_utc-4'] = response_df['datetime_utc+0'].dt.tz_convert(tz='US/Eastern')
        response_df['datetime'] = response_df['datetime_utc-4'].dt.tz_localize(tz=None)

        # Clean tweet texts
        response_df['tweet_text'] = response_df['text'].apply(lambda tweet: self.clean_tweet(tweet))
        
        # Get polarity and sentiment of tweets
        response_df['polarity'] = response_df['tweet_text'].apply(lambda tweet: self.get_tweet_sentiment(tweet)[0])
        response_df['sentiment'] = response_df['tweet_text'].apply(lambda tweet: self.get_tweet_sentiment(tweet)[1])

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

    def parse_tweet_list(self, json_response):
        '''
        NEEDS DOCUMENTATION
        '''
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

    def get_user_data(self, users_list):
        '''
        NEEDS DOCUMENTATION
        '''
        followers_count_dict = {}
        for user in users_list:
            followers_count_dict[user['id']] = user['public_metrics']['followers_count']
        return followers_count_dict

    def get_ot_metrics(self, json_response):
        '''
        NEEDS DOCUMENTATION
        '''
        original_tweet_list = json_response['includes']['tweets']
        original_tweet_dict = {}
        for tweet in original_tweet_list:
            original_tweet_dict[tweet['id']] = (tweet['public_metrics']['retweet_count'], tweet['public_metrics']['like_count'])
        return original_tweet_dict

    def get_user_metrics(self, user_id, followers_counts):
        '''
        NEEDS DOCUMENTATION
        '''
        if user_id in followers_counts:
            followers_count = followers_counts[user_id]
            return followers_count
        else:
            return ''

    def clean_tweet(self, tweet):
        ''' 
        Utility function to clean tweet text by removing links, special characters 
        using simple regex statements. 
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def get_tweet_sentiment(self, tweet):
        ''' 
        Utility function to classify sentiment of passed tweet 
        using textblob's sentiment method 
        '''
        # create TextBlob object of passed tweet text 
        analysis = TextBlob(self.clean_tweet(tweet))
        polarity = analysis.sentiment.polarity
        # set sentiment 
        if analysis.sentiment.polarity > 0: 
            sentiment = 'positive'
        elif analysis.sentiment.polarity == 0: 
            sentiment = 'neutral'
        else: 
            sentiment = 'negative'
        return polarity, sentiment

    def get_since_id(self):
        '''
        NEEDS DOCUMENTATION
        '''
        # Import most recent Tweet ID from MySQL database
        mysql_query = '''
        SELECT tweet_id
        FROM stock_sentiment_project.''' + self.db_table + '''
        ORDER BY `datetime` desc
        LIMIT 1;
        '''

        since_id_df = pd.read_sql_query(mysql_query, self.engine)

        # Set most recent Tweet ID as 'since_id' parameter so we don't pull Tweets we have already pulled
        since_id = since_id_df['tweet_id'].iloc[0]
        return since_id

    def ot_metrics_in_db(self, tweet_id):
        '''
        Support function for calculate_rt_metrics().
        Queries the database to check if a certain tweet has already been
        added to the database. If so, returns the retweet and like counts
        of the tweet. If not, returns None for both values.
        '''
        mysql_query = '''
        SELECT retweet_count, like_count
        FROM stock_sentiment_project.''' + self.db_table + '''
        WHERE tweet_id = ''' + tweet_id + ''';'''
        tweet_in_db = pd.read_sql_query(mysql_query, self.engine)
        if tweet_in_db.shape[0] > 0:
            retweets = tweet_in_db['retweet_count'].iloc[0]
            likes = tweet_in_db['like_count'].iloc[0]
        else:
            retweets, likes = None, None
        return retweets, likes

    def rt_metrics_in_db(self, original_tweet_id):
        '''
        Support function for calculate_rt_metrics().
        Queries the database to get the sum of retweet and like counts for all
        retweets of the original tweet specified by original_tweet_id.
        '''
        mysql_query= '''
        SELECT SUM(retweet_count), SUM(like_count)
        FROM stock_sentiment_project.''' + self.db_table + '''
        WHERE original_tweet_id = ''' + original_tweet_id + ''';'''
        rt_metrics_in_db = pd.read_sql_query(mysql_query, self.engine)
        retweets = rt_metrics_in_db['SUM(retweet_count)'].iloc[0]
        likes = rt_metrics_in_db['SUM(like_count)'].iloc[0]
        if type(retweets) != int or type(likes) != int:
            retweets, likes = 0, 0
        return retweets, likes
    
    def dist_metrics(self, id, follower_dict, results_df, total_likes=None, total_retweets=None):
        '''
        Support function for calculate_rt_metrics().
        Given values of like and retweet counts in addition to a dictionary of,
        follower counts by tweet user, this function distributes the total like
        and retweet counts to all relevant tweets proportional to follower count.
        '''
        if sum(follower_dict.values()) == 0:
            return
        proportion = follower_dict[id]/sum(follower_dict.values())
        if total_likes:
            results_df.loc[results_df['tweet_id'] == id, 'like_count'] = total_likes * proportion
        if total_retweets:
            results_df.loc[results_df['tweet_id'] == id, 'retweet_count'] = total_retweets * proportion

    def calculate_rt_metrics(self, original_tweet_df, results_df):
        '''
        This function 
        '''
        results_df_copy = results_df.copy()
        for _, original_tweet in original_tweet_df.iterrows():
            if original_tweet['tweet_id'] in results_df['tweet_id']:
                # The original tweet appears in our results dataframe so we don't have to check the db.
                relevant_subset = results_df_copy[(results_df_copy['tweet_id'] == original_tweet['tweet_id']) | (results_df_copy['original_tweet_id'] == original_tweet['tweet_id'])]
                
                total_likes, total_retweets = original_tweet['like_count'], original_tweet['retweet_count']
                
                followers_dict = dict(zip(relevant_subset['tweet_id'], relevant_subset['followers_count']))
                
                relevant_subset['tweet_id'].apply(lambda id: self.dist_metrics(id, followers_dict, results_df, total_likes, total_retweets))
            
            elif isinstance(self.ot_metrics_in_db(original_tweet['tweet_id'])[0], int):
                # The original tweet exists in our db because our query returned metrics
                hist_retweets, hist_likes = self.rt_metrics_in_db(original_tweet['tweet_id'])
                orig_retweets, orig_likes = self.ot_metrics_in_db(original_tweet['tweet_id'])
                
                new_retweets = original_tweet['retweet_count'] - (hist_retweets + orig_retweets)
                new_likes = original_tweet['like_count'] - (hist_likes + orig_likes)

                relevant_subset = results_df_copy[(results_df_copy['original_tweet_id'] == original_tweet['tweet_id'])]
                followers_dict = dict(zip(relevant_subset['tweet_id'], relevant_subset['followers_count']))

                relevant_subset['tweet_id'].apply(lambda id: self.dist_metrics(id, followers_dict, results_df, new_likes, new_retweets))
            
            else:
                # The original tweet exists neither in our db or our results.
                # This is only applicable for when the original tweet was tweeted before I started
                # running this script.
                relevant_subset = results_df_copy[(results_df_copy['original_tweet_id'] == original_tweet['tweet_id'])]

                followers_dict = dict(zip(relevant_subset['tweet_id'], relevant_subset['followers_count']))
                if sum(followers_dict.values()) == 0:
                    followers_dict[original_tweet['tweet_id']] = 1
                else:
                    followers_dict[original_tweet['tweet_id']] = sum(followers_dict.values())

                relevant_subset = relevant_subset.append(original_tweet)
                results_df = results_df.append(original_tweet)

                total_likes, total_retweets = original_tweet['like_count'], original_tweet['retweet_count']

                relevant_subset['tweet_id'].apply(lambda id: self.dist_metrics(id, followers_dict, results_df, total_likes, total_retweets))
    
    def run(self):
        # Main results dataframe and dataframe of original tweets (tweets which were retweeted at some point)
        results_df, original_tweet_df = self.aggregate_query_results(self.query_terms)

        # Now we need to update the public metrics of retweets. For more information on this step, see
        # the docstring at the beginning of the class.
        self.calculate_rt_metrics(original_tweet_df, results_df)

        return results_df