from operator import index
import unittest
import pandas as pd
from datetime import datetime

from twitter_scraper_methods_class import TwitterScraper

class TestRTMetricsCalc(unittest.TestCase):
    '''
    Testing important methods from twitter_scraper_methods_class.py.
    - TwitterScraper.dist_metrics()
    - TwitterScraper.calculate_rt_metrics()
    This testing module was written specifically to ensure these methods are
    functioning correctly since it is almost impossible to confirm by observing
    normal results from TwitterScraper.
    '''
    def setUp(self):
        '''
        This function sets up an instance of the TwitterScraper class.
        It also loads two sample data files as pandas dataframes:
            - Fake original tweet expansion
            - Fake results
        '''
        self.scraper = TwitterScraper(query_terms='palantir', db_table='palantir_tweets', use_since_id=False)
        self.ot_df = pd.read_csv('test_files/input_ot_df.csv', index_col=[0])
        self.results_df = pd.read_csv('test_files/input_results_df.csv', index_col=[0])
    
    def compare_cols(self, col_1, col_2):
        '''
        This function confirms that two numerical columns are identical.
        '''
        similar = False
        results = col_1 - col_2
        if results.sum() == 0:
            similar = True
        return similar

    def test_dist_metrics(self):
        '''
        This test determines if TwitterScraper.dist_metrics() is correctly distributing
        like and retweet metrics for an original tweet and its retweets.
        '''
        tweet_id = 7
        results_df_copy = self.results_df.copy()
        # Select retweets of the original tweet and the original tweet from the dataframe
        relevant_subset = results_df_copy[(results_df_copy['tweet_id'] == tweet_id) | (results_df_copy['original_tweet_id'] == tweet_id)]
        
        # Create a dict of Tweet IDs and corresponding follower counts for the author.
        followers_dict = dict(zip(relevant_subset['tweet_id'], relevant_subset['followers_count']))

        # Get likes and retweets
        total_likes = self.ot_df.loc[self.ot_df['tweet_id'] == tweet_id, 'like_count'].item()
        total_retweets = self.ot_df.loc[self.ot_df['tweet_id'] == tweet_id, 'retweet_count'].item()

        # Apply TwitterScraper.dist_metrics to the subset.
        relevant_subset['tweet_id'].apply(lambda tweet_id: self.scraper.dist_metrics(tweet_id, followers_dict, self.results_df, total_likes, total_retweets))

        # Import expected results
        expected_results = pd.read_csv('test_files/test_dist_metrics/expected_results.csv')

        # Save and timestamp our results
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M")
        destination = f'test_files/test_dist_metrics/results/{timestamp}-results.csv'
        self.results_df.to_csv(destination, index=False)

        # Check if likes and retweets were updated correctly
        correct_likes = self.compare_cols(self.results_df['like_count'], expected_results['like_count'])
        correct_rts = self.compare_cols(self.results_df['retweet_count'], expected_results['retweet_count'])

        # Assert that self.compare_cols() returned True for both likes and retweets.
        self.assertTrue(correct_likes & correct_rts)
    
    def test_calculate_rt_metrics(self):
        '''
        This test determines if TwitterScraper.calculate_rt_metrics() is correctly
        calculating like and retweet metrics for retweets.
        TwitterScraper.calculate_rt_metrics() relies on TwitterScraper.dist_metrics()
        to distribute likes and retweets among an original tweet and its retweets,
        so if TwitterScraper.dist_metrics() is not working, this test is not helpful.
        However, if TwitterScraper.dist_metrics() passes its test but TwitterScraper.calculate_rt_metrics()
        fails, this suggests the problem is specific to TwitterScraper.calculate_rt_metrics().
        '''
        # Run TwitterScraper.calculate_rt_metrics() on the dataframes.
        results_df = self.scraper.calculate_rt_metrics(self.ot_df, self.results_df)
        
        # Import expected results
        expected_results = pd.read_csv('test_files/test_calculate_rt_metrics/expected_results.csv')

        # Save and timestamp our results
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M")
        destination = f'test_files/test_calculate_rt_metrics/results/{timestamp}-results.csv'
        results_df.to_csv(destination, index=False)

        # Check if likes and retweets were updated correctly
        correct_likes = self.compare_cols(results_df['like_count'], expected_results['like_count'])
        correct_rts = self.compare_cols(results_df['retweet_count'], expected_results['retweet_count'])

        # Assert that self.compare_cols() returned True for both likes and retweets.
        self.assertTrue(correct_likes & correct_rts)

if __name__ == '__main__':
    unittest.main()