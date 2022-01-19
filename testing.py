import unittest
from test import support
import pandas as pd

from twitter_scraper_methods_class import TwitterScraper

class TestRTMetricsCalc(unittest.TestCase):
    '''
    Testing important methods from twitter_scraper_methods_class.py.
    - TwitterScraper.dist_metrics()
    '''
    def setUp(self):
        scraper = TwitterScraper()
        results_df = pd.DataFrame()
        ot_df = pd.DataFrame()
        follower_dict = {}
        return scraper, results_df, ot_df, follower_dict
    
    def test_dist_metrics(self):
        scraper, results_df, ot_df, follower_dict = self.setUp()
        total_likes, total_retweets = 10000, 10000
        
        scraper.dist_metrics()
    
    def test_calculate_rt_metrics(self):
        scraper, results_df, ot_df, follower_dict = self.setUp()
        scraper.calculate_rt_metrics(ot_df, results_df)

if __name__ == '__main__':
    unittest.main()