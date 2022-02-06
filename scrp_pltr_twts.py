from twitter_scraper_class import TwitterScraper

def main():
    scraper = TwitterScraper(query_terms='palantir', db_table='palantir_tweets', use_since_id=False)
    results = scraper.run()
    results.to_csv('test_011022.csv')

if __name__ == "__main__":
    main()