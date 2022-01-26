import sqlalchemy
import os
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

mysql_query_1 = '''
SELECT SUM(retweet_count), SUM(like_count)
FROM stock_sentiment_project.palantir_tweets
WHERE original_tweet_id = 5;'''

mysql_query_2 = '''SELECT retweet_count, like_count
FROM stock_sentiment_project.palantir_tweets
WHERE tweet_id = 5;'''

mysql_user = os.getenv('MYSQL_USER')
mysql_pwd = os.getenv('MYSQL_PWD')
mysql_host = os.getenv('MYSQL_HOST')
mysql_db = os.getenv('MYSQL_DB')

engine_str = 'mysql+pymysql://' + mysql_user + ':' + mysql_pwd + '@' + mysql_host  + '/' + mysql_db
engine = sqlalchemy.create_engine(engine_str)

rt_metrics_in_db = pd.read_sql_query(mysql_query_1, engine)

ot_in_db = pd.read_sql_query(mysql_query_2, engine)

print(rt_metrics_in_db)

print(ot_in_db)