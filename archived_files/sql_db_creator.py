import pandas as pd
import sqlalchemy

import os
from dotenv import load_dotenv
load_dotenv()

mysql_user = os.getenv('MYSQL_USER')
mysql_pwd = os.getenv('MYSQL_PWD')
mysql_host = os.getenv('MYSQL_HOST')
mysql_db = os.getenv('MYSQL_DB')

engine_str = 'mysql+pymysql://' + mysql_user + ':' + mysql_pwd + '@' + mysql_host  + '/' + mysql_db
engine = sqlalchemy.create_engine(engine_str)

df = pd.read_csv('twitter_data/palantir/palantir_tweets.csv')
df.to_sql(
    name = 'palantir_tweets',
    con=engine,
    index=False,
    if_exists='replace'
)
# mysql_query = '''
# SELECT id_str
# FROM stock_sentiment_project.palantir_tweets
# ORDER BY `index` desc
# LIMIT 1;
# '''

# since_id_df = pd.read_sql_query(mysql_query, engine)

# since_id = since_id_df['id_str'].iloc[0]

# print(since_id)