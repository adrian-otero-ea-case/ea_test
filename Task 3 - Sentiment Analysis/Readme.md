
# Install libraries


```python
import os
import pandas as pd
import numpy as np
import tweepy
import csv
import json 
import datetime

from pandas.io import sql
from sqlalchemy import create_engine
from mysql import connector
from google.cloud import storage
from google.cloud import bigquery

from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', -1)
```

# Configure Twitter API Credentials


```python
consumer_key = 'JHi9h2byuCWorokMLCIbh20KI'
consumer_secret = 'k0ysvIDGu4YGswrqab76NFmroVmOxN9r2JuapfaoYYoXb3BrXO'
access_token = '1186208049058500608-V3EVjBiZbwx9Aqmn3asu65Gb1j05zx'
access_token_secret = 'QgesjDM7QGVCHCkDQqhXs2WUYk6jmTM9iy01Cc6n4VtOC'
```


```python
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)
```

# Configure Natural Language API Client


```python
nlp_client = language.LanguageServiceClient()
```

# Configure connection with Cloud SQL


```python
user='ea-developer'
host='35.205.32.16'
port='3306'
db='ea_datalake'
database_connection = create_engine('mysql+mysqlconnector://{0}:@{1}:{2}/{3}'.format(user, host, port, db))
```

# Configure connection with Bigquery


```python
bigquery_client = bigquery.Client()
```

# Configure connection with Cloud Storage


```python
# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = 'ea-datalake-dev'
```


```python
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))
    
def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs()

    for blob in blobs:
        print(blob.name)
```

# Extract data from Twitter using API


```python
hashtag = '#FIFA20'

columns=['tweet_id', 'tweet_createat', 'tweet_text', 'tweet_lang', 
         'tweet_country', 'tweet_country_code',
         'tweet_retweets', 'tweet_favourites',
         'user_id', 'user_name', 'user_location', 'user_followers',
         'sentiment_score', 'sentiment_magnitude']

support_langs = ['zh','zh-Hant','en','fr','de','it','ja','ko','pt','es']
```

### Use Search API to find located tweets with the hashtag #FIFA20


```python
msgs = []
msg =[]

for tweet in tweepy.Cursor(api.search, q=hashtag, rpp=100).items():
    
        # Extract location details
        country = ''
        country_code = ''    
        if tweet.place is not None:
            country = tweet.place.country
            country_code = tweet.place.country_code
            
        if tweet.lang is not None:
            if tweet.lang in support_langs:
                
                NOK = NOK - 1
                OK = OK + 1
                print('OK: ', OK, ' // NOK: ', NOK)

                # Use Google NLP API to Analyze the sentiment of the text
                document = types.Document(
                    content=tweet.text,
                    language=tweet.lang,
                    type=enums.Document.Type.PLAIN_TEXT)
                sentiment = client.analyze_sentiment(document=document).document_sentiment

                # Create a tuple with all the fields (Twitter + Sentiment)
                msg = [tweet.id_str, tweet.created_at, tweet.text, tweet.lang, 
                       country, country_code, 
                       tweet.retweet_count, tweet.favorite_count, 
                       tweet.user.id_str, tweet.user.screen_name, tweet.user.location, tweet.user.followers_count,
                       sentiment.score, sentiment.magnitude]

            msg = tuple(msg)
            msgs.append(msg)
```


```python
# Add column names
df_tweets = pd.DataFrame(msgs)
df_tweets.columns = columns
df_tweets = df_tweets[df_tweets.tweet_country.notnull()]
```


```python
df_tweets['tweet_sentiment'] = np.where(df_tweets['sentiment_score'] > 0.3, 
                                        '5. Very Positive',
                               np.where((df_tweets['sentiment_score'] >= 0.1) & (df_tweets['sentiment_score'] < 0.3),
                                        '4. Positive',
                               np.where((df_tweets['sentiment_score'] > (-0.3)) & (df_tweets['sentiment_score'] <= (-0.1)), 
                                        '2. Negative',
                               np.where(df_tweets['sentiment_score'] <= (-0.3), 
                                        '1. Very Negative', '3. Neutral'))))
```


```python
dataset_ref = bigquery_client.dataset('ea')
table_ref = dataset_ref.table('ea_fifa_20_tweets')

bigquery_client.load_table_from_dataframe(df_tweets, table_ref).result()
```

    /home/adrian_otero_ea_case/anaconda3/lib/python3.5/site-packages/google/cloud/bigquery/_pandas_helpers.py:275: UserWarning: Unable to determine type of column 'tweet_id'.
      warnings.warn(u"Unable to determine type of column '{}'.".format(column))





    <google.cloud.bigquery.job.LoadJob at 0x7f6e5285b080>


