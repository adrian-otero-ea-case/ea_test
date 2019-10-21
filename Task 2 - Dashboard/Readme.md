
# Install libraries


```python
import os
import pandas as pd
import numpy as np
import matplotlib
import pandas_profiling as pf

from io import BytesIO
from pandas.io import sql
from sqlalchemy import create_engine
from mysql import connector
from google.cloud import storage
from google.cloud import bigquery

%matplotlib inline
import matplotlib.pyplot as plt
#plt.switch_backend('agg')

pd.set_option('display.max_columns', None)
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

# Prepare Data Visualization

Load data from Cloud SQL Database


```python
df_players = pd.read_sql('SELECT * FROM ea_players_language_top', con=database_connection)
```

Load data into Google Cloud Bigquery in order to be visualized by using Qlik Sense


```python
dataset_ref = bigquery_client.dataset('ea')
table_ref = dataset_ref.table('ea_players_language_top')

bigquery_client.load_table_from_dataframe(df_players, table_ref).result()
```

    /home/adrian_otero_ea_case/anaconda3/lib/python3.5/site-packages/google/cloud/bigquery/_pandas_helpers.py:275: UserWarning: Unable to determine type of column 'Name'.
      warnings.warn(u"Unable to determine type of column '{}'.".format(column))





    <google.cloud.bigquery.job.LoadJob at 0x7f25012f4080>


