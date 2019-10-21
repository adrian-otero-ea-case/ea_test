
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

Define functions to load data into Cloud Storage


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

# Download data from Kaggle Dataset
https://www.kaggle.com/thec03u5/fifa-18-demo-player-dataset/data


```python
!kaggle datasets download -d thec03u5/fifa-18-demo-player-dataset
!unzip fifa-18-demo-player-dataset.zip
!mkdir data
!mkdir data/fifa-18-demo-player-dataset/
!mv *Data*.csv data/fifa-18-demo-player-dataset/
!rm -rf fifa-18-demo-player-dataset.zip
!ls data/fifa-18-demo-player-dataset/
```

    Warning: Your Kaggle API key is readable by other users on this system! To fix this, you can run 'chmod 600 /home/adrian_otero_ea_case/.kaggle/kaggle.json'
    Downloading fifa-18-demo-player-dataset.zip to /home/adrian_otero_ea_case
      0%|                                               | 0.00/3.82M [00:00<?, ?B/s]
    100%|██████████████████████████████████████| 3.82M/3.82M [00:00<00:00, 82.0MB/s]
    Archive:  fifa-18-demo-player-dataset.zip
      inflating: CompleteDataset.csv     
      inflating: PlayerAttributeData.csv  
      inflating: PlayerPersonalData.csv  
      inflating: PlayerPlayingPositionData.csv  
    CompleteDataset.csv	 PlayerPersonalData.csv
    PlayerAttributeData.csv  PlayerPlayingPositionData.csv


# Download data from GIT repo
https://github.com/annexare/Countries


```python
!wget https://github.com/annexare/Countries/archive/master.zip
!unzip master.zip
!mkdir data/countries
!mv Countries-master/data/* data/countries
!rm -rf master.zip
!rm -rf Countries-master
```

    --2019-10-20 22:39:21--  https://github.com/annexare/Countries/archive/master.zip
    Resolving github.com (github.com)... 140.82.118.3
    Connecting to github.com (github.com)|140.82.118.3|:443... connected.
    HTTP request sent, awaiting response... 302 Found
    Location: https://codeload.github.com/annexare/Countries/zip/master [following]
    --2019-10-20 22:39:21--  https://codeload.github.com/annexare/Countries/zip/master
    Resolving codeload.github.com (codeload.github.com)... 192.30.253.120
    Connecting to codeload.github.com (codeload.github.com)|192.30.253.120|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: unspecified [application/zip]
    Saving to: ‘master.zip’
    
    master.zip              [  <=>               ] 236.09K   725KB/s    in 0.3s    
    
    2019-10-20 22:39:22 (725 KB/s) - ‘master.zip’ saved [241759]
    
    Archive:  master.zip
    abfdb128bd43ac42708a953c5a46971cd4e678b5
       creating: Countries-master/
      inflating: Countries-master/.editorconfig  
      inflating: Countries-master/.eslintrc.json  
      inflating: Countries-master/.gitignore  
      inflating: Countries-master/.npmignore  
      inflating: Countries-master/.travis.yml  
      inflating: Countries-master/LICENSE  
      inflating: Countries-master/README.md  
      inflating: Countries-master/bower.json  
      inflating: Countries-master/composer.json  
       creating: Countries-master/data/
      inflating: Countries-master/data/continents.json  
      inflating: Countries-master/data/countries.json  
      inflating: Countries-master/data/languages.json  
       creating: Countries-master/dist/
      inflating: Countries-master/dist/continents.json  
      inflating: Countries-master/dist/continents.min.json  
      inflating: Countries-master/dist/countries.csv  
      inflating: Countries-master/dist/countries.emoji.json  
      inflating: Countries-master/dist/countries.emoji.min.json  
      inflating: Countries-master/dist/countries.json  
      inflating: Countries-master/dist/countries.min.json  
      inflating: Countries-master/dist/data.json  
      inflating: Countries-master/dist/data.min.json  
      inflating: Countries-master/dist/data.sql  
      inflating: Countries-master/dist/index.d.ts  
      inflating: Countries-master/dist/index.es5.min.js  
      inflating: Countries-master/dist/index.es5.min.js.map  
      inflating: Countries-master/dist/index.es5.min.test.js  
      inflating: Countries-master/dist/index.js  
      inflating: Countries-master/dist/index.test.js  
      inflating: Countries-master/dist/languages.all.json  
      inflating: Countries-master/dist/languages.all.min.json  
      inflating: Countries-master/dist/languages.json  
      inflating: Countries-master/dist/languages.min.json  
       creating: Countries-master/dist/minimal/
      inflating: Countries-master/dist/minimal/README.md  
      inflating: Countries-master/dist/minimal/countries.en.min.json  
      inflating: Countries-master/dist/minimal/countries.minimal.min.json  
      inflating: Countries-master/dist/minimal/languages.en.min.json  
      inflating: Countries-master/dist/minimal/languages.minimal.min.json  
      inflating: Countries-master/gulpfile.js  
      inflating: Countries-master/index.tpl.d.ts  
      inflating: Countries-master/package-lock.json  
      inflating: Countries-master/package.json  


# Load data about Countries and Languages

Load countries information and transpose columns


```python
df_countries = pd.read_json(r'/home/adrian_otero_ea_case/data/countries/countries.json')
df_countries = df_countries.transpose()
df_countries = df_countries.reset_index()
df_countries = df_countries[['name','languages','capital','continent','currency','native','phone']]
df_countries = df_countries.rename(columns = {'name':'country','languages':'language_code'})
```

Convert the array of languages into different columns


```python
df_countries_lang = pd.DataFrame(df_countries.language_code.values.tolist(), index=df_countries.index)
df_countries_lang = df_countries_lang.add_prefix('language_code_')
df_countries_lang.fillna(value='', inplace=True)
df_countries = pd.concat([df_countries, df_countries_lang], axis=1)
df_countries.drop('language_code', axis=1, inplace=True)
```

Load languages information and transpose columns


```python
df_languages = pd.read_json(r'/home/adrian_otero_ea_case/data/countries/languages.json')
df_languages = df_languages.transpose()
df_languages = df_languages.reset_index()
df_languages = df_languages.rename(columns = {'index':'language_code','name':'language','native':'language_native'})
df_languages = df_languages[['language_code','language']]
```

Convert language code to language name joining countries and languages datasets


```python
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_0'], right_on=['language_code']).\
    rename(columns = {'language':'language_0'}).drop({'language_code','language_code_0'},axis=1)
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_1'], right_on=['language_code']).\
    rename(columns = {'language':'language_1'}).drop({'language_code','language_code_1'},axis=1)
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_2'], right_on=['language_code']).\
    rename(columns = {'language':'language_2'}).drop({'language_code','language_code_2'},axis=1)
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_3'], right_on=['language_code']).\
    rename(columns = {'language':'language_3'}).drop({'language_code','language_code_3'},axis=1)
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_4'], right_on=['language_code']).\
    rename(columns = {'language':'language_4'}).drop({'language_code','language_code_4'},axis=1)
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_5'], right_on=['language_code']).\
    rename(columns = {'language':'language_5'}).drop({'language_code','language_code_5'},axis=1)
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_6'], right_on=['language_code']).\
    rename(columns = {'language':'language_6'}).drop({'language_code','language_code_6'},axis=1)
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_7'], right_on=['language_code']).\
    rename(columns = {'language':'language_7'}).drop({'language_code','language_code_7'},axis=1)
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_8'], right_on=['language_code']).\
    rename(columns = {'language':'language_8'}).drop({'language_code','language_code_8'},axis=1)
df_countries = pd.merge(df_countries, df_languages, how='left', left_on=['language_code_9'], right_on=['language_code']).\
    rename(columns = {'language':'language_9'}).drop({'language_code','language_code_9'},axis=1)
df_countries.fillna(value='', inplace=True)
```

Export data to a csv file and load it into a Clod Storage bucket


```python
df_countries.to_csv(r'data/countries/countries_languages.csv')
```


```python
upload_blob(bucket_name, r'data/countries/countries_languages.csv', r'export/countries_languages.csv')
```

    File data/countries/countries_languages.csv uploaded to export/countries_languages.csv.


Move data to Cloud SQL


```python
df_countries.to_sql(con=database_connection, name='ea_countries_languages', if_exists='replace')
```

# Load data about EA's FIFA 18 players

Read data from CSV files obtained from Kaggle


```python
df_complete_dataset = pd.read_csv(r'data/fifa-18-demo-player-dataset/CompleteDataset.csv', index_col=0, header=0, low_memory=False)
```

Load data into Cloud SQL database


```python
df_complete_dataset.to_sql(con=database_connection, name='ea_complete_dataset', if_exists='replace',index=False)
```
