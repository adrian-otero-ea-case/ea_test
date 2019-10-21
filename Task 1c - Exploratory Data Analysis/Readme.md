
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

# Exploratory Data Analysis

#### Focused on the different languages of the players speak


```python
df_players = pd.read_sql('SELECT * FROM ea_players_language', con=database_connection)
```

### Top Nationalities
United Kingdom is the nationallity with most players associated


```python
nationalities = df_players.groupby("Nationality").ID.count().sort_values(ascending=False).head(10)
nationalities.plot.pie(autopct='%.1f', fontsize=12, figsize=(8, 8))
plt.show()
```


![png](output_12_0.png)


### Top languages

United Kingdom is the top one nationality, however, Spanish is the most frequent as primary language


```python
languages = df_players.groupby("Primary Language").ID.count().sort_values(ascending=False).head(10)
languages.plot.pie(autopct='%.1f', fontsize=12, figsize=(8, 8))
plt.show()
```


![png](output_15_0.png)


Filter out languages with a low number of registers, in order to keep just the languages with a statiscally relevant number of representants


```python
df_top_lang = df_players.groupby("Primary Language").ID.count().reset_index()
df_top_lang = df_top_lang[df_top_lang.ID>=100]
top_lang = list(df_top_lang['Primary Language'])
df_languages_top = df_players[df_players['Primary Language'].isin(top_lang)]
```

Load data from Cloud SQL Database


```python
df_languages_top.to_sql(con=database_connection, name='ea_players_language_top', if_exists='replace',index=False)
```

### Which LANGUAGE is present in more CLUBs?


```python
df_languages_top.groupby("Primary Language").Club.nunique().sort_values(ascending=True).reset_index().\
    plot.barh(x='Primary Language',use_index=True,legend=False,figsize=(12,6))    
plt.show()
```


![png](output_21_0.png)


### What is the PREFERRED POSITION by LANGUAGE?


```python
preferred = df_languages_top.groupby(['Primary Language','Preferred Position']).ID.count().\
    sort_values(ascending=False).reset_index().rename(columns = {'ID':'Freq'})

preferred.loc[preferred.reset_index().groupby(['Primary Language'])['Freq'].idxmax()]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Primary Language</th>
      <th>Preferred Position</th>
      <th>Freq</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>61</th>
      <td>Arabic</td>
      <td>CB</td>
      <td>68</td>
    </tr>
    <tr>
      <th>175</th>
      <td>Croatian</td>
      <td>CB</td>
      <td>20</td>
    </tr>
    <tr>
      <th>76</th>
      <td>Danish</td>
      <td>CB</td>
      <td>58</td>
    </tr>
    <tr>
      <th>38</th>
      <td>Dutch</td>
      <td>CB</td>
      <td>112</td>
    </tr>
    <tr>
      <th>1</th>
      <td>English</td>
      <td>CB</td>
      <td>496</td>
    </tr>
    <tr>
      <th>20</th>
      <td>French</td>
      <td>ST</td>
      <td>213</td>
    </tr>
    <tr>
      <th>14</th>
      <td>German</td>
      <td>CB</td>
      <td>244</td>
    </tr>
    <tr>
      <th>187</th>
      <td>Greek</td>
      <td>CB</td>
      <td>17</td>
    </tr>
    <tr>
      <th>62</th>
      <td>Irish</td>
      <td>CB</td>
      <td>66</td>
    </tr>
    <tr>
      <th>32</th>
      <td>Italian</td>
      <td>CB</td>
      <td>135</td>
    </tr>
    <tr>
      <th>60</th>
      <td>Japanese</td>
      <td>CB</td>
      <td>70</td>
    </tr>
    <tr>
      <th>70</th>
      <td>Korean</td>
      <td>CB</td>
      <td>61</td>
    </tr>
    <tr>
      <th>75</th>
      <td>Norwegian</td>
      <td>CM</td>
      <td>59</td>
    </tr>
    <tr>
      <th>83</th>
      <td>Polish</td>
      <td>GK</td>
      <td>52</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Portuguese</td>
      <td>CB</td>
      <td>190</td>
    </tr>
    <tr>
      <th>80</th>
      <td>Russian</td>
      <td>GK</td>
      <td>56</td>
    </tr>
    <tr>
      <th>126</th>
      <td>Serbian</td>
      <td>CB</td>
      <td>33</td>
    </tr>
    <tr>
      <th>0</th>
      <td>Spanish</td>
      <td>CB</td>
      <td>524</td>
    </tr>
    <tr>
      <th>65</th>
      <td>Swedish</td>
      <td>CB</td>
      <td>63</td>
    </tr>
    <tr>
      <th>101</th>
      <td>Turkish</td>
      <td>GK</td>
      <td>43</td>
    </tr>
  </tbody>
</table>
</div>



### Which LANGUAGES have the higher POTENTIAL based on players stats?

The Greek and Serbian speaker players have the higher potencial


```python
df_languages_top.groupby("Primary Language").Potential.median().sort_values(ascending=True).reset_index().\
    plot.barh(x='Primary Language',use_index=True,legend=False,figsize=(12,6))    
plt.show()
```


![png](output_25_0.png)


### Which LANGUAGES have the higher OVERALL based on players stats?

The portuguese speaker players have the higher overall on average


```python
df_languages_top.groupby("Primary Language").Overall.median().sort_values(ascending=True).reset_index().\
    plot.barh(x='Primary Language',use_index=True,legend=False,figsize = (12,6))    
plt.show()
```


![png](output_27_0.png)


### What is the median AGE for each LANGUAGE?

The dutch speaker players are the youngest on average


```python
df_languages_top.groupby("Primary Language").Age.median().sort_values(ascending=False).reset_index().\
    plot.barh(x='Primary Language',use_index=True,legend=False,figsize = (12,6))    
plt.show()
```


![png](output_29_0.png)


### What is the average WAGE by LANGUAGE?

The players who speak Croatian have the highest wage on average


```python
df_languages_top.groupby("Primary Language").Wage.mean().sort_values(ascending=False).apply(lambda x: '%.0f' % x)
```




    Primary Language
    Croatian      24546
    Portuguese    17233
    Serbian       16707
    Russian       16577
    Dutch         15313
    Italian       14353
    French        14280
    Turkish       13615
    Spanish       13006
    German        10892
    Arabic        10299
    English        9916
    Greek          8915
    Danish         7202
    Polish         7122
    Swedish        6374
    Irish          5882
    Norwegian      4060
    Japanese       4038
    Korean         3685
    Name: Wage, dtype: object



### Which is the LANGUAGE associated to the higher VALUE players on average?

The players who speak Croatian are the most valued on average


```python
df_languages_top.groupby("Primary Language").Value.mean().sort_values(ascending=False).apply(lambda x: '%.0f' % x)
```




    Primary Language
    Croatian      5165046
    Portuguese    4073830
    Serbian       3735159
    Dutch         3524551
    French        3230223
    Spanish       3067250
    Greek         3045236
    Italian       2694292
    German        2317796
    Turkish       2294845
    Russian       1973443
    Arabic        1734456
    Danish        1596257
    Polish        1496399
    English       1367656
    Swedish       1343757
    Norwegian      971556
    Korean         946488
    Japanese       798923
    Irish          682578
    Name: Value, dtype: object


