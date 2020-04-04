from collections import Counter
import nltk
import string
from nltk.tokenize import word_tokenize

import math

from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.graph_objs as go

init_notebook_mode(connected=True)

# load in the data.
df_list = []
cities = ['boston', 'chicago', 'la', 'montreal', 'ny', 'sf', 'toronto', 'vancouver']

for city in cities:
    df_tmp = pd.read_pickle('data_scientist_{}.pkl'.format(city))
    df_tmp['city'] = city
    df_list.append(df_tmp)

df = pd.concat(df_list).reset_index(drop=True)

# make the city names nicer.
msk = df['city'] == 'la'
df.loc[msk, 'city'] = 'los angeles'

msk = df['city'] == 'ny'
df.loc[msk, 'city'] = 'new york'

msk = df['city'] == 'sf'
df.loc[msk, 'city'] = 'san francisco'

# If it's the same job description in the same city, for the same job title, we consider it duplicate.
print(df.shape)
df = df.drop_duplicates(subset=['job_description', 'city', 'job_title'])
print(df.shape)