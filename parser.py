### PubMed data collection tool for PhD research ###
### Karl Hall, 13/03/2024                     ###

import pandas as pd
import os
from functools import reduce

# Throttle CPU usage
os.environ['NUMEXPR_MAX_THREADS'] = '4'
os.environ['NUMEXPR_NUM_THREADS'] = '2'

# Fetch API key to allow for higher API request limit
API_KEY = os.environ.get('NCBI_API_KEY')

# Performs PubMed search using the below keywords
keyword ='alzheimers disease genetics'

# Sets number of returned articles
num_of_articles = 10000

from metapub import PubMedFetcher
fetch = PubMedFetcher()

pmids = fetch.pmids_for_query(keyword, retmax=num_of_articles)

# Get article PubMed IDs
articles = {}
for pmid in pmids:
    try:
        articles[pmid] = fetch.article_by_pmid(pmid)
    except: articles[pmid] = "Error"

# Get article titles
titles = {}
for pmid in pmids:
    try:
        titles[pmid] = fetch.article_by_pmid(pmid).title
    except: titles[pmid] = "Error"
Title = pd.DataFrame(list(titles.items()),columns = ['pmid','Title'])

# Get article abstracts
abstracts = {}
for pmid in pmids:
    try: 
        abstracts[pmid] = fetch.article_by_pmid(pmid).abstract
    except: abstracts[pmid] = "Error"
Abstract = pd.DataFrame(list(abstracts.items()),columns = ['pmid','Abstract'])

# Get article author list
authors = {}
for pmid in pmids:
    try:
        authors[pmid] = fetch.article_by_pmid(pmid).authors
    except: authors[pmid] = "Error"
Author = pd.DataFrame(list(authors.items()),columns = ['pmid','Author'])

# Get article year of publication
years = {}
for pmid in pmids:
    try:
        years[pmid] = fetch.article_by_pmid(pmid).year
    except: years[pmid] = "Error"
Year = pd.DataFrame(list(years.items()),columns = ['pmid','Year'])

# Create dataframe with the above metadata
data_frames = [Title, Abstract, Author, Year]

df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['pmid'],
                                            how='outer'), data_frames)

# Export dataframe to csv file
df_merged.to_csv('outputfile.csv')