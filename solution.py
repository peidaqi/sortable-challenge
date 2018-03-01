
# coding: utf-8

import pandas as pd
from functools import reduce
import numpy as np
from nltk.tokenize import wordpunct_tokenize
import string
import json
from multiprocessing import Pool
import os

#
# Use cpuinfo and grep to get number of cores - since Linux is the only platform to consider
#
def get_cpu_core_counts():
    ret = os.popen('cat /proc/cpuinfo | grep \'processor\' | sort | uniq | wc -l').readlines()
    return int(ret[0].strip())


prod_df = pd.read_json('products.txt', orient='records', lines=True)
list_df = pd.read_json('listings.txt', orient='records', lines=True)

#
# Tokenize product names using nltk and convert them into sets
# Remove punctuations in this process
#

prod_df['product_name_set'] = prod_df['product_name'].apply(lambda x: set(wordpunct_tokenize(''.join([elm.upper() if not elm in string.punctuation else ' ' 
                                         for elm in x]))))
list_df['title_set'] = list_df['title'].apply(lambda x: set(wordpunct_tokenize(''.join([elm.upper() if not elm in string.punctuation else ' ' 
                                         for elm in x]))))


if __name__ == '__main__':
    list_ref_array = np.copy(prod_df.loc[:, 'product_name_set'].values)
    
    def fn(val):
        #
        # Find all list items which the current product name is a subset of
        #
        list_df['prod_idx'] = False
        def fn1(row1):
            if val.issubset(row1['title_set']):
                row1['prod_idx'] = True
            return row1

        tmp_df = list_df.apply(fn1, axis=1)
        return list(tmp_df[tmp_df['prod_idx'] == True].index)

    #   
    # Using all cores available for processing
    #
    with Pool(processes=get_cpu_core_counts()) as pool:
        result = pool.map_async(fn, list_ref_array)
        list_ref_array = result.get()

#
# Generate title first pass listings from title set
#
prod_df['listings_ref'] = list_ref_array
prod_df['initial_listings'] = prod_df['listings_ref'].apply(lambda x: list_df.drop('title_set', axis=1).loc[x, :].to_json(orient='records', lines=False))


#
# We need to filter out some invalid items, e.g. camera tri-pods which includes the same name as the camera.
# These are normally much cheaper or more expensive, so we use a mean +/- mult * std as the cutoff
#
prod_df['listings_ref_price'] = list_ref_array
PRICE_CUT_STD_MULT = 1.8

def fn1(row):
    row['listings_ref_price'] = list(list_df.loc[row['listings_ref'], 'price'])
    mean = np.mean(row['listings_ref_price'])
    std = np.std(row['listings_ref_price'])
    final_listings = []
    for idx, elm in enumerate(row['listings_ref_price']):
        if elm >= mean - std * PRICE_CUT_STD_MULT and elm <= mean + std * PRICE_CUT_STD_MULT:
            final_listings.append(json.loads(row['initial_listings'])[idx])
    row['listings'] = final_listings
    return row

prod_df = prod_df.apply(fn1, axis=1)



prod_df[['product_name', 'listings']].to_json('results.txt', orient='records', lines=True)

