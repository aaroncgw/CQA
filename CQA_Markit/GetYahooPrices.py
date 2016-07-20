# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 18:33:21 2015

@author: Guanwen
"""

import pandas as pd
from urllib2 import Request, urlopen
import urllib, re

PATH = "C:\Users\Guanwen\Google Drive\CQA\\"
BETA_NAME = "stock_universe.csv"
STOCKS_NAME = "CQA_picks.xlsx"


def _request(symbols, tags):
    symbol_str = str()        
    for symbol in symbols:    
        symbol_str = symbol_str + symbol + '+'
    symbol_str = symbol_str[:-1]
    tag_str = str() 
    for tag in tags:    
        tag_str = tag_str + tag
    url = 'http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s' % (symbol_str, tag_str)
    req = Request(url)
    resp = urlopen(req)
    return str(resp.read().decode('utf-8').strip())

def get_prices(symbols, pre_close=False):  
    tag = 'l1'    
    if pre_close:
        tag = 'p'
    price_str = _request(symbols, tag)
    price_list = price_str.split()
    price_dict = dict(zip(symbols, price_list))
    price_df = pd.DataFrame(price_dict.items(), columns=['Tick', 'value'])
    #if 'GOOG' in symbols:    
        #ratio_df[ratio_df['ticker']=='GOOG']['ticker'] = 'GOOGL'    
    price_df.set_index('Tick', inplace=True)
    price_df = price_df.value.apply(lambda x:pd.Series(x.split(',')))
    price_df.columns = ['price']
    price_df = price_df[price_df['price'] != 'N/A']
    price_df["price"] = price_df["price"].astype(float)
    return price_df
    
def get_sector_industry(symbols):
    return_dict = {}    
    for symbol in symbols:    
        url = 'http://finance.yahoo.com/q/pr?s=' + symbol + '+Profile'
        htmlfile = urllib.urlopen(url)
        htmltext = htmlfile.read()
        sector = 'html">(.+?)</a></td></tr><tr><td class="yfnc_tablehead1" width="50%">Industry:'
        industry = '<a href="http://biz.yahoo.com/ic/(.+?)</a></td></tr><tr><td class="yfnc_tablehead1" width="50%">Full Time Employees:'
        sector_pattern = re.compile(sector)
        industry_pattern = re.compile(industry)
        sector = re.findall(sector_pattern, htmltext)
        industry = re.findall(industry_pattern, htmltext)
        return_dict[symbol] = (sector + industry)
    return_df = pd.DataFrame(return_dict.items(), columns=['tic', 'sector_industry'])
    return_df.set_index('tic', inplace=True)    
    return return_df
    
stocks_list = pd.ExcelFile(PATH + STOCKS_NAME)

df_long = stocks_list.parse("Long")
df_long.set_index('Tick', inplace=True)
df_long_prices = get_prices(df_long.index.tolist())
df_long_prices.sort_index(inplace=True)

df_short = stocks_list.parse("Short")
df_short.set_index('Tick', inplace=True)
df_short_prices = get_prices(df_short.index.tolist())
df_short_prices.sort_index(inplace=True)

df_prices = pd.concat([df_long_prices, df_short_prices])

df_beta = pd.read_csv(PATH + BETA_NAME, dtype='unicode')
df_beta = df_beta[df_beta['Tick'].isin(df_prices.index.tolist())] 
df_beta.set_index("Tick", inplace=True)

#comp_info = get_sector_industry(df_prices.index.tolist())
#df = df_prices.join(comp_info)

#df = df.join(df_beta)[['Company', 'sector_industry', 'Beta', 'price']]
df = df_prices.join(df_beta)[['Beta', 'price']]

df.to_csv(r"C:\Users\Guanwen\Google Drive\CQA\CQA_Prices.csv")    

    

