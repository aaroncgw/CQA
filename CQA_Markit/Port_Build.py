# -*- coding: utf-8 -*-
"""
Created on Sun Nov 01 16:53:34 2015

@author: Guanwen
"""

import pandas as pd
import numpy as np

from urllib2 import Request, urlopen

PATH = "C:\Users\Guanwen\Google Drive\CQA\Data\\"
BETA_NAME = "stock_universe.csv"
RANK_NAME = "Ranks_IllinoisTechGcheng_09302015.xls"

CAPITAL = 10000000

columns = ["Ticker", "Sector", "Name", "Price", "MktCap", "rank_score", "Book_Lever", "EPS_Dis", "Ahead_EPS", "6m_Return", "52w_Trend", "3m_EPS_Dis", "Dollar_Sens", "DS_ratio", "Profit_Assets", "Profit_Margin", "EBITDA_EV", "Sales_Yield"]

secotr_bets_dict = { "Cyclical Goods & Services": 18,
                     "Energy" : 16,
                     "Non-Cyclical Goods & Services" : 14,                          
                     "Utilities": 12,
                     "Industrials" : 10,
                     "Basic Materials" : 10,
                     "Technology" : 6,
                     "Telecommunication Services" : 6,
                     "Financials" : 4,
                     "Healthcare" : 4 }

def weights_calc(data_df, sector, bets, sector_capital):    
    sector_df = data_df[data_df["Sector"] == sector].copy()
    sector_df.sort_index(by="score", inplace=True, ascending=False)
    
    buy_df = sector_df.head(int(0.5*bets)).copy()
    buy_df["Beta_inverse"] = 1 / buy_df["Beta"]
    s1 = buy_df["Beta_inverse"].sum()
    buy_df["weights"] =  np.round(1 / (buy_df["Beta"] * s1), 2)
    buy_df["raw_shares"] = buy_df["weights"] * sector_capital / buy_df["current_price"]
    buy_df["shares"] = 100 * np.round(buy_df["raw_shares"] * 0.01, 0)
    buy_df["capital"] = buy_df["current_price"] * buy_df["shares"]
    
    sell_df = sector_df.tail(int(0.5*bets)).copy()
    sell_df["Beta_inverse"] = 1 / sell_df["Beta"]
    s1 = sell_df["Beta_inverse"].sum()
    sell_df["weights"] =  np.round(1 / (sell_df["Beta"] * s1), 2)
    sell_df["raw_shares"] = sell_df["weights"] * sector_capital / sell_df["current_price"]
    sell_df["shares"] = -100 * np.round(sell_df["raw_shares"] * 0.01, 0)
    sell_df["capital"] = sell_df["current_price"] * sell_df["shares"]
    
    return pd.concat([buy_df, sell_df])


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

def get_prices(symbols):  
    price_str = _request(symbols, 'p')
    price_list = price_str.split()
    price_dict = dict(zip(symbols, price_list))
    price_df = pd.DataFrame(price_dict.items(), columns=['ticker', 'value'])
    #if 'GOOG' in symbols:    
        #ratio_df[ratio_df['ticker']=='GOOG']['ticker'] = 'GOOGL'    
    price_df.set_index('ticker', inplace=True)
    price_df = price_df.value.apply(lambda x:pd.Series(x.split(',')))
    price_df.columns = ['current_price']
    price_df = price_df[price_df['current_price'] != 'N/A']
    price_df["current_price"] = price_df["current_price"].astype(float)
    return price_df

df_beta = pd.read_csv(PATH + BETA_NAME, dtype='unicode')
df_beta.set_index("Tick", inplace=True)
df_beta['Beta'] = df_beta['Beta'].astype(float)
df_beta = df_beta[np.isfinite(df_beta['Beta'])]
df_beta = df_beta[df_beta["Beta"] > 0]

df_prices = get_prices(df_beta.index.tolist())

xl = pd.ExcelFile(PATH + RANK_NAME)
df_ranks = xl.parse("Ranks")
df_ranks.columns = columns
df_ranks.set_index("Ticker", inplace=True)

df_ranks[["rank_score", "Book_Lever", "EPS_Dis", "Ahead_EPS", "6m_Return", "52w_Trend", "3m_EPS_Dis", "Dollar_Sens", "DS_ratio", "Profit_Assets", "Profit_Margin", "EBITDA_EV", "Sales_Yield"]] = df_ranks[["rank_score", "Book_Lever", "EPS_Dis", "Ahead_EPS", "6m_Return", "52w_Trend", "3m_EPS_Dis", "Dollar_Sens", "DS_ratio", "Profit_Assets", "Profit_Margin", "EBITDA_EV", "Sales_Yield"]].fillna(0)                     
df_ranks[["rank_score", "Book_Lever", "EPS_Dis", "Ahead_EPS", "6m_Return", "52w_Trend", "3m_EPS_Dis", "Dollar_Sens", "DS_ratio", "Profit_Assets", "Profit_Margin", "EBITDA_EV", "Sales_Yield"]] = df_ranks[["rank_score", "Book_Lever", "EPS_Dis", "Ahead_EPS", "6m_Return", "52w_Trend", "3m_EPS_Dis", "Dollar_Sens", "DS_ratio", "Profit_Assets", "Profit_Margin", "EBITDA_EV", "Sales_Yield"]].replace('%','',regex=True).astype('float')
df_ranks["score"] = df_ranks["Book_Lever"] + df_ranks["EPS_Dis"] + df_ranks["Ahead_EPS"] + df_ranks["6m_Return"] + df_ranks["52w_Trend"] + df_ranks["3m_EPS_Dis"] + df_ranks["Dollar_Sens"] + df_ranks["DS_ratio"] + df_ranks["Profit_Assets"] + df_ranks["EBITDA_EV"] + df_ranks["Book_Lever"] + df_ranks["Sales_Yield"] + df_ranks["Book_Lever"]

df = df_beta.join(df_ranks)
df = df_prices.join(df)

buysell_df = pd.DataFrame()

for key, value in secotr_bets_dict.iteritems():
    if value != 0:    
        buysell_df = pd.concat([buysell_df, weights_calc(df, key, value, 0.01*value*CAPITAL)])
        
buysell_df.to_csv(r"C:\Users\Guanwen\Google Drive\CQA\CQA_Positions.csv")

    
             


            
             

