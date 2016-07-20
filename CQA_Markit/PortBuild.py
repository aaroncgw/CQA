import pandas as pd
import numpy as np

from urllib2 import Request, urlopen

PATH = "C:\Users\Guanwen\Google Drive\CQA\Data\\"
BETA_NAME = "stock_universe.csv"
STOCKS_NAME = "CQA_picks.xlsx"

CAPITAL = 10000000
LONG_CAPITAL = 9900000
SHORT_CAPITAL = 9900000

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

def portfolio_calc(raw_data_df, capital):
    data_df = raw_data_df.copy()    
    data_df["Beta_inverse"] = 1 / data_df["Beta"]
    s = data_df["Beta_inverse"].sum()
    data_df["weights"] =  data_df.apply(lambda row: weights_calc(row, s),axis=1)
    data_df["raw_shares"] = data_df["weights"] * capital / data_df["price"]
    data_df["shares"] = 100 * np.round(data_df["raw_shares"] * 0.01, 0)
    data_df["capital"] = data_df["price"] * data_df["shares"]
    data_df["actual_weights"] = data_df["capital"] / CAPITAL
    data_df["actual_beta"] = data_df["actual_weights"] * data_df["Beta"]
    return data_df
    
def weights_calc(row, s):      
    weight = 1 / (row["Beta"] * s)    
    if row["score"] < 0:
        weight = -weight
    return weight

df_beta = pd.read_csv(PATH + BETA_NAME, dtype='unicode')
df_beta.set_index("Tick", inplace=True)
df_beta['Beta'] = df_beta['Beta'].astype(float)
df_beta = df_beta[np.isfinite(df_beta['Beta'])]
df_beta = df_beta[df_beta["Beta"] > 0]


stocks_list = pd.ExcelFile(PATH + STOCKS_NAME)
df_long = stocks_list.parse("Long")
df_long['score'] = 1
df_long.set_index("Tick", inplace=True)
df_short = stocks_list.parse("Short")
df_short['score'] = -1
df_short.set_index("Tick", inplace=True)


df_long_prices = get_prices(df_long.index.tolist())
df_long = df_long.join(df_long_prices)
df_long = df_long.join(df_beta)
long_positions_df = portfolio_calc(df_long, LONG_CAPITAL)

df_short_prices = get_prices(df_short.index.tolist())
df_short = df_short.join(df_short_prices)
df_short = df_short.join(df_beta)
short_positions_df = portfolio_calc(df_short, SHORT_CAPITAL)


'''
longshort_df = pd.concat([df_long, df_short])
#longshort_df.set_index("Tick", inplace=True)

df_prices = get_prices(longshort_df.index.tolist())
longshort_df = longshort_df.join(df_prices)
longshort_df = longshort_df.join(df_beta)

positions_df  = portfolio_calc(longshort_df, CAPITAL)
'''
positions_df = pd.concat([long_positions_df, short_positions_df])

positions_df.to_csv(r"C:\Users\Guanwen\Google Drive\CQA\CQA_Positions.csv")


actual_long_capital = positions_df[positions_df['score']>0]['capital'].sum()
actual_short_capital = positions_df[positions_df['score']<0]['capital'].sum()

net_mkt_value = actual_long_capital + actual_short_capital
#positions_df[positions_df['score']>0]['capital'].sum()
#positions_df[positions_df['score']<0]['capital'].sum()

total_beta = positions_df['actual_beta'].sum()













