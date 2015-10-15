from urllib2 import Request, urlopen
import numpy as np
import pandas as pd


def _request(symbols, stat):
    symbol_str = str()    
    for symbol in symbols:    
        symbol_str = symbol_str + symbol + '+'
    symbol_str = symbol_str[:-1]
    url = 'http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s' % (symbol_str, stat)
    req = Request(url)
    resp = urlopen(req)
    return str(resp.read().decode('utf-8').strip())

def get_PS(symbols):
    value_str = _request(symbols, 'p5')
    values = value_str.split()
    ps_dict = dict(zip(symbols, values))
    ps_df = pd.DataFrame(ps_dict.items(), columns=['ticker', 'PS'])
    ps_df.set_index('ticker', inplace=True)        
    return ps_df
    
def get_PB(symbols):
    value_str = _request(symbols, 'p6')
    values = value_str.split()
    pb_dict = dict(zip(symbols, values))        
    pb_df = pd.DataFrame(pb_dict.items(), columns=['ticker', 'PS'])
    pb_df.set_index('ticker', inplace=True)        
    return pb_df
    
def get_PE(symbols):
    value_str = _request(symbols, 'r')
    values = value_str.split()
    pe_dict = dict(zip(symbols, values))        
    pe_df = pd.DataFrame(pe_dict.items(), columns=['ticker', 'PS'])
    pe_df.set_index('ticker', inplace=True)        
    return pe_df

def get_market_cap(symbols):
    mkt_cap = list()    
    value_str = _request(symbols, 'j1')
    values = value_str.split()
    for value in values:    
        if 'M' in value:
            value = float(value[:len(value)-2])
        elif 'B' in value:
            value = float(value[:len(value)-2]) * pow(10, 3)
        mkt_cap.append(value)
    mkt_cap_dict = dict(zip(symbols, mkt_cap))
    mkt_cap_df = pd.DataFrame(mkt_cap_dict.items(), columns=['ticker', 'mkt_cap'])
    mkt_cap_df.set_index('ticker', inplace=True)
    return mkt_cap_df
    
def get_returns(symbols):  
    rnt_dict = dict()    
    for symbol in symbols:    
        try:        
            url = 'http://ichart.finance.yahoo.com/table.csv?s=%s&a=09&b=9&c=2014&d=09&e=9&f=2015&g=d&ignore=.csv' % (symbol)
            req = Request(url)
            resp = urlopen(req)
            value_str = str(resp.read().decode('utf-8').strip())
            prices = value_str.split('\n')
            cur_price = float(prices[1].split(',')[-1])
            pre_price = float(prices[-1].split(',')[-1])
            rnt = (cur_price - pre_price) / pre_price
            rnt_dict[symbol] = rnt
        except:
            rnt_dict[symbol] = np.nan
    rnt_df = pd.DataFrame(rnt_dict.items(), columns=['ticker', '1yr_rtn'])
    rnt_df.set_index('ticker', inplace=True)
    return rnt_df