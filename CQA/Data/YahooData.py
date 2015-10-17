from urllib2 import Request, urlopen
import numpy as np
import pandas as pd

ratios_tags_dict = {'PS':'p5', 'PB':'p6', 'PE':'r'}
value_tags_dict = {'Mkt_cap':'j1', 'EBITDA':'j4'}

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

def get_ratios(symbols, ratios):
    tags = [ratios_tags_dict[ratio] for ratio in ratios]
    ratio_str = _request(symbols, tags)
    ratio_list = ratio_str.split()
    ratio_dict = dict(zip(symbols, ratio_list))
    ratio_df = pd.DataFrame(ratio_dict.items(), columns=['ticker', 'value'])
    #if 'GOOG' in symbols:    
        #ratio_df[ratio_df['ticker']=='GOOG']['ticker'] = 'GOOGL'    
    ratio_df.set_index('ticker', inplace=True)
    ratio_df = ratio_df.value.apply(lambda x:pd.Series(x.split(',')))
    ratio_df.columns = ratios
    return ratio_df
    
def get_value(symbols, value_name):
    tag = value_tags_dict[value_name]
    tickers = [tic.replace('.', '-') for tic in symbols]    
    value_list = list()    
    value_str = _request(tickers, tag)
    values = value_str.split()
    for value in values:    
        if 'M' in value:
            value = float(value[:len(value)-2])
        elif 'B' in value:
            value = float(value[:len(value)-2]) * pow(10, 3)
        value_list.append(value)
    value_dict = dict(zip(symbols, value_list))
    value_df = pd.DataFrame(value_dict.items(), columns=['ticker', value_name])
    #if 'GOOG' in symbols:    
        #value_df[value_df['ticker']=='GOOG']['ticker'] = 'GOOGL'
    value_df.set_index('ticker', inplace=True)
    return value_df
    
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
    #if 'GOOG' in symbols:    
        #rnt_df[rnt_df['ticker']=='GOOG']['ticker'] = 'GOOGL'    
    rnt_df.set_index('ticker', inplace=True)
    return rnt_df