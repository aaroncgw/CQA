from urllib2 import Request, urlopen
import urllib, re
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime

ratios_tags_dict = {'PS':'p5', 'PB':'p6', 'PE':'r', '50ma':'m3', '200ma':'m4', 'Price':'p'}
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
    cur_date = datetime.datetime.now()
    cur_year = str(cur_date.year)
    cur_month = str(cur_date.month - 2)
    cur_day = str(cur_date.day)    
    pre_date = datetime.datetime.now() - relativedelta(years=1)
    pre_year = str(pre_date.year)
    pre_month = str(pre_date.month - 1)
    pre_day = str(pre_date.day)        
    for symbol in symbols:    
        try:        
            url = 'http://ichart.finance.yahoo.com/table.csv?s='+symbol+'&a='+pre_month+'&b='+pre_day+'&c='+pre_year+'&d='+cur_month+'&e='+cur_day+'&f='+cur_year+'&g=d&ignore=.csv'
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

def get_ev(symbols):
    ev = None
    ev_dict = dict()    
    for symbol in symbols:
        value = keystatfunc(symbol)[0]
        if 'M' in value:
            ev = float(value[:len(value)-2])
        elif 'B' in value:
            ev = float(value[:len(value)-2]) * pow(10, 3)
        ev_dict[symbol] = ev
    ev_df = pd.DataFrame(ev_dict.items(), columns=['ticker', 'ev'])
    return ev_df
    
def get_ev_ebitda(symbols):
    ebitda_ev_dict = dict()    
    for symbol in symbols:
        try: 
            value = keystatfunc(symbol)[7]
            if value == "N/A":
                ebitda_ev_dict[symbol] = np.nan
            else:
                ebitda_ev_dict[symbol] = 1 / float(value)
        except:
            ebitda_ev_dict[symbol] = np.nan
    ebitda_ev_df = pd.DataFrame(ebitda_ev_dict.items(), columns=['ticker', 'ebitda_ev'])
    ebitda_ev_df.set_index('ticker', inplace=True)
    return ebitda_ev_df
    
value_name_list = ['Enterprise Value', 
                    'Trailing P/E', 
                    'Forward P/E', 
                    'PEG Ratio', 
                    'P/S', 
                    'P/B', 
                    'EV/Revenue', 
                    'EV/EBITDA', 
                    'Fiscal Year Ends:', 
                    'Most Recent Quarter', 
                    'Profit Margin', 
                    'Operating Margin', 
                    'ROA', 
                    'ROE', 
                    'Revenue', 
                    'Revenue Per Share', 
                    'Qtrly Revenue Growth',
                    'Gross Profit',
                    'EBITDA',
                    'Net Income Avl to Common',
                    'Diluted EPS',
                    'Qtrly Earnings Growth',
                    'Total Cash',
                    'Total Cash Per Share',
                    'Total Debt',
                    'Total Debt/Equity',
                    'Current Ratio',
                    'Book Value Per Share',
                    'Operating Cash Flow',
                    'Levered Free Cash Flow']  
                    
def keystatfunc(symbol):
    keystat = '<td class="yfnc_tabledata1">(.+?)</td>'  
    url = 'http://finance.yahoo.com/q/ks?s=' + symbol + '+Key+Statistics'
    htmlfile = urllib.urlopen(url)
    htmltext = htmlfile.read()
    regex = '<span id="yfs_j10_' + symbol + '">(.+?)</span>'
    pattern = re.compile(regex)
    pattern2 = re.compile(keystat)
    marketcap = re.findall(pattern, htmltext)
    keystats = re.findall(pattern2, htmltext)
    return (marketcap + keystats[1:31])
    

















