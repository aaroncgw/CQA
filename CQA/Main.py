# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 10:13:12 2015

@author: Guanwen
"""

from Data import YahooData
from Data import Utility
from Calculation import MF_calc

'''{'BF.A',
 'CMCSK',
 'DISCK',
 'FOX',
 'GOOG',
 'IGT',
 'LBRDA',
 'LEN.B',
 'LMCK',
 'NWS',
 'STAY.U',
 'VIA',
 'Z'}'''
 
 '''
NRF        4.59B
EQC        3.75B
MSG        4.45B
ALLE       5.73B
GHC        3.37B
HME        4.37B
FLOW       22.72B 

JW.A       JW-A
HUB.B      HUB-B
FCE.A      FCE-A
STAY.U     STAY
BF.B       BF-B
BF.A       BF-A
BRK.B      BRK-B
LEN.B      LEN-B
'''

comp_data = Utility.get_compustat_data('CQA_MF_data.csv', exchanges=['11', '12', '14'])
universe = Utility.get_stock_universe('stock_universe.csv')

u_tick = universe['Tick'].unique().tolist()
u_comp_data = comp_data[comp_data['tic'].isin(u_tick)]

u_comp_data['ticker'] = u_comp_data['tic']
u_comp_data = u_comp_data.set_index('ticker')

mkt_cap_df = YahooData.get_market_cap(u_tick)
mkt_cap_df[mkt_cap_df['mkt_cap']=='N/A'] = np.nan
raw_data = u_comp_data.join(mkt_cap_df, how='left')

MF_result = MF_calc.Calc(raw_data)
#raw_data = raw_data.fillna(0)

'''
f = lambda x:x.sort('datadate', ascending=False).head(1)
raw_data_mr = raw_data.groupby('tic').apply(f)

tickers = raw_data_mr[raw_data_mr['mkvaltq']>1000]['tic'].unique().tolist()
mkt_cap_df = pd.DataFrame(get_market_cap(tickers).items(), columns=['ticker', 'mkt_cap'])
mkt_cap_df.set_index('ticker', inplace=True)
raw_data['ticker'] = raw_data['tic']
raw_data.set_index('ticker',inplace=True)
mkt_cap_df[mkt_cap_df['mkt_cap']=='N/A'] = np.nan
raw_data = raw_data.join(mkt_cap_df, how='left')


returns = get_returns(tickers)
returns_df = pd.DataFrame(returns.items(), columns=['ticker', '1yr_rtn'])
returns_df.set_index('ticker', inplace=True)

test = MF_result.join(PIO_result)
test = test.join(MOH_result)
tickers = test.index.tolist()


test.to_csv(r'C:\Users\Guanwen\Google Drive\CQA_MF_PIO_MOH_score.csv')


universe = pd.read_csv(r'C:\Users\Guanwen\Google Drive\CQA\stock_universe.csv')
universe.set_index('Tick', inplace=True)