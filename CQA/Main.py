# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 10:13:12 2015

@author: Guanwen
"""

from Data import YahooData
from Data import Utility
from Calculation import MF_calc, PIO_calc, MOH_calc
import numpy as np
import pandas as pd

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
MSG        4.45B
ALLE       5.73B
GHC        3.37B
HME        4.37B
FLOW       22.72B
STAY.U     3.72B 

JW.A       JW-A
HUB.B      HUB-B
FCE.A      FCE-A
STAY.U     STAY
BF.B       BF-B
BF.A       BF-A
BRK.B      BRK-B
LEN.B      LEN-B
'''

universe = Utility.get_stock_universe('stock_universe.csv')
u_tick = universe['Tick'].unique().tolist()

#comp_MF_data = Utility.get_compustat_data('CQA_MF_data.csv', exchanges=['11', '12', '14'])
#comp_MOH_ad_data = Utility.get_compustat_data('CQA_MOH_AD_data.csv', exchanges=['11', '12', '14'])
#MOH_result = MOH_calc.Calc(comp_MOH_data, u_tick, comp_MOH_ad_data)

#get data from Yahoo
returns = YahooData.get_returns(u_tick)
EBITDAs = YahooData.get_value(u_tick, 'EBITDA')
ratios = YahooData.get_ratios(u_tick, ['PS', 'PE', 'PB'])
mkt_cap_df = YahooData.get_value(u_tick, 'Mkt_cap')
mkt_cap_df.ix['FLOW'] = 22720
mkt_cap_df.ix['MSG'] = 4450
mkt_cap_df.ix['ALLE'] = 5730
mkt_cap_df.ix['GHC'] = 3370
mkt_cap_df.ix['HME'] = 4370
mkt_cap_df[mkt_cap_df['Mkt_cap']=='N/A'] = np.nan

#calculate score based on compustat data
comp_MF_data = Utility.get_compustat_data('CQA_MF_data.csv', exchanges=['11', '12', '14'])
comp_PIO_data = Utility.get_compustat_data('CQA_PIO_data.csv', exchanges=['11', '12', '14'])
comp_MOH_data = Utility.get_compustat_data('CQA_MOH_data.csv', exchanges=['11', '12', '14'])
comp_MOH_ad_data = Utility.get_compustat_data('CQA_MOH_AD_data.csv', exchanges=['11', '12', '14'])
MF_result = MF_calc.Calc(comp_MF_data, u_tick, mkt_cap_df)
PIO_result = PIO_calc.Calc(comp_PIO_data, u_tick)
MOH_result = MOH_calc.Calc(comp_MOH_data, u_tick, comp_MOH_ad_data)


#join all the dataframe
universe.set_index('Tick', inplace=True)
result = universe.join(returns)
result = result.join(ratios)
result = result.join(mkt_cap_df)
result = result.join(EBITDAs)
result = result.join(MF_result)
result = result.join(PIO_result)
result = result.join(MOH_result)

result.sort_index(by='MF_score', ascending=False, inplace=True)
'''
result[result['PB']=='N/A'] = np.nan
result[result['PE']=='N/A'] = np.nan
result[result['PS']=='N/A'] = np.nan

result['bp'] = 1 / result['PB'].astype(float)
result['ep'] = 1 / result['PE'].astype(float)
result['sp'] = 1 / result['PS'].astype(float)
result['cp'] = result['trail_oancfy'] / result['Mkt_cap']

test = result[['1yr_rtn', 'bp', 'ep', 'sp', 'cp', 'ebit_ev']].copy()
test_rank = test.rank()
test_rank['value_score'] = test_rank['bp'] + test_rank['ep'] + test_rank['sp'] + test_rank['cp'] + test_rank['ebit_ev']
test_rank['value_rank'] = test_rank['value_score'].rank()
test_rank['composite_score'] = test_rank['value_rank'] + test_rank['1yr_rtn']
test_rank.sort_index(by='composite_score', ascending=False, inplace=True)
test_rank.to_csv(r'C:\Users\Guanwen\Google Drive\CQA\composite_score.csv')
#result.to_csv(r'C:\Users\Guanwen\Google Drive\CQA\CQA_MF_PIO_MOH_score.csv')
'''

















