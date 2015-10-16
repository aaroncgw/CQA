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

comp_MF_data = Utility.get_compustat_data('CQA_MF_data.csv', exchanges=['11', '12', '14'])


universe = Utility.get_stock_universe('stock_universe.csv')



u_tick = universe['Tick'].unique().tolist()
u_comp_data = comp_MF_data[comp_MF_data['tic'].isin(u_tick)].copy()
u_comp_data['ticker'] = u_comp_data['tic']
u_comp_data = u_comp_data.set_index('ticker')


mkt_cap_df = YahooData.get_market_cap(u_tick)
mkt_cap_df.ix['FLOW'] = 22720
mkt_cap_df.ix['MSG'] = 4450
mkt_cap_df.ix['ALLE'] = 5730
mkt_cap_df.ix['GHC'] = 3370
mkt_cap_df.ix['HME'] = 4370

mkt_cap_df[mkt_cap_df['mkt_cap']=='N/A'] = np.nan

raw_data = u_comp_data.join(mkt_cap_df, how='left')

MF_result = MF_calc.Calc(raw_data)

comp_PIO_data = Utility.get_compustat_data('CQA_PIO_data.csv', exchanges=['11', '12', '14'])
comp_MOH_data = Utility.get_compustat_data('CQA_MOH_data.csv', exchanges=['11', '12', '14'])
PIO_result = PIO_calc.Calc(comp_PIO_data, MF_result.index.tolist())
MOH_result = MOH_calc.Calc(comp_MOH_data, MF_result.index.tolist())

universe.set_index('Tick', inplace=True)
result = universe.join(MF_result)
result = result.join(PIO_result)
result = result.join(MOH_result)
result.sort_index(by='MF_score', ascending=False, inplace=True)

result.to_csv(r'C:\Users\Guanwen\Google Drive\CQA\CQA_MF_PIO_MOH_score.csv')
