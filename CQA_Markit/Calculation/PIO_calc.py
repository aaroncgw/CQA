# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 17:12:13 2015

@author: Guanwen
"""

import pandas as pd
import numpy as np
from Data import YahooData

def Calc(PIO_data, tickers=None, details=False):
    
    if tickers is not None:
        raw_data = PIO_data[PIO_data['tic'].isin(tickers)].copy()
    else:
        raw_data = PIO_data.copy() 
    
    raw_data[['revtq', 'cogsq', 'ibq', 'atq', 'dlttq', 'actq', 'lctq', 'cshoq']] = raw_data[['revtq', 'cogsq', 'ibq', 'atq', 'dlttq', 'actq', 'lctq', 'cshoq']].fillna(0)
    raw_data[['revtq', 'cogsq', 'ibq', 'atq', 'dlttq', 'actq', 'lctq', 'cshoq']] = raw_data[['revtq', 'cogsq', 'ibq', 'atq', 'dlttq', 'actq', 'lctq', 'cshoq']].astype(float)
    
    #keep the first eight rows of each tic
    f_8q = lambda x:x.sort('datadate', ascending=False).head(8)
    raw_data = raw_data.groupby('tic').apply(f_8q)

    data = raw_data.copy()   
    data['profit'] = data['revtq'] - data['cogsq']
    
    #calculate two year's trailling ibq, profit. rev
    f1st_y = lambda x:x.sort('datadate', ascending=False)[0:4]
    group_cur = data.groupby('tic').apply(f1st_y)
    
    f2nd_y = lambda x:x.sort('datadate', ascending=False)[4:8]
    group_pre = data.groupby('tic').apply(f2nd_y)


    cur_trail = group_cur.groupby(['tic'])[['ibq','profit', 'revtq']].sum()
    pre_trail = group_pre.groupby(['tic'])[['ibq','profit', 'revtq']].sum()
    
    cur_trail.columns = ['trail_ibq', 'trail_profit', 'trail_rev']
    pre_trail.columns = ['trail_ibq', 'trail_profit', 'trail_rev']
    
    #calculate most recent and one year before most recent values
    f3 = lambda x:x.sort('datadate', ascending=False)[0:1]
    cur_q = data.groupby('tic').apply(f3)
    
    f4 = lambda x:x.sort('datadate', ascending=False)[4:5]
    pre_q = data.groupby('tic').apply(f4)
    
    trail_cfo = YahooData.get_cfo(tickers)
    
    #combine trailing data with most recent data
    cur = cur_q.join(cur_trail)
    cur = cur.join(trail_cfo)
    
    #calculate current year's ratios
    cur['roa'] = cur['trail_ibq'] / cur['atq']
    cur['cfo'] = cur['trail_cfo'] / cur['atq']
    cur['lever'] = cur['dlttq'] / cur['atq']
    cur['liquid'] = cur['actq'] / cur['lctq']
    cur['eq_offer'] = cur['cshoq']
    cur['margin'] = cur['trail_profit'] / cur['trail_rev']
    cur['turnover'] = cur['trail_rev'] / cur['atq'] 
    cur['accrual'] = cur['trail_ibq'] - cur['trail_cfo']
    
    #calculate last year's ratios 
    pre = pre_q.join(pre_trail)
    
    pre['roa'] = pre['trail_ibq'] / pre['atq']
    pre['lever'] = pre['dlttq'] / pre['atq']
    pre['liquid'] = pre['actq'] / pre['lctq']
    pre['eq_offer'] = pre['cshoq']
    pre['margin'] = pre['trail_profit'] / pre['trail_rev']
    pre['turnover'] = pre['trail_rev'] / pre['atq'] 
    
    data_set = pd.concat([cur, pre])
    
    #calculate pio score
    def pio_score_calc(x):
        x = x.sort('datadate', ascending=False)
        score = 0
        if (x['roa'].head(1) > x['roa'].tail(1)).bool(): 
            score = score + 1
        if (x['cfo'].head(1) > 0).bool(): 
            score = score + 1
        if (x['roa'].head(1) > 0).bool(): 
            score = score + 1
        if (x['accrual'].head(1) <= 0).bool(): 
            score = score + 1
        if (x['lever'].head(1) <= x['lever'].tail(1)).bool(): 
            score = score + 1
        if (x['liquid'].head(1) > x['liquid'].tail(1)).bool(): 
            score = score + 1
        if (x['eq_offer'].head(1) <= x['eq_offer'].tail(1)).bool(): 
            score = score + 1
        if (x['margin'].head(1) > x['margin'].tail(1)).bool(): 
            score = score + 1
        if (x['turnover'].head(1) > x['turnover'].tail(1)).bool(): 
            score = score + 1
        
        return score
        
    PIO_result = data_set.groupby('tic').apply(pio_score_calc)
    PIO_result.name = 'pio_score'
    #cur.reset_index(inplace=True)
    cur.set_index('tic', inplace=True)
    pre.set_index('tic', inplace=True)    
    PIO_score = cur.join(PIO_result)
    if details:
        detailed_result = pd.concat([PIO_score, pre])
        detailed_result['ticker'] = detailed_result.index.tolist()
        detailed_result = detailed_result.sort(['ticker', 'datadate'], ascending=[True, False])        
        detailed_result = detailed_result[['datadate', 'trail_ibq', 'trail_cfo', 'trail_profit', 'trail_rev', 'dlttq', 'atq', 'actq', 'lctq', 'cshoq', 'roa', 'cfo', 'accrual', 'lever', 'liquid', 'eq_offer', 'margin', 'turnover', 'pio_score']]
        detailed_result.columns = ['datadate', 'trail_ibq', 'trail_cfo', 'trail_profit', 'trail_rev', 'long_term_debt', 'total_assets', 'current_assets', 'current_liability', 'shares_outstanding', 'roa', 'cfo', 'accrual', 'lever', 'liquid', 'eq_offer', 'margin', 'turnover', 'pio_score']        
        return detailed_result
    else:
       return PIO_result
 
    #data_set.set_index('tic', inplace=True)
    #data_set.sort_index(inplace=True)
    #data_set.to_csv(r'C:\Users\Guanwen\Google Drive\CQA_PIO_score.csv')
    #raw_data.to_csv(r'C:\Users\Guanwen\Google Drive\CQA_PIO_raw.csv')
    
