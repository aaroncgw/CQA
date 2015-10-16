# -*- coding: utf-8 -*-
"""
Created on Fri Oct 02 09:25:57 2015

@author: Guanwen
"""

import pandas as pd
import numpy as np

def Calc(PIO_data, tickers=None):
    
    if tickers is not None:
        raw_data = PIO_data[PIO_data['tic'].isin(tickers)].copy()
    else:
        raw_data = PIO_data.copy()    
    
    #remove the tic which has less than 4 data points
    tic_count = pd.value_counts(raw_data['tic'])
    tic_list = tic_count[tic_count >= 8]
    raw_data = raw_data[raw_data['tic'].isin(tic_list.index)]
    
    #keep the first eight rows of each tic
    f_8q = lambda x:x.sort('datadate', ascending=False).head(8)
    raw_data = raw_data.groupby('tic').apply(f_8q)

    def missing_data_clean(x, check_list, num):
        x = x.sort('datadate', ascending=False).head(num)
        for col in check_list:
            if 0 in list(x[col]):
                return list(x['tic'])[0]
    
    #remove the tic which has zero revtq, cogsq and ibq             
    col_check = ['revtq', 'cogsq', 'ibq']
    checked_tic = raw_data.groupby('tic').apply(missing_data_clean, check_list=col_check, num=8)
    null_tic = checked_tic[~checked_tic.isnull()]
    criterion = lambda row: row['tic'] not in null_tic
    raw_data = raw_data[raw_data.apply(criterion, axis=1)]
    
    #remove the tic which has zero oancfy
    col_check = ['oancfy']
    checked_tic = raw_data.groupby('tic').apply(missing_data_clean, check_list=col_check, num=5)
    null_tic = checked_tic[~checked_tic.isnull()]
    criterion = lambda row: row['tic'] not in null_tic
    data = raw_data[raw_data.apply(criterion, axis=1)]

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
    
    #calculate one year trailing oancfy
    def trail_oancfy_calc(x):
        x = x.sort('datadate', ascending=False)    
        a = x['oancfy'][3:4].values[0]
        b = x['oancfy'][4:5].values[0]
        c = x['oancfy'][0:1].values[0]
        return a-b+c
        
    trail_oancfy = data.groupby('tic').apply(trail_oancfy_calc)
    trail_oancfy.name = 'trail_oancfy'
    
    #combine trailing data with most recent data
    cur = cur_q.join(cur_trail)
    cur = cur.join(trail_oancfy)
    
    #calculate current year's ratios
    cur['roa'] = cur['trail_ibq'] / cur['atq']
    cur['cfo'] = cur['trail_oancfy'] / cur['atq']
    cur['lever'] = cur['dlttq'] / cur['atq']
    cur['liquid'] = cur['actq'] / cur['lctq']
    cur['eq_offer'] = cur['cshoq']
    cur['margin'] = cur['trail_profit'] / cur['trail_rev']
    cur['turnover'] = cur['trail_rev'] / cur['atq'] 
    cur['accrual'] = cur['roa'] - cur['cfo']
    
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
        
    #PIO_cur = cur
    #PIO_score = PIO_cur.join(PIO_result)
    PIO_result = data_set.groupby('tic').apply(pio_score_calc)
    PIO_result.name = 'pio score'
    
    return PIO_result



#PIO_score.to_csv(r'C:\Users\Guanwen\Google Drive\CQA_PIO_score.csv')