# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 12:16:59 2015

@author: Guanwen
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Oct 03 08:59:14 2015

@author: Guanwen
"""

import pandas as pd
import numpy as np

def Calc(MOH_data, tickers=None, ad_data=None):
    
    if tickers is not None:
        raw_data = MOH_data[MOH_data['tic'].isin(tickers)].copy()
        if ad_data is not None:
            raw_ad_data = ad_data[ad_data['tic'].isin(tickers)].copy()
    else:
        raw_data = MOH_data.copy()
    
    raw_data[['oancfy', 'atq', 'revtq', 'ibq', 'xrdq', 'capxy']] = raw_data[['oancfy', 'atq', 'revtq', 'ibq', 'xrdq', 'capxy']].fillna(0)    
    raw_data[['oancfy', 'atq', 'revtq', 'ibq', 'xrdq', 'capxy']] = raw_data[['oancfy', 'atq', 'revtq', 'ibq', 'xrdq', 'capxy']].astype(float)
    
    #remove the tic which has less than 16 data points
    tic_count = pd.value_counts(raw_data['tic'])
    tic_list = tic_count[tic_count >= 16]
    raw_data = raw_data[raw_data['tic'].isin(tic_list.index)]
    
    #keep the first 16 rows of each tic
    f_16q = lambda x:x.sort('datadate', ascending=False).head(16)
    raw_data = raw_data.groupby('tic').apply(f_16q)

    def missing_data_clean(x, check_list, num):
        x = x.sort('datadate', ascending=False).head(num)
        for col in check_list:
            if 0 in list(x[col]):
                return list(x['tic'])[0]
    
    col_check = ['oancfy']
    checked_tic = raw_data.groupby('tic').apply(missing_data_clean, check_list=col_check, num=5)
    if len(checked_tic.isnull()) != 0:
        null_tic = checked_tic[~checked_tic.isnull()]
        criterion = lambda row: row['tic'] not in null_tic
        raw_data = raw_data[raw_data.apply(criterion, axis=1)]
    
    col_check = ['atq', 'revtq', 'ibq']
    checked_tic = raw_data.groupby('tic').apply(missing_data_clean, check_list=col_check, num=16)
    if len(checked_tic.isnull()) != 0:
        null_tic = checked_tic[~checked_tic.isnull()]
        criterion = lambda row: row['tic'] not in null_tic
        raw_data = raw_data[raw_data.apply(criterion, axis=1)]    
    
    data = raw_data.copy()
    data['sic2'] = data['sic'].apply(str).str[:2]
    data['roa_q'] = data['ibq'] / data['atq']
    
    #current trailing ibq, R&D, capxy         
    f1 = lambda x:x.sort('datadate', ascending=False)[0:4]
    group_cur = data.groupby('tic').apply(f1)
    trail_record = group_cur.groupby(['tic'])[['ibq', 'xrdq', 'capxy']].sum()
    trail_record.name = 'trail_record'
    trail_record.columns = ['trail_ibq', 'trail_xrdq', 'trail_capxy']
    
    #calculate one year trailing oancfy
    def trail_oancfy_calc(x):
        try:        
            x = x.sort('datadate', ascending=False)    
            a = x[x['fqtr']== '4']['oancfy'].head(1).values[0] 
            last_fqtr = x['fqtr'].head(1).values[0]
            b = x[x['fqtr']==last_fqtr]['oancfy'].head(2).values[1] 
            c = x['oancfy'][0:1].values[0]
            return a-b+c
        except:
            print list(x['tic'])[0]
        
    trail_oancfy = data.groupby('tic').apply(trail_oancfy_calc)
    trail_oancfy.name = 'trail_oancfy'
    
    #quartly ROA variance
    f2 = lambda x:x.sort('datadate', ascending=False)[0:16]
    group_roa = data.groupby('tic').apply(f2)
    roa_q_var = group_roa.groupby(['tic'])[['roa_q']].var()
    roa_q_var.name = 'roa_q_var'
    roa_q_var.columns = ['roa_q_var']
    
    #quartly sales growth variance
    def sale_growth_var_calc(x):
        x = x.sort('datadate', ascending=False)
        growth_q_list = []
        for n in range(12):
            growth_q = (x['revtq'][n:n+1].values[0] - x['revtq'][n+4:n+5].values[0]) / x['revtq'][n+4:n+5].values[0]
            growth_q_list.append(growth_q)
            
        sale_growth_var = pd.Series(growth_q_list).var()    
        return sale_growth_var
        
    sale_growth_var = data.groupby('tic').apply(sale_growth_var_calc)
    sale_growth_var.name = 'sale_growth_var'
    sale_growth_var.columns = ['sale_growth_var']
    
    #most recent quartly data
    f3 = lambda x:x.sort('datadate', ascending=False)[0:1]
    cur_q = data.groupby('tic').apply(f3)
    
    #preious assets
    f4 = lambda x:x.sort('datadate', ascending=False)[4:5]
    pre_assets_q = data.groupby('tic').apply(f4)['atq']
    pre_assets_q = pre_assets_q.reset_index()
    pre_assets_q = pre_assets_q[['tic', 'atq']]
    pre_assets_q = pre_assets_q.set_index('tic')
    pre_assets_q.columns = ['atq_pre']
    
    #join tables
    cur = cur_q.join(trail_oancfy)
    cur = cur.join(trail_record)
    cur = cur.join(pre_assets_q)
    cur = cur.join(roa_q_var)
    cur = cur.join(sale_growth_var)
    
    if ad_data is not None:
        cur_ad = raw_ad_data.groupby('tic').apply(f3)
        cur_ad.set_index('tic', inplace=True)
        cur_ad = cur_ad['xad']
        cur = cur.join(cur_ad)
        cur['xad'].fillna(0, inplace=True)
        cur['xad'] = cur['xad'].astype(float)
    
    cur['roa'] = cur['trail_ibq'] / cur['atq']
    cur['cash_roa'] = cur['trail_oancfy'] / cur['atq']
    cur['rd_intensity'] = cur['trail_xrdq'] / cur['atq_pre']
    cur['capxy_intensity'] = cur['trail_capxy'] / cur['atq_pre']
    cur['xad_intensity'] = cur['xad'] / cur['atq_pre']
    
    roa_median = cur.groupby(['sic2'])[['roa']].median()
    roa_median = roa_median.reset_index()
    roa_median.columns = ['sic2', 'roa_median']
    cur = pd.merge(cur, roa_median, how='left', on=['sic2'])
    
    cash_roa_median = cur.groupby(['sic2'])[['cash_roa']].median()
    cash_roa_median = cash_roa_median.reset_index()
    cash_roa_median.columns = ['sic2', 'cash_roa_median']
    cur = pd.merge(cur, cash_roa_median, how='left', on=['sic2'])
    
    rd_intensity_median = cur.groupby(['sic2'])[['rd_intensity']].median()
    rd_intensity_median = rd_intensity_median.reset_index()
    rd_intensity_median.columns = ['sic2', 'rd_intensity_median']
    cur = pd.merge(cur, rd_intensity_median, how='left', on=['sic2'])
    
    capxy_intensity_median = cur.groupby(['sic2'])[['capxy_intensity']].median()
    capxy_intensity_median = capxy_intensity_median.reset_index()
    capxy_intensity_median.columns = ['sic2', 'capxy_intensity_median']
    cur = pd.merge(cur, capxy_intensity_median, how='left', on=['sic2'])
    
    xad_intensity_median = cur.groupby(['sic2'])[['xad_intensity']].median()
    xad_intensity_median = xad_intensity_median.reset_index()
    xad_intensity_median.columns = ['sic2', 'xad_intensity_median']
    cur = pd.merge(cur, xad_intensity_median, how='left', on=['sic2'])
    
    roa_q_var_median = cur.groupby(['sic2'])[['roa_q_var']].median()
    roa_q_var_median = roa_q_var_median.reset_index()
    roa_q_var_median.columns = ['sic2', 'roa_q_var_median']
    cur = pd.merge(cur, roa_q_var_median, how='left', on=['sic2'])
    
    sale_growth_var_median = cur.groupby(['sic2'])[['sale_growth_var']].median()
    sale_growth_var_median = sale_growth_var_median.reset_index()
    sale_growth_var_median.columns = ['sic2', 'sale_growth_var_median']
    cur = pd.merge(cur, sale_growth_var_median, how='left', on=['sic2'])
    
    #set score to N/A for the firm in an industry which has less than 4 firms
    grouped = data.groupby('sic2')
    sic2_firms_count_series = grouped.tic.nunique()
    sic2_firms_count = pd.DataFrame(sic2_firms_count_series)
    sic2_firms_count.columns = ['sic2_firms_count']
    sic2_firms_count.reset_index(inplace=True)
    cur = pd.merge(cur, sic2_firms_count, how='inner', on=['sic2', 'sic2'])    
    
    def MOH_score_calc(x):
        score = 0
        if (x['roa'] > x['roa_median']):
            score = score + 1
        if (x['cash_roa'] > x['cash_roa_median']):
            score = score + 1
        if (x['trail_oancfy'] > x['trail_ibq']):
            score = score + 1
        if (x['rd_intensity'] > x['rd_intensity_median']):
            score = score + 1
        if (x['sale_growth_var'] < x['sale_growth_var_median']):
            score = score + 1
        if (x['capxy_intensity'] > x['capxy_intensity_median']):
            score = score + 1
        if (x['xad_intensity'] > x['xad_intensity_median']):
            score = score + 1
        if (x['roa_q_var'] < x['roa_q_var_median']):
            score = score + 1
        if float(x['sic2_firms_count']) < 4:
            score = 'N/A'
            
        return score
        
    #MOH_cur = cur
    #MOH_score = MOH_cur.join(MOH_result)      
    cur.set_index('tic', inplace=True)
    MOH_result = cur.apply(MOH_score_calc, axis=1)
    #cur.to_csv(r'C:\Users\Guanwen\Google Drive\CQA_MOH_score.csv')
    #raw_data.to_csv(r'C:\Users\Guanwen\Google Drive\CQA_MOH_raw.csv')
    MOH_result.name = 'moh score'
    
    return MOH_result


#MOH_score.to_csv(r'C:\Users\Guanwen\Google Drive\CQA_MOH_score.csv')