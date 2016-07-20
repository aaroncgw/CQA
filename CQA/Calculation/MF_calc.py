import pandas as pd
import numpy as np
import math


def Calc(MF_data, tickers=None, mkt_cap_df=None):
    if tickers is not None:
        raw_data = MF_data[MF_data['tic'].isin(tickers)].copy()
    else:
        raw_data = MF_data.copy()     
                
    mkt_cap = 'mkvaltq'
    if mkt_cap_df is not None:
        mkt_cap = 'Mkt_cap'
        raw_data['ticker'] = raw_data['tic']
        raw_data.set_index('ticker', inplace=True)
        raw_data = raw_data.join(mkt_cap_df, how='left')

    #remove the tic which has less than 4 data points
    tic_count = pd.value_counts(raw_data['tic'])
    tic_list = tic_count[tic_count >= 4]
    raw_data = raw_data[raw_data['tic'].isin(tic_list.index)]    
    
    #keep the first four rows of each tic
    f_4q = lambda x:x.sort('datadate', ascending=False).head(4)
    raw_data = raw_data.groupby('tic').apply(f_4q)
    
    #calculate ebit_q, capital and ev
    raw_data[['ibq', 'miiq', 'txtq', 'xintq', 'ivltq', 'ppentq', 'aoq', 'wcapq', mkt_cap, 'dlcq', 'dlttq', 'pstkq', 'cheq']] = raw_data[['ibq', 'miiq', 'txtq', 'xintq', 'ivltq', 'ppentq', 'aoq', 'wcapq', mkt_cap, 'dlcq', 'dlttq', 'pstkq', 'cheq']].fillna(0) 
    raw_data[['ibq', 'miiq', 'txtq', 'xintq', 'ivltq', 'ppentq', 'aoq', 'wcapq', mkt_cap, 'dlcq', 'dlttq', 'pstkq', 'cheq']] = raw_data[['ibq', 'miiq', 'txtq', 'xintq', 'ivltq', 'ppentq', 'aoq', 'wcapq', mkt_cap, 'dlcq', 'dlttq', 'pstkq', 'cheq']].astype(float)
    raw_data['ebit_q'] = raw_data['ibq'] + raw_data['miiq'] + raw_data['txtq'] + raw_data['xintq']
    raw_data['capital'] = raw_data['ivltq'] + raw_data['ppentq'] + raw_data['aoq'] + raw_data['wcapq']
    raw_data['ev'] = raw_data[mkt_cap] + raw_data['dlcq'] + raw_data['dlttq'] + raw_data['pstkq'] - raw_data['cheq']
    
    #remove the tic which has zero ebit_q, capital and ev
    def missing_data_clean(x, check_list, num):
        x = x.sort('datadate', ascending=False).head(num)
        for col in check_list:
            if 0 in list(x[col]):
                return list(x['tic'])[0]
    
    col_check = ['ebit_q', 'capital', 'ev']
    checked_tic = raw_data.groupby('tic').apply(missing_data_clean, check_list=col_check, num=4)
    if len(checked_tic.isnull()) != 0:        
        null_tic = checked_tic[~checked_tic.isnull()]
        criterion = lambda row: row['tic'] not in null_tic
        raw_data = raw_data[raw_data.apply(criterion, axis=1)]
    
    data = raw_data.copy()   
    #calculate trailling ebit
    trail_ebit = data.groupby(['tic'])['ebit_q'].sum()
    trail_ebit.name = 'trailing_ebit'
    
    #get most recent data for each tic
    f_current = lambda x:x.sort('datadate', ascending=False).head(1)
    most_recent_record = data.groupby('tic').apply(f_current)
    data_set = most_recent_record.join(trail_ebit)
    data_set['tikcer'] = data_set['tic']    
    
    
    #calculate MF score
    data_set['ev_ebit'] = data_set['ev'] / data_set['trailing_ebit']
    data_set['ebit_ev'] = data_set['trailing_ebit'] / data_set['ev']
    data_set['roc'] = data_set['trailing_ebit'] / data_set['capital']
       
    '''
    def mf_score_calc(x):
        score = 0        
        if (x['ebit_ev'] >= 0).bool():
            score = math.sqrt(x['ebit_ev'] * x['roc'])
        elif (x['ebit_ev'] < 0).bool():
            score = 0.5*(x['ebit_ev'] + x['roc']) - 0.5*np.square(x['ebit_ev'] - x['roc'])
        return float(score)
    '''
    
    def mf_score_calc(x):
        score = 0         
        score = 0.5*(x['ebit_ev'] + x['roc']) - 0.5*np.square(x['ebit_ev'] - x['roc'])
        return float(score)
        
    result = data_set.groupby('tic').apply(mf_score_calc)
    result.name = 'MF_score'
    data_set.set_index('tikcer', inplace=True)
    MF_result = data_set.join(result)
    #MF_result = MF_result[np.isfinite(MF_result['MF_score'])]
    MF_result.sort_index(by='MF_score', ascending=False, inplace=True)
    
    return MF_result[['gsector', 'exchg', 'trailing_ebit', 'ev', 'capital', 'ebit_ev', 'roc', 'MF_score']]
