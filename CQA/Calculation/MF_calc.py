import pandas as pd
import numpy as np


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
    null_tic = checked_tic[~checked_tic.isnull()]
    criterion = lambda row: row['tic'] not in null_tic
    data = raw_data[raw_data.apply(criterion, axis=1)]
    
    #calculate trailling ebit
    trail_ebit = data.groupby(['tic'])['ebit_q'].sum()
    trail_ebit.name = 'trailing_ebit'
    
    #get most recent data for each tic
    f_current = lambda x:x.sort('datadate', ascending=False).head(1)
    most_recent_record = data.groupby('tic').apply(f_current)
    result = most_recent_record.join(trail_ebit)
    result = result.set_index('tic')
    
    #calculate MF score
    result['ev_ebit'] = result['ev'] / result['trailing_ebit']
    result['ebit_ev'] = result['trailing_ebit'] / result['ev']
    result['roc'] = result['trailing_ebit'] / result['capital']
    result['MF_score'] = 0.5*(result['ebit_ev'] + result['roc']) - 0.5*np.square(result['ebit_ev'] - result['roc'])
    result = result[np.isfinite(result['MF_score'])]
    MF_result = result.sort_index(by='MF_score', ascending=False)
    
    return MF_result[['gsector', 'exchg', 'trailing_ebit', 'ev', 'capital', 'ebit_ev', 'roc', 'MF_score']]
