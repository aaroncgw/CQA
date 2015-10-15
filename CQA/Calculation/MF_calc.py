import pandas as pd
import numpy as np


def Calc(MF_data):
    raw_data = MF_data.copy()
    
    #remove the tic which has less than 4 data points
    tic_count = pd.value_counts(raw_data['tic'])
    tic_list = tic_count[tic_count >= 4]
    raw_data = raw_data[raw_data['tic'].isin(tic_list.index)]    
    
    #keep the first four rows of each tic
    f_4q = lambda x:x.sort('datadate', ascending=False).head(4)
    raw_data = raw_data.groupby('tic').apply(f_4q)
    
    #calculate ebit_q, capital and ev
    raw_data[['ibq', 'miiq', 'txtq', 'xintq']] = raw_data[['ibq', 'miiq', 'txtq', 'xintq']].fillna(0)    
    raw_data[['ivltq', 'ppentq', 'aoq', 'wcapq']] = raw_data[['ivltq', 'ppentq', 'aoq', 'wcapq']].fillna(0)
    raw_data[['mkt_cap', 'dlcq', 'dlttq', 'pstkq', 'cheq']] = raw_data[['mkt_cap', 'dlcq', 'dlttq', 'pstkq', 'cheq']].fillna(0)
    raw_data['ebit_q'] = raw_data['ibq'].astype(float) + raw_data['miiq'].astype(float) + raw_data['txtq'].astype(float) + raw_data['xintq'].astype(float)
    raw_data['capital'] = raw_data['ivltq'].astype(float) + raw_data['ppentq'].astype(float) + raw_data['aoq'].astype(float) + raw_data['wcapq'].astype(float)
    raw_data['ev'] = raw_data['mkt_cap'].astype(float) + raw_data['dlcq'].astype(float) + raw_data['dlttq'].astype(float) + raw_data['pstkq'].astype(float) - raw_data['cheq'].astype(float)
    
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
    
    return MF_result
    
    
    
    
    
    #MF_result.to_csv(r'C:\Users\Guanwen\Google Drive\CQA_MF_score.csv')
    #MF_set = set(MF_result.index.tolist())

