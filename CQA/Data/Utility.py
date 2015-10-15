# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 22:10:50 2015

@author: Guanwen
"""

import pandas as pd
import numpy as np

DATA_PATH = 'C:\Users\Guanwen\Google Drive\CQA\Data\\'

def get_raw_data(filename, sectors=None, exchanges=None):
    raw_data = pd.read_csv(DATA_PATH + filename)
    raw_data = raw_data[raw_data['stko']==0]
    raw_data['adrrq'] = raw_data['adrrq'].fillna(0)
    raw_data = raw_data[raw_data['adrrq']==0]
    
    if exchanges:
        raw_data = raw_data[raw_data['exchg'].isin(exchanges)]
        
    if sectors:
        raw_data = raw_data[raw_data['gsector'].isin(exchanges)]
    
    return raw_data
    
