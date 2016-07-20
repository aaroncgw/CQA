# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 22:10:50 2015

@author: Guanwen
"""

import pandas as pd
import numpy as np

DATA_PATH = 'C:\Users\Guanwen\Google Drive\CQA\Data\\'

def get_compustat_data(filename, sectors=None, exchanges=None, period=None):
    #sectors: list of string, ['10', '15', '20', '25', '30', '35', '45', '50'] 
    #exchanges: list of string, ['11', '12', '14']
    data = pd.read_csv(DATA_PATH + filename, dtype='unicode')
    data = data[data['stko']=='0']    
    if period is not None:
        data = data[data['adrr'].isnull()]
    else:
        data = data[data['adrrq'].isnull()]
    if exchanges is not None:
        data = data[data['exchg'].isin(exchanges)]
    if sectors is not None:
        data = data[data['gsector'].isin(sectors)]
    return data
    
def get_stock_universe(filename):
    data = pd.read_csv(DATA_PATH + filename, dtype='unicode')
    return data
    
    
