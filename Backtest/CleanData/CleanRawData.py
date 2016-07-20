# -*- coding: utf-8 -*-
"""
Created on Wed Jul 20 17:48:04 2016

@author: Guanwen
"""

import pandas as pd
import numpy as np
import scipy.stats as stats
import math


N = 3
raw_data = pd.read_csv('C:\Users\Guanwen\Documents\GitHub\CQA\Backtest\QIS_2014_set1.csv')



d = raw_data['DATE'].unique().tolist()
f = [a for a in raw_data.columns.tolist() if a.startswith('fret')]
s = raw_data['SECTOR'].unique().tolist()