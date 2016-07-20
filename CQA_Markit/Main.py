# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 20:50:03 2015

@author: Guanwen
"""

from Data import Utility
from Calculation import PIO_calc

u_tick= ['AAPL']
comp_PIO_data = Utility.get_compustat_data('CQA_PIO_data.csv', exchanges=['11', '12', '14'])

PIO_result = PIO_calc.Calc(comp_PIO_data, u_tick, details=True)
PIO_result.to_csv(r'C:\Users\Guanwen\Desktop\test.csv')