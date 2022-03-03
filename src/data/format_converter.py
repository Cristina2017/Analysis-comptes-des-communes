# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 10:24:08 2022

@author: crist
"""


import pandas as pd
import numpy as np

DATADIR = DATADIR = r'C:\Users\crist\mentoring\comptes des communes\src\data'
def transf_data(file):
    data = pd.read_csv(r'C:\Users\crist\mentoring\comptes des communes\data\raw\%s' % (file), sep=";", encoding="utf-8", low_memory=False)
    data['insee'] = data['insee'].astype('str') 
    data.to_parquet('data.parquet')

transf_data('base_communes.csv')