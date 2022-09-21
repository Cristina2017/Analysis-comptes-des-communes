# -*- coding: utf-8 -*-
"""
Created on Thu Apr  7 10:29:06 2022

@author: crist
"""


import pandas as pd
import numpy as np
from src import DATADIR, DATARAW, ROOTDIR


# open_parquet

def open_parquet(file):
    parquet_file = DATADIR/file
    return pd.read_parquet(parquet_file, engine='auto')

# open csv
    
    
def open_csv(file, sep=","):
    return pd.read_csv(DATARAW/file, encoding='latin-1', sep=sep)

