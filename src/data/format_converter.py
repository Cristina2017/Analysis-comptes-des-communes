# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 10:24:08 2022

@author: crist
"""


import pandas as pd
import numpy as np
#from .. import ROOTDIR, RAWDIR, PRODIR
from src import DATADIR, DATARAW, ROOTDIR
 
file = 'base_communes.csv'

def transf_data(file, name):
    data = pd.read_csv(DATARAW/file, sep=";", encoding="utf-8", low_memory=False, dtype={'insee':str})
    data['insee'] = data['insee'].astype('str') 
    data.to_parquet(DATADIR/name +'.parquet')# meter dónde lo guardo
    #print("Je n'ai pas crashe")
#transf_data(file)


if __name__ == "__main__":
    transf_data(file)