# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 16:08:53 2022

@author: crist
"""

import pandas as pd
import numpy as np
from src import DATADIR, DATARAW, ROOTDIR
from src.data import open_files as of


def geoposition(no_missing):
    
    """
    This function will add the latitude and longitud to calculate the distance for each commune

    """
    no_missing = of.open_parquet('no_missing.parquet')
    no_missing['insee'] = no_missing['insee'].apply(lambda x: '0' + x if len(x) ==4 else x)
    corr = of.open_parquet('corr.parquet')
    corr.columns = ['insee', 'CP', 'com_name', 'dep_name', 'reg_name',
           'statut', 'Altitude_Moyenne', 'Superficie', 'Population',
           'geo', 'geo_shape', 'ID_Geofla', 'com_code', 'Code_Canton',
           'Code_Arrondissement', 'dep_code', 'reg_code']
    d_geo = corr.set_index('insee').to_dict()['geo']    
    no_missing['geo']=no_missing['insee'].apply(lambda x: d_geo[x] if x in d_geo else np.nan)    
    commune = no_missing[no_missing['geo'].isnull()]['com_name'].unique()    
    d={}
    for element in commune:
        d[element]=None
        l = []
        for i, row in no_missing[no_missing['com_name'].isin(commune)].iterrows():     
            if row['com_name']==element:
                if row['insee'] not in l:
                    l.append(row['insee'])
        d[element] = l

    insee_missing = [insee for commune in d for insee in d[commune]]
    d2 = {}
    for insee in insee_missing:
        d2[insee] = None
    d2['69123']=45.7640, 4.8357
    d2['75056']=48.8566, 2.3522
    d2['13055']=43.2965, 5.3698
    d2['76095']=49.4558, 1.1214
    d2['55138']=48.7552, 5.2663
    d2['28349']=48,6392, 1,62331
    d2['76601']=49,5095, 1,43994
    no_missing['geo'] = np.where(no_missing['geo'].isnull(), no_missing['insee'].apply(lambda x: d2[x] if x in d2 else x), no_missing['geo'])
    no_missing['geo'] = no_missing['geo'].astype('str')
    no_missing['longitud'] = no_missing['geo'].apply(lambda x: x.split(',')[0][1:] if  x.split(',')[0][0] == '(' else x.split(',')[0]).astype('float')
    no_missing['latitud'] = no_missing['geo'].apply(lambda x: x.split(',')[1][:-1] if  x.split(',')[1][-1] == ')' else x.split(',')[1]).astype('float')
    no_missing['dist'] = round(np.sqrt((no_missing['longitud']**2)+(no_missing['latitud']**2)), 3)
    
    no_missing.to_parquet(DATADIR/'no_missing.parquet')


def dif_pop(no_missing):
    
    """
    This function will calculate for each commune the difference of population between one year and an other.
    First year is 2012 where the difference will be 0.
    """
    no_missing = of.open_parquet('no_missing.parquet')
    years = sorted(no_missing.exer.unique())
    no_missing['perc_diff_ptot'] = np.where(no_missing['exer']==2012,0, np.nan)
    res = no_missing.groupby(['exer','insee']).first().reset_index()
    for year in years:
        globals()['d' + '_' + str(year)] = res[res['exer']==year].set_index('insee').to_dict()['ptot']
        if year !=2012:
            no_missing['perc_diff_ptot'] = np.where((no_missing['perc_diff_ptot'].isnull())&(no_missing['exer']==year), no_missing['insee'].apply(lambda x: ((globals()['d' + '_' + str(year)][x]- globals()['d'+'_'+str(year-1)][x])/globals()['d'+'_'+str(year-1)][x])*100 if x in globals()['d' + '_' + str(year)] and x in globals()['d'+'_'+str(year-1)] else np.nan), no_missing['perc_diff_ptot'])
    no_missing['perc_diff_ptot'] = np.where(no_missing['perc_diff_ptot'].isnull(),1000, no_missing['perc_diff_ptot'])
    no_missing.to_parquet(DATADIR/'no_missing.parquet')
    


def dens(missing):
    """
    This will add the population density for each commune
    """
    no_missing = of.open_parquet('no_missing.parquet')
    density = of.open_csv('densidad.csv')   
    density.columns = ['com_code', 'com_name', 'degre_densité','region','pop_2018', 'dense', 'interm','peu_dense', 'tres_peu_dense']
    columns = ['dense', 'interm','peu_dense', 'tres_peu_dense']
    for column in columns:
        globals()['d' + '_' + column] = density.set_index('com_code').to_dict()[column] 
    for column in columns:
        no_missing[column] = no_missing['com_code'].apply(lambda x: globals()['d' + '_' + column][x] if x in globals()['d' + '_' + column] else np.nan)


#no_missing = of.open_parquet('no_missing.parquet')   





#no_missing['longitud'] = no_missing['longitud'].astype('float')
#no_missing['latitud'] = no_missing['latitud'].astype('float')







