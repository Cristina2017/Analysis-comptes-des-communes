# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 11:15:16 2022

@author: crist
"""

import pandas as pd
import numpy as np
import re

file_name = 'data'
DATADIR = r'C:/Users/crist/mentoring/comptes des communes/src/data'

def open_data(file_name):
    parquet_file = DATADIR + '/%s.parquet' % (file_name)
    return pd.read_parquet(parquet_file, engine='auto')

data = open_data(file_name)

def miss(data):
    data = data.drop(['ordre_analyse1_section1','ordre_analyse1_section2','ordre_analyse1_section3','ordre_analyse2_section1','ordre_analyse2_section2',
'ordre_analyse2_section3','ordre_analyse3_section1','ordre_analyse3_section2','ordre_analyse3_section3','ordre_analyse4_section1',
'ordre_affichage', 'presence_budget'], axis = 1)
    
    return data

data = miss(data)
missing = data[data.isnull().any(axis=1)]
no_missing = data[~data.isnull().any(axis=1)]
    
def keeping_same_missing(missing):
    """
    
    As we have some columns with same information but different format, we are going to keep both columns with the same 
    number of missings.
    
    
    """

   
    reg_no_null = missing[~missing['reg_code'].isnull()]
    reg_code = reg_no_null['reg_code'].unique()
    
    d={}
    for element in reg_code:
        s = reg_no_null.loc[reg_no_null['reg_code'] == element,'reg_name']
        all_none = all(v is None for v in s)
        if all_none == True:
            sol = None
        else:
            sol = next(item for item in s if item is not None)
        d[element] = sol
    
    for i, row in missing.iterrows():
        for element in d:
            if row['reg_code'] == element:
                missing.at[i,'reg_name'] = d[element]
    
    # dep_name and dep_code
    dep_no_null = missing[~missing['dep_code'].isnull()]
    dep_code = dep_no_null['dep_code'].unique()
    
    d={}
    for element in dep_code:
        s = dep_no_null.loc[dep_no_null['dep_code'] == element,'dep_name']
        all_none = all(v is None for v in s) # search if all values in s are None
        if all_none == True:
            sol = None
        else:
            sol = next(item for item in s if item is not None)
        d[element] = sol
    
    for i, row in missing.iterrows():
        for element in d:
            if row['dep_code'] == element:
                missing.at[i,'dep_name'] = d[element]    
    return missing

missing = keeping_same_missing(missing)

def extract_com_name(missing):

    # Composed names
    r1 = re.compile(r"([a-zA-Z0-9.-áè]+-[a-zA-Z'.-]+)")
    
    # One unique name or several names (we keep the last one)
    
    r2 = re.compile(r"[^\W]+$")
    
    for i, row in missing.iterrows():
        missing.at[i,'com_name'] = r1.findall(missing.at[i,'lbudg'])
    for i, row in missing.iterrows():
        if not row['com_name']:
            missing.at[i,'com_name'] = r2.findall(missing.at[i,'lbudg'])
    
    for i, row in missing.iterrows():
        if not row['com_name']:
             missing.at[i,'com_name'] = row['lbudg']
        
    # We undo the list that is automatically done.
           
    for i, row in missing.iterrows():
        missing.at[i,'com_name'] = "".join(row['com_name']) 
    
    # We have detected that in some cases there are some words "RESTAURANT", "EAU", "ASSAT" following for a dash that are in our row and we did not want it.
    #So, we will remove it.
    words = ["ASST-", "RESTAURANT-", "EAU-", "LOTS-", "FORETS-", "1-", "HBS-", "SPANC-", "LOT-"]
    for i, row in missing.iterrows():
        for word in words:
            if word in row['com_name']:
                missing.at[i,'com_name'] = row['com_name'].replace(word, "")
                
    # Now, as in our dataframe "data" we have the column com_name in lowercase, we are going to transform it:
    # For that the easiest way is to convert into lowercase and then make a title:
    for i,row in missing.iterrows():
        missing.at[i,'com_name'] = row['com_name'].lower()
        missing.at[i,'com_name'] = row['com_name'].title()
        
    # we have identified at the end of some words the articles that is necesary to eliminate ['(Le )', '(La )', '(Les)']

    for i, row in missing.iterrows():
        if '(Le )' in row['com_name']:
            row['com_name'] = row['com_name'].replace(' (Le )', "")
            missing.at[i,'com_name'] = "Le" + " " +  row['com_name']
        if '(La )' in row['com_name']:
            row['com_name'] = row['com_name'].replace(' (La )', "")
            missing.at[i,'com_name'] = "La" + " " +  row['com_name']
        if '(Les)' in row['com_name']:
            row['com_name'] = row['com_name'].replace(' (Les)', "")
            missing.at[i,'com_name'] = "Les" + " " +  row['com_name']
    
    # At this point we can look for the unique names to compare and change manually some names if it is necessary
    
    for i, row in missing.iterrows():
        if "Magny" in row['com_name']:
            missing.at[i,'com_name'] = "Le Magny"
        if 'Chapelle-De-Mardore' in row['com_name']:
            missing.at[i,'com_name'] = 'La-Chapelle-De-Mardore'
        if "Bihorel" in row['com_name']:
            missing.at[i,'com_name'] = "Bois-Guillaume-Bihorel"
        if "Dessous" in row['com_name']:
            missing.at[i,'com_name'] = "Saint-Offenge-Dessous"
        if "Chedoue" in row['com_name']:
            missing.at[i,'com_name'] = "Fresnaye-Sur-Chedouet"
        if row['com_name'] == "L'Ile" or row['com_name'] == "Ile" or "Yeu" in row['com_name']:
            missing.at[i,'com_name'] = "L'Ile de Yeu"
    
    return missing

missing = extract_com_name(missing)

def common_siren(missing):
    
    """
    We seek with this fonction if some of the siren in our dataframe missings is in the dataframe no missings, so
    we can extract all the information concerned by this siren.
    
    """
    data_siren = no_missing['siren'].unique()
    missing_siren = missing['siren'].unique()
    common_siren = []
    for element in missing_siren:
        if element in data_siren:
            common_siren.append(element)
    
    d={}
    if common_siren:
        for siren in common_siren:
            miss = missing.loc[missing['siren']==siren].isnull().sum(axis = 0)
            l = []
            for element in miss.index:
                if miss[element] != 0:
                    l.append(element)
                d[siren] = [e for e in l]
    
    d2={}
    exer = data['exer'].unique()
    for siren in common_siren:
        d2[siren] = {}
        for element in d[siren]:
            if element == 'ptot':
                d2[siren]['ptot']={}
                for e in exer:
                    d2[siren]['ptot'][e] = {}
                    pr = no_missing.loc[(no_missing['siren']==siren)&(no_missing['exer']==e)]['ptot'] 
                    all_none = all(v is None for v in pr) # search if all values in s are None
                    if all_none == True:
                        d2[siren]['ptot'][e] = None
                    else:
                        d2[siren]['ptot'][e] =pr[pr.index[0]]  
                    
            else:
                pr = no_missing.loc[no_missing['siren']==siren][element]
                d2[siren][element] = pr[pr.index[0]]
                
    exer = missing.exer.unique()
    for siren in common_siren:
        for element in d[siren]:
            if element != 'ptot':
                for i, row in missing.iterrows():
                    if row['siren'] == siren:
                        missing.at[i,element] = d2[siren][element]
            else:
                for e in exer:
                    for i, row in missing.iterrows():
                        if (row['siren'] == siren) & (row['exer'] == e):
                            missing.at[i, element] = d2[siren][element][e]
    
    missing['euros_par_habitant'] = missing['montant']/missing['ptot']
    
    no_missing2 = missing[~missing.isnull().any(axis=1)]
    no_missing = pd.concat([no_missing, no_missing2], axis = 0)
    missing = missing[missing.isnull().any(axis=1)]
    no_missing.to_parquet('df.parquet')
    #missing = missing.to_parquet('missing.parquet')
    
    
   
file_name = 'df'
df = open_data(file_name)