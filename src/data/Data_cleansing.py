# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 11:15:16 2022

@author: crist
"""

import pandas as pd
import numpy as np
import re
from src import DATADIR, DATARAW, ROOTDIR
import unicodedata

file = 'data.parquet'
#DATADIR = r'C:/Users/crist/mentoring/comptes des communes/src/data'

def open_data(file):
    parquet_file = DATADIR/file
    return pd.read_parquet(parquet_file, engine='auto')

data = open_data(file)



def miss(data):
    
    """
    Eliminate the columns that have no information or have duplicated information
    
    """
    data = data.drop(['ordre_analyse1_section1','ordre_analyse1_section2','ordre_analyse1_section3','ordre_analyse2_section1','ordre_analyse2_section2',
'ordre_analyse2_section3','ordre_analyse3_section1','ordre_analyse3_section2','ordre_analyse3_section3','ordre_analyse4_section1',
'ordre_affichage', 'presence_budget'], axis = 1)
    data = data.replace('nan', np.nan)
    return data

data = miss(data)
missing = missing.replace('nan', np.nan)
missing = data[data.isnull().any(axis=1)]
no_missing = data[~data.isnull().any(axis=1)]
    
def extract_com_name(missing):
    
    """
    We extract the commune name from the lbudg field
    
    """
    
    r1 = re.compile(r"([a-zA-Z0-9.-áè]+-[a-zA-Z'.-]+)") # Composed names
    r2 = re.compile(r"[a-zA-Z0-9_']+$") # One unique name or several names (we keep the last one)
    
    missing['com_name'] = np.where(missing['com_name'].isnull(), missing['lbudg'].apply(lambda x: "".join(r1.findall(x)).title()), missing['com_name'])
    missing['com_name'] = np.where(missing['com_name']=='', missing['lbudg'].apply(lambda x: "".join(r2.findall(x)).title()), missing['com_name'])
    missing['com_name'] = np.where(missing['com_name']=='', missing['lbudg'].apply(lambda x: "".join(re.findall('\((.*?)\)',x)).title())+missing['lbudg'].apply(lambda x: "".join(re.findall('([^ ]+) .*',x)).title()), missing['com_name']) #words = ['(Le )', '(La )', '(Les)']
    
    def clean(x):
        words = ["Asst-", "Restaurant-", "Eau-", "Lots-", "Forets-", "1-", "Hbs-", "Spanc-", "Lot-", "Transat-"]
        for word in words:
            if word in x:
                x = x.replace(word, "")
            else:
                x = x
        return x
    
    missing['com_name'] = missing['com_name'].apply(clean)
    
    def replace(x):
        communes = {"Magny":"Le Magny",'Chapelle-De-Mardore': 'La-Chapelle-De-Mardore',"Bihorel": "Bois-Guillaume-Bihorel",
                 "Dessous":"Saint-Offenge-Dessous", "Chedoue": "Fresnaye-Sur-Chedouet", "Infournas": "Les Infournas",
                 "Louis": "Saint-Louis", "Commerciales": "Voultegon", 'Commerciale':"Unknown",'Photovoltaique': "Unknown", 'Reine': "Unknown"}
        for com in communes:
            if com in x:
                x = communes[com]
            else:
                x = x
        return x
    
    missing['com_name'] = missing['com_name'].apply(replace)
    missing.to_parquet(DATADIR/'missing.parquet')

#missing = extract_com_name(missing)
missing = open_data('missing.parquet')

 
def complete_code_name(missing):

    """
    There are some columns that have the same information one of them in code and the other in name.
    This function will leave both columns with the same information, searching the missing in one of them and filling the other column
    """
 
    reg= pd.read_csv(DATARAW/'regions.csv', encoding='latin-1')
    reg.columns = ['reg_name', 'Population 2019', 'Population estimée 2022', 'code']
    
    dep = pd.read_csv(DATARAW/'departements.csv', encoding='latin-1')
    dep.columns = ['Code', 'Dep']
    
    corr= pd.read_csv(DATARAW/'correspondance.csv', sep = ";", encoding='latin-1')
    corr.columns = ['insee', 'CP', 'com_name', 'dep_name', 'reg_name',
           'statut', 'Altitude_Moyenne', 'Superficie', 'Population',
           'geo_point_2d', 'geo_shape', 'ID_Geofla', 'com_code', 'Code_Canton',
           'Code_Arrondissement', 'dep_code', 'reg_code']
    
    d_dep = dep.set_index('Code').to_dict()['Dep']
    d_reg = reg.set_index('code').to_dict()['reg_name']
        
    missing['reg_name']= np.where(missing['reg_code'].notnull(),missing['reg_code'].apply(lambda x: d_reg[x] if pd.notnull(x) else x), missing['reg_name'])
    missing['dep_name']= np.where(missing['dep_code'].notnull(),missing['dep_code'].apply(lambda x: d_dep[x] if pd.notnull(x) else x), missing['dep_name'])
    #epci_code/epci_name
    
    df = data.groupby("epci_code").first().reset_index()
    d_data = df.set_index('epci_code').to_dict()['epci_name']
    
    for element in d_data:
        missing['epci_name']=np.where(missing['epci_code'] == element,d_data[element], missing['epci_name'])

    missing.to_parquet(DATADIR/'missing.parquet')

missing = complete(missing)

# Todos los ficheros en el mismo formato:

def strip_accents(s):
    if s != None:
        s = ''.join(c for c in unicodedata.normalize('NFD', s) 
                   if unicodedata.category(c) != 'Mn')
    else:
        s = None
    return s


def string_format(df):
    
    """
    We change the strings columns format to leave it exactly in all the dataframes, in order to be able to cross it searching by a commune name.
    """
    columns = ['com_name', 'reg_name', 'dep_name']
    name =[x for x in globals() if globals()[x] is df][0] + "." + "parquet"
    for column in columns:
        df[column] = df[column].apply(strip_accents)
        df[column] = df[column].apply(lambda x: x.title() if x!= None else None)
        df.to_parquet(DATADIR/name)

f = ['missing', 'no_missing', 'corr']
for element in f:
    string_format(globals()[element]) 

missing = open_data('missing.parquet')
no_missing = open_data('no_missing.parquet')
corr = open_data('corr.parquet')

def imp_comparation(df):
    
    """
    We will search the homonyms in the corr dataframe and imput the missings from the corr dataframe and from no_missing dataframe.
    Firstly, we will exclude the homonyms and then the imputation will be by com_name.

    """        
    missing['ptot'] = np.where(missing['ptot'] == 0, 1, missing['ptot'])
    missing['euros_par_habitant'] = np.where(missing['ptot'] == 0, missing['montant']/1, missing['euros_par_habitant'])
    missing['insee'] = np.where(missing['insee'].isnull(), missing['insee']*(-1), missing['insee'])
    missing.to_parquet('missing.parquet')
    
    # Saco los homónimos:
    corr['duplicated']=corr.sort_values('com_name').duplicated(subset=['com_name'])
    corr['hom'] = np.where(corr['duplicated']==True, corr['com_name'], 0)
    hom = set([x for x in corr['hom'] if x !=0])
    hom_in_missing=[element for element in missing['com_name'].unique() if element in hom]
    
    columns = ['reg_name', 'dep_name', 'reg_code', 'dep_code', 'com_code']
    for column in columns:
        globals()['d' + "_" + column] = corr.set_index('com_name').to_dict()[column]
        for element in globals()['d' + "_" + column].copy():
            if element in hom_in_missing:
                del globals()['d' + "_" + column][element]
        missing[column] = np.where(missing[column].isnull(), missing['com_name'].apply(lambda x: globals()['d' + "_" + column][x] if x in globals()['d' + "_" + column] else np.nan), missing[column])
    
    years = missing[missing['ptot'].isnull()]['exer'].unique()       
    imp = no_missing.groupby(['exer','com_name']).first().reset_index()
    
    columns = ['ptot','epci_code','epci_name','tranche_population', 'rural', 'montagne', 'touristique', 'tranche_revenu_imposable_par_habitant','qpv', 'outre_mer']
    for year in years:
        for column in columns:
            globals()['d' + '_' + column +  str(year)] = imp[imp['exer']==year].set_index('com_name').to_dict()[column]
            missing[column] = np.where(missing[column].isnull(), missing['com_name'].apply(lambda x:globals()['d' + '_' + column + str(year)][x] if x in globals()['d' + '_' + column + str(year)] else np.nan ), missing[column])
    
    imp_siren = no_missing.groupby(['exer','siren']).first().reset_index()
    
    for year in years:
        for column in columns:
            globals()['d' + '_' + column +  str(year)] = imp_siren[imp_siren['exer']==year].set_index('siren').to_dict()[column]
            missing[column] = np.where(missing[column].isnull(), missing['siren'].apply(lambda x:globals()['d' + '_' + column + str(year)][x] if x in globals()['d' + '_' + column + str(year)] else np.nan ), missing[column])
    
    missing['euros_par_habitant'] = np.where(missing['ptot'].notnull(), missing['montant']/missing['ptot'], missing['euros_par_habitant'])        
    df.to_parquet(DATADIR/'missing.parquet')






#if __name__ == "__main__":
#    open_data(file)






# 