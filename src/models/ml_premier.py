# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 15:13:14 2022

@author: crist
"""


import pandas as pd
import numpy as np
from src import DATADIR, DATARAW, ROOTDIR
from src.data import format_converter as fc
import unicodedata
from src.data import open_files as of
import matplotlib.pyplot as plt


from sklearn.datasets import make_classification
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import xgboost as xgb

years = [2012,2013,2014,2015,2016,2017,2018]

# We open the endettes fields. They have the information about the communes in debt.
for year in years:
    globals()['data_'+str(year)] = of.open_parquet('endettes_'+str(year) +'.parquet')
for year in years:
    globals()['data_'+str(year)] = globals()['data_'+str(year)].drop(['montant', 'ptot', 'mont_hab', 'perc'], axis = 1)
    globals()['data_'+str(year)]['exer'] = year
for year in years:
    globals()['data_'+str(year)]=globals()['data_'+str(year)][['exer','siren', 'group']]
    globals()['data_'+str(year)].columns = ['exer','siren', 'difficulte']
    globals()['data_'+str(year)]['difficulte'] = globals()['data_'+str(year)]['difficulte'].replace(to_replace=['tres endetté', " "], value = [1,0])
    

for year in years:
    if year < 2018:
        globals()['data_'+str(year)+'_'+str(year+1)]= globals()['data_'+str(year)].merge(right=globals()['data_'+str(year+1)], on='siren', how='inner')
for year in years:
    if year < 2018:
        globals()['data_'+str(year)+'_'+str(year+1)]=globals()['data_'+str(year)+'_'+str(year+1)].drop(['exer_y'], axis = 1)
        globals()['data_'+str(year)+'_'+str(year+1)].columns=['exer', 'siren', 'difficulte_anne', 'difficulte_avenir']


data = pd.concat([data_2012_2013, data_2013_2014, data_2014_2015, data_2015_2016, data_2016_2017, data_2017_2018])

target = data['difficulte_avenir']
feature = data.drop(['difficulte_avenir'], axis = 1)
X_train, X_test, y_train, y_test = train_test_split(feature, target, test_size = 0.2, random_state = 12)

# Random Forest
#model = RandomForestClassifier(n_estimators=10, class_weight='balanced', n_jobs = -1, random_state = 321)
model = RandomForestClassifier(n_estimators=10, n_jobs = -1, random_state = 321)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)
pd.crosstab(y_test, y_pred, rownames = ['Classe réelle'], colnames = ['Classe prédite'])

# Xgboost


train = xgb.DMatrix(data = X_train, label= y_train)
test =  xgb.DMatrix(data = X_test, label = y_test)


params = {'booster':'gbtree', 'learning_rate': 0.01,  'objective': 'binary:logistic'}
xgb = xgb.train(params = params,
                dtrain = train, 
                num_boost_round= 700,
                evals= [(train, 'train'), (test, 'eval')])
preds = xgb.predict(test)

xgbpreds= pd.Series(np.where(preds > 0.5, 1, 0))
pd.crosstab(xgbpreds, pd.Series(y_test))
