# -*- coding: utf-8 -*-
"""
Created on Fri Apr  8 14:23:05 2022

@author: crist
"""

import pandas as pd
import numpy as np
from sklearn import model_selection, preprocessing
from sklearn.model_selection import cross_val_predict, cross_val_score, cross_validate
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error # for calculating the cost function
from src import DATADIR, DATARAW, ROOTDIR
from src.data import open_files as of
from sklearn.preprocessing import OrdinalEncoder

no_missing = of.open_parquet('no_missing.parquet')
# eliminamos 4651 registros nulos
no_missing=no_missing[no_missing['dense'].notnull()]



# cor = df.corr('spearman')
# fig, ax = plt.subplots(figsize=(20,20))
# sns.heatmap(cor, annot=True, ax=ax, cmap="coolwarm");
def prep(no_missing):
    dep = ["4","8", "12","16","18", "22", "28", "32",
           "36", "40", "44", "48", "52", "57",
           "61","65", "69", "73", "77", "81",
           "86", "90", "94"]
    df = no_missing[no_missing['dep_code'].isin(dep)]
 #enc = OrdinalEncoder()
#enc.fit(data['outre_mer', 'rural', 'montagne', 'qpv', 'categ', 'type_de_budget', 'nomen'])
#data = enc.transform([['outre_mer', 'rural', 'montagne', 'qpv', 'categ', 'type_de_budget', 'nomen']])

    target = df['ptot']
    data = df.drop(['ptot','reg_name', 'dep_name', 'epci_name','com_name','agregat','lbudg', 'geo', 'longitud', 'latitud', 'montant'], axis =1)
    data = data.replace(['Non', 'Oui'], value = [0,1])
    data['categ'] = data['categ'].replace(['Commune', 'PARIS'], value = [0,1])
    data['type_de_budget'] = data['type_de_budget'].replace(['Budget annexe', 'Budget principal'], value = [0,1])
    data[['reg_code', 'epci_code', 'tranche_revenu_imposable_par_habitant']]=data[['reg_code', 'epci_code', 'tranche_revenu_imposable_par_habitant']].astype('O')
    data['nomen'] = data['nomen'].replace(['M49A', 'M4', 'M49', 'M43', 'M14', 'M14A', 'M43A', 'M42', 'M41',
       'M22', 'M157', 'M57'], value = [0,1,2,3,4,5,6,7,8,9,10,11])
#data['nomen'] = data['nomen'].map(data['nomen'].value_counts(normalize=True))
    data.to_parquet(DATADIR/'data_population.parquet')

data = of.open_parquet('data_population.parquet')

# Regresion lineal:

def reg_lineal(data)
    X_train, X_test, y_train, y_test = train_test_split(data, target, test_size=0.177655, random_state=789)

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    print("Coefficient de détermination du modèle :", lr.score(X_train, y_train))
    print("Coefficient de détermination obtenu par Cv :", cross_val_score(lr,X_train,y_train).mean())

reg_lineal(data)


#Random forest

def random_forest(data):
    model = RandomForestRegressor(n_estimators = 10, random_state = 0)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    rmse = float(format(np.sqrt(mean_squared_error(y_test, y_pred)), '.3f'))
    SI = (rmse/y_pred.mean())*100
    print("\nRMSE: ", rmse)
    print("Scatter_index: ", SI)

random_forest(data)
# Random forest tuning

from sklearn.model_selection import RandomizedSearchCV

# Number of trees in random forest
n_estimators = [int(x) for x in np.linspace(start = 200, stop = 500, num = 10)]
# Number of features to consider at every split
max_features = ['auto', 'sqrt']
# Maximum number of levels in tree
max_depth = [int(x) for x in np.linspace(10, 110, num = 11)]
max_depth.append(None)
# Minimum number of samples required to split a node
min_samples_split = [2, 5, 10]
# Minimum number of samples required at each leaf node
min_samples_leaf = [1, 2, 4]
# Method of selecting samples for training each tree
bootstrap = [True, False]
# Create the random grid
random_grid = {'n_estimators': n_estimators,
               'max_features': max_features,
               'max_depth': max_depth,
               'min_samples_split': min_samples_split,
               'min_samples_leaf': min_samples_leaf,
               'bootstrap': bootstrap}

rf = RandomForestRegressor()

rf_random = RandomizedSearchCV(estimator = rf, param_distributions = random_grid, n_iter = 50, cv = 3, verbose=2, random_state=42, n_jobs = -1)

rf_random.fit(X_train, y_train)

