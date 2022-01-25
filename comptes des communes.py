# -*- coding: utf-8 -*-
"""
Created on Mon Jan 10 12:03:23 2022

@author: crist
"""

#DATADIR = C:/Users/crist/mentoring/comptes des communes/data/raw
import pandas as pd

data = pd.read_csv(r'C:\Users\crist\mentoring\comptes des communes\data\raw\base_communes.csv', sep=";", encoding="utf-8")
data = data.drop(['reg_code', 'dep_code', 'epci_code','com_code', 'montant_en_millions'], axis=1)
data =data.drop(['ordre_analyse1_section1','ordre_analyse1_section2','ordre_analyse1_section3','ordre_analyse2_section1','ordre_analyse2_section2',
'ordre_analyse2_section3','ordre_analyse3_section1','ordre_analyse3_section2','ordre_analyse3_section3','ordre_analyse4_section1',
'ordre_affichage'], axis = 1)

outre_mer_missings = data['outre_mer'].isnull()
outre_mer_missings = pd.DataFrame(outre_mer_missings)
om = outre_mer_missings[outre_mer_missings['outre_mer']==True]
indice = om.index
outre_mer_missings = data.iloc[indice]

import re
#r3 = re.compile(r"[^-]+$") from - ahead

# Composed names
r1 = re.compile(r"([a-zA-Z0-9.-áè]+-[a-zA-Z'.-]+)")

# One unique name or several names (we keep the last one)

r2 = re.compile(r"[^\W]+$")

for i, row in outre_mer_missings.iterrows():
    outre_mer_missings.at[i,'com_name'] = r1.findall(outre_mer_missings.at[i,'lbudg'])
for i, row in outre_mer_missings.iterrows():
    if not row['com_name']:
        outre_mer_missings.at[i,'com_name'] = r2.findall(outre_mer_missings.at[i,'lbudg'])

for i, row in outre_mer_missings.iterrows():
    if not row['com_name']:
         outre_mer_missings.at[i,'com_name'] = row['lbudg']
    
# We undo the list that is automatically done.
       
for i, row in outre_mer_missings.iterrows():
    outre_mer_missings.at[i,'com_name'] = "".join(row['com_name']) 

# We have detected that in some cases there are some words "RESTAURANT", "EAU", "ASSAT" following for a dash that are in our row and we did not want it.
#So, we will remove it.
words = ["ASST-", "RESTAURANT-", "EAU-", "LOTS-", "FORETS-", "1-", "HBS-", "SPANC-", "LOT-"]
for i, row in outre_mer_missings.iterrows():
    for word in words:
        if word in row['com_name']:
            outre_mer_missings.at[i,'com_name'] = row['com_name'].replace(word, "")
            
# Now, as in our dataframe "data" we have the column com_name in lowercase, we are going to transform it:
# For that the easiest way is to convert into lowercase and then make a title:
for i,row in outre_mer_missings.iterrows():
    outre_mer_missings.at[i,'com_name'] = row['com_name'].lower()
    outre_mer_missings.at[i,'com_name'] = row['com_name'].title()
    
# we have identified at the end of some words the articles that is necesary to eliminate ['(Le )', '(La )', '(Les)']

for i, row in outre_mer_missings.iterrows():
    if '(Le )' in row['com_name']:
        row['com_name'] = row['com_name'].replace(' (Le )', "")
        outre_mer_missings.at[i,'com_name'] = "Le" + " " +  row['com_name']
    if '(La )' in row['com_name']:
        row['com_name'] = row['com_name'].replace(' (La )', "")
        outre_mer_missings.at[i,'com_name'] = "La" + " " +  row['com_name']
    if '(Les)' in row['com_name']:
        row['com_name'] = row['com_name'].replace(' (Les)', "")
        outre_mer_missings.at[i,'com_name'] = "Les" + " " +  row['com_name']

# At this point we can look for the unique names to compare and change manually some names if it is necessary

for i, row in outre_mer_missings.iterrows():
    if "Magny" in row['com_name']:
        outre_mer_missings.at[i,'com_name'] = "Le Magny"
    if 'Chapelle-De-Mardore' in row['com_name']:
        outre_mer_missings.at[i,'com_name'] = 'La-Chapelle-De-Mardore'
    if "Bihorel" in row['com_name']:
         outre_mer_missings.at[i,'com_name'] = "Bois-Guillaume-Bihorel"
    if "Dessous" in row['com_name']:
        outre_mer_missings.at[i,'com_name'] = "Saint-Offenge-Dessous"
    if "Chedoue" in row['com_name']:
        outre_mer_missings.at[i,'com_name'] = "Fresnaye-Sur-Chedouet"
