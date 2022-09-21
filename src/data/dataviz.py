# -*- coding: utf-8 -*-
"""
Created on Thu May  5 11:38:11 2022

@author: crist
"""

import pandas as pd
import numpy as np
from src import DATADIR, DATARAW, ROOTDIR
from src.data import format_converter as fc
import unicodedata
from src.data import open_files as of
import matplotlib.pyplot as plt
import seaborn as sns


years = sorted([e for e in data.exer.unique() if e not in (2019, 2020)])

for year in years:
    globals()["endettes_" + str(year)] = of.open_parquet(
        "endettes_" + str(year) + ".parquet"
    )


# Percentage of communes with debt

d_sirens = {}
for year in years:
    sirens_endettes = globals()["endettes_" + str(year)]["siren"].unique().tolist()
    sirens_non_endettes = globals()["df_" + str(year)]["siren"].unique().tolist()
    d_sirens[year] = {}
    d_sirens[year]["siren_endettes"] = len(sirens_endettes)
    d_sirens[year]["siren_non_endettes"] = len(sirens_non_endettes)


fig, ax = plt.subplots(nrows=len(years), figsize=(13.8, len(years) * 7))
# fig, ax2 = plt.subplots(nrows=len(years), figsize=(13.8,len(years)*7))

for year in years:
    ind = years.index(year)

    # filt = region['Area'] == i

    # don't index into ax2
    # ax2[ind] = ax[ind].twinx()

    # instead, create a local variable ax2 which is the secondary axis
    # on the subplot ax[ind]
    ax2 = ax[ind].twinx()

    ax[ind].bar(
        1,
        d_sirens[year]["siren_endettes"] + d_sirens[year]["siren_non_endettes"],
        0.8,
        color="red",
    )
    ax[ind].set_ylabel("total", color="red", fontsize=14)
    ax2.bar(
        2,
        (
            d_sirens[year]["siren_endettes"]
            / (d_sirens[year]["siren_endettes"] + d_sirens[year]["siren_non_endettes"])
        )
        * 100,
        0.8,
        color="blue",
    )
    ax2.set_ylabel("% indebt", color="blue", fontsize=14)
    ax[ind].set_title(year, size=12)
    ax[ind].tick_params(bottom=False)
    ax[ind].xaxis.set_tick_params(labelsize=0)
    ax[ind].yaxis.set_tick_params(labelsize=10)

    plt.tight_layout()

# #Graph reg_name

# i=1
# plt.figure(figsize = (30,30))
# for year in years:
#     plt.subplot(2,4,i)
#     ax = sns.countplot(x = globals()['group'+'_'+str(year)].reg_name, order= globals()['group'+'_'+str(year)].reg_name.value_counts().index)
#     ax.set(title='Regions with more sirens with debt'+ " " + str(year))
#     ax.set_xticklabels(ax.get_xticklabels(),rotation=45, ha='right', rotation_mode='anchor')
#     plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5)
#     #ax.set(xlabel=None)
#     i+=1

# # # Limitando solo los 5 primeros

# i=1
# plt.figure(figsize = (30,30))
# for year in years:
#     plt.subplot(2,4,i)
#     ax = sns.countplot(x = globals()['endettes'+'_'+str(year)].reg_name, order= globals()['endettes'+'_'+str(year)].reg_name.value_counts().iloc[:5].index)
#     ax.set(title='Regions with more sirens with debt'+ " " + str(year))
#     ax.set_xticklabels(ax.get_xticklabels(),rotation=45, ha='right', rotation_mode='anchor')
#     plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5)
#     #plt.set(xlabel=None)
#     i+=1

# # #rural


i = 1
plt.figure(figsize=(30, 30))
for year in years:
    plt.subplot(2, 4, i)
    ax = sns.countplot(
        x=globals()["endettes" + "_" + str(year)].rural,
        order=globals()["endettes" + "_" + str(year)]
        .rural.value_counts()
        .iloc[:5]
        .index,
    )
    ax.set(title="Rural Regions distribution" + " " + str(year))
    ax.set_xticklabels(
        ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor"
    )
    plt.subplots_adjust(
        left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5
    )
    # ax.set(xlabel=None)
    i += 1
# We can point out that all years, for the rural variable, most of the communes in debt belong to rural. Is that a strong argument?, we must compare with the
# totality of communes that belongs a this category.
di = {}
for year in years:
    rural_endettes = globals()["endettes_" + str(year)][
        globals()["endettes_" + str(year)]["rural"] == "Oui"
    ]["rural"].tolist()
    sirens = globals()["endettes_" + str(year)]["siren"].unique()
    sirens2 = data[data["rural"] == "Oui"]["siren"].unique().tolist()
    rural_non_endettes = (
        data[
            (~data["siren"].isin(sirens))
            & (data["exer"] == year)
            & (data["rural"] == "Oui")
        ]["siren"]
        .unique()
        .tolist()
    )
    di[year] = {}
    di[year]["rural_endettes"] = len(rural_endettes)
    di[year]["rural_non_endettes"] = len(rural_non_endettes)
    di[year]["total"] = di[year]["rural_endettes"] + di[year]["rural_non_endettes"]


fig, ax = plt.subplots(nrows=len(years), figsize=(13.8, len(years) * 7))
# fig, ax2 = plt.subplots(nrows=len(years), figsize=(13.8,len(years)*7))

for year in years:
    ind = years.index(year)

    ax2 = ax[ind].twinx()

    ax[ind].bar(1, di[year]["rural_endettes"], 0.8, color="red")
    ax[ind].set_ylabel("total_rural_endettes", color="red", fontsize=14)
    ax[ind].bar(2, di[year]["total"], 0.8, color="yellow")
    ax2.set_ylabel("% rural_indebt", color="blue", fontsize=14)
    ax2.bar(
        3, (di[year]["rural_endettes"] / di[year]["total"]) * 100, 0.8, color="blue"
    )
    ax[ind].set_title(year, size=12)
    ax[ind].tick_params(bottom=False)
    ax[ind].xaxis.set_tick_params(labelsize=0)
    ax[ind].yaxis.set_tick_params(labelsize=10)

    plt.tight_layout()


# # # montagne
i = 1
plt.figure(figsize=(30, 30))
for year in years:
    plt.subplot(2, 4, i)
    ax = sns.countplot(
        x=globals()["endettes" + "_" + str(year)].montagne,
        order=globals()["endettes" + "_" + str(year)].montagne.value_counts().index,
    )
    ax.set(title="Regions with more sirens with debt" + " " + str(year))
    ax.set_xticklabels(
        ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor"
    )
    plt.subplots_adjust(
        left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5
    )
    # ax.set(xlabel=None)
    i += 1

di = {}
for year in years:
    montagne_endettes = globals()["endettes_" + str(year)][
        globals()["endettes_" + str(year)]["montagne"] == "Oui"
    ]["montagne"].tolist()
    sirens = globals()["endettes_" + str(year)]["siren"].unique()
    sirens2 = data[data["montagne"] == "Oui"]["siren"].unique().tolist()
    montagne_non_endettes = (
        data[
            (~data["siren"].isin(sirens))
            & (data["exer"] == year)
            & (data["montagne"] == "Oui")
        ]["siren"]
        .unique()
        .tolist()
    )
    di[year] = {}
    di[year]["montagne_endettes"] = len(montagne_endettes)
    di[year]["montagne_non_endettes"] = len(montagne_non_endettes)
    di[year]["total"] = (
        di[year]["montagne_endettes"] + di[year]["montagne_non_endettes"]
    )

fig, ax = plt.subplots(nrows=len(years), figsize=(13.8, len(years) * 7))
# fig, ax2 = plt.subplots(nrows=len(years), figsize=(13.8,len(years)*7))

for year in years:
    ind = years.index(year)

    ax2 = ax[ind].twinx()

    ax[ind].bar(1, di[year]["montagne_endettes"], 0.8, color="red")
    ax[ind].set_ylabel("total_montagne_endettes", color="red", fontsize=14)
    ax[ind].bar(2, di[year]["total"], 0.8, color="yellow")
    ax2.set_ylabel("% montagne_indebt", color="blue", fontsize=14)
    ax2.bar(
        3, (di[year]["montagne_endettes"] / di[year]["total"]) * 100, 0.8, color="blue"
    )
    ax[ind].set_title(year, size=12)
    ax[ind].tick_params(bottom=False)
    ax[ind].xaxis.set_tick_params(labelsize=0)
    ax[ind].yaxis.set_tick_params(labelsize=10)

    plt.tight_layout()


# # #qpv

i = 1
plt.figure(figsize=(30, 30))
for year in years:
    plt.subplot(2, 4, i)
    ax = sns.countplot(
        x=globals()["endettes" + "_" + str(year)].qpv,
        order=globals()["endettes" + "_" + str(year)].qpv.value_counts().index,
    )
    ax.set(title="Regions with more sirens with debt" + " " + str(year))
    ax.set_xticklabels(
        ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor"
    )
    plt.subplots_adjust(
        left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5
    )
    # ax.set(xlabel=None)
    i += 1

di = {}
for year in years:
    qpv_endettes = globals()["endettes_" + str(year)][
        globals()["endettes_" + str(year)]["qpv"] == "Oui"
    ]["qpv"].tolist()
    sirens = globals()["endettes_" + str(year)]["siren"].unique()
    sirens2 = data[data["qpv"] == "Oui"]["siren"].unique().tolist()
    qpv_non_endettes = (
        data[
            (~data["siren"].isin(sirens))
            & (data["exer"] == year)
            & (data["qpv"] == "Oui")
        ]["siren"]
        .unique()
        .tolist()
    )
    di[year] = {}
    di[year]["qpv_endettes"] = len(qpv_endettes)
    di[year]["qpv_non_endettes"] = len(qpv_non_endettes)
    di[year]["total"] = di[year]["qpv_endettes"] + di[year]["qpv_non_endettes"]

fig, ax = plt.subplots(nrows=len(years), figsize=(13.8, len(years) * 7))
# fig, ax2 = plt.subplots(nrows=len(years), figsize=(13.8,len(years)*7))

fig, ax = plt.subplots(nrows=len(years), figsize=(10, 14))

# fig, ax2 = plt.subplots(nrows=len(years), figsize=(13.8,len(years)*7))

for year in years:
    ind = years.index(year)

    ax2 = ax[ind].twinx()

    ax[ind].bar(1, di[year]["qpv_endettes"], 0.8, color="red", label="indebt with qpv")
    ax[ind].set_ylabel("total_qpv_endettes", color="red", fontsize=14)
    ax[ind].bar(2, di[year]["total"], 0.8, color="yellow", label="total qpv")
    ax2.set_ylabel("% qpv_indebt", color="blue", fontsize=14)
    ax2.bar(
        3,
        (di[year]["qpv_endettes"] / di[year]["total"]) * 100,
        0.8,
        color="blue",
        label="% indebt with qpv",
    )
    ax[ind].set_title(year, size=12)
    ax[ind].tick_params(bottom=False)
    ax[ind].xaxis.set_tick_params(labelsize=0)
    ax[ind].yaxis.set_tick_params(labelsize=10)
    plt.legend()

    plt.tight_layout()

# # # Tranche_population
# i=1
# plt.figure(figsize = (30,30))
# for year in years:
#     plt.subplot(2,4,i)
#     ax = sns.countplot(x = globals()['endettes'+'_'+str(year)].tranche_population, order= globals()['endettes'+'_'+str(year)].tranche_population.value_counts().index)
#     ax.set(title='Regions with more sirens with debt'+ " " + str(year))
#     ax.set_xticklabels(ax.get_xticklabels(),rotation=45, ha='right', rotation_mode='anchor')
#     plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5)
#     #ax.set(xlabel=None)
#     i+=1

di = {}
for year in years:
    tranche_endettes = globals()["endettes_" + str(year)][
        globals()["endettes_" + str(year)]["tranche_population"] == "Oui"
    ]["tranche_population"].tolist()
    sirens = globals()["endettes_" + str(year)]["siren"].unique()
    sirens2 = data[data["tranche_population"] == "Oui"]["siren"].unique().tolist()
    tranche_non_endettes = (
        data[
            (~data["siren"].isin(sirens))
            & (data["exer"] == year)
            & (data["tranche_population"] == "Oui")
        ]["siren"]
        .unique()
        .tolist()
    )
    di[year] = {}
    di[year]["tranche_endettes"] = len(tranche_endettes)
    di[year]["tranche_non_endettes"] = len(tranche_non_endettes)
    di[year]["total"] = di[year]["tranche_endettes"] + di[year]["tranche_non_endettes"]

fig, ax = plt.subplots(nrows=len(years), figsize=(13.8, len(years) * 7))
# fig, ax2 = plt.subplots(nrows=len(years), figsize=(13.8,len(years)*7))

for year in years:
    ind = years.index(year)

    ax2 = ax[ind].twinx()

    ax[ind].bar(1, di[year]["tranche_endettes"], 0.8, color="red")
    ax[ind].set_ylabel("total_tranches_endettes", color="red", fontsize=14)
    ax[ind].bar(2, di[year]["total"], 0.8, color="yellow")
    ax2.set_ylabel("% tranches_indebt", color="blue", fontsize=14)
    ax2.bar(
        3, (di[year]["tranche_endettes"] / di[year]["total"]) * 100, 0.8, color="blue"
    )
    ax[ind].set_title(year, size=12)
    ax[ind].tick_params(bottom=False)
    ax[ind].xaxis.set_tick_params(labelsize=0)
    ax[ind].yaxis.set_tick_params(labelsize=10)

    plt.tight_layout()


# # # Tranche_revenue_imposable_par_habitant


# # # population

i = 1
for year in years:
    globals()["goal" + "_" + str(year)] = globals()[
        "endettes" + "_" + str(year)
    ].group.str.startswith("tres endetté")
    plt.subplot(2, 4, i)
    ax = sns.distplot(
        np.log10(
            globals()["endettes" + "_" + str(year)].loc[
                globals()["goal" + "_" + str(year)], "ptot"
            ]
        ),
        bins=10,
        kde=True,
        rug=True,
        color="red",
    )
    ax.set(title="Distribution Population" + " " + str(year))
    i += 1

i = 1
for year in years:
    plt.subplot(2, 4, i)
    ax = sns.distplot(
        np.log10(globals()["df" + "_" + str(year)]["ptot"]),
        bins=10,
        kde=True,
        rug=True,
        color="red",
    )
    ax.set(title="Distribution Population" + " " + str(year))
    i += 1

# # Debt

# i=1
# for year in years:
#     globals()['goal' + '_' + str(year)] = globals()['endettes'+'_'+str(year)].group.str.startswith("tres endetté")
#     plt.subplot(2,4,i)
#     ax = sns.distplot(np.log10(globals()['endettes'+'_'+str(year)].loc[globals()['goal' + '_' + str(year)], "debt"]), bins=10, kde=True, rug=True, color='red');
#     ax.set(title='Distribution debt'+ " " + str(year))
#     i+=1


# #  Relation betwen reg_name and debt

# i=1
# plt.figure(figsize = (30,30))
# for year in years:
#     globals()['endettes'+'_'+str(year)] = globals()['endettes'+'_'+str(year)].dropna(axis = 0, how = 'all', subset=['reg_name'])
#     plt.subplot(2,4,i)
#     ax = plt.scatter(x = globals()['endettes'+'_'+str(year)]['reg_name'], y = globals()['endettes'+'_'+str(year)]['debt'])
#     #ax.set(title='Region - debt'+ " " + str(year))
#     ax.set_xticklabels(ax.get_xticklabels(),rotation=45, ha='right', rotation_mode='anchor')
#     plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5)
#     #ax.set(xlabel=None)
#     i+=1
# # # Type de budget


# #corr = of.open_csv('correspondance.csv', sep=";")


# #


# d = {}
# for element in range(data.agregat_niveau.nunique()):
#     df = data[data['agregat_niveau']==element]
#     d[element]=df.agregat.unique()

# We are going to use tje agregat Capacité ou besoin de financement to figuer out if a commune has debt or not. In 2015 the only agregat that can show us this situation
# is the agregat 'Variation du fonds de roulement'

# for year in years:
#     globals()["endettes" + "_" + str(year)].to_parquet(
#         str(DATADIR) + "/" + "endettes_" + str(year) + ".parquet"
#     )
