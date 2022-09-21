# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 08:56:41 2022

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


# We select the communites with debt
years = sorted([e for e in data.exer.unique() if e not in (2019, 2020)])


def selection(data):

    """
    Build the dataframes with the communes in debt.

    """

    for year in years:
        if year == 2015:
            globals()["df" + "_" + str(year)] = data[
                (data["exer"] == year)
                & (data["agregat"] == "Variation du fonds de roulement")
                & (data["montant"] < 0)
            ]
        else:
            globals()["df" + "_" + str(year)] = data[
                (data["exer"] == year)
                & (data["agregat"] == "Capacité ou besoin de financement")
                & (data["montant"] < 0)
            ]
    return globals()["df" + "_" + str(year)]


# We chose only one siren but we need to add before the feature montant:


# for year in years:
#    add(globals['df_'+stre(year)])
def add_population(data):

    """
    We group by to sum all the movement for each siren.
    That will get as a result a dataframe with one line per siren
    and the total montant. We add the population.
    """

    for year in years:
        globals()["group" + "_" + str(year)] = (
            globals()["df" + "_" + str(year)]
            .groupby("siren")
            .agg({"montant": "sum"})
            .reset_index()
        )
        globals()["d" + "_" + str(year)] = {}
        for siren in globals()["df" + "_" + str(year)]["siren"].unique():
            globals()["d" + "_" + str(year)][siren] = (
                globals()["df" + "_" + str(year)]
                .loc[globals()["df" + "_" + str(year)]["siren"] == siren]["ptot"]
                .tolist()[0]
            )
        globals()["group" + "_" + str(year)]["ptot"] = globals()[
            "group" + "_" + str(year)
        ]["siren"].apply(lambda x: globals()["d" + "_" + str(year)][x])
    return globals()["group" + "_" + str(year)]


# for year in years:
#   indebts(globals['group_'+stre(year)])


def indebts(data):
    for year in years:
        globals()["group" + "_" + str(year)]["mont_hab"] = abs(
            globals()["group" + "_" + str(year)]["montant"]
            / globals()["group" + "_" + str(year)]["ptot"]
        )
        globals()["group" + "_" + str(year)]["mont_hab"] = np.where(
            globals()["group" + "_" + str(year)]["mont_hab"] < 0.1,
            0,
            globals()["group" + "_" + str(year)]["mont_hab"],
        )
        globals()["group" + "_" + str(year)]["group"] = np.where(
            globals()["group" + "_" + str(year)]["mont_hab"]
            > globals()["group" + "_" + str(year)]["mont_hab"].mean(),
            "tres endetté",
            " ",
        )
        globals()["endettes" + "_" + str(year)] = globals()["group" + "_" + str(year)][
            globals()["group" + "_" + str(year)]["group"] == "tres endetté"
        ]
    return globals()["endettes_" + str(year)]


def add_features_endettes(data):
    for year in years:
        globals()["d" + "_" + str(year)] = {}
        for siren in globals()["endettes" + "_" + str(year)]["siren"]:
            globals()["d" + "_" + str(year)][siren] = {}
            globals()["d" + "_" + str(year)][siren]["reg_name"] = (
                globals()["df" + "_" + str(year)]
                .loc[globals()["df" + "_" + str(year)]["siren"] == siren]["reg_name"]
                .tolist()[0]
            )
            globals()["d" + "_" + str(year)][siren]["rural"] = (
                globals()["df" + "_" + str(year)]
                .loc[globals()["df" + "_" + str(year)]["siren"] == siren]["rural"]
                .tolist()[0]
            )
            globals()["d" + "_" + str(year)][siren]["qpv"] = (
                globals()["df" + "_" + str(year)]
                .loc[globals()["df" + "_" + str(year)]["siren"] == siren]["qpv"]
                .tolist()[0]
            )
            globals()["d" + "_" + str(year)][siren]["touristique"] = (
                globals()["df" + "_" + str(year)]
                .loc[globals()["df" + "_" + str(year)]["siren"] == siren]["touristique"]
                .tolist()[0]
            )
            globals()["d" + "_" + str(year)][siren]["montagne"] = (
                globals()["df" + "_" + str(year)]
                .loc[globals()["df" + "_" + str(year)]["siren"] == siren]["montagne"]
                .tolist()[0]
            )
            globals()["d" + "_" + str(year)][siren]["tranche_population"] = (
                globals()["df" + "_" + str(year)]
                .loc[globals()["df" + "_" + str(year)]["siren"] == siren][
                    "tranche_population"
                ]
                .tolist()[0]
            )
            globals()["d" + "_" + str(year)][siren]["ptot"] = (
                globals()["df" + "_" + str(year)]
                .loc[globals()["df" + "_" + str(year)]["siren"] == siren]["ptot"]
                .tolist()[0]
            )
            globals()["d" + "_" + str(year)][siren][
                "tranche_revenu_imposable_par_habitant"
            ] = (
                globals()["df" + "_" + str(year)]
                .loc[globals()["df" + "_" + str(year)]["siren"] == siren][
                    "tranche_revenu_imposable_par_habitant"
                ]
                .tolist()[0]
            )

        options = [
            "reg_name",
            "rural",
            "qpv",
            "montagne",
            "touristique",
            "tranche_population",
            "ptot",
            "tranche_revenu_imposable_par_habitant",
        ]
        for year in years:
            for siren in globals()["endettes" + "_" + str(year)]["siren"]:
                for option in options:
                    globals()["endettes" + "_" + str(year)][option] = globals()[
                        "endettes" + "_" + str(year)
                    ]["siren"].apply(
                        lambda x: globals()["d" + "_" + str(year)][x][option]
                    )

    return globals()["endettes_" + str(year)]


if __name__ == "__main__":
    # open_data(file)
    pipeline = [selection, add_population, indebts, add_features_endettes]

    data = of.open_parquet(DATADIR / "data_total.parquet")
    selection(data)
    for year in years:
        add(globals["df_" + stre(year)])
        indebts(globals["group_" + stre(year)])
        add_features_endettes(globals()["endettes_" + str(year)])
        globals["endettes_" + str(year)].to_parquet(
            DATADIR / "data_for_dataviz.parquet"
        )
