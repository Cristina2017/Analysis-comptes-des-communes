# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 09:25:58 2022

@author: crist
"""

import pandas as pd
import numpy as np
import re
from src import DATADIR, DATARAW, ROOTDIR
from src.data import format_converter as fc
import unicodedata
from src.data import open_files as of
import matplotlib.pyplot as plt
import seaborn as sns


# def cleaning(data):
#     data["insee"] = data["insee"].astype("str")
#     data = data.drop(
#         [
#             "geo",
#             "longitud",
#             "latitud",
#             "dist",
#             "perc_diff_ptot",
#             "dense",
#             "interm",
#             "peu_dense",
#             "tres_peu_dense",
#             ],
#         axis=1,
#         )

#     return data

# data.to_parquet(DATADIR / "data_clean.parquet")
# data = of.open_parquet("data_clean.parquet")
def replacing_mistaken_comnames(data):

    """
    To continue with our analysis we need to add the population for some communes that don't have this value.
    We have searched in the internet this data.
    """
    data["insee"] = data["insee"].astype("str")
    data["com_name"] = data["com_name"].replace(
        [
            "Benevent",
            "Charbillac",
            "Bauge",
            "Robin-Montsecret",
            "Villiere-Frenes",
            "Fresnaye",
            "Carelle",
        ],
        [
            "Benevent-Et-Charbillac",
            "Benevent-Et-Charbillac",
            "Bauge-En-Cajou",
            "Montsecret",
            "Frenes",
            "Fresnaye-Sur-Chedouet",
            "Carelles",
        ],
    )

    communes = [
        "Bourg-De-Thizy",
        "La-Chapelle-De-Mardore",
        "Mardore",
        "Marnand",
        "Nuelles",
        "Saint-Germain-Sur-L'Arbresle",
        "Saint-Laurent-De-Vaux",
        "Clux",
        "Chasse",
        "Fresnaye-Sur-Chedouet",
        "Lignieres-La-Carelle",
        "Roullee",
        "Saint-Rigomer-Des-Bois",
        "Saint-Offenge-Dessous",
        "Saint-Offenge-Dessus",
        "Bois-Guillaume-Bihorel",
        "Moret-Sur-Loing",
        "Beaussais",
        "Saint-Clementin",
        "Voultegon",
        "Agnieres-En-Devoluy",
        "Benevent-Et-Charbillac",
        "La Cluse",
        "Les Infournas",
        "Saint-Disdier",
        "Saint-Etienne-En-Devoluy",
        "Notre-Dame-D'Estrees",
        "Auxon-Dessus",
        "Pont-De-Roide",
        "Badinieres",
        "Eclose",
        "Bauge-En-Cajou",
        "Chemille",
        "Clefs",
        "Montpollin",
        "Pontigne",
        "Saint-Martin-D'Arce",
        "Vieil-Bauge",
        "Vaulandry",
        "Pautaines-Augeville",
        "Loisey-Culey",
        "Montherlant",
        "Clairefougere",
        "Frenes",
        "Marcei",
        "Montsecret",
        "Saint-Christophe-Le-Jajolet",
        "Saint-Cornier-Des-Landes",
        "Saint-Jean-Des-Bois",
        "Saint-Loyer-Des-Champs",
        "Tinchebray",
        "Yvrandes",
    ]

    population = [
        2468,
        223,
        196,
        596,
        691,
        1574,
        267,
        113,
        193,
        924,
        376,
        238,
        454,
        776,
        328,
        13255,
        4326,
        423,
        517,
        593,
        291,
        276,
        55,
        24,
        118,
        502,
        152,
        1159,
        4215,
        681,
        753,
        3681,
        7028,
        628,
        243,
        233,
        773,
        1231,
        311,
        22,
        464,
        155,
        126,
        826,
        209,
        539,
        241,
        606,
        183,
        372,
        2606,
        156,
    ]

    d = dict(zip(communes, population))
    for element in d:
        data["ptot"] = np.where(data["com_name"] == element, d[element], data["ptot"])
    # Carelles has the population but there was an error in the name. We search the population and copy
    years = sorted([e for e in data.exer.unique() if e not in (2019, 2020)])
    d = {}
    for year in years:
        df = data[(data["exer"] == year) & (data["com_name"] == "Carelles")][
            "ptot"
        ].unique()
        d[year] = df[0]
    for year in years:
        data["ptot"] = np.where(
            (data["com_name"] == "Carelles") & (data["exer"] == year),
            d[year],
            data["ptot"],
        )
    return data


if __name__ == "__main__":

    missing = of.open_parquet("missing.parquet")

    no_missing = of.open_parquet("no_missing.parquet")

    data = pd.concat([no_missing, missing], axis=0)

    data_for_dataviz = replacing_mistaken_comnames(data)
    data_for_dataviz.to_parquet(DATADIR / "data_total.parquet")
