# -*- coding: utf-8 -*-
"""
Created on Mon Aug 12 13:25:28 2024

@author: laureend
"""

import os
import pandas as pd
import numpy as np

path = os.getcwd() + '/'
data_path = path + "\\Data handler\\full_model\\"
data_path_agg = path + "\\Data handler\\full_model_agg\\"



agg_regions = {"IB": ["ES", "PT"],
               "BK": ["BA", "BG", "SI", "HR", "RS", "MK", "GR", "RO", "HU"]}
all_countries = [x for xs in list(agg_regions.values()) for x in xs]

dict_countries = {"BA": "Bosnia H", "BG": "Bulgaria", 
                  "ES": "Spain", "GR":"Greece", "HR": "Croatia",
                  "HU": "Hungary", "MK": "Macedonia",
                  "PT": "Portugal", "RO": "Romania",
                  "RS": "Serbia", "SI": "Slovenia",
                  }


max_qty = pd.read_excel(data_path + "//Generator.xlsx", sheet_name = "MaxInstalledCapacity",
                        index_col = [0, 1], header = 2, usecols = "A:C")

max_qty = pd.Series(max_qty[max_qty.columns[0]]).unstack()
max_qty = max_qty.loc[(list(dict_countries.values())), ["Solar", "Wind_onshr", "Hydro_ror"]]
max_qty.index = dict_countries.keys()
max_qty.columns = ["solar", "windonshore", "ror"]


def filter_countries(region, df):
    filtered_region = []
    for c in region:
        if c in df.columns:
            filtered_region.append(c)
    return filtered_region

filenames = ['electricload', 'hydroseasonal', 'solar',
             'windoffshore', 'windonshore', "hydroror"]

for name in filenames:
    df = pd.read_csv(data_path + 'Scenariodata/' + name + '.csv')
    date = df.time
    if name in ["hydroseasonal", "hydroror"]:
        df["time"] = pd.to_datetime(df["time"], dayfirst = False)
    else:
        df["time"] = pd.to_datetime(df["time"], dayfirst = True)
        
    df['year'] = df['time'].dt.year
    df = df.loc[df.year.isin([2015,2016,2017,2018,2019]), :]
    df = df.drop(columns=['year'])
    
    balkans = ["BA", "BG", "SI", "HR", "RS", "MK", "GR", "RO", "HU"]
    iberic = ["ES", "PT"]
    
    bk_f = filter_countries(region=balkans, df=df)
    ib_f = filter_countries(region=iberic, df=df)
        
    if name in ['electricload', 'hydroseasonal']:
        for region in list(agg_regions.keys()):
            df[region] = df[locals()[region.lower() + "_f"]].sum(axis=1)
        df = df.drop(columns = bk_f + ib_f)

    elif name == "windonshore":
        df["IB"] = df["ES"]
        df["BK"] = df["RO"]
        df = df.drop(columns = bk_f + ib_f)
        
    elif name == "solar":
        df["IB"] = df["ES"]
        df["BK"] = df["GR"]
        df = df.drop(columns = bk_f + ib_f)
        
        # for region in list(agg_regions.keys()):
        #     region_c = locals()[region.lower() + "_f"]
        #     df2 = df.loc[:, region_c]
        #     df2 = (df2*max_qty.loc[region_c, name]).sum(axis=1) / max_qty.loc[region_c, name].sum()
        #     df[region] = df2
        
        # df = df.drop(columns = bk_f + ib_f)

    elif name == "windoffshore":
        columns = list(df.columns)
        i = columns.index("ES")
        columns[i] = "IB"
        df.columns = columns
    
    elif name == "hydroror":
        for region in list(agg_regions.keys()):
            region_c = locals()[region.lower() + "_f"]
            df[region] = (df[region_c]*max_qty.loc[region_c, "ror"]).sum(
                axis=1) / max_qty.loc[region_c, "ror"].sum()
            df = df.drop(columns = region_c)
        
    df["time"] = date
    df = df.reindex(sorted(df.columns), axis=1)

    df.to_csv(data_path_agg + "\\ScenarioData\\" + name + '.csv', index=False)
    