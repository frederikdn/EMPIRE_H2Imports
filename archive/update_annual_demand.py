# -*- coding: utf-8 -*-
"""
Created on Fri Aug 23 08:55:03 2024

@author: laureend
"""

import os
import pandas as pd
import numpy as np

path = os.getcwd() + '/'
data_path = path + "\\Data handler\\full_model\\"
data_path_agg = path + "\\Data handler\\full_model_agg\\"

countries = ['AT', 'BE', 'BG',  'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', 'GR',
       'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 'LV', 'NL', 'PL', 'PT', 'RO',
       'SE', 'SI', 'SK']

""" TYNDP 2022 """

df = pd.read_excel(path + "\\Data handler\\data_sources\\TYNDP2022_220310_Updated_Electricity_Modelling_Results.xlsx",
                   sheet_name = "Demand", index_col = 0)
df = df.iloc[np.where((df["Type_node"] == "Transmission Node") | (df["Type_node"] == "Prosumer Node"))]

df = df.iloc[np.where( (df["Year"] >= 2030) & (df["Climate Year"] == "CY 2009"))]
df = df.iloc[np.where(df["Scenario"] != "National Trends")]


countries = [i.split("0")[0] for i in df.index]
countries = [i.replace("LUG1", "LU") for i in countries]
countries = [i.replace("UK", "GB") for i in countries]

df.index = pd.MultiIndex.from_arrays([df["Type_node"], countries, df["Year"], df["Scenario"]])
df = df["Value"]

df = df.groupby(level = [1,2,3]).sum()

df_no = df.loc[(["NOM1", "NON1", "NOS"], )].groupby(level = [1,2]).sum()
array1 = ["NO"]*len(df_no.index)
array2 = [2030]*2 + [2040]*2 + [2050]*2
array3 = ["Distributed Energy", "Global Ambition"]*3
df_no.index = pd.MultiIndex.from_arrays([array1, array2, array3])

df_dk = df.loc[(["DKE1", "DKW1"], )].groupby(level = [1,2]).sum()
array1 = ["DK"]*len(df_dk.index)
df_dk.index = pd.MultiIndex.from_arrays([array1, array2, array3])

countries = ['AT', "BA", 'BE', 'BG', "CH", 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', "GB", 'GR',
       'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 'LV', "MK", 'NL', "NO", 'PL', 'PT', 'RO', 'RS', 
       'SE', 'SI', 'SK']


df = pd.concat([df, df_no, df_dk], axis = 0)
df = df.loc[(countries, )]
df = df.unstack(level = [1, 2])
df = df*1000 # conversion in MWh (original data in GWh)

no_shares = [0.223, 0.292, 0.179, 0.154, 0.152]

""" Dataframes with format for Node file"""
df2 = df.copy(deep = True)
df2.columns = df2.columns.swaplevel()
df2 = df2[("Distributed Energy", )]
for i in range(5):
    df2.loc["NO"+str(i+1), ] = df2.loc["NO", ]*no_shares[i]

countries = ['AT', "BA", 'BE', 'BG', "CH", 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', "GB", 'GR',
       'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 'LV', "MK", 'NL'] + ["NO"+str(i) for i in range(1, 6)] + [
           'PL', 'PT', 'RO', 'RS', 'SE', 'SI', 'SK']
df2 = df2.loc[countries, ]
df2.columns = [1, 2, 3]

df2_agg = df2.copy(deep = True)
df2 = df2.unstack()
df2.index = df2.index.swaplevel()
df2.index = pd.MultiIndex.from_tuples(df2.index, names = ["Nodes", "Period"])

balkans = ["BA", "BG", "SI", "HR", "RS", "MK", "GR", "RO", "HU"]
df2_agg.loc["IB", ] = df2_agg.loc["ES", ] + df2_agg.loc["PT", ]
df2_agg.loc["BK", ] = df2_agg.loc[balkans, ].groupby(level = 0).sum().sum()

countries = ['AT', 'BE', "BK", "CH", 'CZ', 'DE', 'DK', 'EE', 'FI', 'FR', "GB",
             "IB", 'IE', 'IT', 'LT', 'LU', 'LV', 'NL'] + ["NO"+str(i) for i in range(1, 6)] + [
           'PL', 'SE',  'SK']
df2_agg = df2_agg.loc[countries, ]
df2_agg = df2_agg.unstack()
df2_agg.index = df2_agg.index.swaplevel()
df2_agg.index = pd.MultiIndex.from_tuples(df2_agg.index, names = ["Nodes", "Period"])



with pd.ExcelWriter(path + "\\Data handler\\data_sources\\annual_electricity_demand.xlsx") as writer:
    df.to_excel(writer, sheet_name = "tyndp2022")
    df2.to_excel(writer, sheet_name = "full_model")
    df2_agg.to_excel(writer, sheet_name = "full_model_agg")

""" Energy demand in transports """
df = pd.read_excel(path + "\\Data handler\\data_sources\\Demand_Scenarios_TYNDP_2024_After_Public_Consultation.xlsb",
                   sheet_name = "3_DEMAND_OUTPUT", index_col = [1, 6], header = [0, 1])

df = df.iloc[np.where((df[df.columns[3]] == "Transport") & (df[df.columns[4]] == "Total"))]
df = df[df.columns[-5:]]
df.index = df.index.swaplevel()

df_total = df.groupby(level = 1).sum()
for s in ["DE", "GA"]:
    df_total[(s, 2030)] =  df_total[("REF", 2019)]+ ( df_total[(s, 2040)] - df_total[("REF", 2019)])/2

df = df.loc[(["Electricity", "Hydrogen", "Methane", ], ), ]
df.index = df.index.swaplevel()

df_eu = df.loc[("EU", ), ]
df_eu.loc["Total", ] = df_total.loc["EU", ]



df = df.unstack()
df_total.columns = pd.MultiIndex.from_arrays([["REF", "DE", "DE", "GA", "GA", "DE", "GA"], [2019, 2040, 2050, 2040, 2050, 2030, 2030], ["Total"]*len(df_total.columns)])
df = pd.concat([df, df_total], axis = 1)
df.to_excel(path + "\\Data handler\\data_sources\\transport_demand.xlsx")
### calculations made my hand in excel file for BA, RS, MK and NO

countries = ['AT',"BA", 'BE', "BG", "CH",'CZ', 'DE', 'DK', 'EE', "ES",  'FI', 'FR', "GB", "GR", 
             "HR", "HU", 'IE', 'IT', 'LT', 'LU', 'LV', "MK", 'NL', "NO", 'PL', "PT",
             "RO","RS",  'SE',"SI", 'SK']

balkans = ["BA", "BG", "SI", "HR", "RS", "MK", "GR", "RO", "HU"]
no_zones = ["NO"+str(i) for i in range(1, 6)]

countries2 = ['AT', 'BE', "BK", "CH", 'CZ', 'DE', 'DK', 'EE', 'FI', 'FR', "GB",
             "IB", 'IE', 'IT', 'LT', 'LU', 'LV', 'NL'] + no_zones + ['PL', 'SE',  'SK']

countries3 = ['AT',"BA", 'BE', "BG", "CH",'CZ', 'DE', 'DK', 'EE', "ES",  'FI', 'FR', "GB", "GR", 
             "HR", "HU", 'IE', 'IT', 'LT', 'LU', 'LV', "MK", 'NL'] + no_zones+ ['PL', "PT",
             "RO","RS",  'SE',"SI", 'SK']

shares = pd.DataFrame(columns = [2030, 2040, 2050], index = ["Electricity", "Hydrogen", "Natural Gas"])
shares.loc["Electricity", ] = [0.15, 0.28, 0.5]
shares.loc["Hydrogen", ] = [0.05, 0.1, 0.2]
shares.loc["Natural Gas", ] = [0.035, 0.065, 0.08]
                         
no_shares = [0.27, 0.27, 0.2, 0.14, 0.12]

                                                       
df = pd.read_excel(path + "\\Data handler\\data_sources\\transport_demand.xlsx",
                   index_col = 0, header = [0, 1 ,2])
df.index = [i.replace("UK", "GB") for i in df.index]
df.columns = df.columns.swaplevel(i = 0)
df = df.loc[countries, ("Total", )]

for s in ["DE", "GA"]:
    df[(2030, s)] =  df[(2019, "REF")]+ ( df[(2040, s)] - df[(2019, "REF")])/2

for i in range(5):
    df.loc["NO"+str(i+1), :] = df.loc["NO", ]*no_shares[i]

df = df.loc[countries3, ]
df2 = df.copy(deep = True)
df2.columns = df.columns.swaplevel()
df2 = df2[("DE", )][[2030, 2040, 2050]]

df_elec = df2*shares.loc["Electricity", ]
df_h2 = df2*shares.loc["Hydrogen", ]
df_ng = df2*shares.loc["Natural Gas", ]


for i in ["elec", "h2", "ng"]:
    df2 = locals()["df_"+i]
    df2.columns = [1, 2, 3]
    df2 = df2.unstack()
    df2.index = df2.index.swaplevel()
    
    df_agg = df2.copy(deep = True)
    df_ib = df_agg.loc["ES", ] + df_agg.loc["PT", ]
    df_ib.index = pd.MultiIndex.from_arrays([["IB"]*3, [1, 2, 3]])
    df_bk = df_agg.loc[balkans, ].groupby(level = 1).sum()
    df_bk.index = pd.MultiIndex.from_arrays([["BK"]*3, [1, 2, 3]])


    df_agg = pd.concat([df_agg, df_bk, df_ib], axis = 0)
    df_agg = df_agg.loc[(countries2, )]
    df_agg = df_agg.sort_index(level = 1)
    
    locals()["df_agg_"+i] = df_agg*1000000
    locals()["df_"+i] = df2*1000000
    
with pd.ExcelWriter(path + "\\Data handler\\data_sources\\transport_demand_for_EMPIRE.xlsx") as writer:
    for i in ["elec", "h2", "ng"]:
        locals()["df_"+i].to_excel(writer, sheet_name = "full_model_" + i)
        locals()["df_agg_"+i].to_excel(writer, sheet_name = "full_model_agg_" + i)
    df.to_excel(writer, sheet_name = "Data")
    shares.to_excel(writer, sheet_name = "Data", startcol = 9)
    pd.Series(no_shares).to_excel(writer, sheet_name = "Data", startcol = 16)


