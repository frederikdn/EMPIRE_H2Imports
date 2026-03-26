from scenario_random import generate_random_scenario
from datetime import datetime
import time
import gc
import os

########
##USER##
########

# User inputs
version = 'DirectedTransition' # full_model for one country represented by one node // full_model_agg for aggregation of Balkans and Iberic countries
num_trees = 3

##### Parameters to determine number of investment and operational periods #####
NoOfPeriods = 3 # i, can be reduced (if input data files are consistent with it)
NoOfRegSeason = 4
lengthRegSeason = 7*24 # can be reduced
regular_seasons = ["winter", "spring", "summer", "fall"]
NoOfPeakSeason = 2
lengthPeakSeason = 24 # can be reduced
LeapYearsInvestment = 5

##### Parameters to determine number of scenarios #####
NoOfScenarios = 3 # sce, can be reduced
# Use a deterministic cost of gas (1) or stochastic (> 1), but if > 1 then NaturalGas.xlsx input file has to correspond
NoOfGasScenarios = 1
TotNoOfScenarios = NoOfScenarios*NoOfGasScenarios

##### Parameter for sectors modelling #####
HEATMODULE = False
north_sea = True

##### Parameters for scenario generation #####
scenariogeneration = True
FIX_SAMPLE = False
LOADCHANGEMODULE = False
filter_make = False                          # True/False, plot clusters with their mean and wasserstein distance
filter_use = False                           #
copula_clusters_make = False                     # Make copula clusters (for copula-strata SGR)
copula_clusters_use = False                      # Copula-based scenario generation (strata/filter approach)
copulas_to_use = ["electricload"]                # Emprical copula construction of ["electricload", "hydroror", "hydroseasonal", "solar", "windoffshore", "windonshore"]
n_cluster = 10                                   # Number of cluster defined in each season
moment_matching = False                          #
n_tree_compare = 20                              # Number of trees to compare for moment matching and copula-sampling


##### Parameters for CVaR implementation #####
use_cvar = False
TotNoOfScenarios = NoOfScenarios*NoOfGasScenarios
cvar_percentile = 1 - 1 / TotNoOfScenarios # [0,1] CVaR target percentile. close to 1 focusses on high-risk scenarios only. 
# The maximum percentile that is meaningful 1 - 1 / NoOfScenarios, which selects only the highest cost scenario in each period
cvar_weight = 0.5 # [0,1] risk-aversity parameter, 0 is risk neutral, 1 is fully risk-averse

#######
##RUN##
#######

name = version

scenario_data_path = 'Data handler/' + name + '/ScenarioData'

FirstHoursOfRegSeason = [lengthRegSeason*i + 1 for i in range(NoOfRegSeason)]
FirstHoursOfPeakSeason = [lengthRegSeason*NoOfRegSeason + lengthPeakSeason*i + 1 for i in range(NoOfPeakSeason)]
Period = [i + 1 for i in range(NoOfPeriods)]
Scenario = ["scenario"+str(i + 1) for i in range(NoOfScenarios)]
GasScenario = [i + 1 for i in range(NoOfGasScenarios)]
gas_stochasticity_flag = True if NoOfGasScenarios > 1 else False

peak_seasons = ['peak'+str(i + 1) for i in range(NoOfPeakSeason)]
Season = regular_seasons + peak_seasons
Operationalhour = [i + 1 for i in range(FirstHoursOfPeakSeason[-1] + lengthPeakSeason - 1)]
HoursOfRegSeason = [(s,h) for s in regular_seasons for h in Operationalhour \
                 if h in list(range(regular_seasons.index(s)*lengthRegSeason+1,
                               regular_seasons.index(s)*lengthRegSeason+lengthRegSeason+1))]
HoursOfPeakSeason = [(s,h) for s in peak_seasons for h in Operationalhour \
                     if h in list(range(lengthRegSeason*len(regular_seasons)+ \
                                        peak_seasons.index(s)*lengthPeakSeason+1,
                                        lengthRegSeason*len(regular_seasons)+ \
                                            peak_seasons.index(s)*lengthPeakSeason+ \
                                                lengthPeakSeason+1))]
HoursOfSeason = HoursOfRegSeason + HoursOfPeakSeason


if "agg" in version:
    dict_countries = {"AT": "Austria", "BK": "Balkans", "BE": "Belgium",
                      "CH": "Switzerland", "CZ": "CzechR", "DE": "Germany",
                      "DK": "Denmark", "EE": "Estonia", "FI": "Finland",
                      "FR": "France", "GB": "GreatBrit.", "IB": "Iberic",
                      "IE": "Ireland", "IT": "Italy", "LT": "Lithuania",
                      "LU": "Luxemb.", "LV": "Latvia", "NL": "Netherlands",
                      "NO": "Norway", "PL": "Poland", "PT": "Portugal", 
                      "SE": "Sweden", "SK": "Slovakia",
                       }
else:
    dict_countries = {"AT": "Austria", "BA": "BosniaH", "BE": "Belgium",
                      "BG": "Bulgaria", "CH": "Switzerland", "CZ": "CzechR",
                      "DE": "Germany", "DK": "Denmark", "EE": "Estonia",
                      "ES": "Spain", "FI": "Finland", "FR": "France",
                      "GB": "GreatBrit.", "GR": "Greece", "HR": "Croatia",
                      "HU": "Hungary", "IE": "Ireland", "IT": "Italy",
                      "LT": "Lithuania", "LU": "Luxemb.", "LV": "Latvia",
                      "MK": "Macedonia", "NL": "Netherlands", "NO": "Norway",
                      "PL": "Poland", "PT": "Portugal", "RO": "Romania",
                      "RS": "Serbia", "SE": "Sweden", "SI": "Slovenia",
                      "SK": "Slovakia",
                      }

dict_offshr_nodes = {"MF": "MorayFirth", "FF": "FirthofForth",
                     "DB": "DoggerBank", "HS": "Hornsea", "OD": "OuterDowsing",
                     "NF": "Norfolk", "EA": "EastAnglia", "BS": "Borssele",
                     "HK": "HollandseeKust", "HB": "HelgoländerBucht", "NS": "Nordsøen",
                     "UN": "UtsiraNord", "SN1": "SørligeNordsjøI", "SN2": "SørligeNordsjøII",
                     "EHGB":"Energyhub Great Britain", "EHNO": "Energyhub Norway",
                     "EHEU": "Energyhub EU", "BalticCountries_BalticSea": "BalticCountries_BalticSea",
                     "BE_PrincessElisabeth": "BE_PrincessElisabeth", "DE_NorthSea":  "DE_NorthSea",
                     "DE_BalticSea": "DE_BalticSea", "DK_NorthSea": "DK_NorthSea", "FI_BalticSea": "FI_BalticSea",
                     "FR_ChannelSea": "FR_ChannelSea", "FR_Atlantic": "FR_Atlantic", "IE_Atlantic": "IE_Atlantic",
                     "NL_Lagelander": "NL_Lagelander", "NO_Vestavind": "NO_Vestavind", "NO_Sørvest": "NO_Sørvest",
                     "PL_BalticSea": "PL_BalticSea", "SE_BotnieGulf": "SE_BotnieGulf", "SE_BalticSea": "SE_BalticSea",
                     "SE_Luleå": "SE_Luleå", "GB_DoggerBank": "GB_DoggerBank", "GB_ScotlandEast": "GB_ScotlandEast",
                     "GB_SheppeyIsland": "GB_SheppeyIsland", "GB_IrelandSea": "GB_IrelandSea", "GB_CelticSea": "GB_CelticSea"}                      

for n in range(1, num_trees + 1):
    tick = time.time()
    tab_file_path = f"OutOfSample/{version}/oos_tree{str(n)}"
    generate_random_scenario(scenario_data_path = scenario_data_path,
                             tab_file_path = tab_file_path,
                             n_scenarios = NoOfScenarios,
                             seasons = regular_seasons,
                             n_periods = NoOfPeriods,
                             lengthRegSeason = lengthRegSeason,
                             lengthPeakSeason = lengthPeakSeason,
                             regularSeasonHours = HoursOfRegSeason,
                             peakSeasonHours = HoursOfPeakSeason, 
                             dict_countries = dict_countries,
                             dict_offshr_nodes = dict_offshr_nodes, 
                             LOADCHANGEMODULE = LOADCHANGEMODULE,
                             filter_make = filter_make,
                             filter_use = filter_use,
                             copulas_to_use = copulas_to_use,
                             copula_clusters_make = copula_clusters_make,
                             copula_clusters_use = copula_clusters_use,
                             n_cluster = n_cluster,
                             moment_matching = moment_matching,
                             n_tree_compare = n_tree_compare,
                             HEATMODULE = HEATMODULE,
                             fix_sample = FIX_SAMPLE,
                             north_sea = north_sea)
    tock = time.time()
    print(f"Scenario generation of tree nr {n} took [sec]: {str(tock - tick)}")
