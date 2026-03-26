from reader import generate_tab_files
from empire import run_empire
from scenario_random import generate_random_scenario
from concat_out_of_sample_results import concat_oos_results
from datetime import datetime
import time
import gc
import os

########
##USER##
########

solstorm_username = "fdn"  # solstorm username
using_solstorm = True
USE_TEMP_DIR = True #True/False # set it to True if the solver does not find the temporary lp file
if using_solstorm:
    base_results_path = f'/storage/users/{solstorm_username}/empire_results/'
    temp_dir = f'{base_results_path}//TempDir'
else:
    base_results_path = '../InternalEMPIRE/Results'
    temp_dir = '../InternalEMPIRE/TempDir'


# Automated scenario naming
filename = os.path.basename(__file__)  # e.g. "run_EMPIRE_eur.py"
stem = os.path.splitext(filename)[0]   # e.g. "run_EMPIRE_eur"
suffix = stem.replace("run_EMPIRE", "")  # e.g. "_eur"
version = f"full_model{suffix}"

####### Out-of-sample parameters
OUT_OF_SAMPLE = False
num_oos_trees = 24
NoOfOOSScenarios = 1
results_to_concat = [ # Lists all result files (time series) that should be concatenated for out-of-sample runs
    "results_power_balance",
    "results_hydrogen_use",
    ]

WRITE_LP = False #True
PICKLE_INSTANCE = False #True
solver = "Gurobi" #"Gurobi" #"CPLEX" #"Xpress"

discountrate = 0.05
WACC = 0.05

##### Parameters to determine number of investment and operational periods #####
NoOfPeriods = 7 # i, can be reduced (if input data files are consistent with it)
NoOfRegSeason = 4
lengthRegSeason = 7*24 # can be reduced
regular_seasons = ["winter", "spring", "summer", "fall"]
NoOfPeakSeason = 2
lengthPeakSeason = 24 # can be reduced
LeapYearsInvestment = 5


##### Parameter for sectors modelling #####
EMISSION_CAP = True #False
HEATMODULE = False
hydrogen = True
FLEX_IND = True
industry = True
steel_ccs_cost_increase = None
steel_CCS_capture_rate = None
north_sea = True


##### Parameters to determine number of scenarios #####
NoOfScenarios = 5 # sce, can be reduced
# Use a deterministic cost of gas (1) or stochastic (> 1), but if > 1 then NaturalGas.xlsx input file has to correspond
NoOfGasScenarios = 1

##### Parameters for scenario generation #####
scenariogeneration = True  #True #False
FIX_SAMPLE = True                              # True to use an existing sampling key
LOADCHANGEMODULE = False                         #
filter_make = False                              # True/False, plot clusters with their mean and wasserstein distance
filter_use = False                               #
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
if use_cvar:
    name += "_CVAR"

if FIX_SAMPLE == False and filter_use == False:
    name += "_" + str(NoOfScenarios) + "WSce"
elif FIX_SAMPLE == False:
    name += "_" + str(NoOfScenarios) + "WstrataSce"

if NoOfGasScenarios > 1:
    name += "_" + str(NoOfGasScenarios) + "GSce"



workbook_path = 'Data handler/' + version
tab_file_path = 'Data handler/' + version + '/Tab_Files_' + name
scenario_data_path = 'Data handler/' + version + '/ScenarioData'
out_of_sample_file_path = sample_file_path = 'OutOfSample/' + version

FirstHoursOfRegSeason = [lengthRegSeason*i + 1 for i in range(NoOfRegSeason)]
FirstHoursOfPeakSeason = [lengthRegSeason*NoOfRegSeason + lengthPeakSeason*i + 1 for i in range(NoOfPeakSeason)]
Period = [i + 1 for i in range(NoOfPeriods)]
if OUT_OF_SAMPLE:
    Scenario = ["scenario"+str(i + 1) for i in range(NoOfOOSScenarios)]
else:
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
                      "NO1": "NO1", "NO2": "NO2", "NO3": "NO3", "NO4": "NO4", "NO5": "NO5",
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

offshoreNodesList = ["Energyhub Great Britain", "Energyhub Norway", "Energyhub EU"]
windfarmNodes = ["Moray Firth","Firth of Forth","Dogger Bank","Hornsea","Outer Dowsing","Norfolk",
  "East Anglia","Borssele","Hollandsee Kust","Helgoländer Bucht","Nordsøen","Utsira Nord",
  "Sørlige Nordsjø I","Sørlige Nordsjø II", "BalticCountries_BalticSea", "BE_PrincessElisabeth",
  "DE_NorthSea", "DE_BalticSea", "DK_NorthSea", "FI_BalticSea", "FR_ChannelSea", "FR_Atlantic",
  "IE_Atlantic", "NL_Lagelander", "NO_Vestavind", "NO_Sørvest", "PL_BalticSea", "SE_BotnieGulf",
  "SE_BalticSea", "SE_Luleå", "GB_DoggerBank", "GB_ScotlandEast", "GB_SheppeyIsland",
  "GB_IrelandSea", "GB_CelticSea"]

if north_sea == False:
    windfarmNodes = []
    offshoreNodesList = []
if version == "test":
    windfarmNodes = ["Moray Firth"]
    offshoreNodesList = ["Energyhub EU"]    

heat_results = ["results_conv_inv", "results_gen_tr_inv",  "results_output_OperationalTR"]

## list of all available results file, possible to change it to reduce the time taken to write results
include_results = ["results_offshoreConverter_inv", "results_elec_transmission_inv", "results_transmission_inv_costs",
                   "results_elec_generation_inv", "results_CO2_sequestration_inv","results_elec_stor_inv",
                    "results_industry_steel_investments", "results_industry_steel_production",
                    "results_industry_cement_investmnvents", "results_industry_ammonia_investments",
                    "results_industry_cement_production", "results_industry_ammonia_production",
                    "results_industry_oil_production", "results_industry_gas_production",
                    "results_natural_gas_transmission", "results_natural_gas_balance",
                    "results_natural_gas_power",  "results_natural_gas_hydrogen",
                    "results_natural_gas_storage", "results_transport_electricity_operations",
                    "results_transport_hydrogen_operations", "results_transport_naturalGas_operations",
                    "results_hydrogen_production_inv", "results_hydrogen_reformer_detailed_inv", 
                    "results_hydrogen_storage_inv","results_hydrogen_storage_operational", "results_hydrogen_production",
                    "results_hydrogen_use", "results_hydrogen_pipeline_inv", "results_hydrogen_pipeline_operational",
                    "results_CO2_pipeline_inv", "results_CO2_pipeline_operational", "results_CO2_sequestration_operational",
                    "results_CO2_flow_balance", "results_hydrogen_costs", "results_output_EuropeSummary",
                    "results_elec_generation_operational", "results_elec_transmission_operational",
                    "results_power_balance_detail", "results_elec_curtailed_prod", "results_cvar"]


if HEATMODULE:
    include_results.append(heat_results)

print('++++++++')
print('+EMPIRE+')
print('++++++++')
print('Solver: ' + solver)
print('Scenario Generation: ' + str(scenariogeneration))
print('OutOfSample: ' + str(OUT_OF_SAMPLE))
print('++++++++')
print('ID: ' + name)
print('++++++++')
print('Hydrogen: ' + str(hydrogen))
print('Industry: ' + str(industry))
print('Heat module: ' + str(HEATMODULE))
print('Gas stochasticity: ' + str(gas_stochasticity_flag))
print('CVaR module: ' + str(use_cvar))
print('++++++++')
print('CVaR weight: ' + str(cvar_weight))
print('++++++++')

if scenariogeneration and not OUT_OF_SAMPLE:
  tick = time.time()
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
  print("{hour}:{minute}:{second}: Scenario generation took [sec]:".format(
                                                                            hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")) + str(tock - tick))

generate_tab_files(filepath = workbook_path, tab_file_path = tab_file_path,
                    HEATMODULE=HEATMODULE, hydrogen = hydrogen, industry = industry, periods = NoOfPeriods, gas_stochasticity_flag=gas_stochasticity_flag)

if steel_ccs_cost_increase is not None:
    steel_ccs_cost_increase = steel_ccs_cost_increase/100

result_file_path = base_results_path + f'/{name}'

if not os.path.exists(result_file_path):
    os.makedirs(result_file_path)

if OUT_OF_SAMPLE:
    for j in range(1, num_oos_trees + 1):
        sample_file_path = out_of_sample_file_path + f'/oos_tree{j}'
        model = run_empire(name = name,
                    tab_file_path = tab_file_path,
                    result_file_path = result_file_path,
                    scenariogeneration = scenariogeneration,
                    scenario_data_path = scenario_data_path,
                    solver = solver,
                    temp_dir = temp_dir,
                    FirstHoursOfRegSeason = FirstHoursOfRegSeason,
                    FirstHoursOfPeakSeason = FirstHoursOfPeakSeason,
                    lengthRegSeason = lengthRegSeason,
                    lengthPeakSeason = lengthPeakSeason,
                    Period = Period,
                    Operationalhour = Operationalhour,
                    Scenario = Scenario,
                    GasScenario = GasScenario,
                    Season = Season,
                    HoursOfSeason = HoursOfSeason,
                    NoOfRegSeason=NoOfRegSeason,
                    NoOfPeakSeason=NoOfPeakSeason,
                    discountrate = discountrate,
                    WACC = WACC,
                    LeapYearsInvestment = LeapYearsInvestment,
                    WRITE_LP = WRITE_LP,
                    PICKLE_INSTANCE = PICKLE_INSTANCE,
                    EMISSION_CAP = EMISSION_CAP,
                    USE_TEMP_DIR = USE_TEMP_DIR,
                    offshoreNodesList = offshoreNodesList,
                    sample_file_path = sample_file_path,
                    OUT_OF_SAMPLE = OUT_OF_SAMPLE,
                    hydrogen = hydrogen,
                    windfarmNodes = windfarmNodes,
                    HEATMODULE=HEATMODULE,
                    industry=industry,
                    FLEX_IND=FLEX_IND,
                    steel_CCS_cost_increase= steel_ccs_cost_increase,
                    steel_CCS_capture_rate = steel_CCS_capture_rate,
                    include_results=include_results,
                    repurposeCostFactor=0.25,
                    repurposeEnergyFlowFactor=0.8,
                    use_cvar=use_cvar,
                    cvar_percentile=cvar_percentile,
                    cvar_weight=cvar_weight,
                    gas_stochasticity_flag=gas_stochasticity_flag
                    )
    concat_oos_results(results_to_concat, result_file_path)
else:
    model = run_empire(name = name,
            tab_file_path = tab_file_path,
            result_file_path = result_file_path,
            scenariogeneration = scenariogeneration,
            scenario_data_path = scenario_data_path,
            solver = solver,
            temp_dir = temp_dir,
            FirstHoursOfRegSeason = FirstHoursOfRegSeason,
            FirstHoursOfPeakSeason = FirstHoursOfPeakSeason,
            lengthRegSeason = lengthRegSeason,
            lengthPeakSeason = lengthPeakSeason,
            Period = Period,
            Operationalhour = Operationalhour,
            Scenario = Scenario,
            GasScenario = GasScenario,
            Season = Season,
            HoursOfSeason = HoursOfSeason,
            NoOfRegSeason=NoOfRegSeason,
            NoOfPeakSeason=NoOfPeakSeason,
            discountrate = discountrate,
            WACC = WACC,
            LeapYearsInvestment = LeapYearsInvestment,
            WRITE_LP = WRITE_LP,
            PICKLE_INSTANCE = PICKLE_INSTANCE,
            EMISSION_CAP = EMISSION_CAP,
            USE_TEMP_DIR = USE_TEMP_DIR,
            offshoreNodesList = offshoreNodesList,
            sample_file_path = sample_file_path,
            OUT_OF_SAMPLE = OUT_OF_SAMPLE,
            hydrogen = hydrogen,
            windfarmNodes = windfarmNodes,
            HEATMODULE=HEATMODULE,
            industry=industry,
            FLEX_IND=FLEX_IND,
            steel_CCS_cost_increase= steel_ccs_cost_increase,
            steel_CCS_capture_rate = steel_CCS_capture_rate,
            include_results=include_results,
            repurposeCostFactor=0.25,
            repurposeEnergyFlowFactor=0.8,
            use_cvar=use_cvar,
            cvar_percentile=cvar_percentile,
            cvar_weight=cvar_weight,
            gas_stochasticity_flag=gas_stochasticity_flag
            )

        
gc.collect()
