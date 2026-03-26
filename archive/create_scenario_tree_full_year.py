import logging
import os
from pathlib import Path
import argparse
import pandas as pd

logger = logging.getLogger(__name__)

def remove_time_index(data):
    data = data.reset_index(drop=True)
    data = data.drop(["time", "year", "month", "dayofweek", "hour"], axis=1)
    return data


def make_datetime(data, time_format):
    data["time"] = pd.to_datetime(data["time"], format=time_format, exact=False)
    data["year"] = data["time"].dt.year
    data["month"] = data["time"].dt.month
    data["hour"] = data["time"].dt.hour
    data["dayofweek"] = data["time"].dt.dayofweek
    return data


def gather_regular_sample(data, regularSeasonHours, sample_hour):
    data = data.reset_index(drop=True)
    sample_data = data.iloc[sample_hour : sample_hour + regularSeasonHours, :]

    # Sort sample_data to start on midnight monday (INACTIVE)
    # sample_data = sample_data.sort_values(by=['dayofweek','hour'])

    # Drop non-country columns
    sample_data = remove_time_index(sample_data)

    hours = list(
        range(1 , regularSeasonHours+1)
    )
    return [sample_data, hours]


def sample_generator(data, regularSeasonHours, scenario, period, generator, sample_hour, dict_countries):
    [sample_data, hours] = gather_regular_sample(data, regularSeasonHours, sample_hour)
    generator_data = pd.DataFrame()
    if generator in ["Windoffshore", "Windoffshorenearshore", "Windoffshorefarshore", "Windoffshoregrounded", "Windoffshorefloating"]:
        startNOnode = 2
    else:
        startNOnode = 1
    for c in sample_data.columns:
        if c == "NO":  # Split country wide norwegian profiles into per elspot area.
            for i in range(startNOnode, 6):
                c_no = c + str(i)
                df = pd.DataFrame(
                    data={
                        "Node": c_no,
                        "IntermitentGenerators": generator,
                        "Operationalhour": hours,
                        "Scenario": "scenario" + str(scenario),
                        "Period": period,
                        "GeneratorStochasticAvailabilityRaw": sample_data[c].values,
                    }
                )
                generator_data = pd.concat([generator_data, df], ignore_index=True)
        elif c in dict_countries.keys():
            df = pd.DataFrame(
                data={
                    "Node": c,
                    "IntermitentGenerators": generator,
                    "Operationalhour": hours,
                    "Scenario": "scenario" + str(scenario),
                    "Period": period,
                    "GeneratorStochasticAvailabilityRaw": sample_data[c].values,
                }
            )
            generator_data = pd.concat([generator_data, df], ignore_index=True)
    return generator_data


def sample_hydro(data, regularSeasonHours, scenario, period, sample_hour):
    [sample_data, hours] = gather_regular_sample(data, regularSeasonHours, sample_hour)
    hydro_data = pd.DataFrame()
    for c in sample_data.columns:
        if c != "time":
            df = pd.DataFrame(
                data={
                    "Node": c,
                    "Period": period,
                    "Season": "winter",
                    "Operationalhour": hours,
                    "Scenario": "scenario" + str(scenario),
                    "HydroGeneratorMaxSeasonalProduction": sample_data[c].values,
                }
            )
            hydro_data = pd.concat([hydro_data, df], ignore_index=True)
    return hydro_data


def sample_load(data, regularSeasonHours, scenario, period, sample_hour):
    [sample_data, hours] = gather_regular_sample(data, regularSeasonHours, sample_hour)
    load = pd.DataFrame()
    for c in sample_data.columns:
        if c != "time":
            df = pd.DataFrame(
                data={
                    "Node": c,
                    "Period": period,
                    "Operationalhour": hours,
                    "Scenario": "scenario" + str(scenario),
                    "ElectricLoadRaw_in_MW": sample_data[c].values,
                }
            )
            load = pd.concat([load, df], ignore_index=True)
    return load

def generate_full_year_scenario(scenario_data_path,
                             n_periods, lengthRegSeason, sample_year,
                             n_trees, version,
                             dict_countries, dict_offshr_nodes,
                             north_sea,
                             ):
    """
    Method to generate full year scenario. 
    """

    time_format = "%d/%m/%Y %H:%M"

    scenario_data_path = Path(scenario_data_path)

    # Load all the raw scenario data
    solar_data = pd.read_csv(scenario_data_path / "solar.csv")
    windonshore_data = pd.read_csv(scenario_data_path / "windonshore.csv")
    windoffshore_data = pd.read_csv(scenario_data_path / "windoffshore.csv")
    hydroror_data = pd.read_csv(scenario_data_path / "hydroror.csv")
    hydroseasonal_data = pd.read_csv(scenario_data_path / "hydroseasonal.csv")
    electricload_data = pd.read_csv(scenario_data_path / "electricload.csv")

    # Make datetime columns
    solar_data = make_datetime(solar_data, time_format)
    windonshore_data = make_datetime(windonshore_data, time_format)
    windoffshore_data = make_datetime(windoffshore_data, time_format)
    hydroror_data = make_datetime(hydroror_data, "%Y-%m-%d %H:%M")
    hydroseasonal_data = make_datetime(hydroseasonal_data, "%Y-%m-%d %H:%M")
    electricload_data = make_datetime(electricload_data, time_format)
    
    solar_season = solar_data.loc[solar_data.year.isin([sample_year]), :]
    windonshore_season = windonshore_data.loc[windonshore_data.year.isin([sample_year]), :]
    windoffshore_season = windoffshore_data.loc[windoffshore_data.year.isin([sample_year]), :]
    hydroror_season = hydroror_data.loc[hydroror_data.year.isin([sample_year]), :]
    hydroseasonal_season = hydroseasonal_data.loc[hydroseasonal_data.year.isin([sample_year]), :]
    electricload_season = electricload_data.loc[electricload_data.year.isin([sample_year]), :]

    sample_hour = 0

    for scenario in range(1, n_trees + 1):

        # Generate dataframes to print as stochastic-files
        genAvail = pd.DataFrame()
        elecLoad = pd.DataFrame()
        hydroSeasonal = pd.DataFrame()

        tab_file_path = f"OutOfSample/{version}/oos_tree{str(scenario)}"

        # Make file_path (if it does not exist) and print .tab-files
        if not os.path.exists(tab_file_path):
            os.makedirs(tab_file_path)
        
        for i in range(1, n_periods + 1):

            # Sample generator availability for regular seasons
            genAvail = pd.concat(
                [
                    genAvail,
                    sample_generator(
                        data=solar_season,
                        regularSeasonHours=lengthRegSeason,
                        scenario=1,
                        period=i,
                        generator="Solar",
                        sample_hour=sample_hour,
                        dict_countries = dict_countries
                    ),
                ],
                ignore_index=True,
            )

            genAvail = pd.concat(
                [
                    genAvail,
                    sample_generator(
                        data=windonshore_season,
                        regularSeasonHours=lengthRegSeason,
                        scenario=1,
                        period=i,
                        generator="Windonshore",
                        sample_hour=sample_hour,
                        dict_countries = dict_countries
                    ),
                ],
                ignore_index=True,
            )
            if north_sea:
                genAvail = pd.concat(
                    [
                        genAvail,
                        sample_generator(
                            data=windoffshore_season,
                            regularSeasonHours=lengthRegSeason,
                            scenario=1,
                            period=i,
                            generator="Windoffshoregrounded",
                            sample_hour=sample_hour,
                            dict_countries = dict_offshr_nodes
                        ),
                    ],
                    ignore_index=True,
                )
                genAvail = pd.concat(
                    [
                        genAvail,
                        sample_generator(
                            data=windoffshore_season,
                            regularSeasonHours=lengthRegSeason,
                            scenario=1,
                            period=i,
                            generator="Windoffshorefloating",
                            sample_hour=sample_hour,
                            dict_countries = dict_offshr_nodes
                        ),
                    ],
                    ignore_index=True,
                )
            genAvail = pd.concat(
                [
                    genAvail,
                    sample_generator(
                        data=windoffshore_season,
                        regularSeasonHours=lengthRegSeason,
                        scenario=1,
                        period=i,
                        generator="Windoffshore",
                        sample_hour=sample_hour,
                        dict_countries = dict_countries
                    ),
                ],
                ignore_index=True,
            )
            genAvail = pd.concat(
                [
                    genAvail,
                    sample_generator(
                        data=hydroror_season,
                        regularSeasonHours=lengthRegSeason,
                        scenario=1,
                        period=i,
                        generator="Hydrorun-of-the-river",
                        sample_hour=sample_hour,
                        dict_countries = dict_countries
                    ),
                ],
                ignore_index=True,
            )

            # Sample electric load for regular seasons
            elecLoad = pd.concat(
                [
                    elecLoad,
                    sample_load(
                        data=electricload_season,
                        regularSeasonHours=lengthRegSeason,
                        scenario=1,
                        period=i,
                        sample_hour=sample_hour,
                    ),
                ],
                ignore_index=True,
            )

            # Sample seasonal hydro limit for regular seasons
            hydroSeasonal = pd.concat(
                [
                    hydroSeasonal,
                    sample_hydro(
                        data=hydroseasonal_season,
                        regularSeasonHours=lengthRegSeason,
                        scenario=1,
                        period=i,
                        sample_hour=sample_hour,
                    ),
                ],
                ignore_index=True,
            )
                
        logger.info("Done generating scenarios.")

        # Replace country codes with country names
        genAvail = genAvail.replace({"Node": dict_countries})
        elecLoad = elecLoad.replace({"Node": dict_countries})
        hydroSeasonal = hydroSeasonal.replace({"Node": dict_countries})

        # Make header for .tab-file
        genAvail = genAvail[
            ["Node", "IntermitentGenerators", "Operationalhour", "Scenario", "Period", "GeneratorStochasticAvailabilityRaw"]
        ]
        elecLoad = elecLoad[["Node", "Operationalhour", "Scenario", "Period", "ElectricLoadRaw_in_MW"]]
        hydroSeasonal = hydroSeasonal[
            ["Node", "Period", "Season", "Operationalhour", "Scenario", "HydroGeneratorMaxSeasonalProduction"]
        ]

        genAvail.loc[genAvail["GeneratorStochasticAvailabilityRaw"] <= 0.001, "GeneratorStochasticAvailabilityRaw"] = 0
        elecLoad.loc[elecLoad["ElectricLoadRaw_in_MW"] <= 0.001, "ElectricLoadRaw_in_MW"] = 0
        hydroSeasonal.loc[
            hydroSeasonal["HydroGeneratorMaxSeasonalProduction"] <= 0.001, "HydroGeneratorMaxSeasonalProduction"
        ] = 0
        
        logger.info("Saving 'Stochastic_StochasticAvailability.tab'.")

        genAvail.to_csv(
            tab_file_path + "/" + "Stochastic_StochasticAvailability.tab", header=True, index=None, sep="\t", mode="w"
        )
        logger.info("Saving 'Stochastic_ElectricLoadRaw.tab'.")
        elecLoad.to_csv(tab_file_path + "/" + "Stochastic_ElectricLoadRaw.tab", header=True, index=None, sep="\t", mode="w")

        logger.info("Saving 'Stochastic_HydroGenMaxSeasonalProduction.tab'.")
        hydroSeasonal.to_csv(
            tab_file_path + "/" + "Stochastic_HydroGenMaxSeasonalProduction.tab", header=True, index=None, sep="\t", mode="w"
        )
        
        sample_hour += lengthRegSeason
    
        print(f"Scenario tree {scenario} done.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run scenario tree generation.")
    parser.add_argument(
        "--version",
        type=str,
        required=True,
        help="Version tag for the run"
    )
    args = parser.parse_args()
    version = args.version
    ##########################################################
    ##      CREATE A FULL YEAR OF OUT-OF-SAMPLE TREES       ##
    ##########################################################    

    ##########################################################
    ## FOR ALL TREES, WE HAVE THE FOLLOWING:                ##
    ## Season = ["winter"]                                  ##
    ## Scenario = ["scenario1"]                             ##
    ## n_periods = number of investment periods             ##
    ## n_trees = number of trees with 1 season-scenario     ##
    ##                                                      ##
    ## Peak seasons are not generated, in run_EMPIRE use:   ##
    ## NoOfPeakSeason = 1                                   ##
    ## lengthPeakSeason = 1                                 ##
    ## Any peak results, ignore.                            ##
    ##                                                      ##
    ##########################################################

    # User inputs (tree-related)
    lengthRegSeason = 365 # must be divisible by 8760
    sample_year = 2015 #Choose what year to sample from, whole year will be included
    
    # User inputs (data-related)

    north_sea = True
    n_periods = 3 # i, can be reduced (if input data files are consistent with it)

    # Normally no change below this line
    n_trees = 8760 / lengthRegSeason

    if 8760 % lengthRegSeason == 0:
        print(f"Generating {int(n_trees)} scenario trees for one complete year (8760 hours).")
        n_trees = int(n_trees)
    else:
        raise ValueError("The division does not result in an integer. Change length of each season.")

    scenario_data_path = 'Data handler/' + version + '/ScenarioData'

    #######
    ##RUN##
    #######

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

    generate_full_year_scenario(scenario_data_path,
                                n_periods, 
                                lengthRegSeason, 
                                sample_year,
                                n_trees,
                                version,
                                dict_countries,
                                dict_offshr_nodes,
                                north_sea,
                             )
