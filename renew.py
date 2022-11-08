"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Existing%20and%20new%20generators.ipynb
"""


import os
import sys

module_path = os.path.abspath(os.getcwd() + "\\..")
if module_path not in sys.path:
    sys.path.append(module_path)
###

import pandas as pd
import numpy as np

from powergenome.resource_clusters import ResourceGroup
from pathlib import Path

import pandas as pd
from powergenome.fuels import fuel_cost_table
from powergenome.generators import GeneratorClusters
from powergenome.util import (
    build_scenario_settings,
    init_pudl_connection,
    load_settings,
    check_settings,
)
from powergenome.eia_opendata import fetch_fuel_prices
import geopandas as gpd
from powergenome.generators import *
from powergenome.external_data import (
    make_demand_response_profiles,
    make_generator_variability,
)

from conversion_functions import (
    switch_fuel_cost_table,
    switch_fuels,
    create_dict_plantgen,
    create_dict_plantpudl,
    plant_dict,
    plant_gen_id,
    plant_pudl_id,
    gen_build_predetermined,
    gen_build_costs_table,
    generation_projects_info,
    hydro_timeseries,
    load_zones_table,
    fuel_market_tables,
    timeseries,
    timepoints_table,
    hydro_timepoints_table,
    graph_timestamp_map_table,
    loads_table,
    variable_capacity_factors_table,
    transmission_lines_table,
    balancing_areas,
)

"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Existing%20and%20new%20generators.ipynb
"""

cwd = Path.cwd()

settings_path = cwd / "settings_TD.yml"
settings = load_settings(settings_path)
settings["input_folder"] = (
    settings_path.parent / "Jupyter Notebooks" / settings["input_folder"]
)
scenario_definitions = pd.read_csv(
    settings["input_folder"] / settings["scenario_definitions_fn"]
)
scenario_settings = build_scenario_settings(settings, scenario_definitions)

pudl_engine, pudl_out, pg_engine = init_pudl_connection(
    freq="AS",
    start_year=min(settings.get("data_years")),
    end_year=max(settings.get("data_years")),
)

check_settings(settings, pg_engine)


# from here
"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Existing%20and%20new%20generators.ipynb
"""

# check this for the correct year
gc = GeneratorClusters(pudl_engine, pudl_out, pg_engine, scenario_settings[2020]["p1"])


existing_gen = gc.create_region_technology_clusters()
existing_gen

new_gen = gc.create_new_generators()
new_gen

cols = [
    "region",
    "technology",
    "cluster",
    "Max_Cap_MW",
    "lcoe",
    "capex_mw",
    "regional_cost_multiplier",
    "Inv_Cost_per_MWyr",
    "plant_inv_cost_mwyr",
    "Start_Cost_per_MW",
    "interconnect_annuity",
    "spur_inv_mwyr",
    "spur_miles",
    "offshore_spur_inv_mwyr",
    "tx_inv_mwyr",
    "profile",
]
# new_gen[cols]


"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Existing%20and%20new%20generators.ipynb
"""

existing_variability = make_generator_variability(existing_gen)
existing_variability

"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Existing%20and%20new%20generators.ipynb
"""

"""
Generation profiles
Hourly generation profiles are saved in a variability column of the dataframe. 
These are then extracted using the function make_generator_variability. 
The variability (generation profile) dataframe is in the same (column) order as rows in the generator dataframe.
"""

existing_variability.columns = (
    existing_gen["region"]
    + "_"
    + existing_gen["Resource"]
    + "_"
    + existing_gen["cluster"].astype(str)
)
existing_variability
############################ check regions by RR
# variable to hold the count
cnt = 0

# list to hold visited values
visited = []

# loop for counting the unique
# values in height
for i in range(0, len(existing_gen["region"])):

    if existing_gen["region"][i] not in visited:

        visited.append(existing_gen["region"][i])

        cnt += 1

print("No.of.unique values :", cnt)

print("unique values :", visited)
##########################

"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Existing%20and%20new%20generators.ipynb
"""

# fix new_gen
make_generator_variability(new_gen)


"""
Based on Greg Schivley's response to 5c in 20220330 PowerGenomeQuestions
"""

potential_build_yr = gc.units_model
potential_build_yr

##  FUELS
"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Fuel%20costs.ipynb
"""

fuel_prices = gc.fuel_prices
fuel_prices

"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Existing%20and%20new%20generators.ipynb
"""

all_gen = gc.create_all_generators()
all_gen.columns
"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Existing%20and%20new%20generators.ipynb
"""

all_gen_variability = make_generator_variability(all_gen)
all_gen_variability.columns = (
    all_gen["region"] + "_" + all_gen["Resource"] + "_" + all_gen["cluster"].astype(str)
)
all_gen_variability

"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Fuel%20costs.ipynb
"""

fuels = fuel_cost_table(gc.fuel_prices, generators=all_gen, settings=gc.settings)

fuels


"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Hourly%20demand.ipynb
"""

from pathlib import Path

import pandas as pd
import numpy as np
from powergenome.load_profiles import (
    make_load_curves,
    add_load_growth,
    make_final_load_curves,
    make_distributed_gen_profiles,
)
from powergenome.external_data import make_demand_response_profiles
from powergenome.generators import GeneratorClusters
from powergenome.util import (
    build_scenario_settings,
    init_pudl_connection,
    load_settings,
    reverse_dict_of_lists,
    remove_feb_29,
    check_settings,
)

"""
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Hourly%20demand.ipynb
"""

load_curves = make_final_load_curves(pg_engine, scenario_settings[2020]["p1"])
load_curves


IPM_regions = settings.get("model_regions")
aeo_fuel_region_map = settings.get("aeo_fuel_region_map")
# aeo_fuel_region_map

fuel_cost = switch_fuel_cost_table(
    aeo_fuel_region_map,
    fuel_prices,
    IPM_regions,
    scenario="reference",
    year_list=[2020, 2030, 2040, 2050],
)

fuel_cost

fuel_cost.to_csv("SWITCH_inputs_east/fuel_cost.csv", index=False)


# co2_intensity based on scenario 178
REAM_co2_intensity = {
    "coal": 0.09552,
    "distillate": 0.07315,
    "naturalgas": 0.05306,
    "uranium": 0,
}


fuels_table = switch_fuels(fuel_prices, REAM_co2_intensity)
fuels_table.loc[len(fuels_table.index)] = [
    "Fuel",
    0,
    0,
]  # adding in a dummy fuel for regional_fuel_market
fuels_table

fuels_table.to_csv("SWITCH_inputs_east/fuels.csv", index=False)


"""
Catalyst Cooperative. “Pudl Data Dictionary.” PUDL Data Dictionary - PUDL 0.5.0 Documentation, 
    https://catalystcoop-pudl.readthedocs.io/en/v0.5.0/data_dictionaries/pudl_db.html. 
"""
# pull in data from PUDL tables
generators_eia860 = pd.read_sql_table("generators_eia860", pudl_engine)

generators_entity_eia = pd.read_sql_table("generators_entity_eia", pudl_engine)
# create copies of PUDL tables and filter to relevant columns
pudl_gen = generators_eia860.copy()
pudl_gen = pudl_gen[
    [
        "plant_id_eia",
        "generator_id",
        "operational_status",
        "retirement_date",
        "planned_retirement_date",
        "current_planned_operating_date",
    ]
]  #'utility_id_eia',

pudl_gen_entity = generators_entity_eia.copy()
pudl_gen_entity = pudl_gen_entity[["plant_id_eia", "generator_id", "operating_date"]]

"""
“U.S. Energy Information Administration - EIA - Independent Statistics and Analysis.” 
Form EIA-860 Detailed Data with Previous Form Data (EIA-860A/860B), 9 Sept. 2021, 
https://www.eia.gov/electricity/data/eia860/.

Used the 2020 zip folder and 3_1_Generator_Y2020 file
"""
# pull in eia_Generator_Y2020 (operable and proposed)
eia_Generator_Y2020 = pd.read_excel("3_1_Generator_Y2020.xlsx", sheet_name=0, header=1)
eia_Generator_Y2020_proposed = pd.read_excel(
    "3_1_Generator_Y2020.xlsx", sheet_name=1, header=1
)

# copy of operable eia_Generator_Y2020 and filter to relevant years
eia_Gen = eia_Generator_Y2020.copy()
eia_Gen = eia_Gen[
    [
        "Utility ID",
        "Utility Name",
        "Plant Code",
        "Plant Name",
        "Generator ID",
        "Operating Year",
        "Planned Retirement Year",
    ]
]
eia_Gen = eia_Gen[eia_Gen["Plant Code"].notna()]

# create identifier to connect to powergenome data
eia_Gen["plant_gen_id"] = (
    eia_Gen["Plant Code"].astype(str) + "_" + eia_Gen["Generator ID"]
)

# copy of proposed eia_Generator_Y2020 and filter to relevant years
eia_Gen_prop = eia_Generator_Y2020_proposed.copy()
eia_Gen_prop = eia_Gen_prop[
    [
        "Utility ID",
        "Utility Name",
        "Plant Code",
        "Plant Name",
        "Generator ID",
        "Effective Year",
    ]
]
eia_Gen_prop = eia_Gen_prop[eia_Gen_prop["Plant Code"].notna()]

# create identifier to connect to powergenome data
eia_Gen_prop["plant_gen_id"] = (
    eia_Gen_prop["Plant Code"].astype(str) + "_" + eia_Gen_prop["Generator ID"]
)
# eia_Gen_prop.head()

# create copies of potential_build_yr (powergenome)
pg_build = potential_build_yr.copy()
pg_build = pg_build[
    [
        "plant_id_eia",
        "generator_id",
        "unit_id_pudl",
        "planned_retirement_date",
        "operating_date",
        "Operating Year",
        "retirement_year",
    ]
]


# found plant names from pd.read_sql_table("plants_entity_eia", pudl_engine)
# did a google search on those names to find build year
manual_build_yr = {
    166.0: 1931,
    1230.0: 1963,
    7456.0: 2001,
    10718.0: 1985,
    50034.0: 1992,
    50177.0: 1980,
    50281.0: 1982,
    50322.0: 1985,
    50513.0: 1992,
    50560.0: 1986,
    50820.0: 1983,
    54355.0: 1993,
    55043.0: 1998,
    55177.0: 2001,
    55734.0: 2002,
    58044.0: 2012,
    59551.0: 2014,
    59553.0: 2014,
    60611.0: 2016,
    1359: 1896,
}
# manual updates based on eia excel file (leading 0s) {plant_gen_id}
plant_gen_manual = {
    "55168.0_1": 2002,
    "55168.0_2": 2002,
    "55168.0_3": 2002,
}
plant_gen_manual_proposed = {"57943.0_6": 2021}
plant_gen_manual_retired = {"64206.0_2004": 2004}

# dictionary of retirement ages, pulled from settings
retirement_ages = settings.get("retirement_ages")


# modify the tables by adding the unique identifies for the plants

# add in the plant+generator ids to pg_build and pudl tables (plant_id_eia + generator_id)
pudl_gen = plant_gen_id(pudl_gen)
pudl_gen_entity = plant_gen_id(pudl_gen_entity)
pg_build = plant_gen_id(pg_build)

# add in the plant+pudl id to the all_gen and pg_build tables (plant_id_eia + unit_pudl_id)
pg_build = plant_pudl_id(pg_build)
all_gen = plant_pudl_id(all_gen)
all_gen

gen_buildpre, gen_build_with_id = gen_build_predetermined(
    all_gen,
    pudl_gen,
    pudl_gen_entity,
    pg_build,
    manual_build_yr,
    eia_Gen,
    eia_Gen_prop,
    plant_gen_manual,
    plant_gen_manual_proposed,
    plant_gen_manual_retired,
    retirement_ages,
)

gen_buildpre

# check for blanks
gen_buildpre[gen_buildpre["build_year"] == "None"]

# these are already retired and should be removed
retired = gen_build_with_id[gen_build_with_id["retirement_year"] < "2021"]
retired_ids = retired["GENERATION_PROJECT"].to_list()
retired_ids


#######################################################################################################################################
### need to run SWITCH_genbuildcosts_helper.jpynb
#  * need to update settings_TD to appropriate year
#         - model_year and model_first_planning_year
#         - other dictionary keys in settings_TD that have model year (demand_response_resources, settings_management)
#     * need to update other inputs from the extra_inputs file (fix the year)
#         - scenario_inputs, heat_load_shifting, emission_policies


# Loop through different deacades #Change year
list_decade = [2020, 2030, 2040, 2050]
newgens = pd.DataFrame()
for y in list_decade:
    print(y)
    gc = GeneratorClusters(pudl_engine, pudl_out, pg_engine, scenario_settings[y]["p1"])
    new_gen_decade = gc.create_new_generators()
    new_gen_decade["build_year"] = y
    newgens = newgens.append(new_gen_decade)


# gc = GeneratorClusters(pudl_engine, pudl_out, pg_engine, scenario_settings[2020]["p1"])
# new_gen_decade = gc.create_new_generators()


# new_gen # check length against new_gen_decade
# new_gen_decade
# new_gen_decade.to_csv('new_gen_2020.csv')

#######################################################################################################################################

build_yr_list = gen_build_with_id["build_year"].to_list()
# using gen_build_with_id because it has plants that were removed for the final gen_build_pred. (ie. build year=2020)
gen_project = gen_build_with_id["GENERATION_PROJECT"].to_list()
build_yr_plantid_dict = dict(zip(gen_project, build_yr_list))

# these csv files are created from the SWITCH_genbuildcosts_helper notebook
# new_gen_2020 = pd.read_csv('new_gen_2020.csv', index_col=0)
# new_gen_2030 = pd.read_csv('new_gen_2030.csv', index_col=0)
# new_gen_2040 = pd.read_csv('new_gen_2040.csv', index_col=0)
# new_gen_2050 = pd.read_csv('new_gen_2050.csv', index_col=0)

gen_build_costs = gen_build_costs_table(
    existing_gen, newgens, build_yr_plantid_dict, all_gen
)
gen_build_costs


# drop retired plants
gen_build_costs.drop(
    gen_build_costs[gen_build_costs["GENERATION_PROJECT"].isin(retired_ids)].index,
    inplace=True,
)
# drop retired plants
gen_buildpre.drop(
    gen_buildpre[gen_buildpre["GENERATION_PROJECT"].isin(retired_ids)].index,
    inplace=True,
)
gen_buildpre
gen_build_costs

gen_buildpre.to_csv("SWITCH_inputs_east/gen_build_predetermined.csv", index=False)
gen_build_costs.to_csv("SWITCH_inputs_east/gen_build_costs.csv", index=False)

# assumed cogen to be false
# based on REAM
cogen_tech = {
    "Onshore Wind Turbine": False,
    "Biomass": False,
    "Conventional Hydroelectric": False,
    "Conventional Steam Coal": False,
    "Natural Gas Fired Combined Cycle": False,
    "Natural Gas Fired Combustion Turbine": False,
    "Natural Gas Steam Turbine": False,
    "Nuclear": False,
    "Solar Photovoltaic": False,
    "Hydroelectric Pumped Storage": False,
    "Offshore Wind Turbine": False,
    "Small Hydroelectric": False,
    "NaturalGas_CCCCSAvgCF_Conservative": False,
    "NaturalGas_CCAvgCF_Moderate": False,
    "NaturalGas_CTAvgCF_Moderate": False,
    "Battery_*_Moderate": False,
    "NaturalGas_CCS100_Moderate": False,
    "heat_load_shifting": False,
}

# based on REAM
baseload_tech = {
    "Onshore Wind Turbine": False,
    "Biomass": False,
    "Conventional Hydroelectric": False,
    "Conventional Steam Coal": True,
    "Natural Gas Fired Combined Cycle": False,
    "Natural Gas Fired Combustion Turbine": False,
    "Natural Gas Steam Turbine": False,
    "Nuclear": True,
    "Solar Photovoltaic": False,
    "Hydroelectric Pumped Storage": False,
    "Offshore Wind Turbine": False,
    "Small Hydroelectric": False,
    "NaturalGas_CCCCSAvgCF_Conservative": False,
    "NaturalGas_CCAvgCF_Moderate": False,
    "NaturalGas_CTAvgCF_Moderate": False,
    "Battery_*_Moderate": False,
    "NaturalGas_CCS100_Moderate": False,
    "heat_load_shifting": False,
}

# based on technology name
energy_tech = {
    "Onshore Wind Turbine": "Wind",
    "Biomass": "Bio Solid",
    "Conventional Hydroelectric": "Water",
    "Conventional Steam Coal": "Coal",
    "Natural Gas Fired Combined Cycle": "Gas",
    "Natural Gas Fired Combustion Turbine": "Gas",
    "Natural Gas Steam Turbine": "Gas",
    "Nuclear": "Uranium",
    "Solar Photovoltaic": "Solar",
    "Hydroelectric Pumped Storage": "Water",
    "Offshore Wind Turbine": "Wind",
    "Small Hydroelectric": "Water",
    "NaturalGas_CCCCSAvgCF_Conservative": "Gas",
    "NaturalGas_CCAvgCF_Moderate": "Gas",
    "NaturalGas_CTAvgCF_Moderate": "Gas",
    "Battery_*_Moderate": "Electricity",
    "NaturalGas_CCS100_Moderate": "Gas",
    "heat_load_shifting": False,
}

# outage rates based on technology average value from REAM scenario 178 (ignored those that were cogen)
sched_outage_tech = {
    "Onshore Wind Turbine": 0.0,
    "Biomass": 0.06,
    "Conventional Hydroelectric": 0.05,
    "Conventional Steam Coal": 0.06,
    "Natural Gas Fired Combined Cycle": 0.6,
    "Natural Gas Fired Combustion Turbine": 0.6,
    "Natural Gas Steam Turbine": 0.6,
    "Nuclear": 0.06,
    "Solar Photovoltaic": 0.0,
    "Hydroelectric Pumped Storage": 0.05,
    "Offshore Wind Turbine": 0.01,
    "Small Hydroelectric": 0.05,
    "NaturalGas_CCCCSAvgCF_Conservative": 0.6,
    "NaturalGas_CCAvgCF_Moderate": 0.6,
    "NaturalGas_CTAvgCF_Moderate": 0.6,
    "Battery_*_Moderate": 0.01,
    "NaturalGas_CCS100_Moderate": 0.6,
    "heat_load_shifting": False,
}
forced_outage_tech = {
    "Onshore Wind Turbine": 0.0,
    "Biomass": 0.04,
    "Conventional Hydroelectric": 0.05,
    "Conventional Steam Coal": 0.04,
    "Natural Gas Fired Combined Cycle": 0.4,
    "Natural Gas Fired Combustion Turbine": 0.4,
    "Natural Gas Steam Turbine": 0.4,
    "Nuclear": 0.04,
    "Solar Photovoltaic": 0.0,
    "Hydroelectric Pumped Storage": 0.05,
    "Offshore Wind Turbine": 0.05,
    "Small Hydroelectric": 0.05,
    "NaturalGas_CCCCSAvgCF_Conservative": 0.4,
    "NaturalGas_CCAvgCF_Moderate": 0.4,
    "NaturalGas_CTAvgCF_Moderate": 0.4,
    "Battery_*_Moderate": 0.02,
    "NaturalGas_CCS100_Moderate": 0.4,
    "heat_load_shifting": False,
}
# take out heat_load_shifting - not in SWITCH

# to help calculate gen_connect_cost_per_mw
spur_capex_mw_mile = settings.get("transmission_investment_cost")["spur"][
    "capex_mw_mile"
]

# to populate gen_max_age
retirement_age = settings.get("retirement_ages")
# add missing keys, values based on https://www.nrel.gov/docs/fy22osti/80641.pdf
retirement_age["Biomass"] = 50
retirement_age[
    "NaturalGas_CCCCSAvgCF_Conservative"
] = 60  # combined cycle and carbon capture sequestration
retirement_age["NaturalGas_CCAvgCF_Moderate"] = 60  # carbon capture
retirement_age["NaturalGas_CTAvgCF_Moderate"] = 50  # combustion turbine
retirement_age["Battery_*_Moderate"] = 15
retirement_age["NaturalGas_CCS100_Moderate"] = 60
retirement_age["heat_load_shifting"] = 10  # deleting


gen_project_info = generation_projects_info(
    all_gen,
    spur_capex_mw_mile,
    retirement_age,
    cogen_tech,
    baseload_tech,
    energy_tech,
    sched_outage_tech,
    forced_outage_tech,
)

gen_project_info


# checking for heat_load_shifting
gen_project_info.loc[gen_project_info["gen_energy_source"] == False]


# drop retired plants
# information based on gen_build_predetermined notebook
gen_project_info.drop(
    gen_project_info[gen_project_info["GENERATION_PROJECT"].isin([3225, 4070])].index,
    inplace=True,
)
gen_project_info


graph_tech_colors_data = {
    "gen_type": [
        "Biomass",
        "Coal",
        "Gas",
        "Geothermal",
        "Hydro",
        "Nuclear",
        "Oil",
        "Solar",
        "Storage",
        "Waste",
        "Wave",
        "Wind",
        "Other",
    ],
    "color": [
        "green",
        "saddlebrown",
        "gray",
        "red",
        "royalblue",
        "blueviolet",
        "orange",
        "gold",
        "aquamarine",
        "black",
        "blue",
        "deepskyblue",
        "white",
    ],
}
graph_tech_colors_table = pd.DataFrame(graph_tech_colors_data)
graph_tech_colors_table.insert(0, "map_name", "default")
graph_tech_colors_table


gen_type_tech = {
    "Onshore Wind Turbine": "Wind",
    "Biomass": "Biomass",
    "Conventional Hydroelectric": "Hydro",
    "Conventional Steam Coal": "Coal",
    "Natural Gas Fired Combined Cycle": "Gas",
    "Natural Gas Fired Combustion Turbine": "Gas",
    "Natural Gas Steam Turbine": "Gas",
    "Nuclear": "Nuclear",
    "Solar Photovoltaic": "Solar",
    "Hydroelectric Pumped Storage": "Hydro",
    "Offshore Wind Turbine": "Wind",
    "NaturalGas_CCCCSAvgCF_Conservative": "Gas",
    "NaturalGas_CCAvgCF_Moderate": "Gas",
    "NaturalGas_CTAvgCF_Moderate": "Gas",
    "Battery_*_Moderate": "Storage",
    "NaturalGas_CCS100_Moderate": "Gas",
}

gen_tech = gen_project_info["gen_tech"].unique()
graph_tech_types_table = pd.DataFrame(
    columns=["map_name", "gen_type", "gen_tech", "energy_source"]
)
graph_tech_types_table["gen_tech"] = gen_tech
graph_tech_types_table["energy_source"] = graph_tech_types_table["gen_tech"].apply(
    lambda x: energy_tech[x]
)
graph_tech_types_table["map_name"] = "default"
graph_tech_types_table["gen_type"] = graph_tech_types_table["gen_tech"].apply(
    lambda x: gen_type_tech[x]
)
graph_tech_types_table


fuels = fuel_prices["fuel"].unique()
fuels = [fuel.capitalize() for fuel in fuels]
non_fuel_table = graph_tech_types_table[
    ~graph_tech_types_table["energy_source"].isin(fuels)
]
non_fuel_energy = list(set(non_fuel_table["energy_source"].to_list()))
non_fuel_energy_table = pd.DataFrame(non_fuel_energy, columns=["energy_source"])
non_fuel_energy_table

gen_project_info["gen_full_load_heat_rate"] = gen_project_info.apply(
    lambda row: "."
    if row.gen_energy_source in non_fuel_energy
    else row.gen_full_load_heat_rate,
    axis=1,
)
gen_project_info

gen_project_info.to_csv("SWITCH_inputs_east/generation_projects_info.csv", index=False)
graph_tech_colors_table.to_csv("SWITCH_inputs_east/graph_tech_colors.csv", index=False)
graph_tech_types_table.to_csv("SWITCH_inputs_east/graph_tech_types.csv", index=False)
non_fuel_energy_table.to_csv(
    "SWITCH_inputs_east/non_fuel_energy_sources.csv", index=False
)

## edited by RR
hydro_forced_outage_tech = {
    "conventional_hydroelectric": 0.05,
    "hydroelectric_pumped_storage": 0.05,
    "small_hydroelectric": 0.05,
}


def match_hydro_forced_outage_tech(x):
    for key in hydro_forced_outage_tech:
        if key in x:
            return hydro_forced_outage_tech[key]


hydro_variability_new = pd.read_csv(
    cwd / "Jupyter Notebooks/extra_inputs/regional_existing_hydro_profiles.csv"
)
##


period_list = ["2020", "2030", "2040", "2050"]

hydro_timeseries_table = hydro_timeseries(
    existing_gen, hydro_variability_new, period_list
)
hydro_timeseries_table
hydro_timeseries_table = hydro_timeseries_table.drop(
    columns=["outage_rate", "hydro_min_flow_mw_raw", "hydro_avg_flow_mw_raw"]
)


hydro_timeseries_table.to_csv("SWITCH_inputs_east/hydro_timeseries.csv", index=False)

pudl_engine, pudl_out, pg_engine = init_pudl_connection()
cwd = Path.cwd()

settings_path = cwd / "settings_TD.yml"
settings = load_settings(settings_path)
settings["input_folder"] = settings_path.parent / settings["input_folder"]
check_settings(settings, pg_engine)


IPM_regions = settings.get("model_regions")
load_zones = load_zones_table(IPM_regions, zone_ccs_distance_km=0)
# add in the dummy loadzone
load_zones.loc[len(load_zones.index)] = [
    "loadzone",
    0,
    load_zones["zone_dbid"].max() + 1,
]
load_zones

load_zones.to_csv("SWITCH_inputs_east/load_zones.csv", index=False)


# Based on REAM
carbon_policies_data = {
    "period": [2020, 2030, 2040, 2050],
    "carbon_cap_tco2_per_yr": [222591761.6, 149423302.5, 76328672.3, 0],
    "carbon_cap_tco2_per_yr_CA": [57699000, 36292500, 11400000, 0],
    "carbon_cost_dollar_per_tco2": [".", ".", ".", "."],
}
carbon_policies_table = pd.DataFrame(carbon_policies_data)
carbon_policies_table

atb_data_year = settings.get("atb_data_year")
# interest and discount based on REAM
financials_data = {
    "base_financial_year": atb_data_year,
    "interest_rate": 0.05,
    "discount_rate": 0.05,
}
financials_table = pd.DataFrame(financials_data, index=[0])
financials_table

# based on REAM
periods_data = {
    "INVESTMENT_PERIOD": [2020, 2030, 2040, 2050],
    "period_start": [2016, 2026, 2036, 2046],
    "period_end": [2025, 2035, 2045, 2055],
}
periods_table = pd.DataFrame(periods_data)
periods_table

carbon_policies_table.to_csv("SWITCH_inputs_east/carbon_policies.csv", index=False)
financials_table.to_csv("SWITCH_inputs_east/financials.csv", index=False)
periods_table.to_csv("SWITCH_inputs_east/periods.csv", index=False)

aeo_fuel_region_map = settings.get("aeo_fuel_region_map")


rfm, zrfm = fuel_market_tables(fuel_prices, aeo_fuel_region_map, scenario="reference")

rfm  # there can't be overlap with fuel_cost. So this table isn't right
zrfm  # there can't be overlap with fuel_cost. So this table isn't right


regional_fuel_markets = pd.DataFrame(
    {"regional_fuel_market": "loadzone-Fuel", "fuel": "Fuel"}, index=[0]
)
regional_fuel_markets

zone_regional_fm = pd.DataFrame(
    {"load_zone": "loadzone", "fuel": "loadzone-Fuel"}, index=[0]
)
zone_regional_fm

# creating dummy values based on one load zone in REAM's input file
# regional_fuel_market should align with the regional_fuel_market table
fuel_supply_curves20 = pd.DataFrame(
    {
        "period": [2020, 2020, 2020, 2020, 2020, 2020],
        "tier": [1, 2, 3, 4, 5, 6],
        "unit_cost": [1.9, 4.0, 487.5, 563.7, 637.8, 816.7],
        "max_avail_at_cost": [651929, 3845638, 3871799, 3882177, 3889953, 3920836],
    }
)
fuel_supply_curves20.insert(0, "regional_fuel_market", "loadzone-Fuel")
fuel_supply_curves30 = fuel_supply_curves20.copy()
fuel_supply_curves30["period"] = 2030
fuel_supply_curves40 = fuel_supply_curves20.copy()
fuel_supply_curves40["period"] = 2040
fuel_supply_curves50 = fuel_supply_curves20.copy()
fuel_supply_curves50["period"] = 2050
fuel_supply_curves = pd.concat(
    [
        fuel_supply_curves20,
        fuel_supply_curves30,
        fuel_supply_curves40,
        fuel_supply_curves50,
    ]
)
fuel_supply_curves

regional_fuel_markets.to_csv(
    "SWITCH_inputs_east/regional_fuel_markets.csv", index=False
)
zone_regional_fm.to_csv(
    "SWITCH_inputs_east/zone_to_regional_fuel_market.csv", index=False
)
fuel_supply_curves.to_csv("SWITCH_inputs_east/fuel_supply_curves.csv", index=False)


timeseries_df = timeseries(
    load_curves,
    max_weight=20.2778,
    avg_weight=283.8889,
    ts_duration_of_tp=4,
    ts_num_tps=6,
)
# dates that should be used in the other tables
timeseries_dates = timeseries_df["timeseries"].to_list()
timeseries_df

# TIMEPOINTS

timestamp_interval = [
    "00",
    "04",
    "08",
    "12",
    "16",
    "20",
]  # should align with ts_duration_of_tp and ts_num_tps
timepoints_df = timepoints_table(timeseries_dates, timestamp_interval)
timepoints_df


# create lists and dictionary for later use
timepoints_timestamp = timepoints_df["timestamp"].to_list()  # timestamp list
timepoints_tp_id = timepoints_df["timepoint_id"].to_list()  # timepoint_id list
timepoints_dict = dict(
    zip(timepoints_timestamp, timepoints_tp_id)
)  # {timestamp: timepoint_id}

# check for duplicated days (april 30th was duplicating due it saying it was max and avg)
timepoints_df[timepoints_df.timestamp.duplicated(keep=False)]

##HYDRO TIMEPOINTS

hydro_timepoints_df = hydro_timepoints_table(timepoints_df)
hydro_timepoints_df


timestamp_interval = [
    "00",
    "04",
    "08",
    "12",
    "16",
    "20",
]  # should align with ts_duration_of_tp and ts_num_tps

graph_timestamp_map = graph_timestamp_map_table(timeseries_df, timestamp_interval)
graph_timestamp_map


## LOADS


period_list = ["2020", "2030", "2040", "2050"]
loads, loads_with_year_hour = loads_table(
    load_curves, timepoints_timestamp, timepoints_dict, period_list
)
loads

# for fuel_cost and regional_fuel_market issue
dummy_df = pd.DataFrame({"TIMEPOINT": timepoints_tp_id})
dummy_df.insert(0, "LOAD_ZONE", "loadzone")
dummy_df.insert(2, "zone_demand_mw", 0)

loads = loads.append(dummy_df)
loads

loads_with_year_hour

year_hour = loads_with_year_hour["year_hour"].to_list()

vcf = variable_capacity_factors_table(
    all_gen_variability, year_hour, timepoints_dict, all_gen
)
vcf

timeseries_df.to_csv("SWITCH_inputs_east/timeseries.csv", index=False)
timepoints_df.to_csv("SWITCH_inputs_east/timepoints.csv", index=False)
hydro_timepoints_df.to_csv("SWITCH_inputs_east/hydro_timepoints.csv", index=False)
graph_timestamp_map.to_csv("SWITCH_inputs_east/graph_timestamp_map.csv", index=False)
loads.to_csv("SWITCH_inputs_east/loads.csv", index=False)
vcf.to_csv("SWITCH_inputs_east/variable_capacity_factors.csv", index=False)


from powergenome.generators import load_ipm_shapefile
from powergenome.GenX import (
    network_line_loss,
    network_max_reinforcement,
    network_reinforcement_cost,
    add_cap_res_network,
)
from powergenome.transmission import (
    agg_transmission_constraints,
    transmission_line_distance,
)
from powergenome.util import init_pudl_connection, load_settings, check_settings
from statistics import mean

"""
pulling in information from PowerGenome transmission notebook
Schivley Greg, PowerGenome, (2022), GitHub repository, 
    https://github.com/PowerGenome/PowerGenome/blob/master/notebooks/Transmission.ipynb
"""

transmission = agg_transmission_constraints(pg_engine=pg_engine, settings=settings)
model_regions_gdf = load_ipm_shapefile(settings)

transmission_line_distance(
    trans_constraints_df=transmission,
    ipm_shapefile=model_regions_gdf,
    settings=settings,
)

line_loss = network_line_loss(transmission=transmission, settings=settings)
network_reinforcement_cost = network_reinforcement_cost(
    transmission=transmission, settings=settings
)
network_max_reinforcement = network_max_reinforcement(
    transmission=transmission, settings=settings
)
transmission = agg_transmission_constraints(pg_engine=pg_engine, settings=settings)
add_cap = add_cap_res_network(transmission, settings)

## transmission lines
# pulled from SWITCH load_zones file
# need zone_dbid information to populate transmission_line column
def load_zones_table(IPM_regions, zone_ccs_distance_km):
    load_zones = pd.DataFrame(
        columns=["LOAD_ZONE", "zone_ccs_distance_km", "zone_dbid"]
    )
    load_zones["LOAD_ZONE"] = IPM_regions
    load_zones["zone_ccs_distance_km"] = 0  # set to default 0
    load_zones["zone_dbid"] = range(1, len(IPM_regions) + 1)
    return load_zones


IPM_regions = settings.get("model_regions")
load_zones = load_zones_table(IPM_regions, zone_ccs_distance_km=0)
zone_dict = dict(
    zip(load_zones["LOAD_ZONE"].to_list(), load_zones["zone_dbid"].to_list())
)

tx_capex_mw_mile_dict = settings.get("transmission_investment_cost")["tx"][
    "capex_mw_mile"
]


transmission_lines = transmission_lines_table(
    line_loss, add_cap, tx_capex_mw_mile_dict, zone_dict, settings
)
transmission_lines


trans_capital_cost_per_mw_km = (
    min(settings.get("transmission_investment_cost")["tx"]["capex_mw_mile"].values())
    * 1.60934
)
trans_params_table = pd.DataFrame(
    {
        "trans_capital_cost_per_mw_km": trans_capital_cost_per_mw_km,
        "trans_lifetime_yrs": 20,
        "trans_fixed_om_fraction": 0.03,
    },
    index=[0],
)
trans_params_table

transmission_lines.to_csv("SWITCH_inputs_east/transmission_lines.csv", index=False)
trans_params_table.to_csv("SWITCH_inputs_east/trans_params.csv", index=False)


IPM_regions = settings.get("model_regions")
bal_areas, zone_bal_areas = balancing_areas(
    pudl_engine,
    IPM_regions,
    all_gen,
    quickstart_res_load_frac=0.03,
    quickstart_res_wind_frac=0.05,
    quickstart_res_solar_frac=0.05,
    spinning_res_load_frac=".",
    spinning_res_wind_frac=".",
    spinning_res_solar_frac=".",
)


bal_areas

# adding in the dummy loadzone for the fuel_cost / regional_fuel_market issue
zone_bal_areas.loc[len(zone_bal_areas.index)] = ["loadzone", "BANC"]
zone_bal_areas

bal_areas.to_csv("SWITCH_inputs_east/balancing_areas.csv", index=False)
zone_bal_areas.to_csv("SWITCH_inputs_east/zone_balancing_areas.csv", index=False)
