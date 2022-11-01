import sys
import pandas as pd
import numpy as np
from datetime import datetime as dt

from powergenome.resource_clusters import ResourceGroup
from pathlib import Path
import sqlalchemy as sa
import typer

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

if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")


def fuel_files(fuel_prices: pd.DataFrame, settings: dict, out_folder: Path):

    years = settings["model_year"]
    IPM_regions = settings.get("model_regions")
    aeo_fuel_region_map = settings.get("aeo_fuel_region_map")
    # aeo_fuel_region_map

    fuel_cost = switch_fuel_cost_table(
        aeo_fuel_region_map,
        fuel_prices,
        IPM_regions,
        scenario="reference",
        year_list=years,
    )

    fuels_table = switch_fuels(fuel_prices, settings["fuel_emission_factors"])
    fuels_table.loc[len(fuels_table.index)] = [
        "Fuel",
        0,
        0,
    ]  # adding in a dummy fuel for regional_fuel_market

    fuel_cost.to_csv(out_folder / "fuel_cost.csv", index=False)
    fuels_table.to_csv(out_folder / "fuels.csv", index=False)


def gen_prebuild_newbuild_files(
    gc: GeneratorClusters,
    pudl_engine: sa.engine,
    settings_list: List[dict],
    out_folder: Path,
):
    out_folder.mkdir(parents=True, exist_ok=True)
    settings = settings_list[0]
    all_gen = gc.create_all_generators()
    existing_gen = all_gen.loc[
        all_gen["plant_id_eia"].notna(), :
    ]  # gc.create_region_technology_clusters()
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
    pudl_gen_entity = pudl_gen_entity[
        ["plant_id_eia", "generator_id", "operating_date"]
    ]

    eia_Gen = gc.operating_860m
    eia_Gen = eia_Gen[
        [
            "utility_id_eia",
            "utility_name",
            "plant_id_eia",
            "plant_name",
            "generator_id",
            "Operating Year",
            "Planned Retirement Year",
        ]
    ]
    eia_Gen = eia_Gen.loc[eia_Gen["plant_id_eia"].notna(), :]

    # create identifier to connect to powergenome data
    eia_Gen["plant_gen_id"] = (
        eia_Gen["plant_id_eia"].astype(str) + "_" + eia_Gen["generator_id"]
    )

    eia_Gen_prop = gc.proposed_gens.reset_index()
    eia_Gen_prop = eia_Gen_prop[
        [
            # "utility_id_eia",
            # "utility_name",
            "plant_id_eia",
            # "plant_name",
            "generator_id",
            "planned_operating_year",
        ]
    ]
    eia_Gen_prop = eia_Gen_prop.loc[eia_Gen_prop["plant_id_eia"].notna(), :]
    eia_Gen_prop["plant_gen_id"] = (
        eia_Gen_prop["plant_id_eia"].astype(str) + "_" + eia_Gen_prop["generator_id"]
    )

    # create copies of potential_build_yr (powergenome)
    pg_build = gc.units_model.copy()
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

    retirement_ages = settings.get("retirement_ages")

    # add in the plant+generator ids to pg_build and pudl tables (plant_id_eia + generator_id)
    pudl_gen = plant_gen_id(pudl_gen)
    pudl_gen_entity = plant_gen_id(pudl_gen_entity)
    pg_build = plant_gen_id(pg_build)

    # add in the plant+pudl id to the all_gen and pg_build tables (plant_id_eia + unit_pudl_id)
    pg_build = plant_pudl_id(pg_build)
    all_gen = plant_pudl_id(all_gen)

    gen_buildpre, gen_build_with_id = gen_build_predetermined(
        all_gen,
        pudl_gen,
        pudl_gen_entity,
        pg_build,
        {},  # manual_build_yr,
        eia_Gen,
        eia_Gen_prop,
        {},  # plant_gen_manual,
        {},  # plant_gen_manual_proposed,
        {},  # plant_gen_manual_retired,
        retirement_ages,
    )

    retired = gen_build_with_id.loc[
        gen_build_with_id["retirement_year"] < settings["model_year"], :
    ]
    retired_ids = retired["GENERATION_PROJECT"].to_list()

    # newbuild options
    df_list = []
    for settings in settings_list:
        gc.settings = settings
        new_gen = gc.create_new_generators()
        new_gen["build_year"] = settings["model_year"]
        df_list.append(new_gen)

    newgens = pd.concat(df_list, ignore_index=True)

    build_yr_list = gen_build_with_id["build_year"].to_list()
    # using gen_build_with_id because it has plants that were removed for the final gen_build_pred. (ie. build year=2020)
    gen_project = gen_build_with_id["GENERATION_PROJECT"].to_list()
    build_yr_plantid_dict = dict(zip(gen_project, build_yr_list))

    gen_build_costs = gen_build_costs_table(
        existing_gen, newgens, build_yr_plantid_dict, all_gen
    )

    gen_build_costs.drop(
        gen_build_costs[gen_build_costs["GENERATION_PROJECT"].isin(retired_ids)].index,
        inplace=True,
    )
    # drop retired plants
    gen_buildpre.drop(
        gen_buildpre[gen_buildpre["GENERATION_PROJECT"].isin(retired_ids)].index,
        inplace=True,
    )

    gen_buildpre.to_csv(out_folder / "gen_build_predetermined.csv", index=False)
    gen_build_costs.to_csv(out_folder / "gen_build_costs.csv", index=False)


def main(settings_file: str, results_folder: str):
    """Create inputs for the Switch model using PowerGenome data

    Parameters
    ----------
    settings_file : str
        The path to a YAML file or folder of YAML files with settings parameters
    results_folder : str
        The folder where results will be saved
    """
    cwd = Path.cwd()
    out_folder = cwd / results_folder
    out_folder.mkdir(exist_ok=True)

    # Load settings, create db connections, and build dictionary of settings across
    # cases/years
    settings = load_settings(path=settings_file)
    pudl_engine, pudl_out, pg_engine = init_pudl_connection(
        freq="AS",
        start_year=min(settings.get("data_years")),
        end_year=max(settings.get("data_years")),
    )
    check_settings(settings, pg_engine)
    input_folder = cwd / settings["input_folder"]
    settings["input_folder"] = input_folder
    scenario_definitions = pd.read_csv(
        input_folder / settings["scenario_definitions_fn"]
    )
    scenario_settings = build_scenario_settings(settings, scenario_definitions)

    # Should switch the case_id/year layers in scenario settings dictionary.
    # Run through the different cases and save files in a new folder for each.
    for case_id in scenario_definitions["case_id"]:
        print(f"starting case {case_id}")
        case_folder = out_folder / case_id
        case_folder.mkdir(parents=True, exist_ok=True)

        settings_list = []
        for year in scenario_definitions.query("case_id == @case_id")["year"]:
            settings_list.append(scenario_settings[year][case_id])

        gc = GeneratorClusters(pudl_engine, pudl_out, pg_engine, settings_list[0])
        gen_prebuild_newbuild_files(gc, pudl_engine, settings_list, case_folder)


if __name__ == "__main__":
    typer.run(main)
