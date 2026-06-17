from pathlib import Path
from typing import Final

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import NA_VALUES, PATH_LCP
from iep.utils import read_duckdb

_VERSION: Final[str] = "v3.0"


def load_basic_data(
    connection: DuckDBPyConnection = duckdb.default_connection(),
    reload: bool = False,
) -> DuckDBPyRelation:
    table_name: str = "1_BasicData"
    return read_duckdb(
        fn=Path(PATH_LCP, f"LCP_database_{_VERSION}", f"{table_name}.csv"),
        dtypes={
            "ID": "INTEGER",
            "MemberState": "VARCHAR",
            "ReferenceYear": "INTEGER",
            "NumberOfPlants": "INTEGER",
            "Organization": "VARCHAR",
            "Address1": "VARCHAR",
            "Address2": "VARCHAR",
            "City": "VARCHAR",
            "State": "VARCHAR",
            "PostalCode": "VARCHAR",
            "NameOfContactPerson": "VARCHAR",
            "Phone": "VARCHAR",
            "EMail": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def load_plant(
    connection: DuckDBPyConnection = duckdb.default_connection(),
    reload: bool = False,
) -> DuckDBPyRelation:
    table_name: str = "2_Plant"
    return read_duckdb(
        fn=Path(PATH_LCP, f"LCP_database_{_VERSION}", f"{table_name}.csv"),
        dtypes={
            "ID": "INTEGER",
            "FK_BasicData_ID": "INTEGER",
            "Unique_Plant_ID": "VARCHAR",
            "Reported_Plant_ID": "VARCHAR",
            "PlantName": "VARCHAR",
            "FacilityName": "VARCHAR",
            "EPRTRNationalId": "VARCHAR",
            "Address1": "VARCHAR",
            "Address2": "VARCHAR",
            "City": "VARCHAR",
            "Region": "VARCHAR",
            "PostalCode": "VARCHAR",
            "Longitude": "DOUBLE",
            "Latitude": "DOUBLE",
            "Updated": "BOOLEAN",
            "UpdateComment": "VARCHAR",
            "GasEngine": "BOOLEAN",
            "GasEngineThermalInput": "DOUBLE",
            "DieselEngine": "BOOLEAN",
            "DieselEngineThermalInput": "DOUBLE",
            "Other": "BOOLEAN",
            "OtherTypeOfCombustion": "VARCHAR",
            "OtherThermalInput": "DOUBLE",
            "OperatingHours": "DOUBLE",
            "Comments": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def load_plant_details(
    connection: DuckDBPyConnection = duckdb.default_connection(), reload: bool = False
) -> DuckDBPyRelation:
    table_name: str = "3_PlantDetails"
    return read_duckdb(
        fn=Path(PATH_LCP, f"LCP_database_{_VERSION}", f"{table_name}.csv"),
        dtypes={
            "ID": "INTEGER",
            "FK_Plant_ID": "INTEGER",
            "Unique_Plant_ID": "VARCHAR",
            "PlantName": "VARCHAR",
            "StatusOfThePlant": "VARCHAR",
            "MWth": "DOUBLE",
            "ExtensionBy50MWOrMore": "BOOLEAN",
            "CapacityAddedMW": "DOUBLE",
            "SubstantialChange": "BOOLEAN",
            "CapacityAffectedMW": "DOUBLE",
            "DateOfStartOfOperation": "VARCHAR",
            "Refineries": "BOOLEAN",
            "OtherSector": "VARCHAR",
            "GasEngine": "BOOLEAN",
            "GasEngineThermalInput": "DOUBLE",
            "DieselEngine": "BOOLEAN",
            "DieselEngineThermalInput": "DOUBLE",
            "Other": "BOOLEAN",
            "OtherTypeOfCombustion": "VARCHAR",
            "OtherThermalInput": "DOUBLE",
            "OperatingHours": "DOUBLE",
            "Comments": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def load_energy_inputs(
    connection: DuckDBPyConnection = duckdb.default_connection(),
    reload: bool = False,
) -> DuckDBPyRelation:
    table_name: str = "4_EnergyInputAndTotalEmissionsToAir"
    return read_duckdb(
        fn=Path(PATH_LCP, f"LCP_database_{_VERSION}", f"{table_name}.csv"),
        dtypes={
            "ID": "INTEGER",
            "FK_Plant_ID": "INTEGER",
            "Unique_Plant_ID": "VARCHAR",
            "PlantName": "VARCHAR",
            "Biomass": "DOUBLE",
            "OtherSolidFuels": "DOUBLE",
            "LiquidFuels": "DOUBLE",
            "NaturalGas": "DOUBLE",
            "OtherGases": "DOUBLE",
            "SO2": "DOUBLE",
            "NOx": "DOUBLE",
            "Dust": "DOUBLE",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def load_optouts(
    connection: DuckDBPyConnection = duckdb.default_connection(), reload: bool = False
) -> DuckDBPyRelation:
    table_name: str = "5_OptOutsAndNERP"
    return read_duckdb(
        fn=Path(PATH_LCP, f"LCP_database_{_VERSION}", f"{table_name}.csv"),
        dtypes={
            "ID": "INTEGER",
            "FK_Plant_ID": "INTEGER",
            "Unique_Plant_ID": "VARCHAR",
            "PlantName": "VARCHAR",
            "OptOutPlant": "BOOLEAN",
            "CapacityOptedOutMW": "DOUBLE",
            "HoursOperated": "DOUBLE",
            "PlantIncludedInNERP": "BOOLEAN",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def load_lcpart15(
    connection: DuckDBPyConnection = duckdb.default_connection(), reload: bool = False
) -> DuckDBPyRelation:
    table_name: str = "6_LcpArt15"
    return read_duckdb(
        fn=Path(PATH_LCP, f"LCP_database_{_VERSION}", f"{table_name}.csv"),
        dtypes={
            "ID": "INTEGER",
            "FK_Plant_ID": "INTEGER",
            "Unique_Plant_ID": "VARCHAR",
            "PlantName": "VARCHAR",
            "Art5_1": "BOOLEAN",
            "OperatingHours": "DOUBLE",
            "ElvSO2": "DOUBLE",
            "NotaBeneAnnexIII": "BOOLEAN",
            "NotaBeneElvSO2": "BOOLEAN",
            "DesulphurisationRate": "DOUBLE",
            "SInput": "DOUBLE",
            "AnnexVI_A_Footnote2": "BOOLEAN",
            "AnnexVI_A_Footnote2_OperatingHours": "DOUBLE",
            "ElvNOx": "DOUBLE",
            "AnnexVI_A_Footnote3": "BOOLEAN",
            "VolatileContents": "DOUBLE",
            "AnnexVI_A_Footnote3_ElvNOx": "DOUBLE",
            "Comments": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )
