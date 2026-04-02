from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep
from iep.config import PATH_INPUT, VERSION
from iep.io import read_duckdb
from iep.misc import NA_VALUES


def load_facility(
    reload: bool = False, connection: DuckDBPyConnection = duckdb.default_connection()
) -> DuckDBPyRelation:
    data = read_duckdb(
        fn=Path(PATH_INPUT, VERSION, "2_ProductionFacility.xlsx"),
        dtypes={
            "fileId_EUReg": "INTEGER",
            "Parent_Site_INSPIRE_ID": "VARCHAR",
            "Facility_INSPIRE_ID": "VARCHAR",
            "ProductionFacility_thematicId": "VARCHAR",
            "ProductionFacility_thematicIdScheme": "VARCHAR",
            "parentCompanyName": "VARCHAR",
            "parentCompanyURL": "VARCHAR",
            "parentCompany_confidentialityReasonCode": "VARCHAR",
            "parentCompany_confidentialityReasonName": "VARCHAR",
            "nameOfFeature": "VARCHAR",
            "facilityName_confidentialityReasonCode": "VARCHAR",
            "facilityName_confidentialityReasonName": "VARCHAR",
            "facilityType": "VARCHAR",
            "pointGeometryLat": "DOUBLE",
            "pointGeometryLon": "DOUBLE",
            "streetName": "VARCHAR",
            "buildingNumber": "VARCHAR",
            "city": "VARCHAR",
            "postalCode": "VARCHAR",
            "countryCode": "VARCHAR",
            "addressDetails_confidentialityReasonCode": "VARCHAR",
            "addressDetails_confidentialityReasonName": "VARCHAR",
            "mainActivityCode": "VARCHAR",
            "mainActivityName": "VARCHAR",
            "RBDSourceCode": "VARCHAR",
            "RBDSourceName": "VARCHAR",
            "NUTSRegionSourceCode": "VARCHAR",
            "NUTSRegionSourceName": "VARCHAR",
            "dateOfStartOfOperation": "DATE_EXCEL",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )
    return data


def load(
    add_function: bool = True,
    add_years_functional: bool = True,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    data = load_facility(reload=reload, connection=connection)
    if add_function:
        function = iep.facility.function.load(
            reload=reload, connection=connection
        ).aggregate(
            "Facility_INSPIRE_ID, FIRST(NACEMainEconomicActivityCode) AS NACEMainEconomicActivityCode"
        )
        data = data.join(
            function,
            condition="Facility_INSPIRE_ID",
            how="left",
        )
    if add_years_functional:
        last_year = (
            iep.facility.details.load(reload=reload, connection=connection)
            .filter("status IN ('functional', 'notRegulated')")
            .aggregate(
                aggr_expr='"Facility_INSPIRE_ID", MAX("reportingYear") AS last_year_functional',
                group_expr='"Facility_INSPIRE_ID"',
            )
        )
        data = data.select(
            '*, YEAR("dateOfStartOfOperation") AS first_year_functional'
        ).join(last_year, condition="Facility_INSPIRE_ID", how="left")
    return data
