import pathlib

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_INPUT, VERSION
from iep.io import read_duckdb
from iep.misc import NA_VALUES


def load(
    reload: bool = False, connection: DuckDBPyConnection = duckdb.default_connection()
) -> DuckDBPyRelation:
    return read_duckdb(
        fn=pathlib.Path(PATH_INPUT, VERSION, "2a_ProductionFacilityDetails.xlsx"),
        dtypes={
            "fileId_EUReg": "INTEGER",
            "fileId_EPRTR_LCP": "INTEGER",
            "ProductionFacilityDetailsID": "INTEGER",
            "Facility_INSPIRE_ID": "VARCHAR",
            "reportingYear": "INTEGER",
            "status": "VARCHAR",
            "remarks": "VARCHAR",
            "numberOfOperatingHours": "INTEGER",
            "numberOfEmployees": "INTEGER",
            "stackHeightClass": "VARCHAR",
            "representativeStackHeightM": "INTEGER",
            "confidentialityReasonCode": "VARCHAR",
            "confidentialityReasonName": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar="true",
        reload=reload,
        connection=connection,
    )
