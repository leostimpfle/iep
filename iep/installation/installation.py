from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_IEP
from iep.io import NA_VALUES, read_duckdb
from iep.versions import VERSION


def load(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "3_ProductionInstallation"
    data = read_duckdb(
        fn=Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EUReg": "INTEGER",
            "Parent_Site_INSPIRE_ID": "VARCHAR",
            "Facility_INSPIRE_ID": "VARCHAR",
            "ProductionInstallation_thematicId": "VARCHAR",
            "ProductionInstallation_thematicIdScheme": "VARCHAR",
            "nameOfFeature": "VARCHAR",
            "installationName_confidentialityReasonCode": "VARCHAR",
            "installationName_confidentialityReasonName": "VARCHAR",
            "installationType": "VARCHAR",
            "pointGeometryLat": "DOUBLE",
            "pointGeometryLon": "DOUBLE",
            "mainActivityCode": "VARCHAR",
            "mainActivityName": "VARCHAR",
            "dateOfStartOfOperation": "DATE_EXCEL",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )
    return data
