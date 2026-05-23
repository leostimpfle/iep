import pathlib

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import NA_VALUES, PATH_IEP, VERSION
from iep.utils import read_duckdb


def load(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "3a_ProductionInstallationDetails"
    return read_duckdb(
        fn=pathlib.Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EUReg": "INTEGER",
            "ProductionInstallationDetailsID": "INTEGER",
            "Installation_INSPIRE_ID": "VARCHAR",
            "reportingYear": "INTEGER",
            "status": "VARCHAR",
            "baselineReportIndicator": "VARCHAR",
            "publicEmissionMonitoring": "VARCHAR",
            "publicEmissionMonitoringURL": "VARCHAR",
            "remarks": "VARCHAR",
            "siteVisitNumber": "INTEGER",
            "siteVisitURL": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar="true",
        reload=reload,
        connection=connection,
    )
