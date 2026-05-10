import pathlib

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
    table_name: str = "4a_ProductionInstallationPartDetails"
    return read_duckdb(
        fn=pathlib.Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EUReg": "INTEGER",
            "fileId_EPRTR_LCP": "INTEGER",
            "ProductionInstallationPartID": "INTEGER",
            "Installation_Part_INSPIRE_ID": "VARCHAR",
            "reportingYear": "INTEGER",
            "status": "VARCHAR",
            "numberOfOperatingHours": "INTEGER",
            "remarks": "VARCHAR",
            "proportionofUsefulHeatProductionForDistrictHeating": "DOUBLE",
            "specificConditions": "VARCHAR",
            "conditionsInformation": "VARCHAR",
            "specificConditionsPermitURL": "VARCHAR",
            "confidentialityReasonCode": "VARCHAR",
            "confidentialityReasonName": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar="true",
        reload=reload,
        connection=connection,
    )
